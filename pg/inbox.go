package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/netbill/ape"
	"github.com/netbill/eventbox"
	"github.com/netbill/eventbox/headers"
	"github.com/netbill/eventbox/pg/sqlc"
	"github.com/netbill/pgdbx"
	"github.com/segmentio/kafka-go"
)

var (
	ErrInboxEventAlreadyExists = ape.DeclareError("INBOX_EVENT_ALREADY_EXISTS")
	ErrInboxEventNotFound      = ape.DeclareError("INBOX_EVENT_NOT_FOUND")
)

type inbox struct {
	db *pgdbx.DB
}

func (i *inbox) queries() *sqlc.Queries {
	return sqlc.New(i.db)
}

// WriteInboxEvent writes a Kafka message to the inbox.
// It extracts the required headers from the message and inserts a new inbox event into the database.
// If an event with the same ID already exists, it returns an error.
func (i *inbox) WriteInboxEvent(
	ctx context.Context,
	message kafka.Message,
) (eventbox.InboxEvent, error) {
	h, err := headers.ParseMessageRequiredHeaders(message.Headers)
	if err != nil {
		return eventbox.InboxEvent{}, err
	}

	row, err := i.queries().InsertInboxEvent(ctx, sqlc.InsertInboxEventParams{
		EventID: pgtype.UUID{Bytes: h.EventID, Valid: true},

		Topic:       message.Topic,
		Key:         string(message.Key),
		Type:        h.EventType,
		Version:     h.EventVersion,
		Producer:    h.Producer,
		Payload:     message.Value,
		Partition:   int32(message.Partition),
		KafkaOffset: message.Offset,

		Status:        eventbox.InboxEventStatusPending,
		Attempts:      0,
		NextAttemptAt: pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true},
		ProducedAt:    pgtype.Timestamptz{Time: message.Time, Valid: true},
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return eventbox.InboxEvent{}, ErrInboxEventAlreadyExists.Raise(err)
		}

		return eventbox.InboxEvent{}, fmt.Errorf("insert inbox event: %w", err)
	}

	return parseInboxEventFromSqlcRow(row), nil
}

// GetInboxEventByID retrieves an inbox event by its ID.
func (i *inbox) GetInboxEventByID(
	ctx context.Context,
	id uuid.UUID,
) (eventbox.InboxEvent, error) {
	res, err := i.queries().GetInboxEventByID(ctx, pgtype.UUID{Bytes: id, Valid: true})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return eventbox.InboxEvent{}, ErrInboxEventNotFound.Raise(err)
		}
		return eventbox.InboxEvent{}, fmt.Errorf("get inbox event by id: %w", err)
	}

	return parseInboxEventFromSqlcRow(res), nil
}

// ReserveInboxEvents reserves a batch of pending inbox events for processing by a specific processor.
// It marks the events as reserved by the given processor ID and returns them for processing.
// The method takes a context, the processor ID, and the maximum number of events to reserve.
// How it works:
// select batch of pending events ordered by produced_at with limit = limit*10 + 100
// after that mark as reserved batch of events with limit = limit where id in (ids of selected events) and reserved_by is null,
// but important don't mark events which key+topic is already reserved by another processor,
// because it can cause deadlocks between processors
func (i *inbox) ReserveInboxEvents(
	ctx context.Context,
	workerID string,
	limit int,
) ([]eventbox.InboxEvent, error) {
	res, err := i.queries().ReserveInboxEvents(ctx, sqlc.ReserveInboxEventsParams{
		WorkerID:   pgtype.Text{String: workerID, Valid: true},
		BatchLimit: int32(limit),
		SortLimit:  int32(limit*10 + 100),
	})
	if err != nil {
		return nil, fmt.Errorf("reserve inbox events: %w", err)
	}

	out := make([]eventbox.InboxEvent, len(res))
	for i := range res {
		out[i] = parseInboxEventFromSqlcRow(res[i])
	}

	return out, nil
}

// CommitInboxEvent marks an inbox event as processed by its ID and the processor ID that reserved it.
// It updates the event's status to processed and sets the processed timestamp.
func (i *inbox) CommitInboxEvent(
	ctx context.Context,
	workerID string,
	eventID uuid.UUID,
) (eventbox.InboxEvent, error) {
	res, err := i.queries().MarkInboxEventAsProcessed(ctx, sqlc.MarkInboxEventAsProcessedParams{
		WorkerID: pgtype.Text{String: workerID, Valid: true},
		EventID:  pgtype.UUID{Bytes: eventID, Valid: true},
	})
	if err != nil {
		return eventbox.InboxEvent{}, fmt.Errorf("mark inbox event as processed: %w", err)
	}

	return parseInboxEventFromSqlcRow(res), nil
}

// DelayInboxEvent marks an inbox event as pending again with a delay for the next attempt.
// It updates the event's status to pending, sets the last error reason, and schedules the next attempt timestamp.
func (i *inbox) DelayInboxEvent(
	ctx context.Context,
	workerID string,
	eventID uuid.UUID,
	reason string,
	nextAttemptAt time.Time,
) (eventbox.InboxEvent, error) {
	res, err := i.queries().MarkInboxEventAsPending(ctx, sqlc.MarkInboxEventAsPendingParams{
		WorkerID:  pgtype.Text{String: workerID, Valid: true},
		EventID:   pgtype.UUID{Bytes: eventID, Valid: true},
		LastError: pgtype.Text{String: reason, Valid: true},
		NextAttemptAt: pgtype.Timestamptz{
			Time:  nextAttemptAt,
			Valid: true,
		},
	})
	if err != nil {
		return eventbox.InboxEvent{}, fmt.Errorf("mark inbox event as pending: %w", err)
	}

	return parseInboxEventFromSqlcRow(res), nil
}

// FailedInboxEvent marks an inbox event as failed by its ID and the processor ID that reserved it.
// It updates the event's status to failed and sets the last error reason.
func (i *inbox) FailedInboxEvent(
	ctx context.Context,
	workerID string,
	eventID uuid.UUID,
	reason string,
) (eventbox.InboxEvent, error) {
	res, err := i.queries().MarkInboxEventAsFailed(ctx, sqlc.MarkInboxEventAsFailedParams{
		WorkerID:  pgtype.Text{String: workerID, Valid: true},
		EventID:   pgtype.UUID{Bytes: eventID, Valid: true},
		LastError: pgtype.Text{String: reason, Valid: true},
	})
	if err != nil {
		return eventbox.InboxEvent{}, fmt.Errorf("mark inbox event as failed: %w", err)
	}

	return parseInboxEventFromSqlcRow(res), nil
}

// CleanProcessingInboxEvents removes all inbox events that are currently marked as processing.
func (i *inbox) CleanProcessingInboxEvents(ctx context.Context, workerIDs ...string) error {
	if len(workerIDs) == 0 {
		err := i.queries().CleanProcessingInboxEvents(ctx)
		if err != nil {
			return fmt.Errorf("clean processing inbox events: %w", err)
		}
	} else {
		err := i.queries().CleanReservedProcessingInboxEvents(ctx, workerIDs)
		if err != nil {
			return fmt.Errorf("clean processing inbox events for processor: %w", err)
		}
	}

	return nil
}

// CleanFailedInboxEvents removes all inbox events that are currently marked as failed.
func (i *inbox) CleanFailedInboxEvents(ctx context.Context) error {
	err := i.queries().CleanFailedInboxEvents(ctx)
	if err != nil {
		return fmt.Errorf("clean failed inbox events: %w", err)
	}

	return nil
}

func (i *inbox) Transaction(ctx context.Context, fn func(ctx context.Context) error) error {
	return i.db.Transaction(ctx, fn)
}

func parseInboxEventFromSqlcRow(row sqlc.InboxEvent) eventbox.InboxEvent {
	event := eventbox.InboxEvent{
		EventID: row.EventID.Bytes,
		Seq:     row.Seq,

		Topic:     row.Topic,
		Key:       row.Key,
		Type:      row.Type,
		Version:   row.Version,
		Producer:  row.Producer,
		Payload:   row.Payload,
		Partition: row.Partition,
		Offset:    row.KafkaOffset,

		Status:   string(row.Status),
		Attempts: row.Attempts,

		ProducedAt: pgtype.Timestamptz{Time: row.ProducedAt.Time, Valid: true}.Time,
		CreatedAt:  pgtype.Timestamptz{Time: row.CreatedAt.Time, Valid: true}.Time,
	}

	if row.ReservedBy.Valid {
		event.ReservedBy = &row.ReservedBy.String
	}
	if row.NextAttemptAt.Valid {
		event.NextAttemptAt = row.NextAttemptAt.Time
	}
	if row.LastAttemptAt.Valid {
		event.LastAttemptAt = &row.LastAttemptAt.Time
	}
	if row.LastError.Valid {
		event.LastError = &row.LastError.String
	}
	if row.ProcessedAt.Valid {
		event.ProcessedAt = &row.ProcessedAt.Time
	}

	return event
}
