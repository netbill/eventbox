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
	"github.com/netbill/msnger"
	"github.com/netbill/msnger/headers"
	sqlc "github.com/netbill/msnger/pg/sqlc"
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

func (i *inbox) WriteInboxEvent(
	ctx context.Context,
	message kafka.Message,
) (msnger.InboxEvent, error) {
	h, err := headers.ParseMessageRequiredHeaders(message.Headers)
	if err != nil {
		return msnger.InboxEvent{}, err
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

		Status:        msnger.InboxEventStatusPending,
		Attempts:      0,
		NextAttemptAt: pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true},
		ProducedAt:    pgtype.Timestamptz{Time: message.Time, Valid: true},
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return msnger.InboxEvent{}, ErrInboxEventAlreadyExists.Raise(err)
		}

		return msnger.InboxEvent{}, fmt.Errorf("insert inbox event: %w", err)
	}

	return parseInboxEventFromSqlcRow(row), nil
}

func (i *inbox) GetInboxEventByID(
	ctx context.Context,
	id uuid.UUID,
) (msnger.InboxEvent, error) {
	res, err := i.queries().GetInboxEventByID(ctx, pgtype.UUID{Bytes: id, Valid: true})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return msnger.InboxEvent{}, ErrInboxEventNotFound.Raise(err)
		}
		return msnger.InboxEvent{}, fmt.Errorf("get inbox event by id: %w", err)
	}

	return parseInboxEventFromSqlcRow(res), nil
}

func (i *inbox) ReserveInboxEvents(
	ctx context.Context,
	processID string,
	limit int,
) ([]msnger.InboxEvent, error) {
	res, err := i.queries().ReserveInboxEvents(ctx, sqlc.ReserveInboxEventsParams{
		ProcessID:  pgtype.Text{String: processID, Valid: true},
		BatchLimit: int32(limit),
		SortLimit:  int32(limit*10 + 100),
	})
	if err != nil {
		return nil, fmt.Errorf("reserve inbox events: %w", err)
	}

	out := make([]msnger.InboxEvent, len(res))
	for i := range res {
		out[i] = parseInboxEventFromSqlcRow(res[i])
	}

	return out, nil
}

func (i *inbox) CommitInboxEvent(
	ctx context.Context,
	processID string,
	eventID uuid.UUID,
) (msnger.InboxEvent, error) {
	res, err := i.queries().MarkInboxEventAsProcessed(ctx, sqlc.MarkInboxEventAsProcessedParams{
		ProcessID: pgtype.Text{String: processID, Valid: true},
		EventID:   pgtype.UUID{Bytes: eventID, Valid: true},
	})
	if err != nil {
		return msnger.InboxEvent{}, fmt.Errorf("mark inbox event as processed: %w", err)
	}

	return parseInboxEventFromSqlcRow(res), nil
}

func (i *inbox) DelayInboxEvent(
	ctx context.Context,
	processID string,
	eventID uuid.UUID,
	reason string,
	nextAttemptAt time.Time,
) (msnger.InboxEvent, error) {
	res, err := i.queries().MarkInboxEventAsPending(ctx, sqlc.MarkInboxEventAsPendingParams{
		ProcessID: pgtype.Text{String: processID, Valid: true},
		EventID:   pgtype.UUID{Bytes: eventID, Valid: true},
		LastError: pgtype.Text{String: reason, Valid: true},
		NextAttemptAt: pgtype.Timestamptz{
			Time:  nextAttemptAt,
			Valid: true,
		},
	})
	if err != nil {
		return msnger.InboxEvent{}, fmt.Errorf("mark inbox event as pending: %w", err)
	}

	return parseInboxEventFromSqlcRow(res), nil
}

func (i *inbox) FailedInboxEvent(
	ctx context.Context,
	processID string,
	eventID uuid.UUID,
	reason string,
) (msnger.InboxEvent, error) {
	res, err := i.queries().MarkInboxEventAsFailed(ctx, sqlc.MarkInboxEventAsFailedParams{
		ProcessID: pgtype.Text{String: processID, Valid: true},
		EventID:   pgtype.UUID{Bytes: eventID, Valid: true},
		LastError: pgtype.Text{String: reason, Valid: true},
	})
	if err != nil {
		return msnger.InboxEvent{}, fmt.Errorf("mark inbox event as failed: %w", err)
	}

	return parseInboxEventFromSqlcRow(res), nil
}

func (i *inbox) CleanProcessingInboxEvents(ctx context.Context) error {
	err := i.queries().CleanProcessingInboxEvents(ctx)
	if err != nil {
		return fmt.Errorf("clean processing inbox events: %w", err)
	}

	return nil
}

func (i *inbox) CleanProcessingInboxEventForProcessor(ctx context.Context, processID string) error {
	err := i.queries().CleanReservedProcessingInboxEvents(ctx, pgtype.Text{String: processID, Valid: true})
	if err != nil {
		return fmt.Errorf("clean processing inbox events for processor: %w", err)
	}

	return nil
}

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

func parseInboxEventFromSqlcRow(row sqlc.InboxEvent) msnger.InboxEvent {
	event := msnger.InboxEvent{
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
