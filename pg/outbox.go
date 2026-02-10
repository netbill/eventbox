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
	ErrOutboxEventAlreadyExists = ape.DeclareError("OUTBOX_EVENT_ALREADY_EXISTS")
	ErrOutboxEventNotFound      = ape.DeclareError("OUTBOX_EVENT_NOT_FOUND")
)

type DelayOutboxEventData struct {
	NextAttemptAt time.Time // this data for field "next_attempt_at" in outbox_events table
	LastAttemptAt time.Time // this data for field "last_attempt_at" in outbox_events table
	Reason        string    // this data for field "last_error" in outbox_events table
}

type CommitOutboxEventParams struct {
	SentAt time.Time // this data for field "sent_at" in outbox_events table
}

type FailedOutboxEventData struct {
	LastAttemptAt time.Time // this data for field "last_attempt_at" in outbox_events table
	Reason        string    // this data for field "last_error" in outbox_events table
}

type outbox struct {
	db *pgdbx.DB
}

func (b *outbox) queries() *sqlc.Queries {
	return sqlc.New(b.db)
}

func (b *outbox) WriteOutboxEvent(
	ctx context.Context,
	message kafka.Message,
) (msnger.OutboxEvent, error) {
	h, err := headers.ParseMessageRequiredHeaders(message.Headers)
	if err != nil {
		return msnger.OutboxEvent{}, err
	}

	row, err := b.queries().InsertOutboxEvent(ctx, sqlc.InsertOutboxEventParams{
		EventID: pgtype.UUID{Bytes: h.EventID, Valid: true},

		Topic:    message.Topic,
		Key:      string(message.Key),
		Type:     h.EventType,
		Version:  h.EventVersion,
		Producer: h.Producer,
		Payload:  message.Value,

		Status:        msnger.OutboxEventStatusPending,
		Attempts:      0,
		NextAttemptAt: pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true},
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return msnger.OutboxEvent{}, ErrOutboxEventAlreadyExists.Raise(err)
		}

		return msnger.OutboxEvent{}, fmt.Errorf("insert outbox event: %w", err)
	}

	return parseOutboxEventFromSqlcRow(row), nil
}

func (b *outbox) GetOutboxEventByID(
	ctx context.Context,
	id uuid.UUID,
) (msnger.OutboxEvent, error) {
	row, err := b.queries().GetOutboxEventByID(ctx, pgtype.UUID{Bytes: id, Valid: true})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return msnger.OutboxEvent{}, ErrOutboxEventNotFound.Raise(err)
		}

		return msnger.OutboxEvent{}, fmt.Errorf("get outbox event by id: %w", err)
	}

	return parseOutboxEventFromSqlcRow(row), nil
}

func (b *outbox) ReserveOutboxEvents(
	ctx context.Context,
	processID string,
	limit int,
) ([]msnger.OutboxEvent, error) {
	rows, err := b.queries().ReserveOutboxEvents(ctx, sqlc.ReserveOutboxEventsParams{
		ProcessID:  pgtype.Text{String: processID, Valid: true},
		BatchLimit: int32(limit),
		SortLimit:  int32(limit*10 + 100),
	})
	if err != nil {
		return nil, fmt.Errorf("reserve outbox events: %w", err)
	}

	result := make([]msnger.OutboxEvent, 0, len(rows))
	for _, row := range rows {
		result = append(result, parseOutboxEventFromSqlcRow(row))
	}

	return result, nil
}

func (b *outbox) CommitOutboxEvents(
	ctx context.Context,
	processID string,
	events map[uuid.UUID]CommitOutboxEventParams,
) error {
	eventIDs := make([]pgtype.UUID, 0, len(events))
	SentAts := make([]pgtype.Timestamptz, 0, len(events))
	for eventID, params := range events {
		eventIDs = append(eventIDs, pgtype.UUID{Bytes: eventID, Valid: true})
		SentAts = append(SentAts, pgtype.Timestamptz{Time: params.SentAt, Valid: true})
	}

	err := b.queries().MarkOutboxEventsAsSent(ctx, sqlc.MarkOutboxEventsAsSentParams{
		ProcessID: pgtype.Text{String: processID, Valid: true},
		EventIds:  eventIDs,
		SentAts:   SentAts,
	})
	if err != nil {
		return fmt.Errorf("mark outbox events as sent: %w", err)
	}

	return nil
}

func (b *outbox) DelayOutboxEvents(
	ctx context.Context,
	processID string,
	events map[uuid.UUID]DelayOutboxEventData,
) error {
	eventIDs := make([]pgtype.UUID, 0, len(events))
	nextAttemptAts := make([]pgtype.Timestamptz, 0, len(events))
	lastAttemptAts := make([]pgtype.Timestamptz, 0, len(events))
	lastErrors := make([]string, 0, len(events))
	for eventID, params := range events {
		eventIDs = append(eventIDs, pgtype.UUID{Bytes: eventID, Valid: true})
		nextAttemptAts = append(nextAttemptAts, pgtype.Timestamptz{Time: params.NextAttemptAt, Valid: true})
		lastAttemptAts = append(lastAttemptAts, pgtype.Timestamptz{Time: params.LastAttemptAt, Valid: true})
		lastErrors = append(lastErrors, params.Reason)
	}

	err := b.queries().MarkOutboxEventsAsPending(ctx, sqlc.MarkOutboxEventsAsPendingParams{
		ProcessID:      pgtype.Text{String: processID, Valid: true},
		EventIds:       eventIDs,
		NextAttemptAts: nextAttemptAts,
		LastAttemptAts: lastAttemptAts,
		LastErrors:     lastErrors,
	})
	if err != nil {
		return fmt.Errorf("mark outbox events as pending: %w", err)
	}

	return nil
}

func (b *outbox) FailOutboxEvents(
	ctx context.Context,
	processID string,
	events map[uuid.UUID]FailedOutboxEventData,
) error {
	eventIDs := make([]pgtype.UUID, 0, len(events))
	lastAttemptAts := make([]pgtype.Timestamptz, 0, len(events))
	lastErrors := make([]string, 0, len(events))
	for eventID, params := range events {
		eventIDs = append(eventIDs, pgtype.UUID{Bytes: eventID, Valid: true})
		lastAttemptAts = append(lastAttemptAts, pgtype.Timestamptz{Time: params.LastAttemptAt, Valid: true})
		lastErrors = append(lastErrors, params.Reason)
	}

	err := b.queries().MarkOutboxEventsAsFailed(ctx, sqlc.MarkOutboxEventsAsFailedParams{
		ProcessID:  pgtype.Text{String: processID, Valid: true},
		EventIds:   eventIDs,
		LastErrors: lastErrors,
	})
	if err != nil {
		return fmt.Errorf("mark outbox events as failed: %w", err)
	}

	return nil
}

func (b *outbox) CleanProcessingOutboxEvent(ctx context.Context) error {
	err := b.queries().CleanProcessingOutboxEvents(ctx)
	if err != nil {
		return fmt.Errorf("clean processing outbox events: %w", err)
	}

	return nil
}

func (b *outbox) CleanProcessingOutboxEventForProcessor(ctx context.Context, processID string) error {
	err := b.queries().CleanReservedProcessingOutboxEvents(ctx, pgtype.Text{String: processID, Valid: true})
	if err != nil {
		return fmt.Errorf("clean processing outbox events for processor: %w", err)
	}

	return nil
}

func (b *outbox) CleanFailedOutboxEvent(ctx context.Context) error {
	err := b.queries().CleanFailedOutboxEvents(ctx)
	if err != nil {
		return fmt.Errorf("clean failed outbox events: %w", err)
	}

	return nil
}

func parseOutboxEventFromSqlcRow(row sqlc.OutboxEvent) msnger.OutboxEvent {
	event := msnger.OutboxEvent{
		EventID: row.EventID.Bytes,
		Seq:     row.Seq,

		Topic:    row.Topic,
		Key:      row.Key,
		Type:     row.Type,
		Version:  row.Version,
		Producer: row.Producer,
		Payload:  row.Payload,

		Status:   string(row.Status),
		Attempts: row.Attempts,

		CreatedAt: pgtype.Timestamptz{Time: row.CreatedAt.Time, Valid: true}.Time,
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
	if row.SentAt.Valid {
		event.SentAt = &row.SentAt.Time
	}

	return event
}
