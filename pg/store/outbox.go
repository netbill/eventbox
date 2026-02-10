package store

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/netbill/msnger"
	"github.com/netbill/msnger/headers"
	"github.com/netbill/msnger/pg"
	"github.com/netbill/msnger/pg/store/sqlc"
	"github.com/netbill/pgdbx"
	"github.com/segmentio/kafka-go"
)

type Outbox struct {
	db *pgdbx.DB
}

func (b *Outbox) queries() *sqlc.Queries {
	return sqlc.New(b.db)
}

func (b *Outbox) WriteOutboxEvent(
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
		Version:  int32(h.EventVersion),
		Producer: h.Producer,
		Payload:  message.Value,

		Status:        msnger.OutboxEventStatusPending,
		Attempts:      0,
		NextAttemptAt: pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true},
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return msnger.OutboxEvent{}, pg.ErrOutboxEventAlreadyExists
		}

		return msnger.OutboxEvent{}, err
	}

	return parseOutboxEventFromSqlcRow(row), nil
}

func (b *Outbox) GetOutboxEventByID(
	ctx context.Context,
	id uuid.UUID,
) (msnger.OutboxEvent, error) {
	row, err := b.queries().GetOutboxEventByID(ctx, pgtype.UUID{Bytes: id, Valid: true})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return msnger.OutboxEvent{}, pg.ErrOutboxEventNotFound
		}

		return msnger.OutboxEvent{}, err
	}

	return parseOutboxEventFromSqlcRow(row), nil
}

func (b *Outbox) ReserveOutboxEvents(
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
		return nil, err
	}

	result := make([]msnger.OutboxEvent, 0, len(rows))
	for _, row := range rows {
		result = append(result, parseOutboxEventFromSqlcRow(row))
	}

	return result, nil
}

func (b *Outbox) CommitOutboxEvents(
	ctx context.Context,
	processID string,
	events map[uuid.UUID]pg.CommitOutboxEventParams,
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
		return err
	}

	return nil
}

func (b *Outbox) DelayOutboxEvents(
	ctx context.Context,
	processID string,
	events map[uuid.UUID]pg.DelayOutboxEventData,
) error {
	eventIDs := make([]pgtype.UUID, 0, len(events))
	NextAttemptAts := make([]pgtype.Timestamptz, 0, len(events))
	LastAttemptAts := make([]pgtype.Timestamptz, 0, len(events))
	LastErrors := make([]string, 0, len(events))
	for eventID, params := range events {
		eventIDs = append(eventIDs, pgtype.UUID{Bytes: eventID, Valid: true})
		NextAttemptAts = append(NextAttemptAts, pgtype.Timestamptz{Time: params.NextAttemptAt, Valid: true})
		LastAttemptAts = append(LastAttemptAts, pgtype.Timestamptz{Time: params.LastAttemptAt, Valid: true})
		LastErrors = append(LastErrors, params.Reason)
	}

	err := b.queries().MarkOutboxEventsAsPending(ctx, sqlc.MarkOutboxEventsAsPendingParams{
		ProcessID:      pgtype.Text{String: processID, Valid: true},
		EventIds:       eventIDs,
		NextAttemptAts: NextAttemptAts,
		LastAttemptAts: LastAttemptAts,
		LastErrors:     LastErrors,
	})
	if err != nil {
		return err
	}

	return nil
}

func (b *Outbox) CleanProcessingOutboxEvent(ctx context.Context) error {
	err := b.queries().CleanProcessingOutboxEvents(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (b *Outbox) CleanProcessingOutboxEventForProcessor(ctx context.Context, processID string) error {
	err := b.queries().CleanReservedProcessingInboxEvents(ctx, pgtype.Text{String: processID, Valid: true})
	if err != nil {
		return err
	}

	return nil
}

func (b *Outbox) CleanFailedOutboxEvent(ctx context.Context) error {
	err := b.queries().CleanFailedOutboxEvents(ctx)
	if err != nil {
		return err
	}

	return nil
}

func parseOutboxEventFromSqlcRow(row sqlc.OutboxEvent) msnger.OutboxEvent {
	event := msnger.OutboxEvent{
		EventID: row.EventID.Bytes,
		Seq:     int(row.Seq),

		Topic:    row.Topic,
		Key:      row.Key,
		Type:     row.Type,
		Version:  int(row.Version),
		Producer: row.Producer,
		Payload:  row.Payload,

		Status:   string(row.Status),
		Attempts: int(row.Attempts),

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
