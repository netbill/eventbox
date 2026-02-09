package pg

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/msnger"
	"github.com/segmentio/kafka-go"
)

type DelayOutboxEventData struct {
	NextAttemptAt time.Time // this data for field "next_attempt_at" in outbox_events table
	Reason        string    // this data for field "last_error" in outbox_events table
}

type CommitOutboxEventParams struct {
	SentAt time.Time // this data for field "sent_at" in outbox_events table
}

type outbox interface {
	// WriteOutboxEvent writes new event to outbox, fields "event_id", "type", "version", "producer"
	// are taken from kafka message headers, "topic", "key", "payload" - from kafka message fields.
	//
	// This method sets:
	// - "status"         to msnger.OutboxEventStatusPending
	// - "attempts"       to 0
	// - "next_attempt_at" to current time
	WriteOutboxEvent(
		ctx context.Context,
		message kafka.Message,
	) (msnger.OutboxEvent, error)

	// WriteAndReserveOutboxEvent writes new event to outbox and reserves it for processing.
	// This method is similar to WriteOutboxEvent, but also sets:
	// - "status"      to msnger.OutboxEventStatusProcessing
	// - "reserved_by" to processID
	//
	// If event already exists and:
	// - "status"      is msnger.OutboxEventStatusPending
	// - "reserved_by" IS NULL
	// this method reserves existing event for processing by updating:
	// - "status"      to msnger.OutboxEventStatusProcessing
	// - "reserved_by" to processID
	// and returns reserved = true.
	//
	// Otherwise this method does not reserve event and returns reserved = false with existing event.
	WriteAndReserveOutboxEvent(
		ctx context.Context,
		message kafka.Message,
		processID string,
	) (msnger.OutboxEvent, bool, error)

	// GetOutboxEventByID retrieves event by ID.
	// Returns typed error if event not found.
	GetOutboxEventByID(ctx context.Context, id uuid.UUID) (msnger.OutboxEvent, error)

	// ReserveOutboxEvents reserves events for processing.
	// This method selects events with:
	// - "status"          = msnger.OutboxEventStatusPending
	// - "next_attempt_at" <= current time
	// Orders by "seq" ascending and limits by "limit" parameter.
	//
	// This method updates selected events:
	// - "status"      to msnger.OutboxEventStatusProcessing
	// - "reserved_by" to processID
	ReserveOutboxEvents(
		ctx context.Context,
		processID string,
		limit int,
	) ([]msnger.OutboxEvent, error)

	// CommitOutboxEvents marks events as sent.
	// This method updates events with ids from "events" map and sets:
	// - "status"          to msnger.OutboxEventStatusSent
	// - "sent_at"         to events[eventID].SentAt
	// - "last_attempt_at" to events[eventID].SentAt
	// - clears "reserved_by"
	//
	// This method updates only events with:
	// - "status"      = msnger.OutboxEventStatusProcessing
	// - "reserved_by" = processID
	CommitOutboxEvents(
		ctx context.Context,
		processID string,
		events map[uuid.UUID]CommitOutboxEventParams,
	) error

	// CommitOutboxEvent marks event as sent.
	// This method does the same thing as CommitOutboxEvents, but for one event.
	// Returns updated event.
	CommitOutboxEvent(
		ctx context.Context,
		eventID uuid.UUID,
		processID string,
		data CommitOutboxEventParams,
	) (msnger.OutboxEvent, error)

	// DelayOutboxEvents delays events for future processing.
	// This method updates events with ids from "events" map and sets:
	// - "status"          to msnger.OutboxEventStatusPending
	// - increments "attempts" by 1
	// - sets "last_attempt_at" to current time
	// - sets "next_attempt_at" to events[eventID].NextAttemptAt
	// - sets "last_error"      to events[eventID].Reason
	// - clears "reserved_by"
	//
	// This method updates only events with:
	// - "status"      = msnger.OutboxEventStatusProcessing
	// - "reserved_by" = processID
	DelayOutboxEvents(
		ctx context.Context,
		processID string,
		events map[uuid.UUID]DelayOutboxEventData,
	) error

	// DelayOutboxEvent delays event processing.
	// This method does the same thing as DelayOutboxEvents, but for one event.
	DelayOutboxEvent(
		ctx context.Context,
		processID string,
		eventID uuid.UUID,
		data DelayOutboxEventData,
	) error

	// CleanProcessingOutboxEvent cleans events with "status" msnger.OutboxEventStatusProcessing.
	// This method updates events and sets:
	// - "status"          to msnger.OutboxEventStatusPending
	// - clears "reserved_by"
	// - sets "next_attempt_at" to current time
	CleanProcessingOutboxEvent(ctx context.Context) error

	// CleanProcessingOutboxEventForProcessor is similar to CleanProcessingOutboxEvent,
	// but only for events with "reserved_by" equal to processID.
	CleanProcessingOutboxEventForProcessor(ctx context.Context, processID string) error

	// CleanFailedOutboxEvent cleans events with "status" msnger.OutboxEventStatusFailed.
	// This method updates events and sets:
	// - "status"          to msnger.OutboxEventStatusPending
	// - clears "reserved_by"
	// - sets "next_attempt_at" to current time
	CleanFailedOutboxEvent(ctx context.Context) error

	// CleanFailedOutboxEventForProcessor is similar to CleanFailedOutboxEvent,
	// but only for events with "reserved_by" equal to processID.
	CleanFailedOutboxEventForProcessor(ctx context.Context, processID string) error
}
