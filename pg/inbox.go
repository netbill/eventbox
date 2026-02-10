package pg

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/ape"
	"github.com/netbill/msnger"
	"github.com/segmentio/kafka-go"
)

var (
	ErrInboxEventAlreadyExists = ape.DeclareError("INBOX_EVENT_ALREADY_EXISTS")
)

type inbox interface {
	// WriteInboxEvent writes new event to inbox, fields "event_id", "type", "version", "producer"
	// are taken from kafka message headers, "topic", "key", "payload" - from kafka message fields,
	// "partition", "offset" - from kafka message metadata.
	//
	// This method sets:
	// - "status"        to msnger.InboxEventStatusPending
	// - "attempts"      to 0
	// - "next_attempt_at" to current time
	//
	// Returns typed error if event already exists.
	WriteInboxEvent(
		ctx context.Context,
		message kafka.Message,
	) (msnger.InboxEvent, error)

	// GetInboxEventByID retrieves event by ID.
	// Returns typed error if event not found.
	GetInboxEventByID(
		ctx context.Context,
		id uuid.UUID,
	) (msnger.InboxEvent, error)

	// ReserveInboxEvents reserves events for processing.
	// This method selects events with:
	// - "status"          = msnger.InboxEventStatusPending
	// - "reserved_by"     IS NULL
	// - "next_attempt_at" <= current time
	// Orders by "seq" ascending and limits by "limit" parameter.
	//
	// This method updates selected events:
	// - "status"      to msnger.InboxEventStatusProcessing
	// - "reserved_by" to processID
	ReserveInboxEvents(
		ctx context.Context,
		processID string,
		limit int,
	) ([]msnger.InboxEvent, error)

	// CommitInboxEvent sets:
	// - "status"        to msnger.InboxEventStatusProcessed
	// - increments "attempts" by 1
	// - sets "last_attempt_at" and "processedAt" to current time
	// - clears "reserved_by"
	//
	// This method requires "reserved_by" equals processID.
	CommitInboxEvent(
		ctx context.Context,
		processID string,
		eventID uuid.UUID,
	) (msnger.InboxEvent, error)

	// DelayInboxEvent delays event processing, sets:
	// - increments "attempts" by 1
	// - sets "last_attempt_at" to current time
	// - sets "next_attempt_at" to nextAttemptAt
	// - sets "last_error" to reason
	// - sets "status" to msnger.InboxEventStatusPending
	// - clears "reserved_by"
	//
	// This method requires "reserved_by" equals processID.
	DelayInboxEvent(
		ctx context.Context,
		processID string,
		eventID uuid.UUID,
		reason string,
		nextAttemptAt time.Time,
	) (msnger.InboxEvent, error)

	// FailedInboxEvent marks event as failed, sets:
	// - increments "attempts" by 1
	// - sets "last_attempt_at" to current time
	// - sets "status" to msnger.InboxEventStatusFailed
	// - sets "last_error" to reason
	// - sets "processedAt" to current time
	// - clears "reserved_by"
	//
	// This method requires "reserved_by" equals processID.
	FailedInboxEvent(
		ctx context.Context,
		processID string,
		eventID uuid.UUID,
		reason string,
	) (msnger.InboxEvent, error)

	// CleanProcessingInboxEvent cleans events with "status" msnger.InboxEventStatusProcessing.
	// Intended for use when processors/topic processing is stopped.
	CleanProcessingInboxEvents(ctx context.Context) error

	// CleanProcessingInboxEventForProcessor is similar to CleanProcessingInboxEvent,
	// but only for events with "reserved_by" equal to processID.
	CleanProcessingInboxEventForProcessor(ctx context.Context, processID string) error

	// CleanFailedInboxEvent cleans events with "status" msnger.InboxEventStatusFailed.
	CleanFailedInboxEvents(ctx context.Context) error

	Transaction(ctx context.Context, fn func(ctx context.Context) error) error
}
