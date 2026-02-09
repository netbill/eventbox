package pg

import (
	"context"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/ape"
	"github.com/netbill/msnger/headers"
	"github.com/segmentio/kafka-go"
)

const (
	// InboxEventStatusPending indicates that the event is pending processing
	InboxEventStatusPending = "pending"
	// InboxEventStatusProcessing indicates this is event already processing by some worker
	InboxEventStatusProcessing = "processing"
	// InboxEventStatusProcessed indicates that the event has been successfully processed
	InboxEventStatusProcessed = "processed"
	// InboxEventStatusFailed indicates that all retry attempts have been exhausted
	InboxEventStatusFailed = "failed"
)

type InboxEvent struct {
	EventID uuid.UUID `json:"event_id"`
	Seq     int       `json:"seq"`

	Topic     string `json:"topic"`
	Key       string `json:"key"`
	Type      string `json:"type"`
	Version   int    `json:"version"`
	Producer  string `json:"producer"`
	Payload   []byte `json:"payload"`
	Partition int    `json:"partition"`
	Offset    int    `json:"offset"`

	ReservedBy *string `json:"reserved_by"`

	Status        string     `json:"status"`
	Attempts      int        `json:"attempts"`
	NextAttemptAt time.Time  `json:"next_attempt_at"`
	LastAttemptAt *time.Time `json:"last_attempt_at"`
	LastError     *string    `json:"last_error"`

	ProcessedAt *time.Time `json:"processedAt"`
	ProducedAt  time.Time  `json:"produced_at"`
	CreatedAt   time.Time  `json:"created_at"`
}

func (e *InboxEvent) ToKafkaMessage() kafka.Message {
	return kafka.Message{
		Topic: e.Topic,
		Key:   []byte(e.Key),
		Value: e.Payload,
		Headers: []kafka.Header{
			{
				Key:   headers.EventID,
				Value: []byte(e.EventID.String()),
			},
			{
				Key:   headers.EventType,
				Value: []byte(e.Type),
			},
			{
				Key:   headers.EventVersion,
				Value: []byte(strconv.FormatInt(int64(e.Version), 10)),
			},
			{
				Key:   headers.Producer,
				Value: []byte(e.Producer),
			},
			{
				Key:   headers.ContentType,
				Value: []byte("application/json"),
			},
		},
	}
}

var (
	ErrInboxEventAlreadyExists = ape.DeclareError("INBOX_EVENT_ALREADY_EXISTS")
)

type inbox interface {
	// WriteInboxEvent writes new event to inbox, fields "event_id", "type", "version", "producer"
	// are taken from kafka message headers, "topic", "key", "payload" - from kafka message fields,
	// "partition", "offset" - from kafka message metadata.
	//
	// This method sets:
	// - "status"        to InboxEventStatusPending
	// - "attempts"      to 0
	// - "next_attempt_at" to current time
	//
	// Returns typed error if event already exists.
	WriteInboxEvent(
		ctx context.Context,
		message kafka.Message,
	) (InboxEvent, error)

	// WriteAndReserveInboxEvent writes new event to inbox and reserves it for processing.
	// This method is similar to WriteInboxEvent, but also sets:
	// - "reserved_by" to workerID
	// - "status"      to InboxEventStatusProcessing
	//
	// Reservation is possible only if event does not exist.
	// Returns typed error if event already exists or reservation is not possible.
	WriteAndReserveInboxEvent(
		ctx context.Context,
		message kafka.Message,
		workerID string,
	) (InboxEvent, error)

	// GetInboxEventByID retrieves event by ID.
	// Returns typed error if event not found.
	GetInboxEventByID(ctx context.Context, id uuid.UUID) (InboxEvent, error)

	// ReserveInboxEvents reserves events for processing.
	// This method selects events with:
	// - "status"          = InboxEventStatusPending
	// - "reserved_by"     IS NULL
	// - "next_attempt_at" <= current time
	// Orders by "seq" ascending and limits by "limit" parameter.
	//
	// This method updates selected events:
	// - "status"      to InboxEventStatusProcessing
	// - "reserved_by" to workerID
	ReserveInboxEvents(
		ctx context.Context,
		workerID string,
		limit int,
	) ([]InboxEvent, error)

	// CommitInboxEvent sets:
	// - "status"        to InboxEventStatusProcessed
	// - increments "attempts" by 1
	// - sets "last_attempt_at" and "processedAt" to current time
	// - clears "reserved_by"
	//
	// This method requires "reserved_by" equals workerID.
	CommitInboxEvent(
		ctx context.Context,
		workerID string,
		eventID uuid.UUID,
	) (InboxEvent, error)

	// DelayInboxEvent delays event processing, sets:
	// - increments "attempts" by 1
	// - sets "last_attempt_at" to current time
	// - sets "next_attempt_at" to nextAttemptAt
	// - sets "last_error" to reason
	// - sets "status" to InboxEventStatusPending
	// - clears "reserved_by"
	//
	// This method requires "reserved_by" equals workerID.
	DelayInboxEvent(
		ctx context.Context,
		workerID string,
		eventID uuid.UUID,
		nextAttemptAt time.Time,
		reason string,
	) (InboxEvent, error)

	// FailedInboxEvent marks event as failed, sets:
	// - increments "attempts" by 1
	// - sets "last_attempt_at" to current time
	// - sets "status" to InboxEventStatusFailed
	// - sets "last_error" to reason
	// - sets "processedAt" to current time
	// - clears "reserved_by"
	//
	// This method requires "reserved_by" equals workerID.
	FailedInboxEvent(
		ctx context.Context,
		workerID string,
		eventID uuid.UUID,
		reason string,
	) (InboxEvent, error)

	// CleanProcessingInboxEvent cleans events with "status" InboxEventStatusProcessing.
	// Intended for use when workers/topic processing is stopped.
	CleanProcessingInboxEvent(ctx context.Context) error

	// CleanProcessingInboxEventForWorker is similar to CleanProcessingInboxEvent,
	// but only for events with "reserved_by" equal to workerID.
	CleanProcessingInboxEventForWorker(ctx context.Context, workerID string) error

	// CleanFailedInboxEvent cleans events with "status" InboxEventStatusFailed.
	CleanFailedInboxEvent(ctx context.Context) error

	// CleanFailedInboxEventForWorker is similar to CleanFailedInboxEvent,
	// but only for events with "reserved_by" equal to workerID.
	CleanFailedInboxEventForWorker(ctx context.Context, workerID string) error

	Transaction(ctx context.Context, fn func(ctx context.Context) error) error
}
