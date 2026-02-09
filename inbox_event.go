package msnger

import (
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/msnger/headers"
	"github.com/segmentio/kafka-go"
)

const (
	// InboxEventStatusPending indicates that the event is pending processing
	InboxEventStatusPending = "pending"
	// InboxEventStatusProcessing indicates this is event already processing by some processor
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
