package eventbox

import (
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/eventbox/headers"
	"github.com/segmentio/kafka-go"
)

const (
	// OutboxEventStatusPending defines pending event status
	OutboxEventStatusPending = "pending"
	// OutboxEventStatusProcessing  indicates this is event already processing by some processor
	OutboxEventStatusProcessing = "processing"
	// OutboxEventStatusSent defines sent event status
	OutboxEventStatusSent = "sent"
	// OutboxEventStatusFailed defines failed event status
	OutboxEventStatusFailed = "failed"
)

type OutboxEvent struct {
	EventID uuid.UUID `json:"event_id"`
	Seq     int64     `json:"seq"`

	Topic    string `json:"topic"`
	Key      string `json:"key"`
	Type     string `json:"type"`
	Version  int32  `json:"version"`
	Producer string `json:"producer"`
	Payload  []byte `json:"payload"`

	ReservedBy *string `json:"reserved_by"`

	Status        string     `json:"status"`
	Attempts      int32      `json:"attempts"`
	NextAttemptAt time.Time  `json:"next_attempt_at"`
	LastAttemptAt *time.Time `json:"last_attempt_at"`
	LastError     *string    `json:"last_error"`

	SentAt    *time.Time `json:"sent_at"`
	CreatedAt time.Time  `json:"created_at"`
}

func (e *OutboxEvent) ToKafkaMessage() kafka.Message {
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
