package pg

import (
	"context"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/netbill/msnger"
	"github.com/netbill/msnger/headers"
)

const (
	// OutboxMsgStatusPending defines pending event status
	OutboxMsgStatusPending = "pending"
	// OutboxMsgStatusProcessing  indicates this is event already processing by some worker
	OutboxMsgStatusProcessing = "processing"
	// OutboxMsgStatusSent defines sent event status
	OutboxMsgStatusSent = "sent"
	// OutboxMsgStatusFailed defines failed event status
	OutboxMsgStatusFailed = "failed"
)

type OutboxMsg struct {
	EventID uuid.UUID `json:"event_id"`
	Seq     uint      `json:"seq"`

	Topic    string `json:"topic"`
	Key      string `json:"key"`
	Type     string `json:"type"`
	Version  uint   `json:"version"`
	Producer string `json:"producer"`
	Payload  []byte `json:"payload"`

	ReservedBy *string `json:"reserved_by"`

	Status        string     `json:"status"`
	Attempts      uint       `json:"attempts"`
	NextAttemptAt time.Time  `json:"next_attempt_at"`
	LastAttemptAt *time.Time `json:"last_attempt_at"`
	LastError     *string    `json:"last_error"`

	SentAt    *time.Time `json:"sent_at"`
	CreatedAt time.Time  `json:"created_at"`
}

func (e *OutboxMsg) ToKafkaMessage() kafka.Message {
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
				Value: []byte(strconv.FormatUint(uint64(e.Version), 10)),
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

type Outbox struct {
}

func NewOutbox() msnger.Outbox {
	return &Outbox{}
}

func (o *Outbox) Run(ctx context.Context) {

}

func (o *Outbox) Stop() error {
	return nil
}
