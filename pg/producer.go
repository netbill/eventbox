package pg

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/netbill/eventbox"
	"github.com/netbill/pgdbx"
	"github.com/segmentio/kafka-go"
)

type producer struct {
	writer *kafka.Writer
	outbox outbox
}

func NewProducer(
	writer *kafka.Writer,
	db *pgdbx.DB,
) eventbox.Producer {
	p := &producer{
		writer: writer,
		outbox: outbox{db: db},
	}

	return p
}

// Publish writes the message directly to Kafka.
// It should be used when the message is not critical and can be lost in case of failure.
func (p *producer) Publish(
	ctx context.Context,
	msg kafka.Message,
) error {
	err := p.writer.WriteMessages(ctx, msg)
	if err != nil {
		return fmt.Errorf("write message to kafka: %w", err)
	}

	return nil
}

// WriteToOutbox writes the message to the outbox table.
func (p *producer) WriteToOutbox(
	ctx context.Context,
	msg kafka.Message,
) (uuid.UUID, error) {
	event, err := p.outbox.WriteOutboxEvent(ctx, msg)
	if err != nil {
		return uuid.Nil, fmt.Errorf("write outbox event: %w", err)
	}

	return event.EventID, nil
}

// Close closes the Kafka writer.
func (p *producer) Close() error {
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}

	return nil
}
