package pg

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/netbill/msnger"
	"github.com/segmentio/kafka-go"
)

type producer struct {
	writer *kafka.Writer
	outbox outbox
}

func NewProducer(
	writer *kafka.Writer,
	outbox outbox,
) msnger.Producer {
	p := &producer{
		writer: writer,
		outbox: outbox,
	}

	return p
}

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

func (p *producer) Close() error {
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}

	return nil
}
