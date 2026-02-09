package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/netbill/logium"
	"github.com/netbill/msnger"
	"github.com/segmentio/kafka-go"
)

type producer struct {
	log    *logium.Logger
	writer *kafka.Writer
	outbox outbox
}

func NewProducer(
	log *logium.Logger,
	writer *kafka.Writer,
	outbox outbox,
) msnger.Producer {
	p := &producer{
		log:    log,
		writer: writer,
		outbox: outbox,
	}

	return p
}

// Publish sends message directly to kafka topic without using outbox.
// Use with caution,if event doesn't really matter, and you admit the risk of losing it in case of failure.
func (p *producer) Publish(
	ctx context.Context,
	msg kafka.Message,
) error {
	return p.writer.WriteMessages(ctx, msg)
}

// WriteToOutbox writes message to outbox, but doesn't reserve it for sending.
// Use it if you want to write event to outbox, but reserve it later in the same transaction with other events.
// This method should be used with handler function in transaction
func (p *producer) WriteToOutbox(
	ctx context.Context,
	msg kafka.Message,
) (uuid.UUID, error) {
	event, err := p.outbox.WriteOutboxEvent(ctx, msg)
	if err != nil {
		return uuid.Nil, err
	}

	return event.EventID, nil
}

// Shutdown closes kafka writer and clean processing outbox events for producer,
// use it when you want to gracefully shutdown producer for example when you want to shut down application,
// and you want to make sure that all events that are in processing will be cleaned,
func (p *producer) Shutdown(ctx context.Context) error {
	var errs []error

	if err := p.writer.Close(); err != nil {
		p.log.WithError(err).Error("failed to close kafka writer")
		errs = append(errs, fmt.Errorf("failed to close kafka writer: %w", err))
	}

	return errors.Join(errs...)
}
