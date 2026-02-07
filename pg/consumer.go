package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/netbill/logium"
	"github.com/netbill/msnger"
	"github.com/netbill/msnger/headers"
)

type Consumer struct {
	log        *logium.Logger
	inbox      inboxForConsumer
	subscriber subscriber

	id     string
	topic  string
	routes map[string]msnger.InHandlerFunc
}

type subscriber interface {
	Consume(ctx context.Context, router func(m kafka.Message) msnger.InHandlerFunc) error
	Header(m kafka.Message, key string) (string, bool)
}

type inboxForConsumer interface {
	WriteAndReserveInboxEvent(
		ctx context.Context,
		message kafka.Message,
		workerID string,
	) (event InboxMsg, reserved bool, err error)

	CommitInboxEvent(ctx context.Context, workerID string, eventID uuid.UUID) error

	DelayInboxEvent(ctx context.Context, workerID string, eventID uuid.UUID, nextAttemptAt time.Duration, reason string) error

	CleanProcessingInboxEventForWorker(ctx context.Context, workerID string) error

	CleanFailedInboxEventForWorker(ctx context.Context, workerID string) error

	Transaction(ctx context.Context, fn func(ctx context.Context) error) error
}

func (c *Consumer) Route(eventType string, handler msnger.InHandlerFunc) {
	_, ok := c.routes[eventType]
	if ok {
		panic(fmt.Errorf("for one type event double define handler"))
	}

	c.routes[eventType] = func(ctx context.Context, m kafka.Message) error {
		header, err := headers.ParseMessageRequiredHeaders(m.Headers)
		if err != nil {
			return c.invalidContent(ctx, m)
		}

		err = c.inbox.Transaction(ctx, func(ctx context.Context) error {
			event, reserved, err := c.inbox.WriteAndReserveInboxEvent(ctx, m, c.id)
			if err != nil {
				c.log.WithError(err).Error("failed to write and reserve inbox event")
				return err
			}
			if !reserved {
				c.log.Debugf("event %s already processed, skipping", header.EventID)
				return nil
			}

			res := handler(ctx, event.ToKafkaMessage())
			if res != nil {
				c.log.WithError(res).Error("failed to handle event")
				return c.inbox.DelayInboxEvent(ctx, c.id, event.EventID, time.Minute, res.Error())
			}

			c.log.Debugf("event %s handled successfully", event.EventID)

			return c.inbox.CommitInboxEvent(ctx, c.id, event.EventID)
		})
		if err != nil {
			return err
		}

		return nil
	}

}

func (c *Consumer) onUnknown(ctx context.Context, m kafka.Message) error {
	c.log.Warnf(
		"onUnknown called for event on topic %s, partition %d, offset %d", m.Topic, m.Partition, m.Offset,
	)

	return nil
}

func (c *Consumer) invalidContent(ctx context.Context, m kafka.Message) error {
	c.log.Warnf(
		"invalid content in message on topic %s, partition %d, offset %d", m.Topic, m.Partition, m.Offset,
	)

	return nil
}

func (c *Consumer) route(m kafka.Message) msnger.InHandlerFunc {
	et, ok := c.subscriber.Header(m, headers.EventType)
	if !ok {
		return c.invalidContent
	}

	if h, ok := c.routes[et]; ok {
		return h
	}

	return c.onUnknown
}

func (c *Consumer) Run(ctx context.Context) {
	backoff := 200 * time.Millisecond
	const maxBackoff = 5 * time.Second

	for ctx.Err() == nil {
		err := c.subscriber.Consume(ctx, c.route)
		if err == nil {
			return
		}

		c.log.Warnf("consumer %s stopped: %v", c.id, err)

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func (c *Consumer) CleanOwnFailedEvents(ctx context.Context) error {
	return c.inbox.CleanFailedInboxEventForWorker(ctx, c.id)
}

func (c *Consumer) CleanOwnProcessingEvents(ctx context.Context) error {
	return c.inbox.CleanProcessingInboxEventForWorker(ctx, c.id)
}

func (c *Consumer) Stop() error {
	err := c.CleanOwnProcessingEvents(context.Background())
	if err != nil {
		return fmt.Errorf("failed to clean processing events: %w", err)
	}

	return nil
}
