package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/netbill/logium"
	"github.com/segmentio/kafka-go"
)

const (
	DefaultMinConsumerBackoff = 200 * time.Millisecond
	DefaultMaxConsumerBackoff = 5 * time.Second
)

// ConsumerConfig holds the configuration for the Consumer.
type ConsumerConfig struct {
	// MinBackoff is the minimum duration to wait before retrying after a failure.
	// If not set, it defaults to DefaultMinConsumerBackoff.
	MinBackoff time.Duration
	// MaxBackoff is the maximum duration to wait before retrying after a failure.
	// If not set, it defaults to DefaultMaxConsumerBackoff.
	MaxBackoff time.Duration
}

type Consumer struct {
	log    *logium.Logger
	inbox  inbox
	config ConsumerConfig
}

// NewConsumer creates a new Consumer with the given logger, inbox, and configuration.
func NewConsumer(
	log *logium.Logger,
	inbox inbox,
	config ConsumerConfig,
) *Consumer {
	if config.MinBackoff <= 0 {
		config.MinBackoff = DefaultMinConsumerBackoff
	}
	if config.MaxBackoff <= 0 {
		config.MaxBackoff = DefaultMaxConsumerBackoff
	}
	if config.MaxBackoff < config.MinBackoff {
		config.MaxBackoff = config.MinBackoff
	}

	return &Consumer{
		log:    log,
		inbox:  inbox,
		config: config,
	}
}

// StartReading starts the consumer loop, which continuously fetches messages from the Kafka topic,
// writes them to the inbox, and commits the messages. It handles errors with an exponential backoff strategy.
// The loop will exit when the context is canceled.
func (c *Consumer) StartReading(
	ctx context.Context,
	readerConfig kafka.ReaderConfig,
) {
	r := kafka.NewReader(readerConfig)
	defer func() {
		if derr := r.Close(); derr != nil {
			c.log.WithError(derr).Errorf("failed to close kafka reader: topic=%s", readerConfig.Topic)
		}
	}()

	backoff := c.config.MinBackoff

	for {
		if ctx.Err() != nil {
			return
		}

		// Fetch the next message from Kafka
		m, err := c.fetchMessage(ctx, r)
		if err != nil {
			if !c.backoffOrStop(ctx, &backoff) {
				return
			}
			continue
		}

		// Write the message to the inbox
		if err = c.writeInbox(ctx, m); err != nil {
			if !c.backoffOrStop(ctx, &backoff) {
				return
			}
			continue
		}

		// Commit the message in Kafka
		if err = c.commitMessage(ctx, r, m); err != nil {
			if !c.backoffOrStop(ctx, &backoff) {
				return
			}
			continue
		}

		backoff = c.config.MinBackoff
	}
}

// fetchMessage attempts to fetch a message from the Kafka reader.
// It returns an error if the context is canceled or if there is a failure in fetching the message.
func (c *Consumer) fetchMessage(ctx context.Context, r *kafka.Reader) (kafka.Message, error) {
	m, err := r.FetchMessage(ctx)

	if err != nil {
		if ctx.Err() != nil {
			return kafka.Message{}, ctx.Err()
		}

		c.log.WithError(err).Errorf("failed to fetch message: topic=%s", r.Config().Topic)
		return kafka.Message{}, fmt.Errorf("fetch message: %w", err)
	}

	return m, nil
}

// writeInbox attempts to write the given Kafka message to the inbox.
// if the message already exists in the inbox, it logs a warning and skips the message without returning an error.
// It returns an error if the context is canceled or if there is a failure in writing to the inbox.
func (c *Consumer) writeInbox(ctx context.Context, m kafka.Message) error {
	event, err := c.inbox.WriteInboxEvent(ctx, m)

	switch {
	case err == nil:
		c.log.Debugf(
			"written event to inbox: event_id=%s topic=%s partition=%d offset=%d",
			event.EventID, event.Topic, event.Partition, event.Offset,
		)
		return nil

	case errors.Is(err, ErrInboxEventAlreadyExists):
		c.log.Warnf(
			"inbox event already exists, skipping message: topic=%s partition=%d offset=%d key=%s",
			m.Topic, m.Partition, m.Offset, string(m.Key),
		)
		return nil

	case ctx.Err() != nil:
		return ctx.Err()

	default:
		c.log.WithError(err).Errorf(
			"failed to write inbox event: topic=%s partition=%d offset=%d key=%s",
			m.Topic, m.Partition, m.Offset, string(m.Key),
		)
		return fmt.Errorf("write inbox event: %w", err)
	}
}

// commitMessage attempts to commit the given Kafka message using the reader.
// It returns an error if the context is canceled or if there is a failure in committing the message.
func (c *Consumer) commitMessage(ctx context.Context, r *kafka.Reader, m kafka.Message) error {
	if err := r.CommitMessages(ctx, m); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		c.log.WithError(err).Errorf(
			"failed to commit message: topic=%s partition=%d offset=%d",
			m.Topic, m.Partition, m.Offset,
		)
		return fmt.Errorf("commit message: %w", err)
	}

	return nil
}

// backoffOrStop implements calculating the next backoff duration using an exponential backoff strategy
// and checks if the context is canceled. It waits for the specified backoff duration before returning true to
// indicate that the caller should retry the operation.
func (c *Consumer) backoffOrStop(ctx context.Context, backoff *time.Duration) bool {
	t := time.NewTimer(*backoff)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-t.C:
	}

	next := *backoff * 2
	if next > c.config.MaxBackoff {
		next = c.config.MaxBackoff
	}
	*backoff = next
	return true
}
