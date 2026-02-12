package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/netbill/eventbox"
	"github.com/netbill/eventbox/logfields"
	"github.com/netbill/logium"
	"github.com/netbill/pgdbx"
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
	log    *logium.Entry
	inbox  inbox
	config ConsumerConfig
}

// NewConsumer creates a new Consumer with the given logger, inbox, and configuration.
func NewConsumer(
	log *logium.Entry,
	db *pgdbx.DB,
	config ConsumerConfig,
) eventbox.Consumer {
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
		inbox:  inbox{db: db},
		config: config,
	}
}

// Read starts the consumer loop that reads messages from Kafka, writes them to the inbox, and commits them.
func (c *Consumer) Read(ctx context.Context, reader *kafka.Reader) {
	c.log.WithField(logfields.EventTopicFiled, reader.Config().Topic).Infof("starting reading process")

	backoff := c.config.MinBackoff
	for {
		if ctx.Err() != nil {
			return
		}

		m, err := c.fetchMessage(ctx, reader)
		if err != nil {
			if !c.backoffOrStop(ctx, &backoff) {
				return
			}
			continue
		}

		if err = c.writeInbox(ctx, m); err != nil {
			if !c.backoffOrStop(ctx, &backoff) {
				return
			}
			continue
		}

		if err = c.commitMessage(ctx, reader, m); err != nil {
			if !c.backoffOrStop(ctx, &backoff) {
				return
			}
			continue
		}

		backoff = c.config.MinBackoff
	}
}

// fetchMessage attempts to fetch a message from the Kafka reader.
// It logs and returns an error if fetching fails.
func (c *Consumer) fetchMessage(ctx context.Context, r *kafka.Reader) (kafka.Message, error) {
	m, err := r.FetchMessage(ctx)
	if err != nil {
		c.log.WithError(err).
			WithField(logfields.EventTopicFiled, r.Config().Topic).
			Errorf("failed to fetch message from Kafka")

		return kafka.Message{}, fmt.Errorf("fetch message: %w", err)
	}

	return m, nil
}

// writeInbox attempts to write the fetched message to the inbox.
// It handles the case where the inbox event already exists and logs appropriately.
func (c *Consumer) writeInbox(ctx context.Context, m kafka.Message) error {
	_, err := c.inbox.WriteInboxEvent(ctx, m)
	if err != nil {
		if errors.Is(err, ErrInboxEventAlreadyExists) {
			c.log.WithFields(logfields.FromMessage(m)).Info("inbox event already exists")
			return nil
		}

		c.log.WithError(err).WithFields(logfields.FromMessage(m)).Errorf("failed to write inbox event")
		return fmt.Errorf("write inbox event: %w", err)
	}

	return nil
}

// commitMessage attempts to commit the processed message in Kafka.
// It logs and returns an error if committing fails.
func (c *Consumer) commitMessage(ctx context.Context, r *kafka.Reader, m kafka.Message) error {
	if err := r.CommitMessages(ctx, m); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		c.log.WithError(err).WithFields(logfields.FromMessage(m)).Errorf("failed to commit message in Kafka")
		return fmt.Errorf("commit message: %w", err)
	}

	return nil
}

// backoffOrStop implements an exponential backoff strategy for retrying operations.
// It waits for the specified backoff duration before allowing a retry.
// If the context is canceled during the wait, it returns false to indicate that the operation should stop.
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
