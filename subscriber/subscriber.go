package subscriber

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/netbill/logium"
	"github.com/netbill/msnger"
)

type Subscriber struct {
	cfg kafka.ReaderConfig
	log *logium.Logger
}

func New(cfg kafka.ReaderConfig, log *logium.Logger) *Subscriber {
	return &Subscriber{
		cfg: cfg,
		log: log,
	}
}

func (s *Subscriber) Consume(
	ctx context.Context,
	handle func(ctx context.Context, m kafka.Message) error,
) error {

	for {

		if err = s.safeCall(ctx, m, handle(ctx, m)); err != nil {
			s.log.WithError(err).Warnf(
				"handler error, not committing: topic=%s partition=%d offset=%d",
				m.Topic, m.Partition, m.Offset,
			)

			select {
			case <-time.After(250 * time.Millisecond):
			case <-ctx.Done():
				return nil
			}
			continue
		}

		if err = r.CommitMessages(ctx, m); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		if err != nil {
			s.log.WithError(err).Warnf(
				"handler error, but committed: topic=%s partition=%d offset=%d",
				m.Topic, m.Partition, m.Offset,
			)
		}
	}
}

func (s *Subscriber) safeCall(ctx context.Context, m kafka.Message, h func(ctx context.Context, m kafka.Message) error) error {
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("handler panic: %v", r)
			s.log.WithError(err).Error("handler panic recovered")
		}
	}()
	return h(ctx, m)
}

func (s *Subscriber) Header(m kafka.Message, key string) (string, bool) {
	for _, h := range m.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}
