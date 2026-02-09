package msnger

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Consumer interface {
	StartReading(ctx context.Context, cfg kafka.ReaderConfig)
}
