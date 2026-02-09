package msnger

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Consumer interface {
	Run(ctx context.Context, cfg kafka.ReaderConfig)
	Shutdown(ctx context.Context) error
}
