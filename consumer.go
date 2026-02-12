package eventbox

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Consumer interface {
	RunReader(ctx context.Context, cfg kafka.ReaderConfig)
}
