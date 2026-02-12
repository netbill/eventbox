package eventbox

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Consumer interface {
	Read(ctx context.Context, reader *kafka.Reader)
}
