package eventbox

import (
	"context"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type Producer interface {
	Publish(ctx context.Context, msg kafka.Message) error
	WriteToOutbox(ctx context.Context, msg kafka.Message) (uuid.UUID, error)

	Close() error
}
