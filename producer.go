package msnger

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type Producer interface {
	WriteMessage(ctx context.Context, msg kafka.Message)

	WriteMessageInBox(ctx context.Context, msg kafka.Message, delay *time.Duration)
	CommitMessageInBox(ctx context.Context, messageID uuid.UUID)

	ShutDown() error
}
