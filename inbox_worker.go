package eventbox

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type InboxHandlerFunc func(ctx context.Context, msg kafka.Message) error

type InboxWorker interface {
	Run(ctx context.Context)
	Stop(ctx context.Context) error

	Route(eventType string, handler InboxHandlerFunc)
}
