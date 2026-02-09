package msnger

import (
	"context"
)

type InboxWorker interface {
	Run(ctx context.Context, workerID string)
	Shutdown(ctx context.Context) error

	Route(eventType string, handler InHandlerFunc)
}
