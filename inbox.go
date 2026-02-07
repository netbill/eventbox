package msnger

import (
	"context"
)

type Inbox interface {
	Run(ctx context.Context)
	Stop() error

	Route(eventType string, handler InHandlerFunc)
}
