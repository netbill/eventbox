package msnger

import (
	"context"
)

type Outbox interface {
	Run(ctx context.Context)
	Stop() error
}
