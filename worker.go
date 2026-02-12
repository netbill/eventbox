package eventbox

import (
	"context"
)

type OutboxWorker interface {
	Run(ctx context.Context)
	Stop(ctx context.Context) error
}
