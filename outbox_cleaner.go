package eventbox

import "context"

type OutboxCleaner interface {
	CleanOutboxProcessing(ctx context.Context, processIDs ...string) error

	CleanOutboxFailed(ctx context.Context) error
}
