package pg

import (
	"context"

	"github.com/netbill/eventbox"
	"github.com/netbill/pgdbx"
)

type OutboxCleaner struct {
	box outbox
}

func NewOutboxCleaner(db *pgdbx.DB) eventbox.OutboxCleaner {
	return &OutboxCleaner{
		box: outbox{db: db},
	}
}

// CleanOutboxProcessing cleans up all reserved events, making them available for processing again.
func (p *OutboxCleaner) CleanOutboxProcessing(ctx context.Context, processIDs ...string) error {
	return p.box.CleanProcessingOutboxEvent(ctx, processIDs...)
}

// CleanOutboxFailed cleans up all failed events, making them available for processing again.
func (p *OutboxCleaner) CleanOutboxFailed(ctx context.Context) error {
	return p.box.CleanFailedOutboxEvent(ctx)
}
