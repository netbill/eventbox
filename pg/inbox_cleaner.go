package pg

import (
	"context"

	"github.com/netbill/eventbox"
	"github.com/netbill/pgdbx"
)

type InboxCleaner struct {
	box inbox
}

func NewInboxCleaner(db *pgdbx.DB) eventbox.InboxCleaner {
	return &InboxCleaner{
		box: inbox{db: db},
	}
}

// CleanInboxProcessing removes all inbox events that are currently marked as processing, regardless of the processor that reserved them.
func (p *InboxCleaner) CleanInboxProcessing(ctx context.Context, processIDs ...string) error {
	return p.box.CleanProcessingInboxEvents(ctx, processIDs...)
}

// CleanInboxFailed removes all inbox events that are currently marked as failed, regardless of the processor that reserved them.
func (p *InboxCleaner) CleanInboxFailed(ctx context.Context) error {
	return p.box.CleanFailedInboxEvents(ctx)
}
