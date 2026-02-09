package msnger

import (
	"context"
)

type OutboxProcessor interface {
	StartProcess(ctx context.Context, processID string)
	StopProcess(ctx context.Context, processID string) error
}

type OutboxMaintenance interface {
	CleanOutboxProcessing(ctx context.Context) error
	CleanOutboxProcessingForProcessID(ctx context.Context, processID string) error

	CleanOutboxFailed(ctx context.Context) error
	CleanOutboxFailedForProcessID(ctx context.Context, processID string) error
}
