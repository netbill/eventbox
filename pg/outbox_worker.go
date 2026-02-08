package pg

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/logium"
	"github.com/segmentio/kafka-go"
)

// OutboxWorkerConfig configures OutboxWorker behavior.
type OutboxWorkerConfig struct {
	// MaxRutin is the maximum number of parallel send loops.
	// If 0, it defaults to 1.
	MaxRutin uint

	// MinSleep is the minimum delay between iterations when there are few/no events.
	MinSleep time.Duration
	// MaxSleep is the maximum delay between iterations during long idle periods.
	MaxSleep time.Duration

	// MinButchSize is the minimum number of events reserved per batch.
	MinButchSize uint
	// MaxButchSize is the maximum number of events reserved per batch.
	// If 0, it defaults to 100.
	MaxButchSize uint
}

// OutboxWorker reads pending events from outbox storage and publishes them to Kafka.
// It uses at-least-once delivery semantics: duplicates are possible and must be handled by consumers.
type OutboxWorker struct {
	log    *logium.Logger
	config OutboxWorkerConfig

	// box provides outbox storage operations (reserve/commit/delay/cleanup).
	box outbox

	// writer publishes messages to Kafka.
	writer *kafka.Writer
}

// NewOutboxWorker creates a new OutboxWorker.
func NewOutboxWorker(
	log *logium.Logger,
	cfg OutboxWorkerConfig,
	box outbox,
	writer *kafka.Writer,
) *OutboxWorker {
	w := &OutboxWorker{
		log:    log,
		config: cfg,
		box:    box,
		writer: writer,
	}

	return w
}

// outboxWorkerJob defines job for sending outbox event to kafka.
type outboxWorkerJob struct {
	ev OutboxEvent
}

// outboxWorkerRes defines result of sending outbox event to kafka.
type outboxWorkerRes struct {
	id     uuid.UUID
	now    time.Time
	err    error
	reason string
}

// Run starts the worker loop and blocks until ctx is cancelled.
//
// Flow:
//  1. Start MaxRutin send loops reading jobs from a channel.
//  2. In a loop: reserve a batch of pending events, send them, then commit or delay.
//  3. On exit: attempts to release events reserved by this worker ( the best effort).
//
// The worker does not guarantee exactly-once delivery.
// Consumers must be idempotent.
func (w *OutboxWorker) Run(ctx context.Context, id string) {
	defer func() {
		if err := w.CleanOwnProcessingEvents(context.Background(), id); err != nil {
			w.log.WithError(err).Error("failed to clean processing events for worker")
		}
	}()

	butchSize := w.config.MinButchSize
	sleep := w.config.MinSleep

	maxParallel := int(w.config.MaxRutin)
	if maxParallel <= 0 {
		maxParallel = 1
	}

	// create to channels for sending jobs to send loops and receiving results back from them
	jobs := make(chan outboxWorkerJob, maxParallel)
	results := make(chan outboxWorkerRes, maxParallel)

	var wg sync.WaitGroup
	wg.Add(maxParallel)

	for i := 0; i < maxParallel; i++ {
		// start send loops that will read reserved events from jobs channel,
		// publish them to Kafka and report results back to results channel
		go w.sendLoop(ctx, &wg, jobs, results)
	}

	defer func() {
		close(jobs)
		wg.Wait()
		close(results)
	}()

	for {
		if ctx.Err() != nil {
			return
		}

		// reserve a batch of pending events for this worker, send them via send loops,
		// and then commit (sent) or delay (retry later) each event
		numEvents := w.processBatch(ctx, id, butchSize, jobs, results)

		butchSize = w.calculateBatch(numEvents)
		sleep = w.calculateSleep(numEvents, sleep)

		select {
		case <-ctx.Done():
			return
		case <-time.After(sleep):
		}
	}
}

// sendLoop reads reserved events from jobs channel, publishes them to Kafka and reports results.
func (w *OutboxWorker) sendLoop(
	ctx context.Context,
	wg *sync.WaitGroup,
	jobs <-chan outboxWorkerJob,
	results chan<- outboxWorkerRes,
) {
	defer wg.Done()

	// read jobs until the channel is closed or context is cancelled
	for job := range jobs {
		if ctx.Err() != nil {
			return
		}

		ev := job.ev
		msg := ev.ToKafkaMessage()

		// try to send message to kafka topic
		sendErr := w.writer.WriteMessages(ctx, msg)
		now := time.Now().UTC()

		if sendErr != nil {
			w.log.WithError(sendErr).Errorf("failed to send event id=%s", ev.EventID)
			// if sending failed, report error back to results channel, so event will be delayed for future processing
			results <- outboxWorkerRes{
				id:     ev.EventID,
				now:    now,
				err:    sendErr,
				reason: sendErr.Error(),
			}
			continue
		}

		// if sending succeeded, report success back to results channel, so event will be committed as sent
		w.log.Debugf("sent event id=%s", ev.EventID)
		results <- outboxWorkerRes{id: ev.EventID, now: now}
	}
}

// processBatch reserves up to batchSize pending events for this worker,
// sends them via send loops, and then commits (sent) or delays (retry later) each event.
func (w *OutboxWorker) processBatch(
	ctx context.Context,
	id string,
	butchSize uint,
	jobs chan<- outboxWorkerJob,
	results <-chan outboxWorkerRes,
) uint {
	// reserve a batch of pending events for this worker
	events, err := w.box.ReserveOutboxEvents(ctx, id, butchSize)
	if err != nil {
		w.log.WithError(err).Error("failed to reserve events")
		return 0
	}
	if len(events) == 0 {
		return 0
	}

	for _, ev := range events {
		select {
		case <-ctx.Done():
			return uint(len(events))
			// send reserved events to send loops via jobs channel, so
			// they will be processed and then committed or delayed
		case jobs <- outboxWorkerJob{ev: ev}:
		}
	}

	commit := make(map[uuid.UUID]CommitOutboxEventParams, len(events))
	pending := make(map[uuid.UUID]DelayOutboxEventData, len(events))

	for i := 0; i < len(events); i++ {
		select {
		case <-ctx.Done():
			return uint(len(events))
		case r := <-results:
			if r.err != nil {
				pending[r.id] = DelayOutboxEventData{
					NextAttemptAt: r.now.Add(5 * time.Minute),
					Reason:        r.reason,
				}
			} else {
				commit[r.id] = CommitOutboxEventParams{SentAt: r.now}
			}
		}
	}

	if len(commit) > 0 {
		// if sending succeeded, commit events as sent, so they won't be processed again
		if err := w.box.CommitOutboxEvents(ctx, id, commit); err != nil {
			w.log.WithError(err).Error("failed to mark events as sent")
		}
	}

	if len(pending) > 0 {
		// if sending failed, delay events for future processing, so they will be retried later
		if err := w.box.DelayOutboxEvents(ctx, id, pending); err != nil {
			w.log.WithError(err).Error("failed to delay events")
		}
	}

	return uint(len(events))
}

// calculateBatch adjusts the next batch size based on how many events were processed last time.
func (w *OutboxWorker) calculateBatch(
	numEvents uint,
) uint {
	minBatch := w.config.MinButchSize
	maxBatch := w.config.MaxButchSize
	if maxBatch == 0 {
		maxBatch = 100
	}

	var batch uint
	switch {
	case numEvents == 0:
		batch = minBatch
	case numEvents >= maxBatch:
		batch = maxBatch
	default:
		batch = numEvents * 2
	}

	if batch < minBatch {
		batch = minBatch
	}
	if batch > maxBatch {
		batch = maxBatch
	}

	return batch
}

// calculateSleep adjusts the delay before the next iteration based on how many events were processed.
func (w *OutboxWorker) calculateSleep(
	numEvents uint,
	lastSleep time.Duration,
) time.Duration {
	minSleep := w.config.MinSleep
	maxSleep := w.config.MaxSleep

	var sleep time.Duration

	switch {
	case numEvents == 0:
		if lastSleep == 0 {
			sleep = minSleep
		} else {
			sleep = lastSleep * 2
		}

	case numEvents >= w.config.MaxButchSize:
		sleep = 0

	default:
		fill := float64(numEvents) / float64(w.config.MaxButchSize)

		switch {
		case fill >= 0.75:
			sleep = 0
		case fill >= 0.5:
			sleep = minSleep
		case fill >= 0.25:
			sleep = minSleep * 2
		default:
			sleep = minSleep * 4
		}
	}

	if sleep < minSleep {
		sleep = minSleep
	}
	if sleep > maxSleep {
		sleep = maxSleep
	}

	return sleep
}

// CleanOwnFailedEvents moves failed events reserved by this worker back to pending.
func (w *OutboxWorker) CleanOwnFailedEvents(
	ctx context.Context,
	id string,
) error {
	return w.box.CleanFailedOutboxEventForWorker(ctx, id)
}

// CleanOwnProcessingEvents releases processing events reserved by this worker back to pending.
func (w *OutboxWorker) CleanOwnProcessingEvents(
	ctx context.Context,
	id string,
) error {
	return w.box.CleanProcessingOutboxEventForWorker(ctx, id)
}

// Shutdown closes Kafka writer and performs a best-effort cleanup.
// Note: CleanProcessingOutboxEvent affects all processing events, not only this worker.
// Use with caution in multi-worker setups.
func (w *OutboxWorker) Shutdown(ctx context.Context) error {
	var errs []error

	err := w.writer.Close()
	if err != nil {
		w.log.WithError(err).Error("failed to close kafka writer")
		errs = append(errs, err)
	}

	err = w.box.CleanProcessingOutboxEvent(ctx)
	if err != nil {
		w.log.WithError(err).Error("failed to clean processing events for worker")
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}
