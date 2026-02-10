package pg

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/logium"
	"github.com/netbill/msnger"
	"github.com/segmentio/kafka-go"
)

const (
	DefaultOutboxProcessorMaxRutin = 10

	DefaultOutboxProcessorMinSleep = 100 * time.Millisecond
	DefaultOutboxProcessorMaxSleep = 5 * time.Second

	DefaultOutboxProcessorMinBatchSize = 10
	DefaultOutboxProcessorMaxBatchSize = 100

	DefaultOutboxProcessorMinNextAttempt = time.Minute
	DefaultOutboxProcessorMaxNextAttempt = 10 * time.Minute
)

// OutboxProcessorConfig configures OutboxProcessor behavior.
type OutboxProcessorConfig struct {
	// MaxRutin is the maximum number of parallel send loops.
	// If 0, it defaults to DefaultOutboxProcessorMaxRutin.
	MaxRutin int

	// MinSleep is the minimum delay between iterations when there are few/no events.
	// If 0, it defaults to DefaultOutboxProcessorMinSleep.
	MinSleep time.Duration
	// MaxSleep is the maximum delay between iterations during long idle periods.
	// If 0, it defaults to DefaultOutboxProcessorMaxSleep.
	MaxSleep time.Duration

	// MinBatchSize is the minimum number of events reserved per batch.
	// If 0, it defaults to DefaultOutboxProcessorMinBatchSize.
	MinBatchSize int
	// MaxBatchSize is the maximum number of events reserved per batch.
	// If 0, it defaults to DefaultOutboxProcessorMaxBatchSize.
	MaxBatchSize int

	// MinNextAttempt is the minimum delay before next attempt to process failed event.
	// If 0, it defaults to DefaultOutboxProcessorMinNextAttempt.
	MinNextAttempt time.Duration
	// MaxNextAttempt is the maximum delay before next attempt to process failed event.
	// If 0, it defaults to DefaultOutboxProcessorMaxNextAttempt.
	MaxNextAttempt time.Duration
}

// OutboxProcessor reads pending outbox events from storage and publishes them to Kafka.
//
// Semantics:
//   - At-least-once delivery: duplicates are possible. Consumers must be idempotent.
//   - A processor runs as a long-living process identified by processID.
//   - StartProcess does NOT automatically release/clean events reserved by this process.
//     The caller must decide what to do on shutdown (e.g. call StopProcess or a maintenance cleanup).
type OutboxProcessor struct {
	log    *logium.Logger
	config OutboxProcessorConfig

	// box provides outbox storage operations (reserve/commit/delay/cleanup).
	box outbox

	// writer publishes messages to Kafka.
	writer *kafka.Writer
}

// NewOutboxProcessor creates a new OutboxProcessor.
func NewOutboxProcessor(
	log *logium.Logger,
	cfg OutboxProcessorConfig,
	box outbox,
	writer *kafka.Writer,
) *OutboxProcessor {
	if cfg.MaxRutin < 0 {
		cfg.MaxRutin = DefaultOutboxProcessorMaxRutin
	}
	if cfg.MinSleep <= 0 {
		cfg.MinSleep = DefaultOutboxProcessorMinSleep
	}
	if cfg.MaxSleep <= 0 {
		cfg.MaxSleep = DefaultOutboxProcessorMaxSleep
	}
	if cfg.MinBatchSize <= 0 {
		cfg.MinBatchSize = DefaultOutboxProcessorMinBatchSize
	}
	if cfg.MaxBatchSize < cfg.MinBatchSize {
		cfg.MaxBatchSize = DefaultOutboxProcessorMaxBatchSize
	}
	if cfg.MinNextAttempt <= 0 {
		cfg.MinNextAttempt = DefaultOutboxProcessorMinNextAttempt
	}
	if cfg.MaxNextAttempt < cfg.MinNextAttempt {
		cfg.MaxNextAttempt = DefaultOutboxProcessorMaxNextAttempt
	}

	w := &OutboxProcessor{
		log:    log,
		config: cfg,
		box:    box,
		writer: writer,
	}

	return w
}

// outboxProcessorJob defines job for sending outbox event to kafka.
type outboxProcessorJob struct {
	ev msnger.OutboxEvent
}

// outboxProcessorRes defines result of sending outbox event to kafka.
type outboxProcessorRes struct {
	eventID     uuid.UUID
	err         error
	nextAttempt time.Time
	processedAt time.Time
}

// StartProcess starts the processor loop and blocks until ctx is cancelled.
//
// Flow:
//  1. Start MaxRutin send loops reading jobs from a channel.
//  2. In a loop: reserve a batch of pending events, send them, then commit or delay.
//
// Notes:
//   - processID identifies this running process in outbox storage (reservation/locks).
//   - StartProcess stops when ctx is cancelled.
//   - StartProcess DOES NOT perform any cleanup on exit.
//     If the process is shutting down, the caller may call StopProcess(processID)
//     or use maintenance methods to release reserved events.
func (w *OutboxProcessor) StartProcess(ctx context.Context, id string) {
	BatchSize := w.config.MinBatchSize
	sleep := w.config.MinSleep

	maxParallel := w.config.MaxRutin
	if maxParallel <= 0 {
		maxParallel = 1
	}

	// create to channels for sending jobs to send loops and receiving results back from them
	jobs := make(chan outboxProcessorJob, maxParallel)
	results := make(chan outboxProcessorRes, maxParallel)

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

		// reserve a batch of pending events for this processor, send them via send loops,
		// and then commit (sent) or delay (retry later) each event
		numEvents := w.processBatch(ctx, id, BatchSize, jobs, results)

		BatchSize = w.calculateBatch(numEvents)
		sleep = w.calculateSleep(numEvents, sleep)

		select {
		case <-ctx.Done():
			return
		case <-time.After(sleep):
		}
	}
}

// sendLoop reads reserved events from jobs channel, publishes them to Kafka and reports results.
func (w *OutboxProcessor) sendLoop(
	ctx context.Context,
	wg *sync.WaitGroup,
	jobs <-chan outboxProcessorJob,
	results chan<- outboxProcessorRes,
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
		if sendErr != nil {
			w.log.WithError(sendErr).Errorf(
				"failed to send event id=%s, topic=%s, event_type%s, attempts+%v",
				ev.EventID, ev.Topic, ev.Type, ev.Attempts,
			)
			// if sending failed, report error back to results channel, so event will be delayed for future processing
			results <- outboxProcessorRes{
				eventID:     ev.EventID,
				err:         sendErr,
				nextAttempt: w.nextAttemptAt(ev.Attempts),
				processedAt: time.Now().UTC(),
			}
			continue
		}

		// if sending succeeded, report success back to results channel, so event will be committed as sent
		w.log.Debugf("sent event id=%s", ev.EventID)
		results <- outboxProcessorRes{
			eventID:     ev.EventID,
			processedAt: time.Now().UTC()}
	}
}

// processBatch reserves up to batchSize pending events for this processor,
// sends them via send loops, and then commits (sent) or delays (retry later) each event.
func (w *OutboxProcessor) processBatch(
	ctx context.Context,
	id string,
	BatchSize int,
	jobs chan<- outboxProcessorJob,
	results <-chan outboxProcessorRes,
) int {
	// reserve a batch of pending events for this processor
	events, err := w.box.ReserveOutboxEvents(ctx, id, BatchSize)
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
			return len(events)
			// send reserved events to send loops via jobs channel, so
			// they will be processed and then committed or delayed
		case jobs <- outboxProcessorJob{ev: ev}:
		}
	}

	commit := make(map[uuid.UUID]CommitOutboxEventParams, len(events))
	pending := make(map[uuid.UUID]DelayOutboxEventData, len(events))

	for i := 0; i < len(events); i++ {
		select {
		case <-ctx.Done():
			return len(events)
		case r := <-results:
			if r.err != nil {
				pending[r.eventID] = DelayOutboxEventData{
					NextAttemptAt: r.nextAttempt,
					Reason:        r.err.Error(),
				}
			} else {
				commit[r.eventID] = CommitOutboxEventParams{SentAt: r.processedAt}
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

	return len(events)
}

// nextAttemptAt calculates when next attempt to process a failed event based on the number of attempts.
func (w *OutboxProcessor) nextAttemptAt(attempts int) time.Time {
	res := time.Second * time.Duration(30*attempts)
	if res < w.config.MinNextAttempt {
		return time.Now().UTC().Add(w.config.MinNextAttempt)
	}
	if res > w.config.MaxNextAttempt {
		return time.Now().UTC().Add(w.config.MaxNextAttempt)
	}

	return time.Now().UTC().Add(res)
}

// calculateBatch adjusts the next batch size based on how many events were processed last time.
func (w *OutboxProcessor) calculateBatch(
	numEvents int,
) int {
	minBatch := w.config.MinBatchSize
	maxBatch := w.config.MaxBatchSize
	if maxBatch == 0 {
		maxBatch = 100
	}

	var batch int
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
func (w *OutboxProcessor) calculateSleep(
	numEvents int,
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

	case numEvents >= w.config.MaxBatchSize:
		sleep = 0

	default:
		fill := float64(numEvents) / float64(w.config.MaxBatchSize)

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

// StopProcess performs a best-effort cleanup for the given processID.
//
// It is intended to be called by the owner of the process on shutdown.
// Typical implementation releases events reserved by this process back to pending,
// so they can be picked up by other processes.
//
// StopProcess does not stop goroutines started by StartProcess.
// Stopping is controlled via ctx cancellation passed to StartProcess.
func (w *OutboxProcessor) StopProcess(ctx context.Context, id string) error {
	err := w.box.CleanProcessingOutboxEventForProcessor(ctx, id)
	if err != nil {
		w.log.WithError(err).Error("failed to clean processing events for processor")
		return err
	}

	return nil
}

// CleanOutboxProcessing performs storage-level cleanup of processing events.
//
// WARNING:
//   - This is a maintenance operation.
//   - In multi-instance setups it may affect events reserved by other processes,
//     depending on the storage implementation.
//
// Prefer CleanOutboxProcessingForProcessID when you need to clean only one process.
func (w *OutboxProcessor) CleanOutboxProcessing(ctx context.Context) error {
	err := w.box.CleanProcessingOutboxEvent(ctx)
	if err != nil {
		w.log.WithError(err).Error("failed to clean processing events")
		return err
	}

	return nil
}

// CleanOutboxProcessingForProcessID performs storage-level cleanup of processing events
// reserved by the specified processID.
func (w *OutboxProcessor) CleanOutboxProcessingForProcessID(ctx context.Context, id string) error {

	err := w.box.CleanProcessingOutboxEventForProcessor(ctx, id)
	if err != nil {
		w.log.WithError(err).Error("failed to clean processing events for processor")
		return err
	}

	return nil
}

// CleanOutboxFailed performs storage-level cleanup of failed events.
//
// WARNING: This is a maintenance operation and its exact behavior depends on storage implementation.
func (w *OutboxProcessor) CleanOutboxFailed(ctx context.Context) error {

	err := w.box.CleanFailedOutboxEvent(ctx)
	if err != nil {
		w.log.WithError(err).Error("failed to clean failed events")
		return err
	}

	return nil
}
