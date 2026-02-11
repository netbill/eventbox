package pg

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/logium"
	"github.com/netbill/msnger"
	"github.com/netbill/msnger/logfields"
	"github.com/netbill/pgdbx"
	"github.com/segmentio/kafka-go"
)

const (
	DefaultOutboxProcessorRoutines = 10

	DefaultOutboxProcessorMinSleep = 100 * time.Millisecond
	DefaultOutboxProcessorMaxSleep = 5 * time.Second

	DefaultOutboxProcessorMinBatchSize = 10
	DefaultOutboxProcessorMaxBatchSize = 100

	DefaultOutboxProcessorMinNextAttempt = time.Minute
	DefaultOutboxProcessorMaxNextAttempt = 10 * time.Minute
)

// OutboxProcessorConfig configures OutboxProcessor behavior.
type OutboxProcessorConfig struct {
	// Routines is the maximum number of parallel send loops.
	// If 0, it defaults to DefaultOutboxProcessorRoutines.
	Routines int

	// MinSleep is the minimum delay between iterations when there are few/no events.
	// If 0, it defaults to DefaultOutboxProcessorMinSleep.
	MinSleep time.Duration
	// MaxSleep is the maximum delay between iterations during long idle periods.
	// If 0, it defaults to DefaultOutboxProcessorMaxSleep.
	MaxSleep time.Duration

	// MinBatch is the minimum number of events reserved per batch.
	// If 0, it defaults to DefaultOutboxProcessorMinBatchSize.
	MinBatch int
	// MaxBatch is the maximum number of events reserved per batch.
	// If 0, it defaults to DefaultOutboxProcessorMaxBatchSize.
	MaxBatch int

	// MinNextAttempt is the minimum delay before next attempt to process failed event.
	// If 0, it defaults to DefaultOutboxProcessorMinNextAttempt.
	MinNextAttempt time.Duration
	// MaxNextAttempt is the maximum delay before next attempt to process failed event.
	// If 0, it defaults to DefaultOutboxProcessorMaxNextAttempt.
	MaxNextAttempt time.Duration

	//MaxAttempts is the maximum number of attempts to process an event before it is marked as failed.
	// If 0, it defaults to 5.
	MaxAttempts int32
}

// OutboxProcessor reads pending outbox events from storage and publishes them to Kafka.
//
// Semantics:
//   - At-least-once delivery: duplicates are possible. Consumers must be idempotent.
//   - A processor runs as a long-living process identified by processID.
//   - StartProcess does NOT automatically release/clean events reserved by this process.
//     The caller must decide what to do on shutdown (e.g. call StopProcess or a maintenance cleanup).
type OutboxProcessor struct {
	log    *logium.Entry
	config OutboxProcessorConfig

	// box provides outbox storage operations (reserve/commit/delay/cleanup).
	box outbox

	// writer publishes messages to Kafka.
	writer *kafka.Writer
}

// NewOutboxProcessor creates a new OutboxProcessor.
func NewOutboxProcessor(
	log *logium.Entry,
	cfg OutboxProcessorConfig,
	db *pgdbx.DB,
	writer *kafka.Writer,
) *OutboxProcessor {
	if cfg.Routines < 0 {
		cfg.Routines = DefaultOutboxProcessorRoutines
	}
	if cfg.MinSleep <= 0 {
		cfg.MinSleep = DefaultOutboxProcessorMinSleep
	}
	if cfg.MaxSleep <= 0 {
		cfg.MaxSleep = DefaultOutboxProcessorMaxSleep
	}
	if cfg.MinBatch <= 0 {
		cfg.MinBatch = DefaultOutboxProcessorMinBatchSize
	}
	if cfg.MaxBatch < cfg.MinBatch {
		cfg.MaxBatch = DefaultOutboxProcessorMaxBatchSize
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
		box:    outbox{db: db},
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
	topic       string
	key         string
	eType       string
	err         error
	attempts    int32
	nextAttempt time.Time
	processedAt time.Time
}

// RunProcess starts the outbox processing loop for the given process ID.
func (w *OutboxProcessor) RunProcess(ctx context.Context, id string) {
	BatchSize := w.config.MinBatch
	sleep := w.config.MinSleep

	maxParallel := w.config.Routines
	if maxParallel <= 0 {
		maxParallel = 1
	}

	jobs := make(chan outboxProcessorJob, maxParallel)
	results := make(chan outboxProcessorRes, maxParallel)

	var wg sync.WaitGroup
	wg.Add(maxParallel)

	for i := 0; i < maxParallel; i++ {
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

// sendLoop is a worker loop that sends outbox events to Kafka and reports results.
func (w *OutboxProcessor) sendLoop(
	ctx context.Context,
	wg *sync.WaitGroup,
	jobs <-chan outboxProcessorJob,
	results chan<- outboxProcessorRes,
) {
	defer wg.Done()

	for job := range jobs {
		if ctx.Err() != nil {
			return
		}

		ev := job.ev
		msg := ev.ToKafkaMessage()

		sendErr := w.writer.WriteMessages(ctx, msg)
		if sendErr != nil {
			w.log.WithError(sendErr).
				WithField(logfields.ProcessID, ev.EventID).
				WithFields(logfields.FromOutboxEvent(ev)).
				Errorf("failed to send outbox event to Kafka")

			results <- outboxProcessorRes{
				eventID:     ev.EventID,
				topic:       ev.Topic,
				key:         ev.Key,
				eType:       ev.Type,
				err:         sendErr,
				attempts:    ev.Attempts + 1,
				nextAttempt: w.nextAttemptAt(ev.Attempts),
				processedAt: time.Now().UTC(),
			}
			continue
		}

		results <- outboxProcessorRes{
			eventID:     ev.EventID,
			topic:       ev.Topic,
			key:         ev.Key,
			eType:       ev.Type,
			err:         nil,
			attempts:    ev.Attempts + 1,
			processedAt: time.Now().UTC()}
	}
}

// processBatch reserves a batch of events, sends them to workers via jobs channel, and collects results.
func (w *OutboxProcessor) processBatch(
	ctx context.Context,
	processID string,
	BatchSize int,
	jobs chan<- outboxProcessorJob,
	results <-chan outboxProcessorRes,
) int {
	events, err := w.box.ReserveOutboxEvents(ctx, processID, BatchSize)
	if err != nil {
		w.log.WithError(err).
			WithField(logfields.ProcessID, processID).
			Error("failed to reserve outbox events")

		return 0
	}
	if len(events) == 0 {
		return 0
	}

	for _, ev := range events {
		select {
		case <-ctx.Done():
			return len(events)
		case jobs <- outboxProcessorJob{ev: ev}:
		}
	}

	commit := make(map[uuid.UUID]CommitOutboxEventParams, len(events))
	pending := make(map[uuid.UUID]DelayOutboxEventData, len(events))
	failed := make(map[uuid.UUID]FailedOutboxEventData, len(events))

	for i := 0; i < len(events); i++ {
		select {
		case <-ctx.Done():
			return len(events)
		case r := <-results:
			if r.err != nil {
				if r.attempts != 0 && r.attempts >= w.config.MaxAttempts {
					failed[r.eventID] = FailedOutboxEventData{
						LastAttemptAt: r.processedAt,
						Reason:        r.err.Error(),
					}

					w.log.WithFields(logium.Fields{
						logfields.ProcessID:         processID,
						logfields.EventIDFiled:      r.eventID,
						logfields.EventTopicFiled:   r.topic,
						logfields.EventTypeFiled:    r.eType,
						logfields.EventAttemptFiled: r.attempts,
					}).Errorf("event marked as failed after reaching max attempts")

					continue
				}

				pending[r.eventID] = DelayOutboxEventData{
					NextAttemptAt: r.nextAttempt,
					Reason:        r.err.Error(),
				}

				w.log.WithFields(logium.Fields{
					logfields.ProcessID:         processID,
					logfields.EventIDFiled:      r.eventID,
					logfields.EventTopicFiled:   r.topic,
					logfields.EventTypeFiled:    r.eType,
					logfields.EventAttemptFiled: r.attempts,
				}).Warnf("event will be delayed for future processing after failed attempt")
			} else {
				commit[r.eventID] = CommitOutboxEventParams{SentAt: r.processedAt}
			}
		}
	}

	if len(commit) > 0 {
		// if sending succeeded, commit events as sent, so they won't be processed again
		if err = w.box.CommitOutboxEvents(ctx, processID, commit); err != nil {
			w.log.WithError(err).
				WithField(logfields.ProcessID, processID).
				Error("failed to mark events as sent")
		}
	}

	if len(pending) > 0 {
		// if sending failed, delay events for future processing, so they will be retried later
		if err = w.box.DelayOutboxEvents(ctx, processID, pending); err != nil {
			w.log.WithError(err).
				WithField(logfields.ProcessID, processID).
				Error("failed to delay events")
		}
	}

	return len(events)
}

// nextAttemsptAt calculates when next attempt to process a failed event based on the number of attempts.
func (w *OutboxProcessor) nextAttemptAt(attempts int32) time.Time {
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
	minBatch := w.config.MinBatch
	maxBatch := w.config.MaxBatch
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

	case numEvents >= w.config.MaxBatch:
		sleep = 0

	default:
		fill := float64(numEvents) / float64(w.config.MaxBatch)

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

// StopProcess stops the outbox processing for the given process ID by cleaning up any reserved events.
func (w *OutboxProcessor) StopProcess(ctx context.Context, processID string) error {
	err := w.box.CleanProcessingOutboxEventForProcessor(ctx, processID)
	if err != nil {
		w.log.WithError(err).
			WithField(logfields.ProcessID, processID).
			Error("failed to clean processing events for processor")
		return err
	}

	return nil
}

// CleanOutboxProcessing cleans up all reserved events, making them available for processing again.
func (w *OutboxProcessor) CleanOutboxProcessing(ctx context.Context) error {
	err := w.box.CleanProcessingOutboxEvent(ctx)
	if err != nil {
		w.log.WithError(err).Error("failed to clean processing events")
		return err
	}

	return nil
}

// CleanOutboxProcessingForProcessID cleans up reserved events for a specific process ID,
// making them available for processing again.
func (w *OutboxProcessor) CleanOutboxProcessingForProcessID(ctx context.Context, processID string) error {
	err := w.box.CleanProcessingOutboxEventForProcessor(ctx, processID)
	if err != nil {
		w.log.WithError(err).
			WithField(logfields.ProcessID, processID).
			Error("failed to clean processing events for processor")
		return err
	}

	return nil
}

// CleanOutboxFailed cleans up all failed events, making them available for processing again.
func (w *OutboxProcessor) CleanOutboxFailed(ctx context.Context) error {
	err := w.box.CleanFailedOutboxEvent(ctx)
	if err != nil {
		w.log.WithError(err).Error("failed to clean failed events")
		return err
	}

	return nil
}
