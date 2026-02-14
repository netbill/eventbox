package pg

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/eventbox"
	"github.com/netbill/eventbox/logfields"
	"github.com/netbill/logium"
	"github.com/netbill/pgdbx"
	"github.com/segmentio/kafka-go"
)

const (
	DefaultOutboxWorkerRoutines = 10

	DefaultOutboxWorkerSleep = 200 * time.Millisecond

	DefaultOutboxWorkerBatch = 100

	DefaultOutboxWorkerMinNextAttempt = time.Minute
	DefaultOutboxWorkerMaxNextAttempt = 10 * time.Minute
)

// OutboxWorkerConfig configures OutboxWorker behavior.
type OutboxWorkerConfig struct {
	// Routines is the maximum number of parallel send loops.
	// If 0, it defaults to DefaultOutboxWorkerRoutines.
	Routines int

	// Slots is the maximum number of in-flight processing events.
	// If 0, it defaults to Routines * 4.
	Slots int

	// Sleep is the duration to sleep when there are no events to process or when all processing slots are occupied.
	// If 0, it defaults to DefaultOutboxWorkerSleep.
	Sleep time.Duration

	// BatchSize is the number of events to reserve in one batch.
	// If 0, it defaults to DefaultOutboxWorkerBatch.
	BatchSize int

	// MinNextAttempt is the minimum delay before next attempt to process failed event.
	// If 0, it defaults to DefaultOutboxWorkerMinNextAttempt.
	MinNextAttempt time.Duration
	// MaxNextAttempt is the maximum delay before next attempt to process failed event.
	// If 0, it defaults to DefaultOutboxWorkerMaxNextAttempt.
	MaxNextAttempt time.Duration

	//MaxAttempts is the maximum number of attempts to process an event before it is marked as failed.
	// If 0, it defaults to 5.
	MaxAttempts int32
}

type OutboxWorker struct {
	id     string
	log    *logium.Entry
	writer *kafka.Writer

	box    outbox
	config OutboxWorkerConfig
}

func NewOutboxWorker(
	id string,
	log *logium.Entry,
	db *pgdbx.DB,
	writer *kafka.Writer,
	config OutboxWorkerConfig,
) eventbox.OutboxWorker {
	if config.Routines <= 0 {
		config.Routines = DefaultOutboxWorkerRoutines
	}
	if config.Slots <= 0 {
		config.Slots = config.Routines * 4
	}
	if config.Sleep <= 0 {
		config.Sleep = DefaultOutboxWorkerSleep
	}
	if config.BatchSize <= 0 {
		config.BatchSize = DefaultOutboxWorkerBatch
	}
	if config.MinNextAttempt <= 0 {
		config.MinNextAttempt = DefaultOutboxWorkerMinNextAttempt
	}
	if config.MaxNextAttempt <= 0 {
		config.MaxNextAttempt = DefaultOutboxWorkerMaxNextAttempt
	}
	if config.MaxNextAttempt < config.MinNextAttempt {
		config.MaxNextAttempt = config.MinNextAttempt
	}

	w := &OutboxWorker{
		id:     id,
		log:    log.WithField("worker_id", id),
		config: config,
		box:    outbox{db: db},
		writer: writer,
	}

	return w
}

type outboxWorkerJob struct {
	event eventbox.OutboxEvent
}

func sendOutboxJob(ctx context.Context, jobs chan<- outboxWorkerJob, job outboxWorkerJob) bool {
	select {
	case <-ctx.Done():
		return false
	case jobs <- job:
		return true
	}
}

type outboxWorkerRes struct {
	event eventbox.OutboxEvent
	err   error

	processedAt time.Time
}

func sendOutboxResult(ctx context.Context, results chan<- outboxWorkerRes, res outboxWorkerRes) bool {
	select {
	case <-ctx.Done():
		return false
	case results <- res:
		return true
	}
}

func getOutboxResult(ctx context.Context, results <-chan outboxWorkerRes) (outboxWorkerRes, bool) {
	select {
	case <-ctx.Done():
		return outboxWorkerRes{}, false
	case r, ok := <-results:
		return r, ok
	}
}

func (w *OutboxWorker) Run(ctx context.Context) {
	w.log.Info("starting outbox worker")

	jobs := make(chan outboxWorkerJob, w.config.Slots)
	results := make(chan outboxWorkerRes, w.config.Slots)

	var wg sync.WaitGroup
	wg.Add(w.config.Routines)

	for i := 0; i < w.config.Routines; i++ {
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

		processed := w.processBatch(ctx, w.config.BatchSize, jobs, results)

		if processed == 0 {
			w.sleep(ctx)
		}
	}
}

func (w *OutboxWorker) sendLoop(
	ctx context.Context,
	wg *sync.WaitGroup,
	jobs <-chan outboxWorkerJob,
	results chan<- outboxWorkerRes,
) {
	defer wg.Done()

	for job := range jobs {
		outboxEvent := job.event
		msg := outboxEvent.ToKafkaMessage()

		entry := w.log.WithFields(logfields.FromOutboxEvent(outboxEvent))

		err := w.writer.WriteMessages(ctx, msg)
		if err != nil {
			entry.WithError(err).Error("failed to send outbox event")

			_ = sendOutboxResult(ctx, results, outboxWorkerRes{
				event:       outboxEvent,
				err:         err,
				processedAt: time.Now().UTC(),
			})
		} else {
			entry.Debug("outbox event sent successfully")

			_ = sendOutboxResult(ctx, results, outboxWorkerRes{
				event:       outboxEvent,
				err:         nil,
				processedAt: time.Now().UTC(),
			})
		}
	}
}

func (w *OutboxWorker) processBatch(
	ctx context.Context,
	batch int,
	jobs chan<- outboxWorkerJob,
	results <-chan outboxWorkerRes,
) int {
	events, err := w.box.ReserveOutboxEvents(ctx, w.id, batch)
	if err != nil {
		w.log.WithError(err).Error("failed to reserve outbox events")

		return 0
	}
	if len(events) == 0 {
		return 0
	}

	for _, ev := range events {
		if !sendOutboxJob(ctx, jobs, outboxWorkerJob{event: ev}) {
			return 0
		}
	}

	commit := make(map[uuid.UUID]CommitOutboxEventParams, len(events))
	pending := make(map[uuid.UUID]DelayOutboxEventData, len(events))
	failed := make(map[uuid.UUID]FailedOutboxEventData, len(events))

	for i := 0; i < len(events); i++ {
		r, ok := getOutboxResult(ctx, results)
		if !ok {
			return 0
		}

		entry := w.log.WithFields(logfields.FromOutboxEvent(r.event))

		if r.err != nil {
			if w.config.MaxAttempts != 0 && r.event.Attempts+1 >= w.config.MaxAttempts {
				failed[r.event.EventID] = FailedOutboxEventData{
					LastAttemptAt: r.processedAt,
					Reason:        r.err.Error(),
				}

				entry.Error("event marked as failed after reaching max attempts")
			} else {
				pending[r.event.EventID] = DelayOutboxEventData{
					NextAttemptAt: w.nextAttemptAt(r.event.Attempts + 1),
					Reason:        r.err.Error(),
				}

				entry.Warn("event will be delayed for future processing after failed attempt")
			}
		}

		commit[r.event.EventID] = CommitOutboxEventParams{SentAt: r.processedAt}
	}

	if len(commit) > 0 {
		if err = w.box.CommitOutboxEvents(ctx, w.id, commit); err != nil {
			w.log.WithError(err).Error("failed to mark events as sent")
		}

		w.log.WithField("count", len(commit)).Debug("events marked as sent")
	}

	if len(pending) > 0 {
		if err = w.box.DelayOutboxEvents(ctx, w.id, pending); err != nil {
			w.log.WithError(err).Error("failed to delay events")
		}

		w.log.WithField("count", len(pending)).Debug("events delayed for future processing")
	}

	if len(failed) > 0 {
		if err = w.box.FailedOutboxEvents(ctx, w.id, failed); err != nil {
			w.log.WithError(err).Error("failed to mark events as failed")
		}

		w.log.WithField("count", len(failed)).Debug("events marked as failed after reaching max attempts")
	}

	return len(events)
}

func (w *OutboxWorker) nextAttemptAt(attempts int32) time.Time {
	res := time.Second * time.Duration(30*attempts)
	if res < w.config.MinNextAttempt {
		return time.Now().UTC().Add(w.config.MinNextAttempt)
	}
	if res > w.config.MaxNextAttempt {
		return time.Now().UTC().Add(w.config.MaxNextAttempt)
	}

	return time.Now().UTC().Add(res)
}

func (w *OutboxWorker) sleep(
	ctx context.Context,
) {
	t := time.NewTimer(w.config.Sleep)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return
	case <-t.C:
		return
	}
}

func (w *OutboxWorker) Stop(ctx context.Context) error {
	err := w.box.CleanProcessingOutboxEvent(ctx, w.id)
	if err != nil {
		w.log.WithError(err).Error("failed to clean processing events for worker")
		return err
	}

	return nil
}
