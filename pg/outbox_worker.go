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

	DefaultOutboxWorkerMinSleep = 100 * time.Millisecond
	DefaultOutboxWorkerMaxSleep = 5 * time.Second

	DefaultOutboxWorkerMinBatchSize = 10
	DefaultOutboxWorkerMaxBatchSize = 100

	DefaultOutboxWorkerMinNextAttempt = time.Minute
	DefaultOutboxWorkerMaxNextAttempt = 10 * time.Minute
)

// OutboxWorkerConfig configures OutboxWorker behavior.
type OutboxWorkerConfig struct {
	// Routines is the maximum number of parallel send loops.
	// If 0, it defaults to DefaultOutboxWorkerRoutines.
	Routines int

	// MinSleep is the minimum delay between iterations when there are few/no events.
	// If 0, it defaults to DefaultOutboxWorkerMinSleep.
	MinSleep time.Duration
	// MaxSleep is the maximum delay between iterations during long idle periods.
	// If 0, it defaults to DefaultOutboxWorkerMaxSleep.
	MaxSleep time.Duration

	// MinBatch is the minimum number of events reserved per batch.
	// If 0, it defaults to DefaultOutboxWorkerMinBatchSize.
	MinBatch int
	// MaxBatch is the maximum number of events reserved per batch.
	// If 0, it defaults to DefaultOutboxWorkerMaxBatchSize.
	MaxBatch int

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
	log *logium.Entry,
	db *pgdbx.DB,
	writer *kafka.Writer,
	id string,
	config OutboxWorkerConfig,
) eventbox.OutboxWorker {
	if config.Routines <= 0 {
		config.Routines = DefaultOutboxWorkerRoutines
	}
	if config.MinSleep <= 0 {
		config.MinSleep = DefaultOutboxWorkerMinSleep
	}
	if config.MaxSleep <= 0 {
		config.MaxSleep = DefaultOutboxWorkerMaxSleep
	}
	if config.MinBatch <= 0 {
		config.MinBatch = DefaultOutboxWorkerMinBatchSize
	}
	if config.MaxBatch <= 0 {
		config.MaxBatch = DefaultOutboxWorkerMaxBatchSize
	}
	if config.MaxBatch < config.MinBatch {
		config.MaxBatch = config.MinBatch
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
		log:    log.WithField("component", "outbox-worker").WithField("worker_id", id),
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

func (p *OutboxWorker) Run(ctx context.Context) {
	p.log.Info("starting outbox worker")

	batch := p.config.MinBatch
	sleep := p.config.MinSleep

	maxParallel := p.config.Routines
	if maxParallel <= 0 {
		maxParallel = 1
	}

	jobs := make(chan outboxWorkerJob, maxParallel)
	results := make(chan outboxWorkerRes, maxParallel)

	var wg sync.WaitGroup
	wg.Add(maxParallel)

	for i := 0; i < maxParallel; i++ {
		go p.sendLoop(ctx, &wg, jobs, results)
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

		numEvents := p.processBatch(ctx, batch, jobs, results)

		batch = calculateBatch(numEvents, p.config.MinBatch, p.config.MaxBatch)

		sleep = p.sleep(ctx, numEvents, sleep)
	}
}

func (p *OutboxWorker) sendLoop(
	ctx context.Context,
	wg *sync.WaitGroup,
	jobs <-chan outboxWorkerJob,
	results chan<- outboxWorkerRes,
) {
	defer wg.Done()

	for job := range jobs {
		outboxEvent := job.event
		msg := outboxEvent.ToKafkaMessage()

		err := p.writer.WriteMessages(ctx, msg)
		if err != nil {
			p.log.WithError(err).
				WithFields(logfields.FromOutboxEvent(outboxEvent)).
				Errorf("failed to send outbox event to Kafka")

			_ = sendOutboxResult(ctx, results, outboxWorkerRes{
				event:       outboxEvent,
				err:         err,
				processedAt: time.Now().UTC(),
			})
			continue
		}

		_ = sendOutboxResult(ctx, results, outboxWorkerRes{
			event:       outboxEvent,
			err:         nil,
			processedAt: time.Now().UTC(),
		})
	}
}

func (p *OutboxWorker) processBatch(
	ctx context.Context,
	batch int,
	jobs chan<- outboxWorkerJob,
	results <-chan outboxWorkerRes,
) int {
	events, err := p.box.ReserveOutboxEvents(ctx, p.id, batch)
	if err != nil {
		p.log.WithError(err).Error("failed to reserve outbox events")

		return 0
	}
	if len(events) == 0 {
		return 0
	}

	for _, ev := range events {
		if !sendOutboxJob(ctx, jobs, outboxWorkerJob{event: ev}) {
			return len(events)
		}
	}

	commit := make(map[uuid.UUID]CommitOutboxEventParams, len(events))
	pending := make(map[uuid.UUID]DelayOutboxEventData, len(events))
	failed := make(map[uuid.UUID]FailedOutboxEventData, len(events))

	for i := 0; i < len(events); i++ {
		r, ok := getOutboxResult(ctx, results)
		if !ok {
			return len(events)
		}

		entry := p.log.WithFields(logfields.FromOutboxEvent(r.event))

		if r.err != nil {
			if p.config.MaxAttempts != 0 && r.event.Attempts+1 >= p.config.MaxAttempts {
				failed[r.event.EventID] = FailedOutboxEventData{
					LastAttemptAt: r.processedAt,
					Reason:        r.err.Error(),
				}

				entry.Errorf("event marked as failed after reaching max attempts")
				continue
			}

			pending[r.event.EventID] = DelayOutboxEventData{
				NextAttemptAt: p.nextAttemptAt(r.event.Attempts + 1),
				Reason:        r.err.Error(),
			}

			entry.Warnf("event will be delayed for future processing after failed attempt")
			continue
		}

		commit[r.event.EventID] = CommitOutboxEventParams{SentAt: r.processedAt}
	}

	if len(commit) > 0 {
		if err = p.box.CommitOutboxEvents(ctx, p.id, commit); err != nil {
			p.log.WithError(err).Error("failed to mark events as sent")
		}
	}

	if len(pending) > 0 {
		if err = p.box.DelayOutboxEvents(ctx, p.id, pending); err != nil {
			p.log.WithError(err).Error("failed to delay events")
		}
	}

	if len(failed) > 0 {
		if err = p.box.FailedOutboxEvents(ctx, p.id, failed); err != nil {
			p.log.WithError(err).Error("failed to mark events as failed")
		}
	}

	return len(events)
}

func (p *OutboxWorker) nextAttemptAt(attempts int32) time.Time {
	res := time.Second * time.Duration(30*attempts)
	if res < p.config.MinNextAttempt {
		return time.Now().UTC().Add(p.config.MinNextAttempt)
	}
	if res > p.config.MaxNextAttempt {
		return time.Now().UTC().Add(p.config.MaxNextAttempt)
	}

	return time.Now().UTC().Add(res)
}

func (p *OutboxWorker) sleep(
	ctx context.Context,
	numEvents int,
	lastSleep time.Duration,
) time.Duration {
	sleep := calculateSleep(numEvents, p.config.MaxBatch, lastSleep, p.config.MinSleep, p.config.MaxSleep)

	t := time.NewTimer(sleep)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return sleep
	case <-t.C:
		return sleep
	}
}

func (p *OutboxWorker) Stop(ctx context.Context) error {
	err := p.box.CleanProcessingOutboxEvent(ctx, p.id)
	if err != nil {
		p.log.WithError(err).Error("failed to clean processing events for worker")
		return err
	}

	return nil
}
