package pg

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/netbill/msnger"
	"github.com/segmentio/kafka-go"

	"github.com/netbill/logium"
)

const (
	DefaultInboxWorkerMaxRutin = 10

	DefaultInboxWorkerMaxSleep = 5 * time.Second
	DefaultInboxWorkerMinSleep = 50 * time.Millisecond

	DefaultInboxWorkerMaxBatch = 100
	DefaultInboxWorkerMinBatch = 10

	DefaultInboxWorkerMinNextAttempt = time.Minute
	DefaultInboxWorkerMaxNextAttempt = 10 * time.Minute
)

// InboxWorkerConfig defines configuration for InboxWorker.
type InboxWorkerConfig struct {
	// MaxRutin is the maximum number of parallel handle loops.
	// If 0, it defaults to DefaultInboxWorkerMaxRutin.
	MaxRutin int
	// InFlight is the maximum number of events being processed in parallel.
	// If 0, it defaults to MaxRutin * 4.
	InFlight int

	// MinSleep is the minimum delay between iterations when there are few/no events.
	// If 0, it defaults to DefaultInboxWorkerMinSleep.
	MinSleep time.Duration
	// MaxSleep is the maximum delay between iterations during long idle periods.
	// If 0, it defaults to DefaultInboxWorkerMaxSleep.
	MaxSleep time.Duration

	// MinBatch is the minimum number of events reserved per batch.
	// If 0, it defaults to DefaultInboxWorkerMinBatch.
	MinBatch int
	// MaxBatch is the maximum number of events reserved per batch.
	// If 0, it defaults to DefaultInboxWorkerMaxBatch.
	MaxBatch int

	// MinNextAttempt is the minimum delay before next attempt to process failed event.
	// If 0, it defaults to DefaultInboxWorkerMinNextAttempt.
	MinNextAttempt time.Duration
	// MaxNextAttempt is the maximum delay before next attempt to process failed event.
	// If 0, it defaults to DefaultInboxWorkerMaxNextAttempt.
	MaxNextAttempt time.Duration

	// MaxAttempts is the maximum number of attempts to process event before marking it as failed.
	// If 0, the event will always receive the status InboxEventStatusPending in case of failure of processing.
	MaxAttempts int
}

// InboxWorker reads pending events from inbox storage and processes them using registered handlers.
type InboxWorker struct {
	log    *logium.Logger
	config InboxWorkerConfig

	box   inbox
	route map[string]msnger.InHandlerFunc
}

// NewInboxWorker creates a new InboxWorker.
func NewInboxWorker(log *logium.Logger, box inbox, config InboxWorkerConfig) msnger.InboxWorker {
	if config.MaxRutin <= 0 {
		config.MaxRutin = DefaultInboxWorkerMaxRutin
	}
	if config.InFlight <= 0 {
		config.InFlight = config.MaxRutin * 4
	}
	if config.MinSleep <= 0 {
		config.MinSleep = DefaultInboxWorkerMinSleep
	}
	if config.MaxSleep <= 0 {
		config.MaxSleep = DefaultInboxWorkerMaxSleep
	}
	if config.MinBatch <= 0 {
		config.MinBatch = DefaultInboxWorkerMinBatch
	}
	if config.MaxBatch <= 0 {
		config.MaxBatch = DefaultInboxWorkerMaxBatch
	}
	if config.MinNextAttempt <= 0 {
		config.MinNextAttempt = DefaultInboxWorkerMinNextAttempt
	}
	if config.MaxNextAttempt <= 0 {
		config.MaxNextAttempt = DefaultInboxWorkerMaxNextAttempt
	}

	return &InboxWorker{
		log:    log,
		box:    box,
		config: config,
		route:  make(map[string]msnger.InHandlerFunc),
	}
}

// inboxWorkerSlot is a slot for processing one event. It is used to limit the number of events being processed in parallel.
type inboxWorkerSlot struct{}

// takeSlot tries to take a slot for processing an event. It returns false if the context is done.
func takeSlot(ctx context.Context, slots <-chan inboxWorkerSlot) bool {
	select {
	case <-ctx.Done():
		return false
	case <-slots:
		return true
	}
}

// inboxWorkerJob defines job for processing inbox event.
type inboxWorkerJob struct {
	event InboxEvent
}

// sendJob sends a job for processing an event or returns false if context is done.
func sendJob(ctx context.Context, jobs chan<- inboxWorkerJob, job inboxWorkerJob) bool {
	select {
	case <-ctx.Done():
		return false
	case jobs <- job:
		return true
	}
}

// giveSlot gives back a slot after processing an event.
func giveSlot(slots chan<- inboxWorkerSlot) {
	select {
	case slots <- inboxWorkerSlot{}:
	default:
	}
}

// Route registers a handler for the given event type.
// It panics if a handler for this event type is already registered.
func (w *InboxWorker) Route(eventType string, handler msnger.InHandlerFunc) {
	if _, ok := w.route[eventType]; ok {
		panic(fmt.Errorf("double handler for event type=%s", eventType))
	}

	w.route[eventType] = handler
}

// Run starts the worker loop. It reserves pending events for this worker, processes them using registered handlers,
// - "workerID" is used to reserve events for this worker and should be unique among running workers,
// otherwise they will compete for the same events and may cause duplicate processing.
//
// Flow:
//  1. feederLoop reserves pending events for this worker and sends them to handleLoop via jobs channel.
//  2. handleLoop receives events from jobs channel, processes them using registered handlers,
//     and then commits (processed) or delays (retry later) each event.
//  3. On exit, the worker tries to clean up any events reserved for this worker.
//
// The worker loop continues until the context is done. After that, it tries to clean up any events reserved for this worker.
func (w *InboxWorker) Run(ctx context.Context, workerID string) {
	defer func() {
		if err := w.box.CleanFailedInboxEventForWorker(context.Background(), workerID); err != nil {
			w.log.WithError(err).Error("failed to clean own failed events")
		}
	}()

	// create channels for processing events and limit the number of events being processed in parallel using slots channel
	slots := make(chan inboxWorkerSlot, w.config.InFlight)
	for i := 0; i < w.config.InFlight; i++ {
		slots <- inboxWorkerSlot{}
	}

	// create channel for sending jobs to handle loops
	jobs := make(chan inboxWorkerJob, w.config.InFlight)

	var wg sync.WaitGroup
	wg.Add(w.config.MaxRutin + 1)

	go func() {
		defer wg.Done()
		w.feederLoop(ctx, workerID, slots, jobs)
	}()

	for i := 0; i < w.config.MaxRutin; i++ {
		go func() {
			defer wg.Done()
			w.handleLoop(ctx, workerID, slots, jobs)
		}()
	}

	wg.Wait()
}

// feederLoop reserve events for worker by "workerID" and send
//
// Flow:
//  1. reserve events for "workerID"
//  2. if no events or failed to reserve, sleep for a while and try again
//  3. try to take a slot if success try to reserve events
//  4. if successful taken slot try to send job to handleLoop, if failed to send job, give back slot and try again
//
// The loop continues until the context is done. After that, it tries to clean up any events reserved for this worker.
func (w *InboxWorker) feederLoop(
	ctx context.Context,
	workerID string,
	slots chan inboxWorkerSlot,
	jobs chan<- inboxWorkerJob,
) {
	defer close(jobs)

	sleep := w.config.MinSleep
	maxSleep := w.config.MaxSleep

	for {
		free := len(slots)
		if free == 0 {
			if !sleepCtx(ctx, sleep) {
				return
			}
			continue
		}

		limit := free
		if limit > w.config.MaxBatch {
			limit = w.config.MaxBatch
		}
		if limit < w.config.MinBatch {
			limit = w.config.MinBatch
		}

		events, err := w.box.ReserveInboxEvents(ctx, workerID, limit)
		if err != nil {
			w.log.WithError(err).Error("failed to reserve inbox events")
			if !sleepCtx(ctx, sleep) {
				return
			}
			continue
		}

		if len(events) == 0 {
			next := sleep * 2
			if next > maxSleep {
				next = maxSleep
			}
			sleep = next
			if !sleepCtx(ctx, sleep) {
				return
			}

			continue
		}

		sleep = w.config.MinSleep

		for _, ev := range events {
			if !takeSlot(ctx, slots) {
				return
			}
			if !sendJob(ctx, jobs, inboxWorkerJob{event: ev}) {
				giveSlot(slots)
				return
			}
		}
	}
}

// handleLoop receives events from jobs channel, processes them using registered handlers,
// and then commits (processed) or delays (retry later) each event.
//
// Flow:
//  1. receive job from jobs channel
//  2. process event using registered handler
//  3. if processing is successful, commit event,
//     otherwise delay it for retry later or mark as failed if attempts exceeded
//
// The loop continues until the jobs channel is closed and all jobs are processed, or the context is done.
func (w *InboxWorker) handleLoop(
	ctx context.Context,
	workerID string,
	slots chan inboxWorkerSlot,
	jobs <-chan inboxWorkerJob,
) {
	for job := range jobs {
		ev := job.event

		err := w.box.Transaction(ctx, func(ctx context.Context) error {
			var err error

			hErr := w.handleEvent(ctx, ev.Type, ev.ToKafkaMessage())
			if hErr != nil {
				switch {
				case w.config.MaxAttempts != 0 && ev.Attempts >= w.config.MaxAttempts:
					ev, err = w.box.FailedInboxEvent(ctx, workerID, ev.EventID, hErr.Error())
					if err != nil {
						return fmt.Errorf("failed to mark inbox event as failed id=%s: %w", ev.EventID, err)
					}

					return nil
				default:
					ev, err = w.box.DelayInboxEvent(ctx, workerID, ev.EventID, w.nextAttemptAt(ev.Attempts), hErr.Error())
					if err != nil {
						return fmt.Errorf("failed to delay inbox event id=%s: %w", ev.EventID, err)
					}

					return nil
				}
			}

			ev, err = w.box.CommitInboxEvent(ctx, workerID, ev.EventID)
			if err != nil {
				return fmt.Errorf("failed to commit inbox event id=%s: %w", ev.EventID, err)
			}

			return nil
		})
		if err != nil {
			w.log.WithError(err).Errorf(
				"failed to process inbox event id=%s type=%s attempts=%d",
				ev.EventID, ev.Type, ev.Attempts,
			)
		}

		giveSlot(slots)

		if ctx.Err() != nil {
			return
		}
	}
}

// handleEvent processes the event using the registered handler for the event type.
// If there is no handler for the event type, it logs a warning and returns nil.
func (w *InboxWorker) handleEvent(
	ctx context.Context,
	eventType string,
	message kafka.Message,
) error {
	handler, ok := w.route[eventType]
	if !ok {
		w.log.Warnf(
			"onUnknown called for event topic=%s partition=%d offset=%d",
			message.Topic, message.Partition, message.Offset,
		)

		return nil
	}

	return handler(ctx, message)
}

// nextAttemptAt calculates the time when next attempt to process a failed event based on the number of attempts.
func (w *InboxWorker) nextAttemptAt(attempts int) time.Time {
	res := time.Second * time.Duration(30*attempts)
	if res < w.config.MinNextAttempt {
		return time.Now().UTC().Add(w.config.MinNextAttempt)
	}
	if res > w.config.MaxNextAttempt {
		return time.Now().UTC().Add(w.config.MaxNextAttempt)
	}

	return time.Now().UTC().Add(res)
}

// sleepCtx sleeps for the given duration or until the context is done, whichever comes first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}

	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// Shutdown tries to clean up any events reserved for this worker. It should be called after the context is done.
func (w *InboxWorker) Shutdown(ctx context.Context) error {
	err := w.box.CleanProcessingInboxEvent(ctx)
	if err != nil {
		w.log.WithError(err).Errorf("failed to clean processing inbox worker")
	}
	return err
}
