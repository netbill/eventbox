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
	DefaultInboxProcessorMaxRutin = 10

	DefaultInboxProcessorMaxSleep = 5 * time.Second
	DefaultInboxProcessorMinSleep = 50 * time.Millisecond

	DefaultInboxProcessorMaxBatch = 100
	DefaultInboxProcessorMinBatch = 10

	DefaultInboxProcessorMinNextAttempt = time.Minute
	DefaultInboxProcessorMaxNextAttempt = 10 * time.Minute
)

// InboxProcessorConfig defines configuration for InboxProcessor.
type InboxProcessorConfig struct {
	// MaxRutin is the maximum number of parallel handle loops.
	// If 0, it defaults to DefaultInboxProcessorMaxRutin.
	MaxRutin int
	// InFlight is the maximum number of events being processed in parallel.
	// If 0, it defaults to MaxRutin * 4.
	InFlight int

	// MinSleep is the minimum delay between iterations when there are few/no events.
	// If 0, it defaults to DefaultInboxProcessorMinSleep.
	MinSleep time.Duration
	// MaxSleep is the maximum delay between iterations during long idle periods.
	// If 0, it defaults to DefaultInboxProcessorMaxSleep.
	MaxSleep time.Duration

	// MinBatch is the minimum number of events reserved per batch.
	// If 0, it defaults to DefaultInboxProcessorMinBatch.
	MinBatch int
	// MaxBatch is the maximum number of events reserved per batch.
	// If 0, it defaults to DefaultInboxProcessorMaxBatch.
	MaxBatch int

	// MinNextAttempt is the minimum delay before next attempt to process failed event.
	// If 0, it defaults to DefaultInboxProcessorMinNextAttempt.
	MinNextAttempt time.Duration
	// MaxNextAttempt is the maximum delay before next attempt to process failed event.
	// If 0, it defaults to DefaultInboxProcessorMaxNextAttempt.
	MaxNextAttempt time.Duration

	// MaxAttempts is the maximum number of attempts to process event before marking it as failed.
	// If 0, the event will always receive the status InboxEventStatusPending in case of failure of processing.
	MaxAttempts int
}

// InboxProcessor reads pending inbox events from storage and processes them using registered handlers.
//
// Semantics:
//   - At-least-once processing: the same event can be handled more than once
//     (e.g. due to retries, crashes, or commit failures). Handlers must be idempotent.
//   - A processor runs as a long-living process identified by processID.
//   - StartProcess blocks until ctx is cancelled and drains internal workers.
//   - StartProcess does NOT automatically clean/release events reserved by this process.
//     The caller decides what cleanup to run on shutdown via StopProcess or maintenance methods.
type InboxProcessor struct {
	log    *logium.Logger
	config InboxProcessorConfig

	box   inbox
	route map[string]msnger.InboxHandlerFunc
}

// NewInboxProcessor creates a new InboxProcessor.
func NewInboxProcessor(
	log *logium.Logger,
	box inbox,
	config InboxProcessorConfig,
) *InboxProcessor {
	if config.MaxRutin <= 0 {
		config.MaxRutin = DefaultInboxProcessorMaxRutin
	}
	if config.InFlight <= 0 {
		config.InFlight = config.MaxRutin * 4
	}
	if config.MinSleep <= 0 {
		config.MinSleep = DefaultInboxProcessorMinSleep
	}
	if config.MaxSleep <= 0 {
		config.MaxSleep = DefaultInboxProcessorMaxSleep
	}
	if config.MinBatch <= 0 {
		config.MinBatch = DefaultInboxProcessorMinBatch
	}
	if config.MaxBatch <= 0 {
		config.MaxBatch = DefaultInboxProcessorMaxBatch
	}
	if config.MinNextAttempt <= 0 {
		config.MinNextAttempt = DefaultInboxProcessorMinNextAttempt
	}
	if config.MaxNextAttempt <= 0 {
		config.MaxNextAttempt = DefaultInboxProcessorMaxNextAttempt
	}

	return &InboxProcessor{
		log:    log,
		box:    box,
		config: config,
		route:  make(map[string]msnger.InboxHandlerFunc),
	}
}

// inboxProcessorSlot is a slot for processing one event. It is used to limit the number of events being processed in parallel.
type inboxProcessorSlot struct{}

// takeSlot tries to take a slot for processing an event. It returns false if the context is done.
func takeSlot(ctx context.Context, slots <-chan inboxProcessorSlot) bool {
	select {
	case <-ctx.Done():
		return false
	case <-slots:
		return true
	}
}

// inboxProcessorJob defines job for processing inbox event.
type inboxProcessorJob struct {
	event msnger.InboxEvent
}

// sendJob sends a job for processing an event or returns false if context is done.
func sendJob(ctx context.Context, jobs chan<- inboxProcessorJob, job inboxProcessorJob) bool {
	select {
	case <-ctx.Done():
		return false
	case jobs <- job:
		return true
	}
}

// giveSlot gives back a slot after processing an event.
func giveSlot(slots chan<- inboxProcessorSlot) {
	select {
	case slots <- inboxProcessorSlot{}:
	default:
	}
}

// Route registers a handler for the given event type.
// It panics if a handler for this event type is already registered.
//
// Route is expected to be called during initialization, before StartProcess.
func (w *InboxProcessor) Route(eventType string, handler msnger.InboxHandlerFunc) {
	if _, ok := w.route[eventType]; ok {
		panic(fmt.Errorf("double handler for event type=%s", eventType))
	}

	w.route[eventType] = handler
}

// StartProcess starts the processor loop and blocks until ctx is cancelled.
//
// processID is used to reserve/lock events for this process and should be unique among
// concurrently running processes. If two processes use the same processID, they may
// compete for the same reserved events and cause duplicate work.
//
// Flow:
//  1. feederLoop periodically reserves pending events and sends them to handleLoop via jobs channel.
//  2. handleLoop processes events using registered handlers and then updates event state:
//     commit (processed), delay (retry later), or failed (if MaxAttempts exceeded).
//
// Notes:
//   - StartProcess stops when ctx is cancelled.
//   - StartProcess does NOT perform any cleanup on exit.
//     If the process is shutting down, the caller may call StopProcess(processID)
//     or use maintenance cleanup methods to release reserved events.
func (w *InboxProcessor) StartProcess(ctx context.Context, processID string) {
	// create channels for processing events and limit the number of events being processed in parallel using slots channel
	slots := make(chan inboxProcessorSlot, w.config.InFlight)
	for i := 0; i < w.config.InFlight; i++ {
		slots <- inboxProcessorSlot{}
	}

	// create channel for sending jobs to handle loops
	jobs := make(chan inboxProcessorJob, w.config.InFlight)

	var wg sync.WaitGroup
	wg.Add(w.config.MaxRutin + 1)

	go func() {
		defer wg.Done()
		w.feederLoop(ctx, processID, slots, jobs)
	}()

	for i := 0; i < w.config.MaxRutin; i++ {
		go func() {
			defer wg.Done()
			w.handleLoop(ctx, processID, slots, jobs)
		}()
	}

	wg.Wait()
}

// feederLoop reserve events for processor by "processID" and send
//
// Flow:
//  1. reserve events for "processID"
//  2. if no events or failed to reserve, sleep for a while and try again
//  3. try to take a slot if success try to reserve events
//  4. if successful taken slot try to send job to handleLoop, if failed to send job, give back slot and try again
//
// The loop continues until the context is done. After that, it tries to clean up any events reserved for this processor.
func (w *InboxProcessor) feederLoop(
	ctx context.Context,
	processID string,
	slots chan inboxProcessorSlot,
	jobs chan<- inboxProcessorJob,
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

		events, err := w.box.ReserveInboxEvents(ctx, processID, limit)
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
			if !sendJob(ctx, jobs, inboxProcessorJob{event: ev}) {
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
func (w *InboxProcessor) handleLoop(
	ctx context.Context,
	processID string,
	slots chan inboxProcessorSlot,
	jobs <-chan inboxProcessorJob,
) {
	for job := range jobs {
		ev := job.event

		err := w.box.Transaction(ctx, func(ctx context.Context) error {
			var err error

			hErr := w.handleEvent(ctx, ev.Type, ev.ToKafkaMessage())
			if hErr != nil {
				switch {
				case w.config.MaxAttempts != 0 && ev.Attempts >= w.config.MaxAttempts:
					ev, err = w.box.FailedInboxEvent(ctx, processID, ev.EventID, hErr.Error())
					if err != nil {
						return fmt.Errorf("failed to mark inbox event as failed id=%s: %w", ev.EventID, err)
					}

					return nil
				default:
					ev, err = w.box.DelayInboxEvent(ctx, processID, ev.EventID, w.nextAttemptAt(ev.Attempts), hErr.Error())
					if err != nil {
						return fmt.Errorf("failed to delay inbox event id=%s: %w", ev.EventID, err)
					}

					return nil
				}
			}

			ev, err = w.box.CommitInboxEvent(ctx, processID, ev.EventID)
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
func (w *InboxProcessor) handleEvent(
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
func (w *InboxProcessor) nextAttemptAt(attempts int) time.Time {
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

// StopProcess performs a best-effort cleanup for the given processID.
//
// Current behavior depends on storage implementation. In this pg implementation
// it cleans failed events associated with the processID.
//
// StopProcess does not stop goroutines started by StartProcess.
// Stopping is controlled via ctx cancellation passed to StartProcess.
func (w *InboxProcessor) StopProcess(ctx context.Context, processID string) error {
	err := w.box.CleanFailedInboxEventForProcessor(ctx, processID)
	if err != nil {
		w.log.WithError(err).Errorf("failed to clean own failed events for processID=%s", processID)
	}
	return err
}

// CleanInboxProcessing performs storage-level cleanup of processing events.
//
// WARNING:
//   - This is a maintenance operation and may affect events reserved by other processes,
//     depending on storage implementation.
// Prefer CleanInboxProcessingForProcessID when you need to clean only one process.
func (w *InboxProcessor) CleanInboxProcessing(ctx context.Context) error {
	err := w.box.CleanProcessingInboxEvent(ctx)
	if err != nil {
		w.log.WithError(err).Error("failed to clean processing events")
	}

	return err
}

// CleanInboxProcessingForProcessID performs storage-level cleanup of processing events for the given processID.
func (w *InboxProcessor) CleanInboxProcessingForProcessID(ctx context.Context, processID string) error {
	err := w.box.CleanProcessingInboxEventForProcessor(ctx, processID)
	if err != nil {
		w.log.WithError(err).Errorf("failed to clean processing events for processID=%s", processID)
	}

	return err
}

// CleanInboxFailed performs storage-level cleanup of failed events.
//
// WARNING:
//   - This is a maintenance operation and may affect events reserved by other processes,
//     depending on storage implementation.
// Prefer CleanInboxFailedForProcessID when you need to clean only one process.
func (w *InboxProcessor) CleanInboxFailed(ctx context.Context) error {
	err := w.box.CleanFailedInboxEvent(ctx)
	if err != nil {
		w.log.WithError(err).Error("failed to clean failed events")
	}

	return err
}

// CleanInboxFailedForProcessID performs storage-level cleanup of failed events for the given processID.
func (w *InboxProcessor) CleanInboxFailedForProcessID(ctx context.Context, processID string) error {
	err := w.box.CleanFailedInboxEventForProcessor(ctx, processID)
	if err != nil {
		w.log.WithError(err).Errorf("failed to clean failed events for processID=%s", processID)
	}

	return err
}
