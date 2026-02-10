package pg

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/netbill/logium"
	"github.com/netbill/msnger"
	"github.com/netbill/msnger/logfields"
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
	MaxAttempts int32
}

type InboxProcessor struct {
	log    *logium.Entry
	config InboxProcessorConfig

	box   inbox
	route map[string]msnger.InboxHandlerFunc
}

// NewInboxProcessor creates a new InboxProcessor.
func NewInboxProcessor(
	log *logium.Entry,
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

func (w *InboxProcessor) StartProcess(ctx context.Context, processID string) {
	defer func() {
		if err := w.StopProcess(context.Background(), processID); err != nil {
			w.log.WithError(err).
				WithField(logfields.ProcessID, processID).
				Error("failed to stop inbox processor")
		}
	}()

	slots := make(chan inboxProcessorSlot, w.config.InFlight)
	for i := 0; i < w.config.InFlight; i++ {
		slots <- inboxProcessorSlot{}
	}

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
			w.log.WithError(err).
				WithField(logfields.ProcessID, processID).
				Error("failed to reserve inbox events")
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

			hErr := w.handleEvent(ctx, ev)
			if hErr != nil {
				switch {
				case w.config.MaxAttempts != 0 && ev.Attempts >= w.config.MaxAttempts:
					ev, err = w.box.FailedInboxEvent(ctx, processID, ev.EventID, hErr.Error())
					if err != nil {
						return err
					}

					return nil
				default:
					ev, err = w.box.DelayInboxEvent(ctx, processID, ev.EventID, hErr.Error(), w.nextAttemptAt(ev.Attempts))
					if err != nil {
						return err
					}

					return nil
				}
			}

			ev, err = w.box.CommitInboxEvent(ctx, processID, ev.EventID)
			if err != nil {
				return err
			}

			return nil
		})
		if err != nil {
			w.log.WithError(err).
				WithField(logfields.ProcessID, processID).
				WithFields(logfields.FromInboxEvent(ev)).
				Error("failed to process inbox event")
		}

		giveSlot(slots)

		if ctx.Err() != nil {
			return
		}
	}
}

func (w *InboxProcessor) handleEvent(ctx context.Context, event msnger.InboxEvent) error {
	handler, ok := w.route[event.Type]
	if !ok {
		w.log.WithFields(logfields.FromInboxEvent(event)).
			Infof("no handler for event type=%s", event.Type)

		return nil
	}

	return handler(ctx, event.ToKafkaMessage())
}

func (w *InboxProcessor) nextAttemptAt(attempts int32) time.Time {
	res := time.Second * time.Duration(30*attempts)
	if res < w.config.MinNextAttempt {
		return time.Now().UTC().Add(w.config.MinNextAttempt)
	}
	if res > w.config.MaxNextAttempt {
		return time.Now().UTC().Add(w.config.MaxNextAttempt)
	}

	return time.Now().UTC().Add(res)
}

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

func (w *InboxProcessor) StopProcess(ctx context.Context, processID string) error {
	return w.box.CleanProcessingInboxEventForProcessor(ctx, processID)
}

func (w *InboxProcessor) CleanInboxProcessing(ctx context.Context) error {
	return w.box.CleanProcessingInboxEvents(ctx)
}

func (w *InboxProcessor) CleanInboxProcessingForProcessID(ctx context.Context, processID string) error {
	return w.box.CleanProcessingInboxEventForProcessor(ctx, processID)
}

func (w *InboxProcessor) CleanInboxFailed(ctx context.Context) error {
	return w.box.CleanFailedInboxEvents(ctx)
}
