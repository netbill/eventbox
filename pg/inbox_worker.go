package pg

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/netbill/eventbox"
	"github.com/netbill/eventbox/logfields"
	"github.com/netbill/logium"
	"github.com/netbill/pgdbx"
)

const (
	DefaultInboxWorkerRoutines = 10

	DefaultInboxWorkerSleep = 200 * time.Millisecond

	DefaultInboxWorkerBatch = 100

	DefaultInboxWorkerMinNextAttempt = time.Minute
	DefaultInboxWorkerMaxNextAttempt = 10 * time.Minute
)

// InboxWorkerConfig defines configuration for InboxWorker.
type InboxWorkerConfig struct {
	// Routines is the maximum number of parallel handle loops.
	// If 0, it defaults to DefaultInboxWorkerRoutines.
	Routines int

	// Slots is the maximum number of in-flight processing events.
	// If 0, it defaults to Routines * 4.
	Slots int

	// Sleep is the duration to sleep when there are no events to process or when all processing slots are occupied.
	// If 0, it defaults to DefaultInboxWorkerSleep.
	Sleep time.Duration

	// BatchSize is the number of events to reserve in one batch.
	// If 0, it defaults to DefaultInboxWorkerBatch.
	BatchSize int

	// MinNextAttempt is the minimum delay before next attempt to process failed event.
	// If 0, it defaults to DefaultInboxWorkerMinNextAttempt.
	MinNextAttempt time.Duration
	// MaxNextAttempt is the maximum delay before next attempt to process failed event.
	// If 0, it defaults to DefaultInboxWorkerMaxNextAttempt.
	MaxNextAttempt time.Duration

	// MaxAttempts is the maximum number of attempts to process event before marking it as failed.
	// If 0, the event will always receive the status InboxEventStatusPending in case of failure of processing.
	MaxAttempts int32
}

type InboxWorker struct {
	id  string
	log *logium.Entry

	box    inbox
	config InboxWorkerConfig
	route  map[string]eventbox.InboxHandlerFunc
}

// NewInboxWorker creates a new InboxWorker.
func NewInboxWorker(
	id string,
	log *logium.Entry,
	db *pgdbx.DB,
	config InboxWorkerConfig,
) eventbox.InboxWorker {
	if config.Routines <= 0 {
		config.Routines = DefaultInboxWorkerRoutines
	}
	if config.Slots <= 0 {
		config.Slots = config.Routines * 4
	}
	if config.Sleep <= 0 {
		config.Sleep = DefaultInboxWorkerSleep
	}
	if config.BatchSize <= 0 {
		config.BatchSize = DefaultInboxWorkerBatch
	}
	if config.MinNextAttempt <= 0 {
		config.MinNextAttempt = DefaultInboxWorkerMinNextAttempt
	}
	if config.MaxNextAttempt <= 0 {
		config.MaxNextAttempt = DefaultInboxWorkerMaxNextAttempt
	}
	if config.MaxNextAttempt < config.MinNextAttempt {
		config.MaxNextAttempt = config.MinNextAttempt
	}

	return &InboxWorker{
		id:     id,
		log:    log.WithField("worker_id", id),
		box:    inbox{db: db},
		config: config,
		route:  make(map[string]eventbox.InboxHandlerFunc),
	}
}

type inboxWorkerSlot struct{}

func takeSlot(ctx context.Context, slots <-chan inboxWorkerSlot) bool {
	select {
	case <-ctx.Done():
		return false
	case <-slots:
		return true
	}
}

func giveSlot(ctx context.Context, slots chan<- inboxWorkerSlot) bool {
	select {
	case <-ctx.Done():
		return false
	case slots <- inboxWorkerSlot{}:
		return true
	}
}

type inboxWorkerJob struct {
	event eventbox.InboxEvent
}

func sendJob(ctx context.Context, jobs chan<- inboxWorkerJob, job inboxWorkerJob) bool {
	select {
	case <-ctx.Done():
		return false
	case jobs <- job:
		return true
	}
}

func (w *InboxWorker) Route(eventType string, handler eventbox.InboxHandlerFunc) {
	if _, ok := w.route[eventType]; ok {
		panic(fmt.Errorf("double handler for event type=%s", eventType))
	}

	w.route[eventType] = handler
}

// Run starts processing inbox events for the given process ID.
func (w *InboxWorker) Run(ctx context.Context) {
	w.log.Info("starting inbox worker")

	// Initialize the slots channel with the configured number of in-flight processing slots.
	slots := make(chan inboxWorkerSlot, w.config.Slots)
	for i := 0; i < w.config.Slots; i++ {
		slots <- inboxWorkerSlot{}
	}

	// Initialize the jobs channel for processing events.
	jobs := make(chan inboxWorkerJob, w.config.Slots)

	var wg sync.WaitGroup
	wg.Add(w.config.Routines + 1)

	go func() {
		defer wg.Done()
		w.feederLoop(ctx, slots, jobs)
	}()

	for i := 0; i < w.config.Routines; i++ {
		go func() {
			defer wg.Done()
			w.handleLoop(ctx, slots, jobs)
		}()
	}

	wg.Wait()
}

// feederLoop continuously reserves batches of inbox events and sends them to the jobs channel for processing.
func (w *InboxWorker) feederLoop(ctx context.Context, slots chan inboxWorkerSlot, jobs chan<- inboxWorkerJob) {
	defer close(jobs)

	for {
		if ctx.Err() != nil {
			return
		}

		free := len(slots)
		if free == 0 {
			w.sleep(ctx)
			continue
		}

		limit := w.config.BatchSize
		if limit > free {
			limit = free
		}

		events, err := w.box.ReserveInboxEvents(ctx, w.id, limit)
		if err != nil {
			w.log.WithError(err).Error("failed to reserve inbox events")
			w.sleep(ctx)
			continue
		}

		if len(events) == 0 {
			w.sleep(ctx)
			continue
		}

		for _, ev := range events {
			if !takeSlot(ctx, slots) {
				return
			}
			if !sendJob(ctx, jobs, inboxWorkerJob{event: ev}) {
				giveSlot(ctx, slots)
				return
			}
		}
	}
}

// handleLoop continuously processes inbox events received from the jobs channel.
func (w *InboxWorker) handleLoop(
	ctx context.Context,
	slots chan inboxWorkerSlot,
	jobs <-chan inboxWorkerJob,
) {
	for job := range jobs {
		event := job.event

		log := w.log.WithFields(logfields.FromInboxEvent(event))

		herr := w.handleEvent(ctx, event)
		switch {
		case ctx.Err() != nil:
			return
		case herr != nil && event.Attempts+1 >= w.config.MaxAttempts && w.config.MaxAttempts != 0:
			ev, err := w.box.FailedInboxEvent(ctx, w.id, event.EventID, herr.Error())
			if err != nil {
				log.WithError(err).Error("failed to mark inbox event as failed")
				break
			}

			log.WithError(herr).WithFields(logfields.FromInboxEvent(ev)).
				Error("inbox event marked as failed due to max attempts reached")
		case herr != nil:
			ev, err := w.box.DelayInboxEvent(ctx, w.id, event.EventID, herr.Error(), w.nextAttemptAt(event.Attempts+1))
			if err != nil {
				log.WithError(err).Error("failed to delay inbox event")
				break
			}

			log.WithError(herr).WithFields(logfields.FromInboxEvent(ev)).
				Warn("failed to mark inbox event as delayed ")
		default:
			ev, err := w.box.CommitInboxEvent(ctx, w.id, event.EventID)
			if err != nil {
				log.WithError(err).Error("failed to commit inbox event")
				break
			}

			log.WithFields(logfields.FromInboxEvent(ev)).
				Debug("inbox event handled successfully")
		}

		giveSlot(ctx, slots)
	}
}

// handleEvent processes a single inbox event by looking up the appropriate handler based on the event type.
func (w *InboxWorker) handleEvent(ctx context.Context, event eventbox.InboxEvent) error {
	handler, ok := w.route[event.Type]
	if !ok {
		w.log.WithFields(logfields.FromInboxEvent(event)).
			Infof("no handler for event type=%s", event.Type)

		return nil
	}

	return handler(ctx, event.ToKafkaMessage())
}

// nextAttemptAt calculates the next attempt time for processing a failed event based on the number of attempts
// and the configured minimum and maximum next attempt durations.
func (w *InboxWorker) nextAttemptAt(attempts int32) time.Time {
	res := time.Second * time.Duration(30*attempts)
	if res < w.config.MinNextAttempt {
		return time.Now().UTC().Add(w.config.MinNextAttempt)
	}
	if res > w.config.MaxNextAttempt {
		return time.Now().UTC().Add(w.config.MaxNextAttempt)
	}

	return time.Now().UTC().Add(res)
}

func (w *InboxWorker) sleep(
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

// Stop stops processing inbox events for the given process ID by cleaning up any events that are currently marked as processing for that worker.
func (w *InboxWorker) Stop(ctx context.Context) error {
	return w.box.CleanProcessingInboxEvents(ctx, w.id)
}
