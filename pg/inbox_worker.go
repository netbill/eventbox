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

	DefaultInboxWorkerMaxSleep = 5 * time.Second
	DefaultInboxWorkerMinSleep = 50 * time.Millisecond

	DefaultInboxWorkerMaxBatch = 100
	DefaultInboxWorkerMinBatch = 10

	DefaultInboxWorkerMinNextAttempt = time.Minute
	DefaultInboxWorkerMaxNextAttempt = 10 * time.Minute
)

// InboxWorkerConfig defines configuration for InboxWorker.
type InboxWorkerConfig struct {
	// Routines is the maximum number of parallel handle loops.
	// If 0, it defaults to DefaultInboxWorkerRoutines.
	Routines int

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
	MaxAttempts int32
}

type InboxWorker struct {
	id  string
	log *logium.Entry

	box    inbox
	route  map[string]eventbox.InboxHandlerFunc
	config InboxWorkerConfig
}

// NewInboxWorker creates a new InboxWorker.
func NewInboxWorker(
	log *logium.Entry,
	db *pgdbx.DB,
	id string,
	config InboxWorkerConfig,
) eventbox.InboxWorker {
	if config.Routines <= 0 {
		config.Routines = DefaultInboxWorkerRoutines
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
	if config.MaxBatch < config.MinBatch {
		config.MaxBatch = config.MinBatch
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

func (p *InboxWorker) Route(eventType string, handler eventbox.InboxHandlerFunc) {
	if _, ok := p.route[eventType]; ok {
		panic(fmt.Errorf("double handler for event type=%s", eventType))
	}

	p.route[eventType] = handler
}

// Run starts processing inbox events for the given process ID.
func (p *InboxWorker) Run(ctx context.Context) {
	p.log.Info("starting inbox worker")

	// Initialize the slots channel with the configured number of in-flight processing slots.
	slots := make(chan inboxWorkerSlot, p.config.Routines*4)
	for i := 0; i < p.config.Routines*4; i++ {
		slots <- inboxWorkerSlot{}
	}

	// Initialize the jobs channel for processing events.
	jobs := make(chan inboxWorkerJob, p.config.Routines*4)

	var wg sync.WaitGroup
	wg.Add(p.config.Routines + 1)

	go func() {
		defer wg.Done()
		p.feederLoop(ctx, slots, jobs)
	}()

	for i := 0; i < p.config.Routines; i++ {
		go func() {
			defer wg.Done()
			p.handleLoop(ctx, slots, jobs)
		}()
	}

	wg.Wait()
}

// feederLoop continuously reserves batches of inbox events and sends them to the jobs channel for processing.
func (p *InboxWorker) feederLoop(
	ctx context.Context,
	slots chan inboxWorkerSlot,
	jobs chan<- inboxWorkerJob,
) {
	defer close(jobs)

	sleep := p.config.MinSleep

	for {
		if ctx.Err() != nil {
			return
		}
		
		free := len(slots)
		if free == 0 {
			sleep = p.sleep(ctx, 0, sleep)
			continue
		}

		limit := calculateBatch(free, p.config.MinBatch, p.config.MaxBatch)

		events, err := p.box.ReserveInboxEvents(ctx, p.id, limit)
		if err != nil {
			p.log.WithError(err).Error("failed to reserve inbox events")
			sleep = p.sleep(ctx, 0, sleep)
			continue
		}

		if len(events) == 0 {
			sleep = p.sleep(ctx, 0, sleep)
			continue
		}

		sleep = p.config.MinSleep

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
func (p *InboxWorker) handleLoop(
	ctx context.Context,
	slots chan inboxWorkerSlot,
	jobs <-chan inboxWorkerJob,
) {
	for job := range jobs {
		event := job.event

		err := p.box.Transaction(ctx, func(ctx context.Context) error {
			var err error

			hErr := p.handleEvent(ctx, event)
			if hErr != nil {
				switch {
				case p.config.MaxAttempts != 0 && event.Attempts >= p.config.MaxAttempts:
					event, err = p.box.FailedInboxEvent(ctx, p.id, event.EventID, hErr.Error())
					if err != nil {
						return err
					}

					return nil
				default:
					event, err = p.box.DelayInboxEvent(ctx, p.id, event.EventID, hErr.Error(), p.nextAttemptAt(event.Attempts))
					if err != nil {
						return err
					}

					return nil
				}
			}

			event, err = p.box.CommitInboxEvent(ctx, p.id, event.EventID)
			if err != nil {
				return err
			}

			return nil
		})
		if err != nil {
			p.log.WithError(err).
				WithFields(logfields.FromInboxEvent(event)).
				Error("failed to process inbox event")
		}

		giveSlot(ctx, slots)

		if ctx.Err() != nil {
			return
		}
	}
}

// handleEvent processes a single inbox event by looking up the appropriate handler based on the event type.
func (p *InboxWorker) handleEvent(ctx context.Context, event eventbox.InboxEvent) error {
	handler, ok := p.route[event.Type]
	if !ok {
		p.log.WithFields(logfields.FromInboxEvent(event)).
			Infof("no handler for event type=%s", event.Type)

		return nil
	}

	return handler(ctx, event.ToKafkaMessage())
}

// nextAttemptAt calculates the next attempt time for processing a failed event based on the number of attempts
// and the configured minimum and maximum next attempt durations.
func (p *InboxWorker) nextAttemptAt(attempts int32) time.Time {
	res := time.Second * time.Duration(30*attempts)
	if res < p.config.MinNextAttempt {
		return time.Now().UTC().Add(p.config.MinNextAttempt)
	}
	if res > p.config.MaxNextAttempt {
		return time.Now().UTC().Add(p.config.MaxNextAttempt)
	}

	return time.Now().UTC().Add(res)
}

func (p *InboxWorker) sleep(
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

// Stop stops processing inbox events for the given process ID by cleaning up any events that are currently marked as processing for that worker.
func (p *InboxWorker) Stop(ctx context.Context) error {
	return p.box.CleanProcessingInboxEvents(ctx, p.id)
}
