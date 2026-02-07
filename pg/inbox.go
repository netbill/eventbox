package pg

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/netbill/logium"
	"github.com/netbill/msnger"
	"github.com/netbill/msnger/headers"
)

const (
	// InboxMsgStatusPending indicates that the event is pending processing
	InboxMsgStatusPending = "pending"
	// InboxMsgStatusProcessing indicates this is event already processing by some worker
	InboxMsgStatusProcessing = "processing"
	// InboxMsgStatusProcessed indicates that the event has been successfully processed
	InboxMsgStatusProcessed = "processed"
	// InboxMsgStatusFailed indicates that all retry attempts have been exhausted
	InboxMsgStatusFailed = "failed"
)

type InboxMsg struct {
	EventID uuid.UUID `json:"event_id"`
	Seq     uint      `json:"seq"`

	Topic     string `json:"topic"`
	Key       string `json:"key"`
	Type      string `json:"type"`
	Version   uint   `json:"version"`
	Producer  string `json:"producer"`
	Payload   []byte `json:"payload"`
	Partition uint   `json:"partition"`
	Offset    uint   `json:"offset"`

	ReservedBy *string `json:"reserved_by"`

	Status        string     `json:"status"`
	Attempts      uint       `json:"attempts"`
	NextAttemptAt time.Time  `json:"next_attempt_at"`
	LastAttemptAt *time.Time `json:"last_attempt_at"`
	LastError     *string    `json:"last_error"`

	ProcessedAt *time.Time `json:"processed_at"`
	ProducedAt  time.Time  `json:"produced_at"`
	CreatedAt   time.Time  `json:"created_at"`
}

func (e *InboxMsg) ToKafkaMessage() kafka.Message {
	return kafka.Message{
		Topic: e.Topic,
		Key:   []byte(e.Key),
		Value: e.Payload,
		Headers: []kafka.Header{
			{
				Key:   headers.EventID,
				Value: []byte(e.EventID.String()),
			},
			{
				Key:   headers.EventType,
				Value: []byte(e.Type),
			},
			{
				Key:   headers.EventVersion,
				Value: []byte(strconv.FormatUint(uint64(e.Version), 10)),
			},
			{
				Key:   headers.Producer,
				Value: []byte(e.Producer),
			},
			{
				Key:   headers.ContentType,
				Value: []byte("application/json"),
			},
		},
	}
}

type WorkerConfig struct {
	maxRutin uint
	minSleep time.Duration
	maxSleep time.Duration

	minButchSize uint
	maxButchSize uint
}

type inboxWorWorker interface {
}

type Inbox struct {
	id     string
	log    *logium.Logger
	config WorkerConfig

	box      inboxWorWorker
	handlers map[string]msnger.InHandlerFunc
}

func (i *Inbox) Run(ctx context.Context) {

}

func (i *Inbox) Stop() error {

	return nil
}

func (i *Inbox) Route(eventType string, handler msnger.InHandlerFunc) {
	_, ok := i.handlers[eventType]
	if ok {
		panic(fmt.Errorf("for one type event double define handler"))
	}

	i.handlers[eventType] = handler
}

func (i *Inbox) onUnknown(ctx context.Context, m kafka.Message) error {
	i.log.Warnf(
		"onUnknown called for event on topic %s, partition %d, offset %d", m.Topic, m.Partition, m.Offset,
	)

	return nil
}

func (i *Inbox) invalidContent(ctx context.Context, m kafka.Message) error {
	i.log.Warnf(
		"invalid content in message on topic %s, partition %d, offset %d", m.Topic, m.Partition, m.Offset,
	)

	return nil
}

func (w *Worker) Run(ctx context.Context) {
	defer func() {
		err := w.box.CleanProcessingInboxEventForWorker(context.Background(), w.id)
		if err != nil {
			w.log.WithError(err).Error("Failed to clean processing inbox")
		}
	}()

	butchSize := w.config.maxButchSize
	sleep := w.config.minSleep

	for {
		if ctx.Err() != nil {
			return
		}

		numEvents := w.tick(ctx, butchSize)

		butchSize, sleep = w.calculateButchAndSleep(numEvents, sleep)

		select {
		case <-ctx.Done():
			return
		case <-time.After(sleep):
		}
	}
}

func (w *Worker) tick(ctx context.Context, butchSize uint) uint {
	events, err := w.box.ReserveInboxEvents(ctx, w.id, butchSize)
	if err != nil {
		w.log.WithError(err).Error("failed to reserve inbox events")
		return 0
	}
	if len(events) == 0 {
		return 0
	}

	maxParallel := int(w.config.maxRutin)
	if maxParallel <= 0 {
		maxParallel = 1
	}

	sem := make(chan struct{}, maxParallel)
	errCh := make(chan error, len(events))

	var wg sync.WaitGroup

	for _, ev := range events {
		ev := ev

		sem <- struct{}{}
		wg.Add(1)

		go func() {
			defer func() {
				wg.Done()
				<-sem
			}()

			if ctx.Err() != nil {
				return
			}

			txErr := w.box.Transaction(ctx, func(ctx context.Context) error {
				handler, ok := w.handlers[ev.Type]
				if !ok {
					return w.Unknown(ctx, ev)
				}

				res := handler(ctx, ev)
				if res != nil {
					w.log.WithError(res).Errorf("handler for event %s type=%s failed", ev.EventID, ev.Type)
					return w.box.DelayInboxEvent(ctx, w.id, ev.EventID, w.nextAttemptAt(ev), res.Error())
				}

				w.log.Debugf("event %s type=%s processed successfully", ev.EventID, ev.Type)
				return w.box.CommitInboxEvent(ctx, w.id, ev.EventID)
			})

			if txErr != nil {
				errCh <- txErr
			}
		}()
	}

	wg.Wait()

	close(errCh)

	for e := range errCh {
		w.log.WithError(e).Error("failed to process inbox event")
	}

	return uint(len(events))
}

func (w *Worker) nextAttemptAt(ev Event) time.Duration {
	res := time.Second * time.Duration(30*ev.Attempts)
	if res < time.Minute {
		return time.Minute
	}
	if res > time.Minute*10 {
		return time.Minute * 10
	}

	return res
}

func (w *Worker) calculateButchAndSleep(
	numEvents uint,
	lastSleep time.Duration,
) (uint, time.Duration) {
	if numEvents >= w.config.maxButchSize {
		return w.config.maxButchSize, w.config.minSleep
	}

	var sleep time.Duration
	if numEvents == 0 {
		if lastSleep <= 0 {
			sleep = w.config.minSleep
		} else {
			sleep = lastSleep * 2
			if sleep < w.config.minSleep {
				sleep = w.config.minSleep
			}
		}
		if sleep > w.config.maxSleep {
			sleep = w.config.maxSleep
		}
		if sleep < 0 {
			sleep = 0
		}

		return w.config.minButchSize, sleep
	}

	butchSize := numEvents * 2
	if butchSize < w.config.minButchSize {
		butchSize = w.config.minButchSize
	}
	if butchSize > w.config.maxButchSize {
		butchSize = w.config.maxButchSize
	}

	fill := float64(numEvents) / float64(w.config.maxButchSize)

	switch {
	case fill >= 0.75:
		sleep = 0
	case fill >= 0.50:
		sleep = w.config.minSleep
	case fill >= 0.25:
		sleep = w.config.minSleep * 2
	default:
		sleep = w.config.minSleep * 4
		if sleep < lastSleep {
			sleep = lastSleep
		}
	}

	return butchSize, sleep
}

func (w *Inbox) CleanOwnFailedEvents(ctx context.Context) error {
	return w.box.CleanFailedInboxEventForWorker(ctx, w.id)
}

func (w *Inbox) CleanOwnProcessingEvents(ctx context.Context) error {
	return w.box.CleanProcessingInboxEventForWorker(ctx, w.id)
}
