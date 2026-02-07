package pg

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/netbill/msnger"
)

type producer struct {
}

func NewProducer() msnger.Producer {
	return &producer{}
}

func (p *producer) WriteMessage(
	ctx context.Context,
	msg kafka.Message,
) error {

}

func (p *producer) WriteMessageInBox(
	ctx context.Context,
	msg kafka.Message,
	delay *time.Duration,
) error {

}

func (p *producer) CommitMessageInBox(
	ctx context.Context,
	messageID uuid.UUID,
) error {

}

func (p *producer) ShutDown() error {
	return nil
}
