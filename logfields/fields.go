package logfields

import (
	"github.com/netbill/eventbox"
	"github.com/netbill/eventbox/headers"
	"github.com/netbill/logium"
	"github.com/segmentio/kafka-go"
)

const (
	EventIDFiled       = "event_id"
	EventTypeFiled     = "event_type"
	EventTopicFiled    = "event_topic"
	EventVersionFiled  = "event_version"
	EventProducerFiled = "event_producer"
	EventAttemptFiled  = "event_attempt"
)

func FromHeader(hs headers.MessageRequiredHeaders) logium.Fields {
	return map[string]any{
		EventIDFiled:       hs.EventID,
		EventTypeFiled:     hs.EventType,
		EventProducerFiled: hs.Producer,
	}
}

func FromMessage(m kafka.Message) logium.Fields {
	res := map[string]any{
		EventTopicFiled: m.Topic,
	}

	hs, err := headers.ParseMessageRequiredHeaders(m.Headers)
	if err != nil {
		res[EventTopicFiled] = "unknown"
		res[EventTypeFiled] = "unknown"
		res[EventProducerFiled] = "unknown"

		return res
	}

	for k, v := range FromHeader(hs) {
		res[k] = v
	}

	return res
}

func FromInboxEvent(ev eventbox.InboxEvent) logium.Fields {
	return map[string]any{
		EventIDFiled:       ev.EventID,
		EventTopicFiled:    ev.Topic,
		EventTypeFiled:     ev.Type,
		EventVersionFiled:  ev.Version,
		EventProducerFiled: ev.Producer,
		EventAttemptFiled:  ev.Attempts,
	}
}

func FromOutboxEvent(ev eventbox.OutboxEvent) logium.Fields {
	return map[string]any{
		EventIDFiled:       ev.EventID,
		EventTopicFiled:    ev.Topic,
		EventTypeFiled:     ev.Type,
		EventVersionFiled:  ev.Version,
		EventProducerFiled: ev.Producer,
		EventAttemptFiled:  ev.Attempts,
	}
}
