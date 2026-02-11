package logfields

import (
	"github.com/netbill/logium"
	"github.com/netbill/msnger"
	"github.com/netbill/msnger/headers"
	"github.com/segmentio/kafka-go"
)

const (
	ProcessID = "process_id"

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
		EventVersionFiled:  hs.EventVersion,
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
		res[EventVersionFiled] = "unknown"
		res[EventProducerFiled] = "unknown"

		return res
	}

	for k, v := range FromHeader(hs) {
		res[k] = v
	}

	return res
}

func FromInboxEvent(ev msnger.InboxEvent) logium.Fields {
	return map[string]any{
		EventIDFiled:       ev.EventID,
		EventTopicFiled:    ev.Topic,
		EventTypeFiled:     ev.Type,
		EventVersionFiled:  ev.Version,
		EventProducerFiled: ev.Producer,
		EventAttemptFiled:  ev.Attempts,
	}
}

func FromOutboxEvent(ev msnger.OutboxEvent) logium.Fields {
	return map[string]any{
		EventIDFiled:       ev.EventID,
		EventTopicFiled:    ev.Topic,
		EventTypeFiled:     ev.Type,
		EventVersionFiled:  ev.Version,
		EventProducerFiled: ev.Producer,
		EventAttemptFiled:  ev.Attempts,
	}
}
