package headers

import (
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

const (
	EventID      = "event_id"
	EventType    = "event_type"
	EventVersion = "event_version"
	Producer     = "producer"
	ContentType  = "content_type"
)

func RequiredHeaderValue(hs []kafka.Header, key string) ([]byte, error) {
	var (
		found bool
		value []byte
	)

	for i := range hs {
		if hs[i].Key != key {
			continue
		}

		if found {
			return nil, fmt.Errorf("duplicate %s header", key)
		}

		found = true
		value = hs[i].Value
	}

	if !found {
		return nil, fmt.Errorf("missing %s header", key)
	}

	return value, nil
}

type MessageRequiredHeaders struct {
	EventID      uuid.UUID
	EventType    string
	EventVersion uint
	Producer     string
	ContentType  string
}

func ParseMessageRequiredHeaders(hs []kafka.Header) (MessageRequiredHeaders, error) {
	var out MessageRequiredHeaders
	var err error

	eventIDBytes, err := RequiredHeaderValue(hs, EventID)
	if err != nil {
		return out, err
	}
	out.EventID, err = uuid.ParseBytes(eventIDBytes)
	if err != nil {
		return out, fmt.Errorf("invalid %s header", EventID)
	}

	eventTypeBytes, err := RequiredHeaderValue(hs, EventType)
	if err != nil {
		return out, err
	}
	out.EventType = string(eventTypeBytes)

	eventVersionBytes, err := RequiredHeaderValue(hs, EventVersion)
	if err != nil {
		return out, err
	}
	v64, err := strconv.ParseUint(string(eventVersionBytes), 10, 32)
	if err != nil {
		return MessageRequiredHeaders{}, fmt.Errorf("invalid %s header", EventVersion)
	}
	out.EventVersion = uint(v64)

	producerBytes, err := RequiredHeaderValue(hs, Producer)
	if err != nil {
		return out, err
	}
	out.Producer = string(producerBytes)

	contentTypeBytes, err := RequiredHeaderValue(hs, ContentType)
	if err != nil {
		return out, err
	}
	out.ContentType = string(contentTypeBytes)

	return out, nil
}
