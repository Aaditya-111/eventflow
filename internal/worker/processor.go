package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Aaditya-111/eventflow/internal/model"
)

type Acknowledger interface {
	Ack(ctx context.Context, messageID string) error
}

type EventHandler func(context.Context, model.Event) error

type Processor struct {
	acker Acknowledger
}

func NewProcessor(acker Acknowledger) *Processor {
	return &Processor{acker: acker}
}

// Process validates and handles one stream message.
// The message is acknowledged only if the handler succeeds.
func (p *Processor) Process(
	ctx context.Context,
	message StreamMessage,
	handler EventHandler,
) error {
	if p == nil || p.acker == nil {
		return errors.New("processor acknowledger is not configured")
	}
	if handler == nil {
		return errors.New("event handler is not configured")
	}
	if message.ID == "" {
		return errors.New("stream message ID is empty")
	}

	eventID, err := requiredString(message.Values, "event_id")
	if err != nil {
		return err
	}

	eventType, err := requiredString(message.Values, "type")
	if err != nil {
		return err
	}

	data, err := requiredString(message.Values, "data")
	if err != nil {
		return err
	}
	if !json.Valid([]byte(data)) {
		return errors.New("event data is not valid JSON")
	}

	var decoded interface{}
	if err := json.Unmarshal([]byte(data), &decoded); err != nil {
		return fmt.Errorf("decode event data: %w", err)
	}
	switch decoded.(type) {
	case map[string]interface{}, []interface{}:
		// Supported event payloads are JSON objects or arrays.
	default:
		return errors.New("event data must be a JSON object or array")
	}

	event := model.Event{
		EventID: eventID,
		Type:    eventType,
		Data:    json.RawMessage(data),
	}

	if err := handler(ctx, event); err != nil {
		return fmt.Errorf("process event %q: %w", eventID, err)
	}

	if err := p.acker.Ack(ctx, message.ID); err != nil {
		return fmt.Errorf("acknowledge processed event %q: %w", eventID, err)
	}

	return nil
}

func requiredString(values map[string]interface{}, key string) (string, error) {
	value, ok := values[key]
	if !ok {
		return "", fmt.Errorf("stream message is missing %q", key)
	}

	switch v := value.(type) {
	case string:
		if v != "" {
			return v, nil
		}
	case []byte:
		if len(v) > 0 {
			return string(v), nil
		}
	}

	return "", fmt.Errorf("stream message field %q must be a non-empty string", key)
}
