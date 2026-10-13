package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Aaditya-111/eventflow/internal/model"
)

type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    5,
		InitialBackoff: 500 * time.Millisecond,
		MaxBackoff:     8 * time.Second,
	}
}

// ProcessWithRetry retries handler failures, then transfers exhausted or
// invalid messages to the dead-letter stream. It ACKs only after success
// or after the dead-letter sink confirms the transfer.
func (p *Processor) ProcessWithRetry(
	ctx context.Context,
	message StreamMessage,
	handler EventHandler,
	policy RetryPolicy,
	deadLetters DeadLetterSink,
) error {
	if p == nil || p.acker == nil {
		return errors.New("processor acknowledger is not configured")
	}
	if handler == nil {
		return errors.New("event handler is not configured")
	}
	if deadLetters == nil {
		return errors.New("dead-letter sink is not configured")
	}
	if message.ID == "" {
		return errors.New("stream message ID is empty")
	}
	if policy.MaxAttempts < 1 {
		return errors.New("maximum attempts must be at least 1")
	}
	if policy.InitialBackoff < 0 || policy.MaxBackoff < 0 {
		return errors.New("backoff durations cannot be negative")
	}
	if policy.MaxBackoff < policy.InitialBackoff {
		return errors.New("maximum backoff cannot be less than initial backoff")
	}

	event, err := decodeStreamEvent(message)
	if err != nil {
		return p.deadLetterAndAck(ctx, message, deadLetters, 0, err)
	}

	var processingErr error
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("processing cancelled; message remains pending: %w", err)
		}

		processingErr = handler(ctx, event)
		if processingErr == nil {
			if err := p.acker.Ack(ctx, message.ID); err != nil {
				return fmt.Errorf("acknowledge processed event %q: %w", event.EventID, err)
			}
			return nil
		}

		if attempt < policy.MaxAttempts {
			delay := retryDelay(policy, attempt)
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return fmt.Errorf("processing cancelled; message remains pending: %w", ctx.Err())
			case <-timer.C:
			}
		}
	}

	return p.deadLetterAndAck(ctx, message, deadLetters, policy.MaxAttempts, processingErr)
}

func decodeStreamEvent(message StreamMessage) (model.Event, error) {
	eventID, err := requiredString(message.Values, "event_id")
	if err != nil {
		return model.Event{}, err
	}
	eventType, err := requiredString(message.Values, "type")
	if err != nil {
		return model.Event{}, err
	}
	data, err := requiredString(message.Values, "data")
	if err != nil {
		return model.Event{}, err
	}
	if !json.Valid([]byte(data)) {
		return model.Event{}, errors.New("event data is not valid JSON")
	}

	var decoded interface{}
	if err := json.Unmarshal([]byte(data), &decoded); err != nil {
		return model.Event{}, fmt.Errorf("decode event data: %w", err)
	}
	switch decoded.(type) {
	case map[string]interface{}, []interface{}:
	default:
		return model.Event{}, errors.New("event data must be a JSON object or array")
	}

	return model.Event{
		EventID: eventID,
		Type:    eventType,
		Data:    json.RawMessage(data),
	}, nil
}

func (p *Processor) deadLetterAndAck(
	ctx context.Context,
	message StreamMessage,
	sink DeadLetterSink,
	attempts int,
	cause error,
) error {
	if cause == nil {
		cause = errors.New("unspecified processing failure")
	}
	if err := sink.Publish(ctx, message, attempts, cause.Error()); err != nil {
		return fmt.Errorf("dead-letter transfer failed; original message remains pending: %w", err)
	}
	if err := p.acker.Ack(ctx, message.ID); err != nil {
		return fmt.Errorf("dead-letter published but original ACK failed; duplicate transfer is possible: %w", err)
	}
	return nil
}

func retryDelay(policy RetryPolicy, attempt int) time.Duration {
	delay := policy.InitialBackoff
	for i := 1; i < attempt; i++ {
		if delay >= policy.MaxBackoff/2 {
			return policy.MaxBackoff
		}
		delay *= 2
	}
	if delay > policy.MaxBackoff {
		return policy.MaxBackoff
	}
	return delay
}
