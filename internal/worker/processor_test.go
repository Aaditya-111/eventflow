package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/Aaditya-111/eventflow/internal/model"
)

type fakeAcker struct {
	calls int
	id    string
	err   error
}

func (f *fakeAcker) Ack(_ context.Context, messageID string) error {
	f.calls++
	f.id = messageID
	return f.err
}

func validTestMessage() StreamMessage {
	return StreamMessage{
		ID: "1740000000000-0",
		Values: map[string]interface{}{
			"event_id": "evt_processor_test",
			"type":     "user.created",
			"data":     `{"user_id":"test-user"}`,
		},
	}
}

func TestProcessorAcknowledgesAfterSuccessfulHandling(t *testing.T) {
	acker := &fakeAcker{}
	processor := NewProcessor(acker)

	err := processor.Process(context.Background(), validTestMessage(),
		func(_ context.Context, event model.Event) error {
			if event.EventID != "evt_processor_test" {
				t.Fatalf("unexpected event ID: %q", event.EventID)
			}
			if event.Type != "user.created" {
				t.Fatalf("unexpected event type: %q", event.Type)
			}
			if string(event.Data) != `{"user_id":"test-user"}` {
				t.Fatalf("unexpected event data: %s", event.Data)
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}
	if acker.calls != 1 {
		t.Fatalf("Ack calls = %d; want 1", acker.calls)
	}
	if acker.id != "1740000000000-0" {
		t.Fatalf("acknowledged ID = %q", acker.id)
	}
}

func TestProcessorDoesNotAcknowledgeFailedHandling(t *testing.T) {
	acker := &fakeAcker{}
	processor := NewProcessor(acker)
	handlerErr := errors.New("simulated processing failure")

	err := processor.Process(context.Background(), validTestMessage(),
		func(context.Context, model.Event) error {
			return handlerErr
		},
	)
	if !errors.Is(err, handlerErr) {
		t.Fatalf("expected handler error to be preserved, got: %v", err)
	}
	if acker.calls != 0 {
		t.Fatalf("Ack called %d times after failed handling; want 0", acker.calls)
	}
}

func TestProcessorRejectsInvalidJSONWithoutAcknowledging(t *testing.T) {
	acker := &fakeAcker{}
	processor := NewProcessor(acker)
	message := validTestMessage()
	message.Values["data"] = `{invalid-json}`

	err := processor.Process(context.Background(), message,
		func(context.Context, model.Event) error {
			t.Fatal("handler must not run for invalid JSON")
			return nil
		},
	)
	if err == nil {
		t.Fatal("expected invalid JSON error")
	}
	if acker.calls != 0 {
		t.Fatalf("Ack called %d times for invalid message; want 0", acker.calls)
	}
}

func TestProcessorReturnsAckError(t *testing.T) {
	ackErr := errors.New("simulated Redis acknowledgement failure")
	acker := &fakeAcker{err: ackErr}
	processor := NewProcessor(acker)

	err := processor.Process(context.Background(), validTestMessage(),
		func(context.Context, model.Event) error {
			return nil
		},
	)
	if !errors.Is(err, ackErr) {
		t.Fatalf("expected ack error to be preserved, got: %v", err)
	}
	if acker.calls != 1 {
		t.Fatalf("Ack calls = %d; want 1", acker.calls)
	}
}
