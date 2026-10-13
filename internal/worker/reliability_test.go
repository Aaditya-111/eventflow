package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Aaditya-111/eventflow/internal/model"
)

type fakeDeadLetters struct {
	calls    int
	message  StreamMessage
	attempts int
	reason   string
	err      error
}

func (f *fakeDeadLetters) Publish(
	_ context.Context,
	message StreamMessage,
	attempts int,
	reason string,
) error {
	f.calls++
	f.message = message
	f.attempts = attempts
	f.reason = reason
	return f.err
}

func immediateRetryPolicy(maxAttempts int) RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    maxAttempts,
		InitialBackoff: 0,
		MaxBackoff:     0,
	}
}

func TestProcessWithRetrySucceedsOnSecondAttempt(t *testing.T) {
	acker := &fakeAcker{}
	dlq := &fakeDeadLetters{}
	processor := NewProcessor(acker)
	calls := 0

	err := processor.ProcessWithRetry(
		context.Background(),
		validTestMessage(),
		func(_ context.Context, _ model.Event) error {
			calls++
			if calls == 1 {
				return errors.New("temporary failure")
			}
			return nil
		},
		immediateRetryPolicy(3),
		dlq,
	)
	if err != nil {
		t.Fatalf("ProcessWithRetry returned error: %v", err)
	}
	if calls != 2 {
		t.Fatalf("handler calls = %d; want 2", calls)
	}
	if acker.calls != 1 {
		t.Fatalf("ACK calls = %d; want 1", acker.calls)
	}
	if dlq.calls != 0 {
		t.Fatalf("dead-letter calls = %d; want 0", dlq.calls)
	}
}

func TestProcessWithRetryDeadLettersAfterExhaustion(t *testing.T) {
	acker := &fakeAcker{}
	dlq := &fakeDeadLetters{}
	processor := NewProcessor(acker)
	handlerErr := errors.New("persistent failure")
	calls := 0

	err := processor.ProcessWithRetry(
		context.Background(),
		validTestMessage(),
		func(context.Context, model.Event) error {
			calls++
			return handlerErr
		},
		immediateRetryPolicy(3),
		dlq,
	)
	if err != nil {
		t.Fatalf("ProcessWithRetry returned error: %v", err)
	}
	if calls != 3 {
		t.Fatalf("handler calls = %d; want 3", calls)
	}
	if dlq.calls != 1 {
		t.Fatalf("dead-letter calls = %d; want 1", dlq.calls)
	}
	if dlq.attempts != 3 {
		t.Fatalf("dead-letter attempts = %d; want 3", dlq.attempts)
	}
	if dlq.reason == "" {
		t.Fatal("dead-letter reason should not be empty")
	}
	if acker.calls != 1 {
		t.Fatalf("ACK calls = %d; want 1 after dead-letter transfer", acker.calls)
	}
}

func TestProcessWithRetryKeepsMessagePendingIfDeadLetterFails(t *testing.T) {
	acker := &fakeAcker{}
	dlq := &fakeDeadLetters{err: errors.New("Redis unavailable")}
	processor := NewProcessor(acker)

	err := processor.ProcessWithRetry(
		context.Background(),
		validTestMessage(),
		func(context.Context, model.Event) error {
			return errors.New("permanent failure")
		},
		immediateRetryPolicy(2),
		dlq,
	)
	if err == nil {
		t.Fatal("expected dead-letter failure")
	}
	if acker.calls != 0 {
		t.Fatalf("ACK calls = %d; want 0", acker.calls)
	}
}

func TestProcessWithRetryDeadLettersInvalidPayload(t *testing.T) {
	acker := &fakeAcker{}
	dlq := &fakeDeadLetters{}
	processor := NewProcessor(acker)
	message := validTestMessage()
	message.Values["data"] = `{invalid}`

	err := processor.ProcessWithRetry(
		context.Background(),
		message,
		func(context.Context, model.Event) error {
			t.Fatal("handler must not run for invalid payload")
			return nil
		},
		immediateRetryPolicy(3),
		dlq,
	)
	if err != nil {
		t.Fatalf("invalid message should be dead-lettered: %v", err)
	}
	if dlq.calls != 1 {
		t.Fatalf("dead-letter calls = %d; want 1", dlq.calls)
	}
	if dlq.attempts != 0 {
		t.Fatalf("attempts = %d; want 0 for invalid payload", dlq.attempts)
	}
	if acker.calls != 1 {
		t.Fatalf("ACK calls = %d; want 1 after dead-letter transfer", acker.calls)
	}
}

func TestRetryDelayIsBounded(t *testing.T) {
	policy := RetryPolicy{
		MaxAttempts:    5,
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     4 * time.Second,
	}

	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: time.Second},
		{attempt: 2, want: 2 * time.Second},
		{attempt: 3, want: 4 * time.Second},
		{attempt: 4, want: 4 * time.Second},
	}

	for _, tc := range cases {
		if got := retryDelay(policy, tc.attempt); got != tc.want {
			t.Errorf("retryDelay(%d) = %v; want %v", tc.attempt, got, tc.want)
		}
	}
}
