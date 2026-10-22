package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Aaditya-111/eventflow/internal/repository"
	"github.com/Aaditya-111/eventflow/internal/security"
)

type fakeDeliveryClaimer struct{}

func (f *fakeDeliveryClaimer) ClaimReady(
	context.Context, string, int, time.Duration,
) ([]repository.WebhookDelivery, error) {
	return nil, nil
}

type fakeDeliveryEvents struct {
	event repository.Event
	err   error
}

func (f *fakeDeliveryEvents) GetByID(
	context.Context, string,
) (repository.Event, error) {
	return f.event, f.err
}

type fakeDeliveryTargets struct {
	target repository.WebhookDeliveryTarget
	err    error
}

func (f *fakeDeliveryTargets) GetActiveDeliveryTarget(
	context.Context, string,
) (repository.WebhookDeliveryTarget, error) {
	return f.target, f.err
}

type fakeDeliveryStates struct {
	delivered int
	retried   int
	dead      int

	statusCode int
	lastError  string
	nextRetry  time.Time
	err        error
}

func (f *fakeDeliveryStates) MarkDeliveredByWorker(
	context.Context, int64, string, int,
) error {
	f.delivered++
	return f.err
}

func (f *fakeDeliveryStates) ScheduleRetryByWorker(
	_ context.Context,
	_ int64,
	_ string,
	statusCode int,
	lastError string,
	nextAttemptAt time.Time,
) error {
	f.retried++
	f.statusCode = statusCode
	f.lastError = lastError
	f.nextRetry = nextAttemptAt
	return f.err
}

func (f *fakeDeliveryStates) MarkDeadByWorker(
	_ context.Context,
	_ int64,
	_ string,
	statusCode int,
	lastError string,
) error {
	f.dead++
	f.statusCode = statusCode
	f.lastError = lastError
	return f.err
}

type fakeDeliverySecrets struct {
	secret []byte
	err    error
}

func (f *fakeDeliverySecrets) Decrypt([]byte) ([]byte, error) {
	return append([]byte(nil), f.secret...), f.err
}

type fakeDeliverySender struct {
	result WebhookResult
	err    error
	calls  int
}

func (f *fakeDeliverySender) Send(
	context.Context,
	repository.WebhookDeliveryTarget,
	repository.Event,
	[]byte,
) (WebhookResult, error) {
	f.calls++
	return f.result, f.err
}

func newTestDeliveryWorker(
	t *testing.T,
	eventErr error,
	targetErr error,
	decryptErr error,
	result WebhookResult,
	sendErr error,
	attempts int,
) (*DeliveryWorker, *fakeDeliveryStates, *fakeDeliverySender) {
	t.Helper()

	states := &fakeDeliveryStates{}
	sender := &fakeDeliverySender{
		result: result,
		err:    sendErr,
	}

	worker := newTestDeliveryWorkerWithDependencies(
		t, states, sender, eventErr, targetErr, decryptErr,
	)

	worker.process(context.Background(), repository.WebhookDelivery{
		ID:             123,
		EventID:        "evt-test",
		SubscriptionID: "sub-test",
		Attempts:       attempts,
	})

	return worker, states, sender
}

func newTestDeliveryWorkerWithoutProcessing(
	t *testing.T,
	states *fakeDeliveryStates,
	sender *fakeDeliverySender,
) *DeliveryWorker {
	t.Helper()

	return newTestDeliveryWorkerWithDependencies(
		t, states, sender, nil, nil, nil,
	)
}

func newTestDeliveryWorkerWithDependencies(
	t *testing.T,
	states *fakeDeliveryStates,
	sender *fakeDeliverySender,
	eventErr error,
	targetErr error,
	decryptErr error,
) *DeliveryWorker {
	t.Helper()

	events := &fakeDeliveryEvents{
		event: repository.Event{
			EventID: "evt-test",
			Type:    "order.created",
			Data:    []byte(`{"order_id":"order-test"}`),
		},
		err: eventErr,
	}

	targets := &fakeDeliveryTargets{
		target: repository.WebhookDeliveryTarget{
			SubscriptionID:  "sub-test",
			EndpointURL:     "https://example.com/webhook",
			EncryptedSecret: []byte("fake-encrypted-secret"),
		},
		err: targetErr,
	}

	secrets := &fakeDeliverySecrets{
		secret: []byte("test-signing-secret"),
		err:    decryptErr,
	}

	worker, err := NewDeliveryWorker(
		"test-worker",
		&fakeDeliveryClaimer{},
		events,
		targets,
		states,
		secrets,
		sender,
	)
	if err != nil {
		t.Fatalf("NewDeliveryWorker() error = %v", err)
	}

	return worker
}

func TestDeliveryWorkerSuccessfulDelivery(t *testing.T) {
	_, states, sender := newTestDeliveryWorker(
		t, nil, nil, nil,
		WebhookResult{StatusCode: 204},
		nil, 0,
	)

	if states.delivered != 1 || states.retried != 0 || states.dead != 0 {
		t.Fatalf(
			"unexpected state counts: delivered=%d retry=%d dead=%d",
			states.delivered, states.retried, states.dead,
		)
	}
	if sender.calls != 1 {
		t.Fatalf("sender calls = %d, want 1", sender.calls)
	}
}

func TestDeliveryWorkerRetriesTransientSenderError(t *testing.T) {
	_, states, _ := newTestDeliveryWorker(
		t, nil, nil, nil,
		WebhookResult{StatusCode: 503},
		errors.New("temporary network failure"), 0,
	)

	if states.retried != 1 || states.dead != 0 || states.delivered != 0 {
		t.Fatalf(
			"unexpected state counts: delivered=%d retry=%d dead=%d",
			states.delivered, states.retried, states.dead,
		)
	}
	if time.Until(states.nextRetry) <= 0 {
		t.Fatal("retry time must be in the future")
	}
}

func TestDeliveryWorkerRetryAfterIsCappedAtFiveMinutes(t *testing.T) {
	_, states, _ := newTestDeliveryWorker(
		t, nil, nil, nil,
		WebhookResult{
			StatusCode: 503,
			RetryAfter: 30 * time.Minute,
		},
		errors.New("webhook returned HTTP 503"), 0,
	)

	if states.retried != 1 {
		t.Fatalf("retry count = %d, want 1", states.retried)
	}

	delay := time.Until(states.nextRetry)
	if delay > 5*time.Minute || delay < 4*time.Minute+55*time.Second {
		t.Fatalf("retry delay = %v, want approximately 5 minutes", delay)
	}
}

func TestDeliveryWorkerPermanentHTTPErrorMarksDead(t *testing.T) {
	_, states, _ := newTestDeliveryWorker(
		t, nil, nil, nil,
		WebhookResult{StatusCode: 400},
		errors.New("webhook returned HTTP 400"), 0,
	)

	if states.dead != 1 || states.retried != 0 {
		t.Fatalf("dead=%d retry=%d; want dead=1 retry=0",
			states.dead, states.retried)
	}
}

func TestDeliveryWorkerMissingEventMarksDead(t *testing.T) {
	_, states, sender := newTestDeliveryWorker(
		t, fmt.Errorf("lookup: %w", sql.ErrNoRows),
		nil, nil, WebhookResult{StatusCode: 200}, nil, 0,
	)

	if states.dead != 1 || states.retried != 0 {
		t.Fatalf("dead=%d retry=%d; want dead=1 retry=0",
			states.dead, states.retried)
	}
	if sender.calls != 0 {
		t.Fatalf("sender calls = %d, want 0", sender.calls)
	}
}

func TestDeliveryWorkerTransientEventLookupErrorRetries(t *testing.T) {
	_, states, sender := newTestDeliveryWorker(
		t, errors.New("database temporarily unavailable"),
		nil, nil, WebhookResult{}, nil, 0,
	)

	if states.retried != 1 || states.dead != 0 {
		t.Fatalf("retry=%d dead=%d; want retry=1 dead=0",
			states.retried, states.dead)
	}
	if sender.calls != 0 {
		t.Fatalf("sender calls = %d, want 0", sender.calls)
	}
}

func TestDeliveryWorkerMissingSubscriptionMarksDead(t *testing.T) {
	_, states, sender := newTestDeliveryWorker(
		t, nil, fmt.Errorf("subscription: %w", sql.ErrNoRows),
		nil, WebhookResult{}, nil, 0,
	)

	if states.dead != 1 || states.retried != 0 {
		t.Fatalf("dead=%d retry=%d; want dead=1 retry=0",
			states.dead, states.retried)
	}
	if sender.calls != 0 {
		t.Fatalf("sender calls = %d, want 0", sender.calls)
	}
}

func TestDeliveryWorkerDecryptionFailureMarksDead(t *testing.T) {
	_, states, sender := newTestDeliveryWorker(
		t, nil, nil, errors.New("decryption failed"),
		WebhookResult{}, nil, 0,
	)

	if states.dead != 1 || states.retried != 0 {
		t.Fatalf("dead=%d retry=%d; want dead=1 retry=0",
			states.dead, states.retried)
	}
	if sender.calls != 0 {
		t.Fatalf("sender calls = %d, want 0", sender.calls)
	}
}

func TestDeliveryWorkerMaximumAttemptsMarksDead(t *testing.T) {
	_, states, _ := newTestDeliveryWorker(
		t, nil, nil, nil,
		WebhookResult{StatusCode: 503},
		errors.New("temporary network failure"), deliveryMaxTries-1,
	)

	if states.dead != 1 || states.retried != 0 {
		t.Fatalf("dead=%d retry=%d; want dead=1 retry=0",
			states.dead, states.retried)
	}
}

func TestDeliveryWorkerUnsafeDestinationMarksDead(t *testing.T) {
	_, states, _ := newTestDeliveryWorker(
		t, nil, nil, nil,
		WebhookResult{},
		fmt.Errorf("validate webhook destination: %w: private IP",
			security.ErrUnsafeWebhookURL),
		0,
	)

	if states.dead != 1 || states.retried != 0 {
		t.Fatalf("dead=%d retry=%d; want dead=1 retry=0",
			states.dead, states.retried)
	}
}

func TestDeliveryWorkerDoesNotPersistRawSenderError(t *testing.T) {
	_, states, _ := newTestDeliveryWorker(
		t, nil, nil, nil,
		WebhookResult{StatusCode: 502},
		errors.New(strings.Repeat("x", 300)), 0,
	)

	if states.retried != 1 {
		t.Fatalf("retry count = %d, want 1", states.retried)
	}
	if len(states.lastError) > 200 {
		t.Fatalf("persisted error length = %d, want <= 200", len(states.lastError))
	}
	if strings.Contains(states.lastError, strings.Repeat("x", 300)) {
		t.Fatal("raw sender error should not be persisted")
	}
}

func TestDeliveryWorkerLogsSuccessfulSendWhenStatePersistenceFails(t *testing.T) {
	states := &fakeDeliveryStates{
		err: errors.New("database temporarily unavailable"),
	}
	sender := &fakeDeliverySender{
		result: WebhookResult{StatusCode: 204},
	}

	worker := newTestDeliveryWorkerWithoutProcessing(t, states, sender)

	worker.process(context.Background(), repository.WebhookDelivery{
		ID:             123,
		EventID:        "evt-test",
		SubscriptionID: "sub-test",
		Attempts:       0,
	})

	if sender.calls != 1 {
		t.Fatalf("sender calls = %d, want 1", sender.calls)
	}
	if states.delivered != 1 {
		t.Fatalf("MarkDeliveredByWorker calls = %d, want 1", states.delivered)
	}
	if states.retried != 0 || states.dead != 0 {
		t.Fatalf("retry=%d dead=%d; want both 0", states.retried, states.dead)
	}
}

func TestDeliveryRetryDelayUsesExponentialBackoff(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: 1 * time.Second},
		{attempt: 2, want: 2 * time.Second},
		{attempt: 3, want: 4 * time.Second},
		{attempt: 4, want: 8 * time.Second},
		{attempt: 5, want: 16 * time.Second},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("attempt_%d", tt.attempt), func(t *testing.T) {
			if got := deliveryRetryDelay(tt.attempt); got != tt.want {
				t.Errorf("deliveryRetryDelay(%d) = %v, want %v",
					tt.attempt, got, tt.want)
			}
		})
	}
}

func TestDeliveryRetryDelayIsCappedAtFiveMinutes(t *testing.T) {
	got := deliveryRetryDelay(100)

	if got != deliveryMaxRetry {
		t.Fatalf("deliveryRetryDelay(100) = %v, want %v",
			got, deliveryMaxRetry)
	}
}

func TestDeliveryWorkerRetriesHTTP503WithoutSenderError(t *testing.T) {
	_, states, sender := newTestDeliveryWorker(
		t, nil, nil, nil,
		WebhookResult{StatusCode: 503},
		nil, 0,
	)

	if sender.calls != 1 {
		t.Fatalf("sender calls = %d, want 1", sender.calls)
	}
	if states.retried != 1 || states.dead != 0 || states.delivered != 0 {
		t.Fatalf("unexpected states: delivered=%d retry=%d dead=%d",
			states.delivered, states.retried, states.dead)
	}
	if states.statusCode != 503 {
		t.Fatalf("recorded status code = %d, want 503", states.statusCode)
	}
}

func TestDeliveryWorkerRetriesHTTP429(t *testing.T) {
	_, states, _ := newTestDeliveryWorker(
		t, nil, nil, nil,
		WebhookResult{
			StatusCode: 429,
			RetryAfter: 2 * time.Second,
		},
		nil, 0,
	)

	if states.retried != 1 || states.dead != 0 {
		t.Fatalf("retry=%d dead=%d; want retry=1 dead=0",
			states.retried, states.dead)
	}

	delay := time.Until(states.nextRetry)
	if delay < time.Second || delay > 2*time.Second {
		t.Fatalf("retry delay = %v, want approximately 2 seconds", delay)
	}
}

func TestPermanentHTTPStatusClassification(t *testing.T) {
	tests := []struct {
		status int
		want   bool
	}{
		{status: 200, want: false},
		{status: 400, want: true},
		{status: 401, want: true},
		{status: 408, want: false},
		{status: 425, want: false},
		{status: 429, want: false},
		{status: 500, want: false},
		{status: 503, want: false},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("status_%d", tt.status), func(t *testing.T) {
			if got := isPermanentHTTPStatus(tt.status); got != tt.want {
				t.Errorf("isPermanentHTTPStatus(%d) = %v, want %v",
					tt.status, got, tt.want)
			}
		})
	}
}
