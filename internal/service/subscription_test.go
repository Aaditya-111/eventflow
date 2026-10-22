package service

import (
	"context"
	"errors"
	"testing"

	"github.com/Aaditya-111/eventflow/internal/model"
	"github.com/Aaditya-111/eventflow/internal/security"
)

type fakeSubscriptionStore struct {
	sub             model.WebhookSubscription
	encryptedSecret []byte
	err             error
	calls           int
}

func (f *fakeSubscriptionStore) Create(
	_ context.Context,
	sub model.WebhookSubscription,
	encryptedSecret []byte,
) error {
	f.calls++
	f.sub = sub
	f.encryptedSecret = append([]byte(nil), encryptedSecret...)
	return f.err
}

func newTestSubscriptionService(
	t *testing.T,
	store SubscriptionStore,
) (*SubscriptionService, *security.SecretManager) {
	t.Helper()

	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}

	manager, err := security.NewSecretManager(key)
	if err != nil {
		t.Fatalf("create secret manager: %v", err)
	}

	svc, err := NewSubscriptionService(store, manager)
	if err != nil {
		t.Fatalf("create subscription service: %v", err)
	}

	return svc, manager
}

func validSubscription() model.WebhookSubscription {
	return model.WebhookSubscription{
		SubscriptionID: "sub_test_001",
		EndpointURL:    "https://example.com/webhooks",
		EventTypes:     []string{"order.created"},
		Active:         true,
	}
}

func validSigningSecret() []byte {
	return []byte("0123456789abcdef0123456789abcdef")
}

func TestCreateEncryptsSecretBeforePersistence(t *testing.T) {
	store := &fakeSubscriptionStore{}
	svc, manager := newTestSubscriptionService(t, store)

	plaintext := validSigningSecret()

	err := svc.Create(
		context.Background(),
		validSubscription(),
		plaintext,
	)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if store.calls != 1 {
		t.Fatalf("store calls = %d, want 1", store.calls)
	}

	if string(store.encryptedSecret) == string(plaintext) {
		t.Fatal("plaintext secret was passed to the store")
	}

	decrypted, err := manager.Decrypt(store.encryptedSecret)
	if err != nil {
		t.Fatalf("decrypt stored secret: %v", err)
	}

	if string(decrypted) != string(plaintext) {
		t.Fatal("decrypted secret does not match original")
	}

	if store.sub.SubscriptionID != "sub_test_001" {
		t.Errorf("stored subscription ID = %q", store.sub.SubscriptionID)
	}
}

func TestCreateRejectsInvalidSigningSecretLength(t *testing.T) {
	tests := []struct {
		name   string
		secret []byte
	}{
		{
			name:   "too short",
			secret: []byte("short"),
		},
		{
			name:   "too long",
			secret: make([]byte, 257),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeSubscriptionStore{}
			svc, _ := newTestSubscriptionService(t, store)

			err := svc.Create(
				context.Background(),
				validSubscription(),
				tt.secret,
			)

			if !errors.Is(err, ErrInvalidSubscription) {
				t.Fatalf("Create() error = %v, want ErrInvalidSubscription", err)
			}

			if store.calls != 0 {
				t.Fatal("store called for invalid signing secret")
			}
		})
	}
}

func TestCreateRejectsInvalidEndpoint(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{name: "empty", url: ""},
		{name: "missing scheme", url: "example.com/webhook"},
		{name: "unsupported scheme", url: "ftp://example.com/webhook"},
		{name: "missing hostname", url: "https:///webhook"},
		{name: "embedded credentials", url: "https://user:pass@example.com/webhook"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeSubscriptionStore{}
			svc, _ := newTestSubscriptionService(t, store)

			sub := validSubscription()
			sub.EndpointURL = tt.url

			err := svc.Create(
				context.Background(),
				sub,
				validSigningSecret(),
			)

			if !errors.Is(err, ErrInvalidSubscription) {
				t.Fatalf("Create() error = %v, want ErrInvalidSubscription", err)
			}

			if store.calls != 0 {
				t.Fatal("store called for invalid endpoint")
			}
		})
	}
}

func TestCreateRejectsEmptyOrDuplicateEventTypes(t *testing.T) {
	tests := []struct {
		name       string
		eventTypes []string
	}{
		{name: "empty list", eventTypes: []string{}},
		{name: "blank type", eventTypes: []string{"  "}},
		{name: "duplicate types", eventTypes: []string{"order.created", "order.created"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeSubscriptionStore{}
			svc, _ := newTestSubscriptionService(t, store)

			sub := validSubscription()
			sub.EventTypes = tt.eventTypes

			err := svc.Create(
				context.Background(),
				sub,
				validSigningSecret(),
			)

			if !errors.Is(err, ErrInvalidSubscription) {
				t.Fatalf("Create() error = %v, want ErrInvalidSubscription", err)
			}

			if store.calls != 0 {
				t.Fatal("store called for invalid event types")
			}
		})
	}
}

func TestCreatePropagatesStoreError(t *testing.T) {
	storeErr := errors.New("database unavailable")
	store := &fakeSubscriptionStore{err: storeErr}
	svc, _ := newTestSubscriptionService(t, store)

	err := svc.Create(
		context.Background(),
		validSubscription(),
		validSigningSecret(),
	)

	if !errors.Is(err, storeErr) {
		t.Fatalf("Create() error = %v, want wrapped store error", err)
	}

	if store.calls != 1 {
		t.Fatalf("store calls = %d, want 1", store.calls)
	}
}
