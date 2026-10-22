package service

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/Aaditya-111/eventflow/internal/model"
	"github.com/Aaditya-111/eventflow/internal/repository"
	"github.com/Aaditya-111/eventflow/internal/security"
)

var ErrInvalidSubscription = errors.New("invalid webhook subscription")

type SubscriptionStore interface {
	Create(
		ctx context.Context,
		sub model.WebhookSubscription,
		encryptedSecret []byte,
	) error
}

type SecretEncryptor interface {
	Encrypt(plaintext []byte) ([]byte, error)
}

type SubscriptionService struct {
	store     SubscriptionStore
	encryptor SecretEncryptor
}

func NewSubscriptionService(
	store SubscriptionStore,
	encryptor SecretEncryptor,
) (*SubscriptionService, error) {
	if store == nil {
		return nil, errors.New("subscription store is required")
	}
	if encryptor == nil {
		return nil, errors.New("secret encryptor is required")
	}

	return &SubscriptionService{
		store:     store,
		encryptor: encryptor,
	}, nil
}

// Create encrypts the signing secret before passing it to persistence.
func (s *SubscriptionService) Create(
	ctx context.Context,
	sub model.WebhookSubscription,
	plaintextSecret []byte,
) error {
	if s == nil || s.store == nil || s.encryptor == nil {
		return errors.New("subscription service is not configured")
	}
	if len(plaintextSecret) < 32 || len(plaintextSecret) > 256 {
		return fmt.Errorf(
			"%w: signing secret must contain 32 to 256 bytes",
			ErrInvalidSubscription,
		)
	}

	sub.SubscriptionID = strings.TrimSpace(sub.SubscriptionID)
	if sub.SubscriptionID == "" || len(sub.SubscriptionID) > 200 {
		return fmt.Errorf("%w: invalid subscription ID", ErrInvalidSubscription)
	}

	parsed, err := url.ParseRequestURI(sub.EndpointURL)
	if err != nil ||
		parsed.Hostname() == "" ||
		(parsed.Scheme != "https" && parsed.Scheme != "http") ||
		parsed.User != nil {
		return fmt.Errorf("%w: invalid endpoint URL", ErrInvalidSubscription)
	}

	if len(sub.EventTypes) == 0 {
		return fmt.Errorf("%w: at least one event type is required", ErrInvalidSubscription)
	}

	seen := make(map[string]struct{}, len(sub.EventTypes))
	for i, eventType := range sub.EventTypes {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" || len(eventType) > 200 {
			return fmt.Errorf("%w: invalid event type", ErrInvalidSubscription)
		}
		if _, exists := seen[eventType]; exists {
			return fmt.Errorf("%w: duplicate event type", ErrInvalidSubscription)
		}
		seen[eventType] = struct{}{}
		sub.EventTypes[i] = eventType
	}

	encrypted, err := s.encryptor.Encrypt(plaintextSecret)
	if err != nil {
		return fmt.Errorf("encrypt webhook signing secret: %w", err)
	}
	if len(encrypted) == 0 {
		return errors.New("secret encryptor returned empty ciphertext")
	}

	if err := s.store.Create(ctx, sub, encrypted); err != nil {
		return err
	}

	return nil
}

// Compile-time check that the repository matches the service interface.
var _ SubscriptionStore = (*repository.SubscriptionRepository)(nil)

// Compile-time check that SecretManager matches SecretEncryptor.
var _ SecretEncryptor = (*security.SecretManager)(nil)
