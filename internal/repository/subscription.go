package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/Aaditya-111/eventflow/internal/model"
)

var ErrInvalidSubscription = errors.New("invalid webhook subscription")

type SubscriptionRepository struct {
	db *sql.DB
}

func NewSubscriptionRepository(db *sql.DB) *SubscriptionRepository {
	return &SubscriptionRepository{db: db}
}

func (r *SubscriptionRepository) Create(
	ctx context.Context,
	sub model.WebhookSubscription,
	encryptedSecret []byte,
) error {
	if r == nil || r.db == nil {
		return errors.New("subscription database is not configured")
	}

	sub.SubscriptionID = strings.TrimSpace(sub.SubscriptionID)
	if sub.SubscriptionID == "" || len(sub.SubscriptionID) > 200 {
		return fmt.Errorf("%w: invalid subscription ID", ErrInvalidSubscription)
	}

	if len(encryptedSecret) == 0 {
		return fmt.Errorf("%w: encrypted secret is required", ErrInvalidSubscription)
	}

	parsed, err := url.ParseRequestURI(sub.EndpointURL)
	if err != nil ||
		parsed.Hostname() == "" ||
		(parsed.Scheme != "https" && parsed.Scheme != "http") {
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

	_, err = r.db.ExecContext(ctx, `
		INSERT INTO webhook_subscriptions (
			subscription_id,
			endpoint_url,
			secret_encrypted,
			event_types
		)
		VALUES ($1, $2, $3, $4)
	`,
		sub.SubscriptionID,
		sub.EndpointURL,
		encryptedSecret,
		sub.EventTypes,
	)
	if err != nil {
		return fmt.Errorf("insert webhook subscription: %w", err)
	}

	return nil
}
