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

// WebhookDeliveryTarget contains the private data needed to deliver an event.
// Keep this type inside the repository/delivery path, never in API responses.
type WebhookDeliveryTarget struct {
	SubscriptionID  string
	EndpointURL     string
	EncryptedSecret []byte
}

// ListActiveForEvent returns active subscriptions that match an event type.
// The signing secret remains encrypted in the returned result.
func (r *SubscriptionRepository) ListActiveForEvent(
	ctx context.Context,
	eventType string,
) ([]WebhookDeliveryTarget, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("subscription database is not configured")
	}

	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return nil, errors.New("event type is required")
	}

	rows, err := r.db.QueryContext(ctx, `
        SELECT subscription_id, endpoint_url, secret_encrypted
        FROM webhook_subscriptions
        WHERE active = TRUE
          AND $1 = ANY(event_types)
        ORDER BY subscription_id
    `, eventType)
	if err != nil {
		return nil, fmt.Errorf("list matching webhook subscriptions: %w", err)
	}
	defer rows.Close()

	targets := make([]WebhookDeliveryTarget, 0)

	for rows.Next() {
		var target WebhookDeliveryTarget

		if err := rows.Scan(
			&target.SubscriptionID,
			&target.EndpointURL,
			&target.EncryptedSecret,
		); err != nil {
			return nil, fmt.Errorf("scan webhook delivery target: %w", err)
		}

		targets = append(targets, target)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate webhook subscriptions: %w", err)
	}

	return targets, nil
}

func (r *SubscriptionRepository) GetActiveDeliveryTarget(
	ctx context.Context,
	subscriptionID string,
) (WebhookDeliveryTarget, error) {
	if r == nil || r.db == nil {
		return WebhookDeliveryTarget{}, errors.New(
			"subscription database is not configured",
		)
	}

	subscriptionID = strings.TrimSpace(subscriptionID)
	if subscriptionID == "" {
		return WebhookDeliveryTarget{}, errors.New(
			"subscription ID is required",
		)
	}

	var target WebhookDeliveryTarget
	err := r.db.QueryRowContext(ctx, `
		SELECT subscription_id, endpoint_url, secret_encrypted
		FROM webhook_subscriptions
		WHERE subscription_id = $1
		  AND active = TRUE
	`, subscriptionID).Scan(
		&target.SubscriptionID,
		&target.EndpointURL,
		&target.EncryptedSecret,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WebhookDeliveryTarget{}, fmt.Errorf(
				"active subscription %q not found: %w",
				subscriptionID,
				err,
			)
		}
		return WebhookDeliveryTarget{}, fmt.Errorf(
			"get active delivery target: %w",
			err,
		)
	}

	return target, nil
}
