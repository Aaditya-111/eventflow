package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Aaditya-111/eventflow/internal/model"
	"github.com/Aaditya-111/eventflow/internal/repository"
)

type SubscriptionLister interface {
	ListActiveForEvent(
		ctx context.Context,
		eventType string,
	) ([]repository.WebhookDeliveryTarget, error)
}

type DeliveryEnqueuer interface {
	Enqueue(ctx context.Context, eventID, subscriptionID string) error
}

// NewDeliveryEnqueueHandler returns an event handler that creates durable
// delivery records for all active subscriptions matching an event type.
//
// Enqueue is idempotent for an event/subscription pair, so retrying this
// handler after a partial failure does not create duplicate delivery jobs.
func NewDeliveryEnqueueHandler(
	subscriptions SubscriptionLister,
	deliveries DeliveryEnqueuer,
) (EventHandler, error) {
	if subscriptions == nil {
		return nil, errors.New("subscription lister is required")
	}
	if deliveries == nil {
		return nil, errors.New("delivery enqueuer is required")
	}

	return func(ctx context.Context, event model.Event) error {
		event.EventID = strings.TrimSpace(event.EventID)
		event.Type = strings.TrimSpace(event.Type)

		if event.EventID == "" {
			return errors.New("event ID is required")
		}
		if event.Type == "" {
			return errors.New("event type is required")
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		targets, err := subscriptions.ListActiveForEvent(ctx, event.Type)
		if err != nil {
			return fmt.Errorf("list subscriptions for event %q: %w", event.EventID, err)
		}

		for _, target := range targets {
			if err := ctx.Err(); err != nil {
				return err
			}

			subscriptionID := strings.TrimSpace(target.SubscriptionID)
			if subscriptionID == "" {
				return errors.New("matching subscription has an empty ID")
			}

			if err := deliveries.Enqueue(
				ctx,
				event.EventID,
				subscriptionID,
			); err != nil {
				return fmt.Errorf(
					"enqueue delivery for event %q and subscription %q: %w",
					event.EventID,
					subscriptionID,
					err,
				)
			}
		}

		return nil
	}, nil
}
