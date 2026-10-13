package model

import "time"

type WebhookSubscription struct {
	SubscriptionID string    `json:"subscription_id"`
	EndpointURL    string    `json:"endpoint_url"`
	EventTypes     []string  `json:"event_types"`
	Active         bool      `json:"active"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}
