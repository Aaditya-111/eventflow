CREATE TABLE webhook_deliveries (
    id BIGSERIAL PRIMARY KEY,

    event_id TEXT NOT NULL
        REFERENCES events(event_id) ON DELETE CASCADE,

    subscription_id TEXT NOT NULL
        REFERENCES webhook_subscriptions(subscription_id) ON DELETE CASCADE,

    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,

    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_status_code INTEGER,
    last_error TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at TIMESTAMPTZ,

    CONSTRAINT webhook_delivery_status_valid
        CHECK (status IN ('pending', 'delivered', 'dead')),

    CONSTRAINT webhook_delivery_attempts_nonnegative
        CHECK (attempts >= 0),

    CONSTRAINT webhook_delivery_unique_event_subscription
        UNIQUE (event_id, subscription_id)
);

CREATE INDEX idx_webhook_deliveries_pending
    ON webhook_deliveries(next_attempt_at, id)
    WHERE status = 'pending';

CREATE INDEX idx_webhook_deliveries_event
    ON webhook_deliveries(event_id);

CREATE INDEX idx_webhook_deliveries_subscription
    ON webhook_deliveries(subscription_id);
