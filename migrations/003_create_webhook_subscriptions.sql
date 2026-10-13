CREATE TABLE webhook_subscriptions (
    id BIGSERIAL PRIMARY KEY,
    subscription_id TEXT NOT NULL UNIQUE,
    endpoint_url TEXT NOT NULL,
    secret_encrypted BYTEA NOT NULL,
    event_types TEXT[] NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT webhook_subscription_id_not_empty
        CHECK (length(trim(subscription_id)) > 0),

    CONSTRAINT webhook_endpoint_url_not_empty
        CHECK (length(trim(endpoint_url)) > 0),

    CONSTRAINT webhook_event_types_not_empty
        CHECK (cardinality(event_types) > 0)
);

CREATE INDEX idx_webhook_subscriptions_active
    ON webhook_subscriptions (active)
    WHERE active = TRUE;
