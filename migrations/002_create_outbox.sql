
CREATE TABLE outbox (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES events(event_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error TEXT
);

CREATE INDEX idx_outbox_pending
    ON outbox (next_attempt_at, id)
    WHERE published_at IS NULL;

CREATE INDEX idx_outbox_event_id
    ON outbox (event_id);
