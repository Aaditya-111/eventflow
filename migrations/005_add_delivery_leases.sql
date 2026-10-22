ALTER TABLE webhook_deliveries
    ADD COLUMN locked_by TEXT,
    ADD COLUMN locked_until TIMESTAMPTZ;

CREATE INDEX idx_webhook_deliveries_expired_leases
    ON webhook_deliveries(locked_until)
    WHERE status = 'pending';
