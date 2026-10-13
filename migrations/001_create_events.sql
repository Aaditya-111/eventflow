
CREATE TABLE IF NOT EXISTS events (
    event_id   TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    data       JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT events_event_id_not_empty
        CHECK (length(trim(event_id)) > 0),

    CONSTRAINT events_event_type_not_empty
        CHECK (length(trim(event_type)) > 0),

    CONSTRAINT events_data_is_object_or_array
        CHECK (jsonb_typeof(data) IN ('object', 'array'))
);

CREATE INDEX IF NOT EXISTS idx_events_created_at
    ON events (created_at DESC);
