CREATE TABLE IF NOT EXISTS events (
    id           BIGSERIAL    PRIMARY KEY,
    aggregate_id VARCHAR(255) NOT NULL,
    event_type   VARCHAR(255) NOT NULL,
    data         JSONB        NOT NULL,
    occurred_at  TIMESTAMPTZ  NOT NULL,
    version      INT          NOT NULL,
    UNIQUE (aggregate_id, version)
);

CREATE INDEX IF NOT EXISTS idx_events_aggregate_id ON events (aggregate_id);
