CREATE TABLE IF NOT EXISTS events (
    id           BIGSERIAL    PRIMARY KEY,
    aggregate_id VARCHAR(255) NOT NULL,
    event_type   VARCHAR(255) NOT NULL,
    data         JSONB        NOT NULL,
    occurred_at  TIMESTAMPTZ  NOT NULL,
    version      INT          NOT NULL,
    published    BOOLEAN      NOT NULL DEFAULT FALSE,
    published_at TIMESTAMPTZ,
    UNIQUE (aggregate_id, version)
);

CREATE INDEX IF NOT EXISTS idx_events_aggregate_id ON events (aggregate_id);
CREATE INDEX IF NOT EXISTS idx_events_unpublished   ON events (id) WHERE published = FALSE;

-- 読み取りモデル（プロジェクション）の永続化
CREATE TABLE IF NOT EXISTS account_views (
    id         VARCHAR(255) PRIMARY KEY,
    owner      VARCHAR(255) NOT NULL DEFAULT '',
    balance    BIGINT       NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Saga 冪等性管理（同じイベントを二重処理しない）
CREATE TABLE IF NOT EXISTS saga_state (
    saga_type    VARCHAR(255) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    version      INT          NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (saga_type, aggregate_id, version)
);
