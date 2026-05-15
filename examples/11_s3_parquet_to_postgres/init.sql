-- Target table for the S3 → Postgres demo.
-- The streaming pipeline drains parquet files from MinIO's
-- `ematix-demo` bucket and inserts each batch here.

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.events (
    event_id     BIGINT       NOT NULL,
    user_id      BIGINT       NOT NULL,
    event_type   TEXT         NOT NULL,
    payload      TEXT,
    event_ts     TIMESTAMPTZ  NOT NULL
);

CREATE INDEX IF NOT EXISTS events_event_ts_idx
    ON analytics.events (event_ts);
