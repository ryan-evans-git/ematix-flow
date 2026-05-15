-- Target table for the clickstream demo.
-- Each batch the streaming daemon writes lands here as an INSERT.
-- Watch rows accumulate with:
--   docker exec -it ematix-flow-pg psql -U postgres \
--     -c "SELECT count(*), max(event_ts) FROM analytics.clicks"

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.clicks (
    user_id     BIGINT       NOT NULL,
    url         TEXT         NOT NULL,
    event_ts    TIMESTAMPTZ  NOT NULL,
    referrer    TEXT
);

CREATE INDEX IF NOT EXISTS clicks_event_ts_idx
    ON analytics.clicks (event_ts);
