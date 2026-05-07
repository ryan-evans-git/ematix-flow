-- Phase Δ PR 6: source-side schema for the CDC demo.
--
-- Loaded by `pg-source` on first container start (Postgres image
-- runs every `*.sql` in `/docker-entrypoint-initdb.d/`).
--
-- `REPLICA IDENTITY FULL` makes Debezium emit the full pre-image
-- on UPDATE / DELETE rather than just the changed columns + PK.
-- Cheaper alternatives (`DEFAULT`, `USING INDEX`) work too but
-- the demo stays simpler when every event carries a complete
-- before/after pair.

CREATE TABLE public.customers (
    id     BIGINT PRIMARY KEY,
    email  TEXT,
    name   TEXT
);

ALTER TABLE public.customers REPLICA IDENTITY FULL;

INSERT INTO public.customers (id, email, name) VALUES
    (1, 'alice@example.com', 'Alice'),
    (2, 'bob@example.com',   'Bob');
