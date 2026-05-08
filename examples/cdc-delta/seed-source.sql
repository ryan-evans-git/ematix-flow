-- End-to-end Delta CDC example: source-side schema.
--
-- Loaded by `pg-source` on first container start (the Postgres
-- image runs every `*.sql` in `/docker-entrypoint-initdb.d/`).
--
-- `REPLICA IDENTITY FULL` makes Debezium emit the full pre-image
-- on UPDATE / DELETE rather than just the changed columns + PK.
-- Identical to the cdc-debezium example — we share the source
-- schema; only the *target* (Delta vs Postgres) differs.

CREATE TABLE public.customers (
    id     BIGINT PRIMARY KEY,
    email  TEXT,
    name   TEXT
);

ALTER TABLE public.customers REPLICA IDENTITY FULL;

INSERT INTO public.customers (id, email, name) VALUES
    (1, 'alice@example.com', 'Alice'),
    (2, 'bob@example.com',   'Bob');
