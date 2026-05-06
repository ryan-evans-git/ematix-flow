-- Phase Δ PR 6: target-side schema for the CDC demo.
--
-- The mirror table the ematix-flow consumer applies CDC events
-- to. Schema must match the source table's column set —
-- `Backend::run_cdc` reflects this on startup and the schema-
-- evolution policy (default `Skip`) compares incoming `after`
-- payloads against this column set.

CREATE TABLE public.customers (
    id     BIGINT PRIMARY KEY,
    email  TEXT,
    name   TEXT
);
