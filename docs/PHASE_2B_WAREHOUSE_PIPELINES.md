# Phase 2b — Snowflake / BigQuery / Redshift as pipeline backends

**Date**: 2026-05-20
**Predecessor**: Phase 2 (Python `Connection` dataclasses +
`*_query_to_arrow` adapters; see `python/ematix_flow/warehouses.py`).

## What 2b adds

Phase 2 shipped the **toolkit**: typed connections and Arrow query
adapters users call manually. Phase 2b adds the **pipeline-level
factory + orchestrator** so users can declare warehouse sources and
targets in the same shape as Postgres / Kafka pipelines:

```python
from ematix_flow import Source, Target
from ematix_flow.warehouses import SnowflakeConnection, BigQueryConnection

snow = SnowflakeConnection(name="snow", account=..., user=..., password=..., warehouse=...)
bq   = BigQueryConnection(name="bq", project=..., dataset="analytics")

# Read from Snowflake, transform with SQL, write to BigQuery.
result = run_warehouse_pipeline(
    source=Source.snowflake_query(snow, "SELECT * FROM orders WHERE region='US'"),
    target=Target.bigquery_table(bq, "us_orders"),
    transform_sql="SELECT order_id, SUM(amount) AS total FROM source GROUP BY 1",
)
```

## Scope (honest)

This slice ships the **Python-side bridge** — factories, orchestrator,
write-side adapters. The `@ematix.pipeline` decorator (which goes
through the Rust runner for scheduling / watermarks / retries) still
doesn't know about warehouse kinds — that's a Phase 2c follow-up
requiring real Rust work (PyO3 callbacks, schema discovery without
running the query, error propagation).

**What 2b ships today:**
- `Source.snowflake_query` / `bigquery_query` / `redshift_query`
  factories.
- `Target.snowflake_table` / `bigquery_table` / `redshift_table`
  factories with write-side adapters:
  - Snowflake: `write_pandas` via `snowflake-connector-python`.
  - BigQuery: `load_table_from_dataframe` via `google-cloud-bigquery`.
  - Redshift: `COPY FROM s3://...` via `redshift-connector` +
    `boto3` (requires `s3_staging_dir` + `iam_role` on the connection).
- `run_warehouse_pipeline(source, target, transform_sql=None)` —
  pure-Python orchestrator: read → optional in-memory SQL transform
  (via DuckDB) → bulk write. Synchronous; for streaming pipelines
  users still use Kafka / Kinesis sources.
- Tests with mocked SDK clients so the test suite doesn't need
  cloud credentials.

**What 2b explicitly defers to 2c (full Rust integration):**
- `@ematix.pipeline(source=Source.snowflake_query(...))` decorator
  semantics — scheduling, retries, watermarks, run-history
  recording for warehouse pipelines.
- Streaming-target writes (e.g. Snowflake Snowpipe).
- Push-down: cross-backend syncs today materialize the full result
  in coordinator memory. Phase 2c can add Arrow-Flight streaming so
  Snowflake → BigQuery doesn't go through the coordinator.

## Why Python-only first

The Rust drivers for Snowflake (`snowflake-arrow`) and BigQuery
(`bigquery-rs`) are immature; the Python SDKs are battle-tested
and emit Arrow natively. A Python-side bridge:

- Gets users value today against real credentials.
- Locks in the user-facing API (`Source.snowflake_query(...)`)
  before the Rust integration land — Phase 2c is a pure backend
  swap with no API change.
- Sidesteps the codegen-sensitivity findings (Σ.H.1d / Σ.K.A /
  Σ.F-T2 all regressed TPC-H from new Rust-side code in
  `ematix-flow-core`). Python additions don't touch that hot path.

## API shape

### `Source.snowflake_query(conn, sql)` etc

Returns a `Source` instance with a new `kind` discriminator. The
existing pipeline-construction path detects the discriminator and
routes through `run_warehouse_pipeline` when invoked manually.

For `@ematix.pipeline` decorator semantics, users today bridge via:

```python
@ematix.pipeline(schedule="0 * * * *")
def hourly_warehouse_sync():
    result = run_warehouse_pipeline(
        source=Source.snowflake_query(snow, "..."),
        target=Target.bigquery_table(bq, "out"),
        transform_sql="...",
    )
    return result
```

The scheduler fires the function on cron; the function does the
warehouse-to-warehouse move. Future Phase 2c will let users declare
this declaratively without the function body.

### `run_warehouse_pipeline(source, target, transform_sql=None)`

Reads → optional DuckDB SQL transform → writes. Returns a
`WarehouseRunResult` with row counts + duration. Raises
`WarehouseSyncError` on read/transform/write failures.

DuckDB is the in-memory transform engine because:
- Tiny dependency (already in the workspace via `[runlog-duckdb]`).
- Reads pyarrow.Table directly.
- Standard SQL — same surface the user already writes.

### Write-side adapters

Each `Target.*_table` factory carries the connection + table name.
On invocation, the adapter:

1. Validates the Arrow table's schema against the destination
   table (creates the table if missing, with types inferred from
   the Arrow schema).
2. Routes to the warehouse-specific bulk-write call.

Snowflake's `write_pandas` does the COPY internally. BigQuery's
`load_table_from_dataframe` schedules a load job and polls. Redshift
requires explicit S3 staging — the adapter uploads to
`s3_staging_dir` (provided on the connection), runs `COPY`, then
deletes the staging files.

## Tests

- Source factory shape + schema check.
- Target factory shape + schema check.
- `run_warehouse_pipeline` end-to-end with mocked SDK clients (read,
  transform, write each verified independently).
- DuckDB transform: simple aggregation produces the expected output.
- Errors: missing credentials, schema mismatch, network failure all
  surface as `WarehouseSyncError` with the underlying message.

## Out of scope (Phase 2c and beyond)

- Real-Snowflake / -BigQuery / -Redshift integration tests (skipped
  in CI without creds; pytest skipif decorator).
- Streaming-target writes (Snowpipe, BigQuery streaming inserts).
- Cross-backend Arrow Flight streaming (avoid materializing the
  full result in coordinator memory).
- Rust-side dialect dispatch — bigger lift, separate PR.
