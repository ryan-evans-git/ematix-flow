# ematix-flow — Product Requirements Document (v0.1)

Status: draft, post-intake interview
Owner: ryanevans23@gmail.com
Last updated: 2026-04-30

---

## 1. Summary

`ematix-flow` is an open-source Python data engineering library, with a Rust
core, that lets engineers declare a destination table and a load strategy (SCD1,
SCD2, append-only, truncate-and-replace) and have the library handle all the
boilerplate around comparing source data to the target, generating SQL,
managing metadata columns, tracking watermarks, and (optionally) running the
pipeline on a schedule.

The product's center of gravity is **table management + load strategies**.
Sources, scheduling, and deployment are thin wrappers around that core.

The v0.1 backend target is **Postgres only**, with the adapter abstraction
designed so DuckDB / Snowflake / BigQuery can be added later without breaking
the public API.

Public API is **SQLAlchemy-style declarative classes**, but the library does
**not** depend on SQLAlchemy — `Column` / `Integer` / `String` / etc. are
defined inside ematix-flow.

## 2. Goals

1. Replace hand-written SCD1/SCD2/merge/append boilerplate with a single
   declarative `ManagedTable` + a `pipeline.sync(...)` call.
2. Be ergonomic for Python data engineers from line one (PyPI install, no
   external services required to run a pipeline against Postgres).
3. Be fast where it matters: Rust core handles row hashing, change detection,
   and SQL planning. Python handles configuration, validation, and orchestration
   glue.
4. Stay small enough to read end-to-end. v0.1 is a focused library, not a
   platform.
5. Ship to PyPI as `ematix-flow` with a working `pip install ematix-flow` and
   one fully documented end-to-end example (Postgres → Postgres SCD2).

## 3. Non-goals (v0.1)

- Streaming, Kafka, or CDC ingestion.
- Backends other than Postgres (DuckDB / Snowflake / BigQuery / Redshift /
  Databricks come post-v0.1; the adapter trait is designed but only Postgres
  is implemented).
- Non-SQL sources (no S3/Parquet/CSV/JSON/API/Polars-DF sources in v0.1).
- Distributed / multi-node execution.
- A general-purpose orchestration UI, run dashboard, or web service.
- Replacing dbt: ematix-flow handles writes (loads/updates), not arbitrary SQL
  models.

## 4. Personas

- **Primary**: Python data engineer building Postgres-backed analytics tables
  who currently writes `MERGE INTO ... USING ...` by hand. Wants SCD2 to stop
  being a copy-paste exercise.
- **Primary**: Analytics engineer wiring up dimensional models who wants a
  lighter-weight option than dbt-snapshot for SCD2.
- **Secondary**: Backend engineer building a small ELT job in a service repo
  who doesn't want to deploy Airflow/Dagster but does want a cron-driven
  scheduler.

## 5. Core concepts

### 5.1 `ManagedTable`

Declarative class describing the destination table. Defined using
ematix-flow's own `Column` types (no SQLAlchemy dependency).

```python
from ematix_flow import ManagedTable, Column, Integer, String, Timestamp

class CustomerDim(ManagedTable):
    __tablename__ = "dim_customers"
    __schema__ = "analytics"

    customer_id = Column(Integer, primary_key=True)
    email       = Column(String)
    status      = Column(String)
    updated_at  = Column(Timestamp)
```

Responsibilities:
- Carries enough type info for ematix-flow to issue `CREATE TABLE`.
- Carries primary key / natural key info for strategies.
- Optional: partitioning hints, indexes, table options (likely v0.2+).

### 5.2 `Source`

Describes where rows come from. v0.1 supports two source variants only:

- `Source.postgres_query(connection=..., query="select ... from raw.x")`
- `Source.postgres_table(connection=..., schema="raw", table="customers")`

The `connection` may be the same connection as the target (same-DB
short-circuit path) or a different one (cross-DB staging path).

### 5.3 `Strategy` / load `mode`

Determines how source rows modify the target. v0.1 strategies:

| Mode             | Class            | Description                                   |
|------------------|------------------|-----------------------------------------------|
| `append`         | `AppendOnly`     | Insert all source rows; no key matching.      |
| `truncate`       | `TruncateReplace`| `TRUNCATE` target then insert source rows.    |
| `merge` / `scd1` | `MergeUpsert`    | Upsert by key; update columns in place.       |
| `scd2`           | `SCD2`           | Versioned history with hash-based diff.       |

### 5.4 `Pipeline` / `pipeline.sync(...)`

Top-level entry point that ties source, target, and strategy together and
runs one execution.

```python
from ematix_flow import pipeline

pipeline.sync(
    source=Source.postgres_query(conn, "select * from raw.customers"),
    target=CustomerDim,
    mode="scd2",
    keys=["customer_id"],
    compare_columns=["email", "status"],
    incremental_column="updated_at",   # optional
)
```

A `Pipeline` object exists for cases where the user wants to attach metadata
(name, schedule) and re-run it:

```python
p = Pipeline(
    name="customers_scd2",
    source=...,
    target=CustomerDim,
    mode="scd2",
    keys=["customer_id"],
    compare_columns=["email", "status"],
    schedule="0 * * * *",        # in v0.1
)
p.run()
```

## 6. Load strategies in detail

### 6.1 AppendOnly

- Insert all source rows into target.
- Optional managed columns added to target if not declared:
  - `_loaded_at TIMESTAMPTZ DEFAULT now()`
  - `_batch_id UUID` (per-run batch identifier)

### 6.2 TruncateReplace

- Single transaction: `BEGIN; TRUNCATE target; INSERT ... SELECT ...; COMMIT;`
- For cross-DB sources: stage into a temp table first, then truncate+insert
  inside one transaction.

### 6.3 MergeUpsert (SCD1)

- Configurable `keys` (natural key columns).
- Configurable `update_columns` (default: all non-key columns).
- Same-DB path: `INSERT ... ON CONFLICT (keys) DO UPDATE SET ...`.
- Cross-DB path: stage to temp table, then `INSERT ... ON CONFLICT ...
  FROM stage`.

### 6.4 SCD2

- Configurable: `keys`, `compare_columns`, optional `effective_from` /
  `effective_to` / `current_flag` / `hash_column` names (with defaults
  `valid_from`, `valid_to`, `is_current`, `row_hash`).
- Change detection uses a **library-managed row hash**:
  - On every load, library computes `row_hash = hash(compare_columns)` for
    incoming rows.
  - Compare incoming `row_hash` to the current version's `row_hash` for the
    same natural key.
  - If different: close out the existing current row
    (`valid_to = now(), is_current = false`) and insert a new current row
    (`valid_from = now(), is_current = true`).
  - If same: no-op.
- Hash function: deterministic, NULL-safe, column-order-stable. Likely
  SHA-256 over a canonical text encoding of the compare columns. Implemented
  in Rust.
- Hashes are computed in the destination database via SQL where possible
  (Postgres `digest()` from pgcrypto, or `md5()` of concatenated coalesced
  text); the Rust implementation is the source of truth and used for
  cross-DB / file paths in the future.

## 7. DDL ownership

ematix-flow **owns destination DDL in v0.1**. On first `pipeline.sync()`:

1. If the target table does not exist, ematix-flow issues `CREATE TABLE`
   from the `ManagedTable` declaration plus any strategy-required metadata
   columns (e.g. SCD2 adds `valid_from`, `valid_to`, `is_current`,
   `row_hash`).
2. If the target table exists, ematix-flow compares the live schema against
   the declared schema:
   - **Compatible** (declared columns exist with matching types): proceed.
   - **Drift detected** (extra column in DB that isn't declared, or
     mismatched type): error by default with a clear diff message; user can
     pass `on_drift="ignore"` to bypass.
3. v0.1 does **not** auto-`ALTER` tables. Schema migrations are deferred
   to v0.2 (likely behind a `migrate=True` flag).

## 8. Source query model

Two execution paths, picked automatically based on whether the source and
target connections point at the same Postgres database:

**Same-DB short-circuit** (source conn == target conn, same database):
- Strategy is implemented as a single SQL statement (or short transaction).
- Example for SCD2: a CTE that selects the source query, computes the hash,
  joins to the current version of the dimension, and emits the close-out +
  insert in two statements inside one transaction. No Python row movement.

**Cross-DB staging path** (different connection or different database):
1. Pull source rows over the wire (Rust `tokio-postgres` streaming).
2. `COPY` into a staging table created in the target database (named
   `_ematix_stage_<pipeline_name>_<batch_id>` and dropped at end).
3. Run the strategy SQL against the staging table inside a transaction.

The user does not have to choose; ematix-flow detects equality of the
`(host, port, dbname)` tuple and selects the path. Override available via
`force_path="staging"` for testing.

## 9. Incremental loading

Per-pipeline, optional. Two modes:

- **Full scan (default)**: every run pulls the full source query.
- **Watermark**: user declares `incremental_column="updated_at"`. ematix-flow
  stores the high-watermark per pipeline in a metadata table
  (`ematix_flow.watermarks`) and rewrites the source query as
  `SELECT * FROM (<user query>) src WHERE updated_at > :last_watermark`.

The watermark is updated at the end of a successful run to
`max(source.<incremental_column>)`. If the run fails, the watermark is not
advanced.

## 10. Delete handling

v0.1 default: **deletes are ignored**. A row that has disappeared from the
source has no effect on the target.

Opt-in via `handle_deletes="soft" | "hard"`:

- `"soft"`: SCD2 closes out missing keys (`is_current=false, valid_to=now()`).
  Merge/SCD1 sets a configurable `is_deleted` flag (column auto-added if
  absent) to true.
- `"hard"`: Merge/SCD1 deletes rows whose key isn't in the source.

Both opt-in modes **require a full source scan** (incremental + delete
handling is mutually exclusive). The library raises a configuration error
if a user combines `incremental_column` with `handle_deletes`.

## 11. Scheduling and CLI

v0.1 ships first-class scheduling, kept intentionally minimal.

- `Pipeline(schedule="0 * * * *", ...)` accepts a 5-field cron string.
- A `Pipelines` registry collects all `Pipeline` instances declared in a
  module: `from my_pipelines import flows`.
- CLI command `flow` (entry point of `ematix_flow.cli`):
  - `flow list` — list registered pipelines and their schedules.
  - `flow run <name>` — run a single pipeline once.
  - `flow run-due` — run all pipelines whose schedule says they're due.
    Designed to be invoked from cron / k8s CronJob / GitHub Actions
    minutely.
- Run history is stored in `ematix_flow.run_history` (started_at, ended_at,
  status, error, rows_in, rows_inserted, rows_updated, rows_closed_out,
  watermark_before, watermark_after).

A long-lived `flow daemon` (in-process scheduler that fires due pipelines
itself, with per-pipeline locks) is **deferred to v0.2**. v0.1 users invoke
`flow run-due` from cron, k8s CronJob, GitHub Actions schedule, or any
external scheduler. This covers ~90% of users without the operational
complexity of running a long-lived ematix-flow process.

## 12. Metadata tables

ematix-flow creates a `ematix_flow` schema in the target database (or
`public` if the database doesn't support schemas) containing:

- `ematix_flow.watermarks (pipeline_name PK, column_name, last_value, updated_at)`
- `ematix_flow.run_history (run_id PK, pipeline_name, started_at, ...)`
- `ematix_flow.schema_history` (record of declared-schema fingerprints, for
  drift detection)

These are created lazily on first run.

## 13. Type system

Without SQLAlchemy, ematix-flow defines its own minimal type DSL:

```python
from ematix_flow import (
    Column, Integer, BigInt, SmallInt,
    String, Text, Boolean,
    Float, Double, Numeric,
    Date, Timestamp, TimestampTZ,
    JSON, JSONB, UUID, Bytes,
)
```

- Each type knows its Postgres SQL spelling and its Python runtime type.
- `Column(type, primary_key=False, nullable=True, default=None, unique=False)`.
- The full type catalogue lives in `ematix_flow/types.py` and is the single
  source of truth for the DDL planner.
- Designed to be extensible: future backends register their dialect mapping
  for each type.

## 14. Architecture

### 14.1 Rust core (crate `ematix-flow-core`)

Responsibilities:
- Postgres I/O via `tokio-postgres` + `deadpool-postgres`.
- DDL planner (declared schema → `CREATE TABLE` SQL).
- Strategy planner (declared strategy + schemas → SQL plan).
- Row hash function (canonical encoding + SHA-256).
- Watermark + run-history persistence.
- COPY-based staging path.

### 14.2 Python bindings (crate `ematix-flow-py`, package `ematix_flow`)

- `pyo3` + `maturin` for build/distribution.
- Pure-Python layer: `ManagedTable`, `Column`/types, `Source`, `Pipeline`,
  CLI. These build a config object that is passed to the Rust core.
- The Python layer is allowed to do all *configuration* work; it must not
  do row-by-row work (that lives in Rust).

### 14.3 Layering

```
                    user code (Python)
                          │
                          ▼
   ematix_flow (Python) ── ManagedTable / Column / Pipeline / CLI
                          │      builds:
                          ▼
                    PipelineSpec (PyO3-bridged dataclass)
                          │
                          ▼
   ematix_flow_core (Rust) ── plan(spec) → execute(plan) → metrics
                          │
                          ▼
               tokio-postgres ↔ Postgres
```

## 15. Public API surface (target shape for v0.1)

```python
from ematix_flow import (
    ManagedTable, Column,
    Integer, BigInt, String, Text, Boolean, Float, Numeric,
    Date, Timestamp, TimestampTZ, JSON, JSONB, UUID,
    Source,
    Pipeline, pipeline,
    AppendOnly, TruncateReplace, MergeUpsert, SCD2,
)
```

The string-based `mode=` form (`mode="scd2"`) and the class-based form
(`strategy=SCD2(...)`) are both supported; the string form lowers to the
class form.

## 16. Success criteria for v0.1

- `pip install ematix-flow` installs cleanly on macOS, Linux x86_64, and
  Linux aarch64 (built wheels via `maturin` + GitHub Actions).
- The four strategies (Append, TruncateReplace, MergeUpsert, SCD2) each
  have a passing end-to-end integration test against a real Postgres
  (`testcontainers`).
- A user can go from `pip install` to a working SCD2 pipeline in < 10
  minutes by following the README quickstart.
- `flow run-due` works as a one-shot cron entry.
- README, quickstart, API reference (mkdocs), and one full SCD2 tutorial
  shipped.

## 17. Open questions / for v0.2+

- `flow daemon` long-lived scheduler (per-pipeline locks, SIGTERM-safe).
- DuckDB adapter (likely first multi-backend addition since it's free,
  embedded, and a great test environment).
- Snowflake / BigQuery adapters.
- Non-SQL sources: Parquet on S3, JSON-over-HTTP, Polars/Arrow tables.
- Schema migrations (auto-`ALTER` behind a flag).
- CDC / streaming ingestion.
- Observability hooks (OpenTelemetry traces per run).
- Pluggable hash functions.
- Bulk-write optimization for very wide / very large tables.
