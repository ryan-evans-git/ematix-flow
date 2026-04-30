# ematix-flow

> Declarative table management and load strategies (SCD1, SCD2, append-only,
> merge/upsert, truncate+replace) for Postgres — with a Rust core, a Python
> API, normalization, post-load transforms, ML feature-store extensions, and
> DataFrame interop (polars / pandas / pyspark).

**Status: alpha.** v0.1 scope shipped; on PyPI as `ematix-flow` once
the wheel-build CI tasks land. Tested against Postgres 16; ~239 unit
tests + 181 integration tests + 110 Rust tests, all green on macOS +
Linux.

## Quickstart

```python
from typing import Annotated
from ematix_flow import ematix, pk
from ematix_flow.types import BigInt, String, Text, TimestampTZ
from ematix_flow.normalize import lower, trim, empty_to_null, parse_timestamp

@ematix.table(schema="analytics")
class CustomerDim:
    customer_id: Annotated[BigInt, pk()]
    email: Annotated[String[256] | None, lower(), trim(), empty_to_null()]
    name: Text | None
    updated_at: Annotated[TimestampTZ, parse_timestamp()]

@ematix.pipeline(
    target=CustomerDim,
    schedule="0 * * * *",
    mode="scd2",
    keys=["customer_id"],
    compare_columns=["email", "name"],
)
def sync_customers(conn):
    return "SELECT customer_id, email, name, updated_at FROM raw.customers"
```

Run from cron / k8s CronJob / GitHub Actions:

```sh
flow run-due --module my_pipelines      # fires schedules in the last interval
flow run     --module my_pipelines sync_customers   # one-shot
flow preview --module my_pipelines sync_customers   # what would it do?
flow validate --module my_pipelines sync_customers  # EXPLAIN against the DB
```

## What's in v0.1

- **Strategies**: append, truncate, merge / scd1, scd2 (with optional
  event-time `valid_from` and TTL expiry).
- **Cross-DB**: same-DB short-circuit + COPY BINARY staging path; auto-
  detected, force-overrideable.
- **Watermarks + run history**: lazy `ematix_flow.run_history`,
  `watermarks` tables. Restart-safe.
- **Declarative API**: `@ematix.table` / `@ematix.pipeline` / `pk()` /
  `natural_key()` / PEP 593 `Annotated` markers.
- **Normalization**: per-column markers (`trim`, `lower`,
  `empty_to_null`, `parse_timestamp(formats=...)`, `default(...)`,
  `parse_int(on_failure=...)`, `regex_replace`, `derive(...)`, raw `sql(...)`)
  + pipeline-level `transforms_pre=[deduplicate_by(...), filter_where(...), ...]`.
  All compile to in-database SQL.
- **Post-load transforms**: `transforms_post=[sql_string, callable,
  ematix.transform_ref("name")]`. Each runs in own tx with optional
  `continue_on_failure_post`.
- **DataFrame interop**: `pip install ematix-flow[df]` →
  `conn.read_df(sql, prefer="auto")` and
  `conn.write_df(df, "schema.table", mode=, target=, keys=)`. Polars or
  pandas. Routes through the strategy executor for all five modes.
- **Spark interop**: `pip install ematix-flow[spark]` →
  `conn.read_spark_df(spark, sql)` and `conn.write_spark_df(df, ...)` via
  Postgres JDBC.
- **ML feature store**: `@ematix.feature_view(schema=, feature_version=,
  ttl=, event_timestamp_column=, online=)`. PIT helpers
  (`Cls.point_in_time(...)` / `Cls.historical_features(...)`), online
  materialized view (`Cls.online_features(...)`), training-set builder
  (`ematix.training_set(conn, spine=, feature_views=[...])` returns a DataFrame).
- **CLI**: `flow list / run / run-due / preview / dry-run / validate /
  transform list / transform run / connections {list, check, set}`.
- **Connections**: env vars (`EMATIX_FLOW_DSN_<NAME>`) +
  `~/.ematix-flow/connections.toml`. `connect("warehouse")` resolves both.

## Install

```sh
# Core
pip install ematix-flow

# DataFrame helpers (polars or pandas, plus psycopg2)
pip install "ematix-flow[df]"
pip install polars            # or pandas

# Spark helpers (heavy: pulls in pyspark + JVM JDBC requirement)
pip install "ematix-flow[spark]"
```

## Development

```sh
# Build Rust workspace
cargo build

# Build + install Python extension into a venv
python -m venv .venv && source .venv/bin/activate
pip install maturin
maturin develop --release

# Run tests
cargo test
pytest                          # default suite (no Docker)
pytest -m integration           # full integration (uses testcontainers + Docker)
pytest -m spark                 # opt-in Spark E2E (needs JVM + pyspark)
```

## Roadmap

The original v0.1 scope (Phases 0–14) plus a substantial post-v0.1
extension set (Phases 15–28) are all shipped. See
[`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md) for the
phase log, and the design docs:

- [`docs/PRD.md`](docs/PRD.md) — original v0.1 product spec
- [`docs/ERGONOMICS_PLAN.md`](docs/ERGONOMICS_PLAN.md) — decorator API design
- [`docs/NORMALIZATION_TRANSFORMS_PLAN.md`](docs/NORMALIZATION_TRANSFORMS_PLAN.md)
  — Phases 26–28
- [`docs/ML_FEATURE_STORE_PLAN.md`](docs/ML_FEATURE_STORE_PLAN.md) —
  Phases 15–20

## License

Apache-2.0
