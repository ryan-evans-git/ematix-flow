# ematix-flow

> Declarative table management, load strategies, and streaming
> pipelines — Rust core, Python API. Multi-backend (Postgres, MySQL,
> SQLite, DuckDB, Object Stores, Delta Lake) with streaming sources
> (Kafka, RabbitMQ, GCP Pub/Sub, AWS Kinesis), Schema-Registry-aware
> Avro/Protobuf, manual-ack at-least-once, and a `flow consume` CLI
> with Prometheus metrics + supervised restart.

**Status: alpha.** Core scope shipped through Phase 38; on PyPI as
`ematix-flow` once wheel-build CI tasks land. ~296 Rust core unit
tests + 23 CLI unit tests + ~30 Docker integration tests across all
backends, plus the original Python test suite. clippy + fmt clean
on stable Rust.

## What it is

Two complementary surfaces in one repo:

1. **Declarative table management for Postgres** (the original v0.1
   scope). Decorator-driven schemas, normalization markers, SCD2 with
   event-time, run history, watermarks, post-load transforms, polars/
   pandas/pyspark interop, ML feature store. **Stable.**

2. **Multi-backend streaming pipelines** (Phases 30-38, post-v0.1).
   Source from any of 4 streaming backends, write to any of 6
   storage backends, with manual offset commits, dead-letter
   patterns, Confluent Schema Registry support, and a long-running
   `flow consume` daemon binary. **Recently shipped.**

Both share a common Rust core that does Arrow record-batch IO under
the hood; bridging from streaming to a target table is an Arrow path
end-to-end.

## Quickstart 1: declarative Postgres pipeline (v0.1)

```python
from typing import Annotated
from ematix_flow import ematix, pk
from ematix_flow.types import BigInt, String, Text, TimestampTZ
from ematix_flow.normalize import lower, trim, empty_to_null, parse_timestamp

@ematix.table(schema="analytics")
class CustomerDim:
    customer_id: Annotated[BigInt, pk()]    # ← keys live on the table
    email: Annotated[String[256] | None, lower(), trim(), empty_to_null()]
    name: Text | None
    updated_at: Annotated[TimestampTZ, parse_timestamp()]

@ematix.pipeline(
    target=CustomerDim,
    schedule="0 * * * *",
    mode="scd2",
    compare_columns=["email", "name"],
    # `keys=` omitted — the pipeline infers ["customer_id"] from
    # `pk()` on the table. Override only if your merge keys
    # differ from the declared primary key.
)
def sync_customers(conn):
    return "SELECT customer_id, email, name, updated_at FROM raw.customers"
```

### How merge keys are resolved

For `merge` and `scd2` pipelines, `keys=` is **optional**. The
decorator picks them in this priority order, falling through on
absence:

1. Explicit `keys=("col_a", "col_b")` on `@ematix.pipeline` /
   `pipeline.sync(keys=...)` — highest priority, silences any
   warnings.
2. `__merge_keys__ = ("col_a", "col_b")` class dunder on the
   target — useful when the merge key isn't the primary key.
3. First `natural_key()` group on the table — for SCD2 where the
   business key (e.g. `customer_id`) is distinct from the
   versioned primary key (e.g. `(customer_id, valid_from)`).
4. Columns marked `pk()` — the default in the example above.

When 2 or 3 resolve to keys that *differ* from `pk()`, the
pipeline emits a `UserWarning` so you know what got picked. Pass
explicit `keys=` to silence.

For SCD2 specifically, the natural pattern is to leave the table
PK as the business key (`customer_id` here) — the framework
augments the table with `valid_from` / `valid_to` / `is_current` /
`row_hash` columns and merges on `customer_id`. The PK becomes
`(customer_id, valid_from)` after augmentation. You don't need to
think about that unless you're hand-rolling DDL.

`natural_key()` is for the orthogonal case where you have a
*non-PK* column that should also be UNIQUE (e.g. `email`), or
where you want SCD2 to key off something other than the declared
`pk()` — see `help(natural_key)`.

Fired from cron / k8s CronJob / GitHub Actions:

```sh
flow run-due --module my_pipelines           # fires schedules in last interval
flow run     --module my_pipelines sync_customers  # one-shot
flow preview --module my_pipelines sync_customers  # what would it do?
flow validate --module my_pipelines sync_customers # EXPLAIN against the DB
```

## Quickstart 2: streaming pipeline (post-v0.1)

A long-running consumer that drains a Kafka topic and writes batches
to Postgres, with manual at-least-once offset commits, Prometheus
metrics on `:9100`, and exponential-backoff restart on error:

**1. Write a TOML config:**

```toml
# pipeline.toml
pipeline_name = "events-to-pg"
source_query = "events"
idle_pause_ms = 500

[source]
kind = "kafka"
bootstrap_servers = "localhost:9092"
group_id = "ematix-flow"

[target]
kind = "postgres"
url = "postgres://localhost/mydb"

[target.table]
schema = "public"
name = "events"
```

**2. Run from Python:**

```python
from ematix_flow import run_pipeline

run_pipeline(config="pipeline.toml", metrics_port=9100)
```

**3. Or run from the Rust binary** (build from source for now —
the binary is named `flow` so it shadows the Python CLI; we plan to
namespace this in a future cleanup):

```sh
cargo run --release --bin flow -- consume pipeline.toml \
    --metrics-port 9100 \
    --restart-on-error \
    --max-backoff-ms 30000
```

## Backend matrix

| Backend | Source | Target | DDL planning | Strategy executors (append/merge/scd2/truncate) |
|--|:--:|:--:|:--:|:--:|
| Postgres | — | ✅ | ✅ | ✅ (native + COPY BINARY) |
| MySQL | — | ✅ | ✅ | ✅ (native, ON DUPLICATE KEY) |
| SQLite | — | ✅ | ✅ | ✅ |
| DuckDB | — | ✅ | ✅ | ✅ |
| Delta Lake (local + S3) | ✅ | ✅ | n/a | ✅ (DataFusion-backed MERGE) |
| Object stores (parquet / csv / orc / jsonl, local + S3) | ✅ | ✅ | n/a | append + truncate |
| Kafka | ✅ | ✅ | n/a | append (cross-backend) |
| RabbitMQ | ✅ | ✅ | n/a | append (cross-backend) |
| GCP Pub/Sub | ✅ | ✅ | n/a | append (cross-backend) |
| AWS Kinesis | ✅ | ✅ | n/a | append (cross-backend) |

Streaming-source semantics:

- **Manual offset commit / ack** — pipelines call `commit_offsets()` on
  the source only after a durable target write, giving at-least-once.
  Mirrors Kafka offset commits, RabbitMQ `basic_ack`, Pub/Sub handler
  acks, Kinesis `committed_sequence_number` per-shard.
- **DLQ** — both app-level (`StreamingPipeline.dead_letter_topic`,
  routes failed batch rows to a separate target) and broker-level
  (RabbitMQ `nack_pending(requeue=False)` + `x-dead-letter-exchange`,
  Pub/Sub `nack_pending` + subscription `dead_letter_policy`).
- **Schema Registry** — Avro decode/encode (Phase 36h.3/.4) and
  Protobuf decode/encode (Phase 36h.5/.6) via Confluent SR or
  Apicurio. Validated against a live emulator container.
- **Exactly-once** — Kafka producer-side via transactions
  (Phase 36j); consumer-coordinated end-to-end via
  `KafkaToKafkaEosPipeline` (Phase 36j.2).

## Python API: streaming backends from a notebook

```python
from ematix_flow._core import KafkaBackend
import pyarrow as pa

backend = KafkaBackend.open(
    "localhost:9092",
    group_id="ematix-flow",
    payload_format="avro",
    schema_registry_url="http://localhost:8081",
    sasl_plain_username="alice",
    sasl_plain_password="secret",
)
backend.ping()

# Lazy iterator — yields one batch at a time, no list materialization.
for batch in backend.iter_arrow_stream("events"):
    process(batch)  # batch is pyarrow.RecordBatch

backend.commit_offsets()  # at-least-once: ack only after success
```

The same pattern works for `RabbitMQBackend`, `PubSubBackend`,
`KinesisBackend` (each in `ematix_flow._core`).

## What's in it

### v0.1 (declarative Postgres) — stable
- **Strategies**: append, truncate, merge / scd1, scd2 (with optional
  event-time `valid_from` and TTL expiry).
- **Cross-DB**: same-DB short-circuit + COPY BINARY staging path; auto-
  detected, force-overrideable.
- **Watermarks + run history**: lazy `ematix_flow.run_history`,
  `watermarks` tables. Restart-safe.
- **Declarative API**: `@ematix.table` / `@ematix.pipeline` / `pk()` /
  `natural_key()` / PEP 593 `Annotated` markers.
- **Normalization markers** (`trim`, `lower`, `empty_to_null`,
  `parse_timestamp`, `default`, `parse_int`, `regex_replace`,
  `derive`, raw `sql`) + pipeline-level
  `transforms_pre=[deduplicate_by(...), filter_where(...), ...]`.
  All compile to in-database SQL.
- **Post-load transforms**: `transforms_post=[sql_string, callable,
  ematix.transform_ref("name")]`. Each runs in own tx with optional
  `continue_on_failure_post`.
- **DataFrame interop**: `pip install ematix-flow[df]` → polars or
  pandas. **Spark interop**: `pip install ematix-flow[spark]`.
- **ML feature store**: `@ematix.feature_view`, PIT helpers,
  online materialized view, training-set builder.
- **CLI**: `flow list / run / run-due / preview / dry-run / validate /
  transform list / transform run / connections {list, check, set}`.
- **Connections**: env vars (`EMATIX_FLOW_DSN_<NAME>`) +
  `~/.ematix-flow/connections.toml`.

### Post-v0.1 (multi-backend + streaming) — recently shipped
- **DB backends** (Phases 31-33): MySQL, SQLite, DuckDB — same
  strategy executor surface as Postgres; cross-DB Arrow streaming
  bridge between any pair.
- **Object stores** (Phase 34): Parquet / CSV / ORC / JSONL on local
  FS or S3 (via MinIO in tests). Append + truncate.
- **Delta Lake** (Phase 35): local FS or S3. DataFusion-backed MERGE.
- **Streaming** (Phases 36-37): Kafka (with SASL/PLAIN, SASL/SCRAM,
  mTLS, AWS MSK IAM), RabbitMQ, GCP Pub/Sub, AWS Kinesis. Manual ack,
  DLQ patterns, Schema Registry.
- **CLI** (Phase 38): `flow consume <toml>` long-running daemon with
  `--metrics-port` (Prometheus `/metrics`) and `--restart-on-error`
  (exponential-backoff supervisor).
- **Python streaming bindings** (Phases Py.1-Py.6): `run_pipeline`
  in-process runner; pyclass wrappers for each streaming backend
  with PyArrow record-batch IO; sync iterator
  (`ArrowBatchIter`) for lazy batch consumption.

## Install

```sh
# Core
pip install ematix-flow

# DataFrame helpers (polars or pandas, plus psycopg2)
pip install "ematix-flow[df]"
pip install polars            # or pandas

# Spark helpers (heavy: pulls in pyspark + JVM JDBC requirement)
pip install "ematix-flow[spark]"

# PyArrow (required for the streaming-backend pyclasses)
pip install pyarrow
```

The streaming backends, the `flow consume` binary, and the
`run_pipeline` Python entrypoint are all part of the core install —
no extras needed.

## Development

```sh
# Build the Rust workspace (core + CLI + Python extension crate)
cargo build --release

# Build + install the Python extension into a venv
python -m venv .venv && source .venv/bin/activate
pip install maturin
maturin develop --release

# The flow consume binary is built into target/release/flow
target/release/flow --help

# Run tests
cargo test --workspace --lib              # default (no Docker)
cargo test --workspace -- --ignored       # Docker integration tests
                                          # (Kafka, RabbitMQ, Pub/Sub
                                          # emulator, Kinesis via
                                          # LocalStack, MinIO,
                                          # Schema Registry, etc.)

pytest                                    # default Python suite
pytest -m integration                     # full integration (Docker)
pytest -m spark                           # opt-in Spark E2E
```

## Roadmap

The original v0.1 scope (Phases 0–14) and a substantial post-v0.1
extension set (Phases 15–38, plus Python bindings catch-up Py.1-.6)
are all shipped. See [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md)
for the original phase log and the design docs:

- **[`docs/PRD.md`](docs/PRD.md)** — original v0.1 product spec
- **[`docs/MULTI_BACKEND_PLAN.md`](docs/MULTI_BACKEND_PLAN.md)** —
  Phases 30-37 (multi-DB + streaming)
- **[`docs/ERGONOMICS_PLAN.md`](docs/ERGONOMICS_PLAN.md)** — decorator
  API design
- **[`docs/NORMALIZATION_TRANSFORMS_PLAN.md`](docs/NORMALIZATION_TRANSFORMS_PLAN.md)**
  — Phases 26–28
- **[`docs/ML_FEATURE_STORE_PLAN.md`](docs/ML_FEATURE_STORE_PLAN.md)** —
  Phases 15–20

Future-phase / deferred design docs (capture both the design and the
"why we haven't built it"):

- **[`docs/SQL_TRANSFORMS_PLAN.md`](docs/SQL_TRANSFORMS_PLAN.md)** —
  Phase 39, DataFusion-backed mid-stream SQL transforms. Designed but
  unbuilt; awaiting a concrete user need.
- **[`docs/ICEBERG_PLAN.md`](docs/ICEBERG_PLAN.md)** — Iceberg
  backend. Deferred because `iceberg` 0.x still pins arrow 57 vs our
  arrow 58. Delta covers the use case today.

## License

Apache-2.0
