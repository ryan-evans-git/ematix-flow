# ematix-flow

**Move data between databases, files, and streams from Python.
5.87× faster than PySpark. No JVM needed.**

ematix-flow is a Python library for moving data between
databases (Postgres, MySQL, SQLite, DuckDB), files (Parquet,
CSV, JSON, ORC, Delta Lake — local disk or S3), and streaming
sources (Kafka, Pub/Sub, Kinesis, RabbitMQ). Declare a target
table and a load strategy — append, merge, or slowly-changing
dimension — and the Rust + Apache Arrow engine handles the
correctness work: schema evolution, watermarks, at-least-once
delivery, change-data-capture.

**How it compares.** No scheduler, no warehouse, no JVM, no
cluster — one `pip install`. Benchmarks show ematix-flow at
**5.87× the speed of single-node PySpark on TPC-H**
(SF=1, geomean across all 22 queries — see
[`docs/BENCHMARKS.md`](https://github.com/ryan-evans-git/ematix-flow/blob/main/docs/BENCHMARKS.md)).
If you've been writing one-off pandas / SQL scripts, ematix-flow
gives you correctness guarantees without the operational weight
of Airflow + Spark; if you've been running Airflow + dbt,
ematix-flow handles the load logic and streaming sources without
the scheduler tier.

> Status: **v0.1.2 on PyPI** as `ematix-flow`. All four surfaces
> below are shipped. See
> [`docs/ROADMAP.md`](https://github.com/ryan-evans-git/ematix-flow/blob/main/docs/ROADMAP.md)
> for what's deferred.

## Why ematix-flow

- **Declarative, not framework boilerplate.**
  `@ematix.table` + `@ematix.pipeline` is the whole API surface
  for a typical pipeline. Schemas, primary keys, compare columns,
  SCD2 event-time, watermarks — all PEP-593 type-annotated. No DAG
  plumbing, no scheduler stub, no per-source connector wiring.

- **One binary, one dependency tree.** Distributed as a Rust
  library with Python bindings (~150 MB image including all 10
  backends). No JVM, no separate scheduler/executor processes,
  no cluster service to operate. Distributed batch SQL is
  *opt-in* via a peer-to-peer worker mesh — not a top-down
  cluster manager you have to deploy.

- **Multi-backend, write once.** SQL databases (Postgres, MySQL,
  SQLite, DuckDB), object stores + Delta Lake (Parquet, CSV,
  JSON, ORC — local FS or S3), and streaming sources (Kafka,
  RabbitMQ, GCP Pub/Sub, AWS Kinesis) all live behind one
  `Backend` trait. Switch a pipeline's target by changing one
  line — either the decorator's `target_connection=` argument
  or, if you prefer config-as-data, a TOML field. Both
  surfaces compile to the same backend descriptor and run the
  identical Rust execution path; pick whichever fits the
  workflow. The SQL stays the same.

- **Correct by default.** Watermarks restart-safe via row-level
  advance-after-commit. Stateful streaming windows and joins
  serialize their per-key state to a durable `StateStore`
  (Postgres or in-memory) with **atomic state + offset commits**
  on every emit. Manual-ack at-least-once across all four
  streaming sources; Kafka exactly-once via transactions.

- **Faster than single-node PySpark, with a real distributed
  story when you need it.** SF=1 TPC-H (M3 Pro, 22 queries):
  DataFusion via ematix-flow beats PySpark `local[*]` by a
  geomean of **5.87×** (range 1.78× to 16.74×). At SF=10 on the
  representative set: **3.3× geomean**. Distributed batch SQL
  across ematix-flow processes is available today via the
  bundled `flow-worker` peer mesh; cross-host scaling claims are
  honestly framed as deferred (no cluster hardware in this
  project's runway — see BENCHMARKS.md).

## What it is

Four complementary surfaces in one repo, all sharing a Rust +
Apache Arrow core. Data moves through Arrow record batches end-
to-end — no row-by-row serialization, no JVM hop, no
intermediate file roundtrip.

1. **Declarative table management** *(Phases 0–25)*. Decorator-
   driven schemas with PEP-593 type annotations + normalization
   markers (`trim`, `lower`, `parse_timestamp`, `regex_replace`,
   ...), SCD2 with event-time, run history, watermarks, post-
   load transforms, polars / pandas / pyspark interop, ML
   feature store. The original v0.1 scope.

2. **Multi-backend pipelines** *(Phases 30–38)*. Source from
   any of 4 streaming backends, write to any of 10 storage
   backends. Manual offset commits, app-level + broker-level
   dead-letter patterns, Confluent Schema Registry-aware
   Avro/Protobuf, long-running `flow consume` daemon with
   Prometheus metrics + supervised restart, `flow consume
   --module` typed-Python pipeline registry.

3. **Stream processing** *(Phase 39)*. DataFusion-backed mid-
   stream SQL transforms (filter / project / cast / lookup-
   join), tumbling / hopping / session windows with watermark-
   driven emit + late-data handling (`drop` / `reopen` /
   `dlq`), keyed time-windowed stream-stream joins (inner +
   outer + retained-buffer reopen for late matches). Per-key
   state persists to a durable `StateStore` with atomic
   state+offset commits across crashes.

4. **Distributed batch SQL** *(Σ.B)*. Optional peer-to-peer
   distributed execution via the bundled `flow-worker` binary;
   set `[transform] engine = "distributed"` + `peers = [...]`
   and the SQL fans out across processes via Apache Arrow
   Flight. mTLS for the worker mesh, cross-pod lookup
   broadcast, no separate cluster service. SQL dialect
   translator (Spark / DuckDB → DataFusion) makes existing
   queries portable without rewrites — 103/103 PASS on the
   canonical Apache Spark TPC-DS suite.

**Test surface (workspace-wide, every PR):**

- 459 Rust core unit tests
- 124 Rust CLI unit tests + 27 backend-config scaffold round-
  trip tests across all 10 backends + distributed + TLS config
- ~80 Rust testcontainers integration tests (Docker-gated)
- 376 default Python tests + ~196 testcontainers-gated Python
  tests
- 22-query TPC-H audit: **22/22 PASS** at SF=1
- 103-query TPC-DS Spark-dialect audit: **103/103 PASS** plan-time

clippy + fmt clean on stable Rust; `cargo audit` green against
RustSec; ruff + bandit + pip-audit green on the Python side.

→ For a step-by-step walkthrough of every surface, see
**[`docs/USER_GUIDE.md`](docs/USER_GUIDE.md)**.

→ For runnable examples (one per strategy + streaming + windowed +
session + join), see **[`examples/`](examples/)**.

→ For what's left to ship, see **[`docs/ROADMAP.md`](docs/ROADMAP.md)**.

→ For cutting a release, see **[`docs/RELEASE.md`](docs/RELEASE.md)**.

## Quickstart 1: declarative Postgres pipeline (v0.1)

```python
from typing import Annotated
from ematix_flow import ematix, pk, register_connection
from ematix_flow.types import BigInt, String, Text, TimestampTZ
from ematix_flow.normalize import lower, trim, empty_to_null, parse_timestamp

# Connection: declared in code via the `@ematix.connection` class-
# body decorator. `${VAR}` interpolates from the environment at
# backend-build time (so changing the env between definition and
# run picks up the new value). `repr()` redacts secrets.
# Alternatives — env-var only, project TOML file, or
# `register_connection(PostgresConnection(...))` — are documented
# below. All three feed the same registry.
@ematix.connection
class warehouse:
    kind = "postgres"
    url = "${EMATIX_FLOW_DSN}"

@ematix.table(schema="analytics")
class CustomerDim:
    customer_id: Annotated[BigInt, pk()]    # ← keys live on the table
    email: Annotated[String[256] | None, lower(), trim(), empty_to_null()]
    name: Text | None
    updated_at: Annotated[TimestampTZ, parse_timestamp()]

@ematix.pipeline(
    target=CustomerDim,
    target_connection="warehouse",   # ← references the @ematix.connection above
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

### Where does the data come from / go to?

In the example above:

| Element | What | Where it's configured |
|--|--|--|
| The target table | `analytics.customer_dim` | `schema=` on `@ematix.table` + the class name (snake-cased: `CustomerDim` → `customer_dim`). Override the table name with `@ematix.table(schema=..., name="...")`. |
| The source table | `raw.customers` | The SQL string returned from `sync_customers(conn)`. Could equally well be a join, a subquery with filters, etc. |
| The database | The connection named `warehouse` | The `@ematix.connection` block at the top of the file registers it; `target_connection="warehouse"` on `@ematix.pipeline` references it by name. The `conn` parameter passed to `sync_customers` is the resolved *source* connection (defaults to the same as target unless `source_connection=` is set). |

`target_connection=` is omittable. When omitted, the framework
looks up the connection literally named `default` (which the
env var `EMATIX_FLOW_DSN` populates) — handy for one-off
scripts, but the explicit form above keeps the wiring obvious
in code review.

By default, source and target use the same connection (same DB → same-DB
fast path: `INSERT … SELECT`). To cross databases, name them
explicitly:

```python
@ematix.pipeline(
    target=CustomerDim,
    schedule="0 * * * *",
    mode="scd2",
    compare_columns=["email", "name"],
    source_connection="raw_db",       # ← named connection
    target_connection="warehouse",    # ← named connection
)
def sync_customers(conn):
    # `conn` here is the SOURCE connection (raw_db).
    return "SELECT customer_id, email, name, updated_at FROM customers"
```

When `source_connection != target_connection`, the framework switches
to the cross-DB Arrow path: read source rows as Arrow batches, stream
them into the target. No `COPY BINARY` shortcut, but no row-by-row
`INSERT` either.

### Configuring connections

A connection name like `"raw_db"` resolves through this chain (highest
priority first):

1. Env var **`EMATIX_FLOW_DSN_RAW_DB`** (uppercased), e.g.
   `EMATIX_FLOW_DSN_RAW_DB=postgres://user:pw@host/db`.
2. Env var **`EMATIX_FLOW_DSN`** — only for the connection literally
   named `default`.
3. Project file **`./.ematix-flow.toml`** in the working directory:
   ```toml
   [connections.raw_db]
   url = "postgres://user:${RAW_DB_PASSWORD}@host/raw"

   [connections.warehouse]
   url = "postgres://${WAREHOUSE_DSN}"
   ```
4. User file **`~/.ematix-flow/connections.toml`** (same shape).
5. Inline **`config.connect(url=...)`** as a low-level escape hatch.

TOML values support `${VAR}` env-var interpolation, so secrets can stay
out of files. Inspect what got picked with `flow connections list` /
`flow connections check warehouse`.

### Declaring connections in code (decorator)

Beyond env vars and TOML files, connections can also live in your
Python module — useful when you want the pipeline definition and its
DB handle in the same file, or when you want IDE autocomplete on
connection fields. Two interchangeable shapes:

```python
from ematix_flow import (
    ematix,                     # the decorator namespace
    PostgresConnection,
    KafkaConnection,
    SchemaRegistryConnection,
    register_connection,
)

# 1. Decorator form — class body declares the fields, the decorator
#    builds + registers a typed connection instance under the class
#    name. Module-level `warehouse` is now a PostgresConnection
#    instance, registered as "warehouse" in the runtime registry.
@ematix.connection
class warehouse:
    kind = "postgres"
    url = "${WAREHOUSE_DSN}"     # ${VAR} interpolates at use time

@ematix.connection
class kafka_prod:
    kind = "kafka"
    bootstrap_servers = "${KAFKA_BOOTSTRAP}"
    group_id = "ematix-flow"
    sasl_plain_username = "${KAFKA_USER}"
    sasl_plain_password = "${KAFKA_PASS}"

@ematix.connection
class sr_prod:
    kind = "schema_registry"
    url = "${SR_URL}"

# 2. Instance form — same effect; useful when the connection has to
#    be built dynamically (e.g. from an environment-driven dict).
warehouse_2 = register_connection(
    PostgresConnection(name="warehouse_2", url="postgres://localhost/wh"),
)

# Pass the instance directly to a pipeline:
@ematix.pipeline(
    target=CustomerDim,
    schedule="0 * * * *",
    mode="scd2",
    target_connection="warehouse",   # name reference; resolves through registry
)
def sync_customers(conn):
    return "SELECT customer_id, email, name, updated_at FROM raw.customers"
```

Credentials redact in `repr()` by field-name match (`password`,
`secret`, `secret_access_key`, anything containing `_password`, AMQP
URL passwords, etc.) — printing a connection in a notebook won't
spill secrets. The same `@ematix.connection` shape supports every
typed connection: `KafkaConnection`, `RabbitMQConnection`,
`PubSubConnection`, `KinesisConnection`, `SchemaRegistryConnection`,
`PostgresConnection`, `MySQLConnection`, `SQLiteConnection`,
`DuckDBConnection`, `DeltaLocalConnection`, `DeltaS3Connection`,
`ObjectStoreLocalConnection`, `ObjectStoreS3Connection`.

> **Schema Registry as a connection** (Π.1). `KafkaConnection` accepts
> either an inline `schema_registry_url="..."` shorthand or a
> `schema_registry=sr_prod` reference (instance or registered name)
> for Avro/Protobuf pipelines, so SR config lives in the same
> credential-redacting registry as everything else.

### Two function signatures

The pipeline-decorated function can take 0 or 1 args:

| Signature | When to use | What `conn` is |
|--|--|--|
| `def sync(conn): return "SELECT …"` | Source SQL needs computed dynamically (filters, dates, etc.) | The source connection (the active `_core.Connection`). |
| `def sync(): pass` | Static source. Pair with `source_table="raw.customers"` on the decorator and an optional `column_map={"target_col": "source_col", ...}`. | n/a. |

The static form lets you skip writing `SELECT *` boilerplate:

```python
@ematix.pipeline(
    target=CustomerDim,
    schedule="0 * * * *",
    mode="scd2",
    source_table="raw.customers",  # framework synthesizes SELECT
    compare_columns=["email", "name"],
)
def sync_customers():
    pass
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

### Or skip the TOML entirely (Π.3 typed-Python form)

`@ematix.streaming_pipeline` declares the pipeline alongside its
connections, then `flow consume --module my_pipelines events_to_pg`
loads the module, looks the pipeline up by name, and runs it. No
TOML round-trip in user code:

```python
# my_pipelines.py
from ematix_flow import ematix

@ematix.connection
class kafka_prod:
    kind = "kafka"
    bootstrap_servers = "${KAFKA_BOOTSTRAP}"
    group_id = "ematix-flow"

@ematix.connection
class warehouse:
    kind = "postgres"
    url = "${WAREHOUSE_DSN}"

@ematix.streaming_pipeline(
    name="events_to_pg",
    source=kafka_prod,
    source_query="events",
    target=warehouse,
    target_table=("public", "events"),
)
def events_to_pg():
    pass
```

```sh
flow consume      --module my_pipelines events_to_pg --metrics-port 9100
flow consume-list --module my_pipelines
```

The framework renders the equivalent TOML internally and hands it to
the same Rust runtime the TOML form uses — same at-least-once
guarantees, same Prometheus metrics, same `--restart-on-error`.

## Quickstart 3: stream processing (windows + sessions + joins)

Phase 39 layers stateful transforms onto the streaming pipeline. The
canonical shapes:

**Tumbling window aggregation** (count events per user per minute):

```python
from ematix_flow import (
    Aggregation, Window, run_streaming_pipeline,
    KafkaConnection, PostgresConnection,
)

run_streaming_pipeline(
    name="events-per-min",
    source=KafkaConnection(name="src", bootstrap_servers="localhost:9092",
                           group_id="ematix-flow"),
    source_query="events",
    target=PostgresConnection(name="warehouse", url="postgres://localhost/wh"),
    target_table=("public", "events_per_min"),
    transform_sql="SELECT user_id, _event_ts FROM source",
    window=Window(
        kind="tumbling",
        duration_ms=60_000,
        group_by=("user_id",),
        max_groups_per_window=1_000_000,
        aggregations=[Aggregation(agg="count", as_="n")],
    ),
)
```

**Session window** (per-user activity sessions, 5-minute idle gap):

```python
from ematix_flow import StateStore  # new in 39.5a

run_streaming_pipeline(
    name="user-sessions",
    source=KafkaConnection(name="src", bootstrap_servers="localhost:9092",
                           group_id="ematix-flow"),
    source_query="events",
    target=PostgresConnection(name="warehouse", url="postgres://localhost/wh"),
    target_table=("public", "user_sessions"),
    transform_sql="SELECT user_id, page, _event_ts FROM source",
    window=Window(
        kind="session",
        gap_ms=300_000,                   # 5 min idle = session boundary
        max_session_duration_ms=86_400_000,  # 24h hard cap
        group_by=("user_id",),
        max_groups_per_window=1_000_000,
        aggregations=[
            Aggregation(agg="count", as_="events"),
            Aggregation(agg="first", column="page", as_="entry_page"),
            Aggregation(agg="last", column="page", as_="exit_page"),
        ],
    ),
    # State persistence is mandatory for sessions — Postgres-backed
    # `StateStore` handles atomic per-emit state + Kafka offsets
    # commits. Restart-safe out of the box.
    state_store=StateStore(
        kind="postgres",
        url="postgres://localhost/ematix_state",
    ),
)
```

**Stream-stream join** (orders + payments within a 5-minute window).
Driven from typed Python via `sources=[Source(...)]`:

```python
from ematix_flow import Join, Source

run_streaming_pipeline(
    name="orders-payments",
    sources=[
        Source(connection=KafkaConnection(name="orders_k",   bootstrap_servers="localhost:9092", group_id="ematix-flow"),
               query="orders"),
        Source(connection=KafkaConnection(name="payments_k", bootstrap_servers="localhost:9092", group_id="ematix-flow"),
               query="payments"),
    ],
    target=PostgresConnection(name="warehouse", url="postgres://localhost/wh"),
    target_table=("public", "orders_with_payments"),
    join=Join(
        left_source="orders",
        right_source="payments",
        left_keys=("order_id",),
        right_keys=("order_id",),
        time_window_ms=300_000,                # ±5 min symmetric window
    ),
    state_store=StateStore(kind="postgres", url="postgres://localhost/ematix_state"),
)
```

LEFT / RIGHT / FULL outer joins (`Join(... kind="left_outer")`),
`late_data="reopen"` for retained-buffer late-row matching, and
asymmetric time windows (`min_delta_ms` / `max_delta_ms`) all work
through the same shape.

### Advanced knobs (Π.1)

Every advanced streaming knob is now drivable from typed Python — no
TOML required:

```python
from ematix_flow import Watermark

run_streaming_pipeline(
    name="events-clean",
    source=kafka_prod, source_query="events",
    target=warehouse, target_table=("public", "events"),
    transform_sql="SELECT user_id, payload, _event_ts FROM source",
    # Π.1: per-batch error policy. "fail" (default) | "drop" | "dlq".
    transform_on_error="dlq",
    dead_letter_topic="events-failed",
    # Π.1: tune per-source watermark slack + idleness without TOML.
    watermark=Watermark(lateness_ms=5_000, source_idleness_ms=120_000),
)
```

### Object-store target with compression (Π.1.4)

```python
from ematix_flow import ObjectStoreS3Connection, Target

lake = ObjectStoreS3Connection(
    name="lake",
    endpoint="https://s3.amazonaws.com",
    bucket="ematix-archive",
    region="us-east-1",
    access_key_id="${AWS_ACCESS_KEY_ID}",
    secret_access_key="${AWS_SECRET_ACCESS_KEY}",
    format="parquet",
)

run_streaming_pipeline(
    name="events_archive",
    source=kafka_prod, source_query="events",
    targets=[
        Target(
            connection=lake,
            prefix="events/raw",
            parquet_compression="zstd",   # or "snappy" / "gzip" / "uncompressed"
        ),
    ],
)
```

CSV targets accept `csv_delimiter=";"` and `csv_header=False`. The
typed-Python boundary catches misconfigurations early (e.g. setting
`parquet_compression` on a CSV target raises before TOML round-trip).

Full walkthroughs for each shape (windows, sessions, joins) —
including late-data semantics, recovery behavior on restart, and
the Prometheus metrics emitted — live in
[`docs/USER_GUIDE.md`](docs/USER_GUIDE.md).

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

### Post-v0.1 (multi-backend + streaming) — stable
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

### Stream processing (Phase 39) — recently shipped
- **SQL transforms** (39.1–39.3): mid-stream `SELECT` via
  DataFusion. Filter / project / cast / lookup-join. Static lookups
  loaded from any DB backend at startup; `refresh_interval_ms` per
  lookup runs a background refresh task with atomic `MemTable` swap.
- **Tumbling + hopping windows** (39.4): 9 aggregators including
  HLL+ approximate `count_distinct`. `late_data = "drop"` and
  `"reopen"` (with `allowed_lateness_ms` retention + re-emit on
  dirty). Idle-tick emission. Per-window `max_groups_per_window`
  fail-loud cap. Multi-source `min`-with-idleness watermark.
- **Session windows + durable `StateStore`** (39.5a): gap-based
  per-key sessions with mandatory `max_session_duration_ms` hard
  cap; out-of-order session merging under `Reopen`; Postgres- or
  in-memory-backed `StateStore` with postcard wire format and
  forward-only state-version migrations; per-emit atomic
  state+offsets commit; `seek_to` on Kafka source for crash-safe
  resume. Each pipeline rehydrates per-key session state on startup
  via `StateStore::load`.
- **Stream-stream join** (39.5b): keyed time-windowed inner join.
  Two `[[sources]]` (left + right) with per-side per-key buffers
  and watermark-driven retention; emit on every match within
  `time_window_ms`. Reuses the 39.5a `StateStore` (side-prefixed
  keys, postcard `BufferedRow` blobs). Per-source `BatchContext::source_id`
  routes batches to the correct side.

### CDC source mode (Phase Δ) — recently shipped

- **Per-event apply** (Δ PR 1–3): `[transform.cdc]` /
  `CDC(envelope="debezium")` interprets each Kafka payload as a
  CDC envelope (Debezium / Maxwell / custom shape) and routes
  through `Backend::run_cdc` for per-op transactional dispatch.
  Postgres target supported today; Delta + DuckDB + MySQL are
  catalogued as Phase Δ extensions.
- **Per-PK idempotency gate** (Δ PR 4): `INSERT … ON CONFLICT
  DO UPDATE … WHERE existing.last_seen_ts_ms < EXCLUDED…
  RETURNING 1` against `ematix_flow.cdc_idempotency`, atomic
  with the data write — Kafka redeliveries become a no-op
  without trusting source idempotency.
- **Schema-evolution policy** (Δ PR 5): `Skip` (default) warns +
  drops unknown columns; `Fail` aborts the batch on first drift
  for teams that gate releases on schema sync.
- **End-to-end demo** (Δ PR 6): `examples/cdc-debezium/`
  docker-compose stack (Postgres source + Debezium + Kafka +
  Postgres mirror) with a step-by-step walkthrough.
  See [`docs/USER_GUIDE.md` § "CDC source mode (Δ)"](docs/USER_GUIDE.md#cdc-source-mode-δ).

### Recent additions (Π.1 / Π.3 / Π.1.4)

- **`SchemaRegistryConnection`** (Π.1): SR config lives in the
  typed-connection registry, redacted in `repr()`, resolvable by
  name. `KafkaConnection.schema_registry=sr_prod` for typed
  reference; the legacy inline `schema_registry_url=...` shorthand
  still works.
- **Kafka SR + `payload_format` plumbed through the streaming TOML
  emitter**: the Avro / Protobuf path is now usable end-to-end via
  `run_streaming_pipeline` and `@ematix.streaming_pipeline` (was
  silently dropped before).
- **`flow consume --module my_pipelines <name>`** (Π.3): load
  streaming pipelines from a Python module instead of TOML; the
  `@ematix.streaming_pipeline` decorator registers by name; the
  CLI imports the module, renders the equivalent TOML internally,
  and hands off to the same Rust runner. `flow consume-list
  --module my_pipelines` lists registered pipelines.
- **`Watermark(lateness_ms=, source_idleness_ms=)`** (Π.1): tune
  watermark slack + per-source idleness from the typed-Python
  surface (was hardcoded to defaults). Maps to a `[watermark]`
  TOML block on the runner side.
- **`transform_on_error = "fail" | "drop" | "dlq"`** (Π.1):
  per-batch error policy on `run_streaming_pipeline` /
  `@ematix.streaming_pipeline` (DLQ reuses the existing
  `dead_letter_topic` plumbing).
- **Object-store per-format write options** (Π.1.4): Parquet
  compression (`uncompressed | snappy | gzip | zstd`), CSV
  delimiter, CSV header on `Target` — picks up production-grade
  compression without leaving Python. The typed-Python boundary
  rejects mis-shaped combos (e.g. `parquet_compression` on a CSV
  target) before TOML round-trip.

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

Phases 0–14 (v0.1), 15–38 (multi-backend + streaming), Py.1–Py.6
(Python streaming bindings), and 39.1–39.5b (SQL transforms,
windows, sessions, stream-stream join) are all shipped.

See **[`docs/ROADMAP.md`](docs/ROADMAP.md)** for the consolidated
"what's left" punch list (release polish, deferred-feature
extensions, open design questions).

Phase plans:

- **[`docs/PRD.md`](docs/PRD.md)** — original v0.1 product spec
- **[`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md)** —
  Phases 0–14 phase log
- **[`docs/MULTI_BACKEND_PLAN.md`](docs/MULTI_BACKEND_PLAN.md)** —
  Phases 30–38 (multi-DB + streaming + CLI)
- **[`docs/ERGONOMICS_PLAN.md`](docs/ERGONOMICS_PLAN.md)** — decorator
  API design (Phases 21–25)
- **[`docs/NORMALIZATION_TRANSFORMS_PLAN.md`](docs/NORMALIZATION_TRANSFORMS_PLAN.md)**
  — Phases 26–28
- **[`docs/ML_FEATURE_STORE_PLAN.md`](docs/ML_FEATURE_STORE_PLAN.md)** —
  Phases 15–20
- **[`docs/SQL_TRANSFORMS_PLAN.md`](docs/SQL_TRANSFORMS_PLAN.md)** —
  Phase 39 stream-processing umbrella
- **[`docs/PHASE_39_4_WINDOWS.md`](docs/PHASE_39_4_WINDOWS.md)** —
  tumbling + hopping windows
- **[`docs/PHASE_39_5_SESSIONS.md`](docs/PHASE_39_5_SESSIONS.md)** —
  session windows + `StateStore` foundation
- **[`docs/PHASE_39_5B_JOINS.md`](docs/PHASE_39_5B_JOINS.md)** —
  keyed time-windowed stream-stream join

Deferred design docs (capture both the design and the "why we
haven't built it"):

- **[`docs/UNIFIED_PIPELINE_API.md`](docs/UNIFIED_PIPELINE_API.md)** —
  consolidating the v0.1 decorator and streaming TOML onto one
  declaration surface. Design only.
- **[`docs/ICEBERG_PLAN.md`](docs/ICEBERG_PLAN.md)** — Iceberg
  backend. Deferred because `iceberg-rust` 0.x still pins arrow 57
  vs our arrow 58. Delta covers the use case today.

## License

Apache-2.0
