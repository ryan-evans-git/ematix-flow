# ematix-flow

**A declarative Python framework for moving and transforming data
between databases, files, and streaming sources. Rust + Apache
Arrow under the hood.**

> Status: **v0.1.2 on PyPI** as `ematix-flow`. All four
> surfaces below — declarative pipelines, multi-backend, streaming,
> stream processing — are shipped and stable.

ematix-flow lets you declare a target table and a load strategy
in Python; the framework handles schema evolution, watermarks,
restart-safe state, at-least-once delivery, and change-data-
capture. Pipelines carry their own `schedule="*/5 * * * *"` and
fire from `flow run-due` (drop that into cron / a k8s `CronJob` /
GitHub Actions and you have a working pipeline tier with no
scheduler service to operate). Plug in Airflow / Dagster /
Prefect if you'd rather, by calling each pipeline's `.sync()`
directly. The same primitives power batch loads (Postgres,
MySQL, SQLite, DuckDB), file targets (Parquet, CSV, JSON, ORC,
Delta Lake — local or S3), and long-running streaming consumers
(Kafka, RabbitMQ, GCP Pub/Sub, AWS Kinesis).

The rest of this README walks through how to use it, in the
order you'd reach for each feature.

---

## Table of contents

1. [Install](#install)
2. [Connections](#connections)
3. [Backends](#backends)
4. [Pipelines](#pipelines)
5. [Modes](#modes)
6. [Scheduling](#scheduling)
7. [Streaming pipelines](#streaming-pipelines)
8. [Stream processing](#stream-processing)
9. [Configuration reference](#configuration-reference)
10. [CLI](#cli)
11. [Python API](#python-api)
12. [Performance and comparisons](#performance-and-comparisons)
13. [What's shipped](#whats-shipped)
14. [Development](#development)
15. [License](#license)

---

## Install

```sh
pip install ematix-flow
```

The core install ships every backend, the `flow` CLI binary, and
the `run_pipeline` / `run_streaming_pipeline` Python entrypoints.

### Optional extras

| Extra | What it adds | Install |
|---|---|---|
| `df` | DataFrame interop helpers (polars / pandas) for `to_polars()` / `to_pandas()` materialization. | `pip install "ematix-flow[df]"` then `pip install polars` (or `pandas`). |
| `spark` | PySpark interop helpers (`to_pyspark()` / `from_pyspark()`). Pulls in PySpark + JVM JDBC. Heavy. | `pip install "ematix-flow[spark]"` |
| `pyarrow` | Required for the streaming-backend `pyclass` wrappers (`KafkaBackend`, `KinesisBackend`, …) when you want batch-by-batch iteration in Python. | `pip install pyarrow` |

The `flow` binary, `run_pipeline`, and the typed-Python streaming
API work without any extras. To build from source, see
[Development](#development) at the bottom.

---

## Connections

Connections are the first thing to set up. Every pipeline
references one or more connections by name; ematix-flow resolves
that name through a layered chain so you can keep secrets out of
code.

### Resolution chain

A connection name like `"warehouse"` resolves through these
sources, highest priority first:

1. **Env var `EMATIX_FLOW_DSN_<NAME>`** — uppercase the name
   (`EMATIX_FLOW_DSN_WAREHOUSE=postgres://...`).
2. **Env var `EMATIX_FLOW_DSN`** — only matches the literal name
   `default`. Convenient for one-off scripts.
3. **`./.ematix-flow.toml`** in the current directory.
4. **`~/.ematix-flow/connections.toml`** for user-wide defaults.
5. **In-process registration** — `register_connection(...)`,
   `@ematix.connection`, or inline `config.connect(url=...)`.

```sh
flow connections list             # what's resolved + from where
flow connections check warehouse  # connect + report
```

### Declaring connections

Three interchangeable forms — pick whichever fits the workflow.

#### TOML file

```toml
# ~/.ematix-flow/connections.toml
[connections.warehouse]
url = "postgres://user:${WAREHOUSE_PASSWORD}@host/wh"

[connections.kafka_prod]
kind = "kafka"
bootstrap_servers = "${KAFKA_BOOTSTRAP}"
group_id = "ematix-flow"
```

`${VAR}` interpolation lets secrets stay out of the file.

#### `@ematix.connection` decorator

```python
from ematix_flow import ematix

@ematix.connection
class warehouse:
    kind = "postgres"
    url = "${WAREHOUSE_DSN}"          # ${VAR} interpolates at use time

@ematix.connection
class kafka_prod:
    kind = "kafka"
    bootstrap_servers = "${KAFKA_BOOTSTRAP}"
    group_id = "ematix-flow"
    sasl_plain_username = "${KAFKA_USER}"
    sasl_plain_password = "${KAFKA_PASS}"
```

After import, `warehouse` and `kafka_prod` are typed `Connection`
instances registered in the runtime. Pass them by reference, or
look them up by name string.

#### Typed instance + `register_connection`

```python
from ematix_flow import PostgresConnection, register_connection

warehouse = register_connection(
    PostgresConnection(name="warehouse", url="postgres://localhost/wh"),
)
```

Useful when the connection has to be built dynamically (e.g.
from an environment-driven dict).

### Credential redaction

Every typed connection redacts secrets in `repr()` by field-name
match (`password`, `secret`, `secret_access_key`, anything
matching `_password`, AMQP URL passwords, etc.). Printing a
connection in a notebook will not spill credentials.

### Schema Registry as a connection

Avro / Protobuf pipelines reference a Schema Registry the same way:

```python
@ematix.connection
class sr_prod:
    kind = "schema_registry"
    url = "${SR_URL}"

@ematix.connection
class kafka_avro:
    kind = "kafka"
    bootstrap_servers = "${KAFKA_BOOTSTRAP}"
    payload_format = "avro"
    schema_registry = "sr_prod"      # name reference
```

---

## Backends

Every source and target lives behind one `Backend` trait. Switch a
pipeline's target by changing one line.

| Backend | Batch source | Streaming source | Target | DDL planning | Strategy executors (append / merge / scd2 / truncate) | CDC target |
|---|:--:|:--:|:--:|:--:|:--:|:--:|
| Postgres | ✅ | — | ✅ | ✅ | ✅ (native + COPY BINARY) | ✅ |
| MySQL | ✅ | — | ✅ | ✅ | ✅ (`ON DUPLICATE KEY`) | ✅ |
| SQLite | ✅ | — | ✅ | ✅ | ✅ | ✅ |
| DuckDB | ✅ | — | ✅ | ✅ | ✅ | ✅ |
| Delta Lake (local + S3) | ✅ | ✅ | ✅ | n/a | ✅ (DataFusion-backed `MERGE`) | ✅ |
| Object stores (Parquet / CSV / ORC / JSONL, local + S3) | ✅ | ✅ | ✅ | n/a | append + truncate | — *(see Δ.X3)* |
| Kafka | — | ✅ | ✅ | n/a | append (cross-backend) | source role only |
| RabbitMQ | — | ✅ | ✅ | n/a | append (cross-backend) | — |
| GCP Pub/Sub | — | ✅ | ✅ | n/a | append (cross-backend) | — |
| AWS Kinesis | — | ✅ | ✅ | n/a | append (cross-backend) | — |

**Batch source** = readable by `@ematix.pipeline` (the function
returns a SQL string; the framework executes it against the
source connection).
**Streaming source** = tailable by `flow consume` /
`@ematix.streaming_pipeline` (long-running consumer with manual
offset commit / ack).
**Target** = writable by either pipeline shape. Cross-backend
moves stream Apache Arrow batches end-to-end — same-DB pairs
take the `INSERT … SELECT` fast path automatically.

### Streaming source guarantees

- **Manual offset commit / ack.** Pipelines call `commit_offsets()`
  on the source only after a durable target write — at-least-once
  end-to-end. Kafka offsets, RabbitMQ `basic_ack`, Pub/Sub handler
  acks, and Kinesis per-shard sequence numbers all flow through
  the same surface.
- **Exactly-once.** Kafka producer-side via transactions;
  consumer-coordinated end-to-end via `KafkaToKafkaEosPipeline`.
- **DLQ.** Both app-level (`dead_letter_topic` routes failed batch
  rows to a separate target) and broker-level (RabbitMQ
  `x-dead-letter-exchange`, Pub/Sub subscription
  `dead_letter_policy`).
- **Schema Registry.** Avro and Protobuf decode/encode via
  Confluent SR or Apicurio.

### Cross-backend reads + writes

When source and target are on the same database, ematix-flow uses
an `INSERT … SELECT` fast path. When they differ, the framework
streams Apache Arrow batches between them — no row-by-row
serialization, no intermediate file roundtrip. Switching from
`Postgres → Postgres` to `Postgres → Delta Lake` is a one-line
change.

---

## Pipelines

A pipeline binds a source query to a target table and a load
strategy. The minimum surface:

```python
from typing import Annotated
from ematix_flow import ematix, pk
from ematix_flow.types import BigInt, Text, TimestampTZ

@ematix.connection
class warehouse:
    kind = "postgres"
    url = "${WAREHOUSE_DSN}"

@ematix.table(schema="analytics")
class Events:
    event_id: Annotated[BigInt, pk()]
    name: Text | None
    received_at: TimestampTZ

@ematix.pipeline(
    target=Events,
    target_connection="warehouse",
    schedule="*/5 * * * *",
    mode="append",
)
def ingest_events(conn):
    return "SELECT event_id, name, received_at FROM raw.events"
```

That's the full file. Run it:

```sh
flow run --module my_pipelines ingest_events     # one-shot
flow run-due --module my_pipelines               # cron-style
flow preview --module my_pipelines ingest_events # what would it do?
flow validate --module my_pipelines ingest_events # EXPLAIN against the DB
```

### Tables

`@ematix.table` declares the target schema. Columns are typed
with PEP-593 annotations + markers:

```python
from ematix_flow.normalize import lower, trim, empty_to_null, parse_timestamp
from ematix_flow.types import String

@ematix.table(schema="analytics")
class CustomerDim:
    customer_id: Annotated[BigInt, pk()]                          # primary key
    email: Annotated[String[256] | None, lower(), trim(), empty_to_null()]
    name: Text | None
    updated_at: Annotated[TimestampTZ, parse_timestamp()]
```

The class name maps to the table name (`CustomerDim` →
`customer_dim`); override with `@ematix.table(schema=..., name="...")`.
`pk()` and `natural_key()` markers control merge keys (see
[Modes](#modes) below). Normalization markers (`lower`, `trim`,
`empty_to_null`, `parse_timestamp`, `parse_int`,
`regex_replace`, `default`, `derive`, raw `sql`) compile to
in-database SQL — no Python row loop.

### Source / target wiring

```python
@ematix.pipeline(
    target=CustomerDim,
    source_connection="raw_db",       # if omitted, defaults to target_connection
    target_connection="warehouse",    # required (or set via EMATIX_FLOW_DSN_<NAME>)
    schedule="0 * * * *",
    mode="scd2",
    compare_columns=["email", "name"],
)
def sync_customers(conn):
    # `conn` is the source connection.
    return "SELECT customer_id, email, name, updated_at FROM customers"
```

| Element | Where it's configured |
|---|---|
| Target table | `schema=` on `@ematix.table` + the class name (snake-cased). |
| Source query | The SQL string returned from the decorated function. Can be a join, subquery, parametrised `WHERE`, etc. |
| Connections | `source_connection=` and `target_connection=` reference names from the [Connections](#connections) registry. |
| Source/target separation | Set both to cross databases. ematix-flow auto-switches between same-DB `INSERT … SELECT` and cross-DB Arrow streaming. |

### Two function signatures

```python
def sync(conn):  return "SELECT …"   # dynamic SQL: filters, dates, etc.
def sync():      pass                # static — pair with source_table=
```

The static form skips boilerplate when the source is a single
table:

```python
@ematix.pipeline(
    target=CustomerDim,
    target_connection="warehouse",
    source_table="raw.customers",      # framework synthesizes SELECT
    column_map={"target_col": "source_col"},  # optional rename map
    mode="merge",
    schedule="0 * * * *",
)
def sync_customers():
    pass
```

### Multi-target fan-out

One source query, N targets:

```python
from ematix_flow import Target

@ematix.pipeline(
    targets=[
        Target(table=CustomerDim,    connection="warehouse"),
        Target(table=CustomerArchive, connection="lake"),
    ],
    source_connection="raw_db",
    schedule="0 * * * *",
    mode="merge",
)
def sync_customers(conn):
    return "SELECT customer_id, email, name, updated_at FROM customers"
```

Each target gets its own strategy executor; failures on one
target don't block the others (configurable via
`continue_on_target_failure=True`).

---

## Modes

The load strategy. Set on `@ematix.pipeline(mode=...)`.

### `append`

Insert all source rows. No deduplication, no updates.

```python
@ematix.pipeline(target=Events, mode="append", schedule="*/5 * * * *")
def ingest(conn):
    return "SELECT event_id, name, received_at FROM raw.events"
```

Use with `incremental=True` and a `watermark_column=` to filter to
new rows on each run; ematix-flow tracks the last-loaded
watermark in `ematix_flow.watermarks`.

### `truncate`

`TRUNCATE` (or `DELETE FROM`) the target, then load. Useful for
small reference / dimension tables that get reloaded fully each
run.

### `merge` (a.k.a. `scd1`)

Insert new rows; update existing rows on PK match. Same as
upserting on conflict. Compare columns control which non-key
fields participate in the update:

```python
@ematix.pipeline(
    target=CustomerDim,
    mode="merge",
    compare_columns=["email", "name"],   # only update if these change
    schedule="0 * * * *",
)
def sync(conn):
    return "SELECT customer_id, email, name FROM raw.customers"
```

### `scd2`

Slowly-changing dimension type 2. Closes the previous version of
each row and inserts a new one when the compare columns change.
ematix-flow auto-augments the table with `valid_from`,
`valid_to`, `is_current`, `row_hash`:

```python
@ematix.pipeline(
    target=CustomerDim,
    mode="scd2",
    compare_columns=["email", "name"],
    event_timestamp_column="updated_at",  # use updated_at as valid_from
    ttl_seconds=86_400 * 30,              # auto-close versions older than 30d
    schedule="0 * * * *",
)
def sync(conn):
    return "SELECT customer_id, email, name, updated_at FROM raw.customers"
```

### How merge keys are resolved

For `merge` and `scd2`, `keys=` is **optional**. Resolution
priority:

1. Explicit `keys=("col_a", "col_b")` on `@ematix.pipeline`.
2. `__merge_keys__ = ("col_a", "col_b")` class dunder on the table.
3. First `natural_key()` group on the table — for SCD2 where the
   business key differs from the versioned PK.
4. Columns marked `pk()`.

A `UserWarning` fires when steps 2 or 3 resolve to keys that
*differ* from `pk()`. Pass `keys=` explicitly to silence.

### Incremental loads + watermarks

```python
@ematix.pipeline(
    target=Events,
    mode="append",
    incremental=True,
    watermark_column="received_at",    # filter source rows > last watermark
    schedule="*/5 * * * *",
)
def ingest(conn):
    return "SELECT event_id, name, received_at FROM raw.events"
```

Watermarks live in `ematix_flow.watermarks` and advance only
**after** a successful target commit — restart-safe.

### Pre- and post-load transforms

```python
from ematix_flow.transforms import deduplicate_by, filter_where

@ematix.pipeline(
    target=Events,
    mode="merge",
    transforms_pre=[
        deduplicate_by(keys=("event_id",), order_by="received_at desc"),
        filter_where("name IS NOT NULL"),
    ],
    transforms_post=[
        "ANALYZE analytics.events",         # raw SQL string
        ematix.transform_ref("update_mart"), # named transform
    ],
    continue_on_failure_post=True,
    schedule="*/5 * * * *",
)
def ingest(conn): ...
```

`transforms_pre` compile to in-database SQL ahead of the load.
`transforms_post` each run in their own transaction.

---

## Scheduling

Three ways to fire a pipeline.

### Cron (`schedule=` on the decorator)

```python
@ematix.pipeline(target=Events, mode="append", schedule="*/5 * * * *")
def ingest(conn): ...
```

Then run on any cadence (cron / k8s `CronJob` / GitHub Actions):

```sh
flow run-due --module my_pipelines
```

`run-due` fires every pipeline whose schedule expires within the
last interval. Idempotent — running it twice in the same window
re-fires nothing because watermarks advance only after success.

### One-shot

```sh
flow run     --module my_pipelines ingest        # run now
flow preview --module my_pipelines ingest        # what would it do? (dry-run, no commit)
flow validate --module my_pipelines ingest       # EXPLAIN against the DB
```

### Programmatic

```python
from my_pipelines import ingest
ingest.sync(keys=("event_id",))     # pipelines expose a `.sync()` method
```

Useful for tests, notebooks, or wrapping a pipeline inside
another orchestrator.

### Run history

Every run lands in `ematix_flow.run_history` with a `run_id`,
status, row counts, error message (if any), and metrics JSON.
Inspect via SQL or `flow runs list`.

---

## Streaming pipelines

A long-running consumer that drains a source and writes batches
to a target with manual at-least-once offset commits, Prometheus
metrics, and exponential-backoff supervised restart.

### TOML form

```toml
# pipeline.toml
pipeline_name = "events-to-pg"
source_query  = "events"
idle_pause_ms = 500

[source]
kind = "kafka"
bootstrap_servers = "localhost:9092"
group_id = "ematix-flow"

[target]
kind = "postgres"
url  = "postgres://localhost/mydb"

[target.table]
schema = "public"
name   = "events"
```

Run from Python:

```python
from ematix_flow import run_pipeline
run_pipeline(config="pipeline.toml", metrics_port=9100)
```

Or from the CLI binary:

```sh
flow consume pipeline.toml \
    --metrics-port 9100 \
    --restart-on-error \
    --max-backoff-ms 30000
```

### Typed-Python form (decorator, no TOML)

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
flow consume-list --module my_pipelines              # show registered pipelines
```

The framework renders the equivalent TOML internally and hands it
to the same Rust runtime — same at-least-once guarantees, same
metrics, same supervisor.

### Multi-target fan-out

```python
from ematix_flow import Target, ObjectStoreS3Connection

@ematix.connection
class lake:
    kind = "object_store_s3"
    endpoint = "https://s3.amazonaws.com"
    bucket = "ematix-archive"
    region = "us-east-1"
    access_key_id = "${AWS_ACCESS_KEY_ID}"
    secret_access_key = "${AWS_SECRET_ACCESS_KEY}"
    format = "parquet"

@ematix.streaming_pipeline(
    name="events_fanout",
    source=kafka_prod,
    source_query="events",
    targets=[
        Target(connection=warehouse, table=("public", "events")),
        Target(connection=lake, prefix="events/raw", parquet_compression="zstd"),
    ],
)
def events_fanout(): pass
```

Each target gets its own write path; failures on one don't stop
the others (configurable).

---

## Stream processing

Stateful transforms layered onto a streaming pipeline.

### SQL transforms

Mid-stream `SELECT` via DataFusion — filter / project / cast /
lookup-join. Static lookups load from any backend at startup;
`refresh_interval_ms` per lookup runs a background refresh task.

```python
run_streaming_pipeline(
    name="events-clean",
    source=kafka_prod, source_query="events",
    target=warehouse,  target_table=("public", "events_clean"),
    transform_sql="""
      SELECT s.user_id, s._event_ts, u.tier
      FROM source s
      LEFT JOIN users u ON u.user_id = s.user_id
    """,
    lookups={"users": Lookup(connection=warehouse,
                             query="SELECT user_id, tier FROM dim_users",
                             refresh_interval_ms=60_000)},
)
```

### Tumbling / hopping windows

Nine aggregators including HLL+ approximate `count_distinct`.
`late_data = "drop" | "reopen" | "dlq"`, idle-tick emission,
per-window `max_groups_per_window` fail-loud cap.

```python
from ematix_flow import Window, Aggregation

run_streaming_pipeline(
    name="events-per-min",
    source=kafka_prod, source_query="events",
    target=warehouse,  target_table=("public", "events_per_min"),
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

### Session windows

Gap-based per-key sessions with mandatory `max_session_duration_ms`
hard cap. Out-of-order session merging under `Reopen`. Mandatory
`StateStore` for restart safety.

```python
from ematix_flow import StateStore

run_streaming_pipeline(
    name="user-sessions",
    source=kafka_prod, source_query="events",
    target=warehouse,  target_table=("public", "user_sessions"),
    transform_sql="SELECT user_id, page, _event_ts FROM source",
    window=Window(
        kind="session",
        gap_ms=300_000,                       # 5-min idle = session boundary
        max_session_duration_ms=86_400_000,   # 24h hard cap
        group_by=("user_id",),
        max_groups_per_window=1_000_000,
        aggregations=[
            Aggregation(agg="count", as_="events"),
            Aggregation(agg="first", column="page", as_="entry_page"),
            Aggregation(agg="last",  column="page", as_="exit_page"),
        ],
    ),
    state_store=StateStore(kind="postgres", url="postgres://localhost/ematix_state"),
)
```

### Stream-stream joins

Keyed time-windowed inner / outer joins. Per-side per-key buffers,
watermark-driven retention, retained-buffer reopen for late
matches.

```python
from ematix_flow import Join, Source

run_streaming_pipeline(
    name="orders-payments",
    sources=[
        Source(connection=kafka_prod, query="orders"),
        Source(connection=kafka_prod, query="payments"),
    ],
    target=warehouse,
    target_table=("public", "orders_with_payments"),
    join=Join(
        left_source="orders",
        right_source="payments",
        left_keys=("order_id",),
        right_keys=("order_id",),
        time_window_ms=300_000,                # ±5 min symmetric
        # kind="left_outer",                   # or "right_outer" / "full_outer"
        # min_delta_ms=-60_000, max_delta_ms=300_000,  # asymmetric
        # late_data="reopen", allowed_lateness_ms=10_000,
    ),
    state_store=StateStore(kind="postgres", url="postgres://localhost/ematix_state"),
)
```

### Change-data-capture

Apply Debezium / Maxwell / custom CDC envelopes to a target table
with proper per-op semantics (insert on `c`, update on `u`,
delete or soft-delete on `d`):

```python
from ematix_flow import CDC

@ematix.streaming_pipeline(
    name="mirror_customers",
    source=kafka_prod,
    source_query="dbserver1.public.customers",
    target=customer_mirror,
    target_connection=warehouse,
    cdc=CDC(envelope="debezium"),
)
def mirror_customers(): pass
```

Per-PK idempotency gate (`ematix_flow.cdc_idempotency`) keeps
Kafka redeliveries from double-applying. Schema-evolution policy
(`Skip` / `Fail`) controls drift behaviour. Hard or soft delete
(`delete_mode="soft", soft_delete_column="deleted_at"`).
Supported targets: Postgres, Delta Lake, DuckDB, SQLite, MySQL.

End-to-end demos:
- [`examples/cdc-debezium/`](examples/cdc-debezium/) — Postgres
  source → Debezium → Kafka → Postgres mirror.
- [`examples/cdc-delta/`](examples/cdc-delta/) — same shape but
  the target is a local Delta Lake table.

### Distributed batch SQL (opt-in)

Set `engine = "distributed"` + `peers = [...]` and the SQL fans
out across a peer-to-peer mesh of `flow-worker` processes via
Apache Arrow Flight. mTLS for the mesh, cross-pod lookup
broadcast, no separate cluster service. SQL dialect translator
(Spark / DuckDB → DataFusion) makes existing queries portable
without rewrites.

---

## Configuration reference

Selected knobs that don't fit any single section above. Every
knob is reachable from typed Python; most also have a TOML
equivalent.

### Watermark behaviour

```python
from ematix_flow import Watermark

run_streaming_pipeline(
    ...,
    watermark=Watermark(
        lateness_ms=5_000,           # how late an event can arrive
        source_idleness_ms=120_000,  # advance watermark when source is idle
    ),
)
```

### Per-batch error policy

```python
run_streaming_pipeline(
    ...,
    transform_on_error="dlq",         # "fail" (default) | "drop" | "dlq"
    dead_letter_topic="events-failed",
)
```

### Object-store write options

```python
Target(
    connection=lake,
    prefix="events/raw",
    parquet_compression="zstd",       # or "snappy" / "gzip" / "uncompressed"
)

Target(
    connection=lake,
    prefix="events/csv",
    csv_delimiter=";",
    csv_header=False,
)
```

The typed-Python boundary catches mis-shaped combos (e.g. setting
`parquet_compression` on a CSV target) before TOML round-trip.

### State store

Every stateful transform (sessions, joins) requires a `StateStore`:

```python
StateStore(
    kind="postgres",
    url="postgres://localhost/ematix_state",
    checkpoint_interval_ms=60_000,    # periodic dirty-state flush cadence
)
```

In-memory mode is for tests only. Pipeline config-load emits a
loud warning if `kind="in_memory"` is paired with a stateful
transform.

### Schema Registry + payload format

```python
@ematix.connection
class kafka_avro:
    kind = "kafka"
    bootstrap_servers = "${KAFKA_BOOTSTRAP}"
    payload_format = "avro"           # or "protobuf" / "json"
    schema_registry = "sr_prod"       # name reference
```

### Connection introspection

```sh
flow connections list
flow connections check warehouse
flow connections set warehouse url=postgres://...
```

---

## CLI

```
flow list                # registered pipelines
flow run <name>          # one-shot
flow run-due             # cron-style fire of all due pipelines
flow preview <name>      # dry-run, no commit
flow validate <name>     # EXPLAIN against the target
flow runs list           # recent runs from ematix_flow.run_history
flow connections list / check / set
flow transform list / run

flow consume <toml>      # streaming daemon (TOML form)
flow consume --module my_pipelines <name>   # typed-Python form
flow consume-list --module my_pipelines     # registered streaming pipelines
```

`--module` points at any importable Python module. `--metrics-port`
exposes Prometheus metrics. `--restart-on-error --max-backoff-ms`
enables the supervised-restart loop.

---

## Python API

For when you want to bypass the pipeline orchestration and use a
streaming backend directly — e.g. in a notebook for ad-hoc
exploration:

```python
from ematix_flow._core import KafkaBackend

backend = KafkaBackend.open(
    "localhost:9092",
    group_id="ematix-flow",
    payload_format="avro",
    schema_registry_url="http://localhost:8081",
    sasl_plain_username="alice",
    sasl_plain_password="secret",
)
backend.ping()

for batch in backend.iter_arrow_stream("events"):
    process(batch)               # batch is pyarrow.RecordBatch

backend.commit_offsets()         # at-least-once: ack only after success
```

The same shape works for `RabbitMQBackend`, `PubSubBackend`, and
`KinesisBackend` (each in `ematix_flow._core`).

For polars / pandas / pyspark interop after a pipeline run, see
the [Install](#install) extras and the
[`docs/USER_GUIDE.md`](docs/USER_GUIDE.md) walkthrough.

---

## Performance and comparisons

ematix-flow uses DataFusion for in-process SQL and Apache Arrow
for cross-backend I/O. Single-node TPC-H benchmarks (M3 Pro):

| Suite | Geomean speedup vs PySpark `local[*]` | Range |
|---|:--:|---|
| TPC-H SF=1 (22 queries) | **5.87×** | 1.78× to 16.74× |
| TPC-H SF=10 (representative subset) | **3.3×** | — |

22/22 PASS on TPC-H SF=1; 103/103 PASS on the canonical Apache
Spark TPC-DS plan-time audit (Spark dialect → DataFusion via the
built-in translator).

Distributed batch SQL across multiple ematix-flow processes is
available via the bundled `flow-worker` peer mesh. Cross-host
scaling claims are honestly framed as deferred — there's no
cluster hardware in this project's runway.

Full methodology, hardware, and per-query numbers:
[`docs/BENCHMARKS.md`](docs/BENCHMARKS.md).

### How it compares

| If you're running… | …ematix-flow replaces… |
|---|---|
| One-off pandas / SQL scripts | Adds correctness guarantees (watermarks, atomic state, schema evolution) without the operational weight of Airflow + Spark. |
| Airflow + dbt | Handles the load logic and streaming sources without a scheduler tier. Cron / k8s `CronJob` / GitHub Actions all fire `flow run-due` — no Airflow worker, no scheduler stub, no DAG plumbing. |
| Kafka Connect + Debezium + custom sinks | First-class CDC source mode dispatches per-op transactionally to your existing target. No JVM connectors to operate. |
| PySpark Structured Streaming (single-node) | Same SQL surface (DataFusion + Spark dialect translator), 5.87× faster geomean, no cluster manager, no JVM. |

---

## What's shipped

All four surfaces are stable. See
[`docs/ROADMAP.md`](docs/ROADMAP.md) for the consolidated
"what's left" punch list (release polish, deferred-feature
extensions, open design questions).

### Test surface (workspace-wide, every PR)

- 459 Rust core unit tests
- 124 Rust CLI unit tests + 27 backend-config scaffold round-trip
  tests across all 10 backends + distributed + TLS config
- ~80 Rust testcontainers integration tests (Docker-gated)
- 376 default Python tests + ~196 testcontainers-gated Python tests
- 22-query TPC-H audit: **22/22 PASS** at SF=1
- 103-query TPC-DS Spark-dialect audit: **103/103 PASS** plan-time

clippy + fmt clean on stable Rust; `cargo audit` green against
RustSec; ruff + bandit + pip-audit green on the Python side.

### Phase plans (for the curious)

- **[`docs/PRD.md`](docs/PRD.md)** — original v0.1 product spec
- **[`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md)** — Phases 0–14
- **[`docs/MULTI_BACKEND_PLAN.md`](docs/MULTI_BACKEND_PLAN.md)** — Phases 30–38 (multi-DB + streaming + CLI)
- **[`docs/ERGONOMICS_PLAN.md`](docs/ERGONOMICS_PLAN.md)** — decorator API design
- **[`docs/SQL_TRANSFORMS_PLAN.md`](docs/SQL_TRANSFORMS_PLAN.md)** — Phase 39 stream-processing umbrella
- **[`docs/PHASE_39_4_WINDOWS.md`](docs/PHASE_39_4_WINDOWS.md)** — tumbling + hopping windows
- **[`docs/PHASE_39_5_SESSIONS.md`](docs/PHASE_39_5_SESSIONS.md)** — session windows + `StateStore`
- **[`docs/PHASE_39_5B_JOINS.md`](docs/PHASE_39_5B_JOINS.md)** — keyed time-windowed stream-stream join
- **[`docs/PHASE_DELTA_CDC_PLAN.md`](docs/PHASE_DELTA_CDC_PLAN.md)** — CDC source mode

→ **Full step-by-step walkthrough:** [`docs/USER_GUIDE.md`](docs/USER_GUIDE.md).
→ **Runnable examples:** [`examples/`](examples/) (one per strategy + streaming + windowed + session + join + CDC).
→ **Cutting a release:** [`docs/RELEASE.md`](docs/RELEASE.md).

---

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

---

## License

Apache-2.0
