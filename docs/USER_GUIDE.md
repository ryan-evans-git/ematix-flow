# ematix-flow — User Guide

Step-by-step walkthroughs for every surface the framework exposes,
from a one-line declarative SCD2 sync to a stream-stream join with
durable state recovery.

**Conventions**

- Code blocks marked `# pipeline.toml` are TOML configs for the
  `flow consume` daemon binary.
- Code blocks marked Python use the typed-connection
  `run_streaming_pipeline` API. The same shape is also reachable via
  the `@ematix.streaming_pipeline` decorator.
- Every example is reachable from production code today. No
  pseudo-code.

**Reading order**

1. [Install](#install) — wheels + dev install.
2. [Connections](#connections) — registry + env vars + TOML files.
3. [Surface 1: declarative Postgres](#surface-1-declarative-postgres-pipelines) — original v0.1.
4. [Surface 2: streaming pipelines](#surface-2-streaming-pipelines) — Phases 30–38.
5. [Surface 3: stream processing](#surface-3-stream-processing) — Phase 39 (transforms, windows, sessions, joins).
6. [Operations](#operations) — RunLog backends, alerters, metrics sinks, restart, DLQ, schema evolution.
7. [Troubleshooting](#troubleshooting).

---

## Install

```sh
pip install ematix-flow
# Or: pip install "ematix-flow[df]"     for polars/pandas helpers
# Or: pip install "ematix-flow[spark]"  for pyspark helpers
pip install pyarrow                    # required for streaming pyclasses
```

The `flow` CLI binary, the `run_pipeline` Python entrypoint, and
every streaming backend ship in the core install — no extras.

For a development build:

```sh
git clone https://github.com/ryan-evans-git/ematix-flow.git
cd ematix-flow
python -m venv .venv && source .venv/bin/activate
pip install maturin
maturin develop --release
cargo build --release --bin flow      # CLI binary at target/release/flow
```

---

## Connections

Connections are referenced by name. A name like `"warehouse"`
resolves through this chain (highest priority first):

1. Env var **`EMATIX_FLOW_DSN_WAREHOUSE`** (uppercase the name).
2. Env var **`EMATIX_FLOW_DSN`** — only for the literal name `default`.
3. **`./.ematix-flow.toml`** in the working directory.
4. **`~/.ematix-flow/connections.toml`** for user-wide defaults.
5. Inline **`config.connect(url=...)`** as an escape hatch.

```toml
# ~/.ematix-flow/connections.toml
[connections.warehouse]
url = "postgres://${WAREHOUSE_DSN}"

[connections.kafka]
kind = "kafka"
bootstrap_servers = "localhost:9092"
group_id = "ematix-flow"
```

`${VAR}` interpolation lets secrets stay out of the file. Inspect
what's resolved with:

```sh
flow connections list
flow connections check warehouse
```

For typed connections in Python (preferred over name strings):

```python
from ematix_flow import (
    KafkaConnection, PostgresConnection, SQLiteConnection,
    DeltaS3Connection, register_connection,
)

src = KafkaConnection(
    name="src",
    bootstrap_servers="localhost:9092",
    group_id="ematix-flow",
    sasl_plain_username="alice",
    sasl_plain_password="secret",
)
register_connection(src)
```

### `@ematix.connection` — declarative connection registration

The same typed connections can also be declared with the
`@ematix.connection` decorator. The class body lists the fields, the
decorator constructs the typed instance, registers it under the
class name, and returns the instance. Idiomatic when the connection
lives in the same module as the pipeline that uses it:

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

# Π.1: Schema Registry as a typed connection
@ematix.connection
class sr_prod:
    kind = "schema_registry"
    url = "${SR_URL}"

# Reference SR by name from a KafkaConnection (no inline URL):
@ematix.connection
class kafka_avro:
    kind = "kafka"
    bootstrap_servers = "${KAFKA_BOOTSTRAP}"
    group_id = "ematix-flow"
    payload_format = "avro"
    schema_registry = "sr_prod"
```

After import, `warehouse`, `kafka_prod`, etc. are typed
`Connection` instances and live in the registry — pass them
directly to a pipeline or use the string name. Credentials redact
in `repr()`. Every typed connection (`PostgresConnection`,
`MySQLConnection`, `SQLiteConnection`, `DuckDBConnection`,
`KafkaConnection`, `RabbitMQConnection`, `PubSubConnection`,
`KinesisConnection`, `SchemaRegistryConnection`,
`DeltaLocalConnection`, `DeltaS3Connection`,
`ObjectStoreLocalConnection`, `ObjectStoreS3Connection`) is
declarable through this same shape; the `kind = "..."` attribute
selects which one.

---

## Workflows + Jobs (v0.7.0)

`ematix-flow` has two top-level concepts:

- **Job** — one unit of work. Declared with `@ematix.job(...)`
  (`@ematix.pipeline` remains as an alias). One target, one mode,
  one retry policy. Inside a workflow, a job's DAG position is
  declared by its `depends_on=[upstream_job, ...]` kwarg.
- **Workflow** — a named group of jobs plus the **trigger conditions**
  that fire it. The DAG between jobs is read from each member job's
  `depends_on=`. The workflow's own kwargs answer "what causes this
  workflow to start running?"

### Trigger surface

All trigger kwargs on a workflow are AND-conjoined (every one set must
be satisfied since the workflow last succeeded):

- `triggered_by=[<job_or_workflow_name>, ...]` — event prereqs that
  must have succeeded since this workflow last succeeded.
- `schedule="cron"` + optional `timezone="IANA"` — cron tick must reach.
- `on_message=<KafkaConnection.topic("...")>` — per-message firing
  (mutually exclusive with the above).

At least one trigger is required, unless the workflow contains a
streaming pipeline (it's then implicitly streaming and the consumer
drives execution).

```python
from ematix_flow import ematix

@ematix.job(
    name="extract_orders",
    target=OrdersExtracted, target_connection="warehouse",
    mode="merge", keys=("order_id",),
)
def extract_orders(conn): return "SELECT ..."

@ematix.job(
    name="enrich_orders",
    target=OrdersEnriched, target_connection="warehouse",
    mode="merge", keys=("order_id",),
    depends_on=["extract_orders"],
)
def enrich_orders(conn): return "SELECT ..."

@ematix.job(
    name="aggregate_orders",
    target=OrdersByCustomer, target_connection="warehouse",
    mode="merge", keys=("customer_id",),
    depends_on=["extract_orders"],
)
def aggregate_orders(conn): return "SELECT ..."

@ematix.job(
    name="report_orders",
    target=OrdersReport, target_connection="warehouse",
    mode="merge", keys=("customer_id",),
    depends_on=["enrich_orders", "aggregate_orders"],
)
def report_orders(conn): return "SELECT ..."

ematix.workflow(
    name="evening_combined_report",
    triggered_by=["workflow_A", "workflow_B"],
    schedule="0 21 * * *",
    timezone="America/New_York",
    jobs=[
        "extract_orders",
        "enrich_orders",
        "aggregate_orders",
        "report_orders",
    ],
)
```

The workflow above fires when all three are true since its last
successful run: `workflow_A` succeeded, `workflow_B` succeeded, and
21:00 America/New_York has been reached. If the workflows finish at
22:30 (B was late), it fires at 22:30 — immediately, since all three
conditions are now satisfied.

### Firing semantics

At each scheduler tick:

1. Look up `last_successful_run` for this workflow.
2. For each `triggered_by` upstream: has its last success time exceeded
   `last_successful_run` of self?
3. For `schedule`: has the next cron tick *after* `last_successful_run`
   of self reached?
4. If every declared condition is true → fire. On success, update
   `last_successful_run`.

### Job-level triggers (standalone jobs)

Jobs that are not members of any workflow keep their own
`schedule=` / `triggered_by=` / `on_message=` and act as workflow-of-one
themselves on the UI. Inside a workflow, those per-job triggers are
ignored (a warning is emitted at registration).

### In the Web UI

- **Workflows tab** — one card per workflow with the trigger summary
  ("After: a, b · Schedule: 0 21 * * * America/New_York") and the
  member-job flowchart inline.
- **Jobs tab** — flat list of every individual job, with filters and
  sort.
- **Runs tab** — execution history; sortable column headers.
- **DAG tab** — full cross-workflow graph as an SVG flowchart;
  `#/dag/<job-name>` focuses the subgraph on a single job.

### Migration from v0.6.0

The v0.6.0 shape `ematix.workflow(name=..., jobs=..., depends_on={dict})`
is removed. To migrate:

1. Drop the workflow-level `depends_on=` dict.
2. Add a workflow trigger (`schedule=`, `triggered_by=`, or
   `on_message=`).
3. Move each edge `{downstream: [upstream]}` onto the downstream
   job's decorator as `depends_on=[upstream]`.

---

## Surface 1: declarative Postgres pipelines

The v0.1 API: declare a target table as a class, declare the load
strategy on a function, fire from cron / k8s.

### A 5-line append pipeline

```python
from typing import Annotated
from ematix_flow import ematix, pk
from ematix_flow.types import BigInt, Text, TimestampTZ

@ematix.table(schema="analytics")
class Events:
    event_id: Annotated[BigInt, pk()]
    name: Text | None
    received_at: TimestampTZ

@ematix.pipeline(target=Events, schedule="*/5 * * * *", mode="append")
def ingest_events(conn):
    return "SELECT event_id, name, received_at FROM raw.events"
```

Run it:

```sh
flow run-due --module my_pipelines           # cron-style: fire schedules in last interval
flow run     --module my_pipelines ingest_events  # one-shot
flow status  --module my_pipelines           # per-pipeline status / next-due / attempts
flow preview --module my_pipelines ingest_events  # what would it do?
flow validate --module my_pipelines ingest_events # EXPLAIN against the DB
```

### Dependencies + retry (Ω.1 / Ω.2)

Pipelines can declare upstream dependencies and a per-pipeline
retry policy. `flow run-due` is DAG-aware: it topologically orders
fires, skips downstream pipelines when an upstream fails today,
and applies exponential backoff between attempts.

```python
@ematix.pipeline(
    target=Events,
    schedule="*/5 * * * *",
    mode="append",
    retry={
        "max_attempts": 5,
        "backoff_seconds": 30,
        "backoff_factor": 2.0,        # 30s, 60s, 120s, 240s, 480s
    },
)
def ingest_events(conn):
    return "SELECT event_id, name, received_at FROM raw.events"

@ematix.pipeline(
    target=DailyRollups,
    schedule="0 2 * * *",
    mode="merge",
    depends_on=["ingest_events"],     # waits for today's success
)
def daily_rollups(conn):
    return "SELECT date_trunc('day', received_at) AS d, COUNT(*) AS n " \
           "FROM analytics.events GROUP BY 1"
```

Cycles are caught at module-load time (`CycleError` with the
offending ring). When `daily_rollups` fires before
`ingest_events` succeeds today, `run-due` skips it and records a
skip event in the RunLog so `flow status` shows *waiting on
upstream*. Attempt state survives process restarts when a durable
[RunLog](#durable-run-history-runlog) is configured.

### SCD2 with event-time

```python
from ematix_flow.normalize import lower, trim, parse_timestamp
from ematix_flow.types import String

@ematix.table(schema="analytics")
class CustomerDim:
    customer_id: Annotated[BigInt, pk()]
    email: Annotated[String[256] | None, lower(), trim()]
    name: Text | None
    updated_at: Annotated[TimestampTZ, parse_timestamp()]

@ematix.pipeline(
    target=CustomerDim,
    schedule="0 * * * *",
    mode="scd2",
    compare_columns=["email", "name"],
    event_timestamp_column="updated_at",  # use updated_at as valid_from
    ttl_seconds=86_400 * 30,              # auto-close versions older than 30d
)
def sync_customers(conn):
    return "SELECT customer_id, email, name, updated_at FROM raw.customers"
```

The framework auto-augments the table with `valid_from` / `valid_to`
/ `is_current` / `row_hash` columns. Watermarks live in
`ematix_flow.watermarks`; run history in `ematix_flow.run_history`.
Restart-safe: every run advances the watermark only after a
successful commit.

### Cross-database loads

When source and target are on the same DB, the framework uses an
`INSERT ... SELECT` fast path. When they differ, it streams Arrow
batches between them:

```python
@ematix.pipeline(
    target=CustomerDim,
    schedule="0 * * * *",
    mode="merge",
    source_connection="raw_db",
    target_connection="warehouse",
)
def sync_customers(conn):
    # `conn` is the SOURCE connection (raw_db).
    return "SELECT customer_id, email, name, updated_at FROM customers"
```

### Multi-target fan-out

One source, N targets:

```python
@ematix.pipeline(
    targets=[
        ematix.target(WarehouseEvents, mode="append"),
        ematix.target(DeltaArchive,   mode="append"),
    ],
    schedule="*/5 * * * *",
    source_connection="raw_db",
)
def fan_out(conn):
    return "SELECT * FROM raw.events WHERE received_at > $watermark"
```

See `docs/ERGONOMICS_PLAN.md` for the full decorator surface.

---

## Surface 2: streaming pipelines

A long-running consumer that drains a Kafka topic into Postgres.

### Minimal config

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

Run it three ways:

```python
# From Python
from ematix_flow import run_pipeline
run_pipeline(config="pipeline.toml", metrics_port=9100)
```

```sh
# From the CLI binary
flow consume pipeline.toml --metrics-port 9100 \
                           --restart-on-error \
                           --max-backoff-ms 30000
```

```python
# Or from typed Python connections (no TOML)
from ematix_flow import (
    KafkaConnection, PostgresConnection, run_streaming_pipeline,
)

run_streaming_pipeline(
    name="events-to-pg",
    source=KafkaConnection(name="src", bootstrap_servers="localhost:9092",
                           group_id="ematix-flow"),
    source_query="events",
    target=PostgresConnection(name="warehouse", url="postgres://localhost/mydb"),
    target_table=("public", "events"),
)
```

### `flow consume --module my_pipelines <name>` (Π.3)

For long-running daemons it's nicer to declare the pipeline in a
Python module and have the CLI load it by name — same shape as
`flow run --module M name` for declarative-DB pipelines:

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

The framework imports the module (firing every decorator),
renders the named pipeline's shape into the same TOML the
`flow consume <toml>` form parses, and dispatches into the same
Rust runner. Same at-least-once semantics, metrics, and supervisor
behavior as the TOML form.

### At-least-once semantics

The pipeline calls `commit_offsets()` on the source only after a
durable target write lands. A crash between read and write
re-delivers the same batch on restart; the idempotent target absorbs
it. Same shape across all four streaming sources:

| Source | What `commit_offsets()` does |
|--|--|
| Kafka | `consumer.commit(offsets, sync)` over the consumer group. |
| RabbitMQ | `basic_ack(delivery_tag, multiple=true)`. |
| Pub/Sub | Per-handler `ack` for every message in the batch. |
| Kinesis | Persists the per-shard `committed_sequence_number`. |

### Schema Registry (Kafka, Avro/Protobuf)

TOML form:

```toml
[source]
kind = "kafka"
bootstrap_servers = "localhost:9092"
group_id = "ematix-flow"
payload_format = "avro"
schema_registry_url = "http://localhost:8081"
```

Typed-Python form (Π.1) — SR is its own typed connection so the URL
+ any future basic-auth credentials live in the registry alongside
DB DSNs and Kafka credentials:

```python
from ematix_flow import (
    KafkaConnection, SchemaRegistryConnection, register_connection,
)

sr_prod = SchemaRegistryConnection(name="sr_prod", url="${SR_URL}")
register_connection(sr_prod)

src = KafkaConnection(
    name="kafka_prod",
    bootstrap_servers="${KAFKA_BOOTSTRAP}",
    group_id="ematix-flow",
    payload_format="avro",
    schema_registry=sr_prod,                  # or: schema_registry="sr_prod"
)
```

`payload_format` accepts `"json"` (default), `"avro"`, `"protobuf"`,
`"raw_bytes"`. Avro/Protobuf paths decode against the SR schema and
project to JSON-compatible columns; the resulting batches feed any
target backend identically. The `schema_registry=` field accepts a
`SchemaRegistryConnection` instance or a registered SR name. SR
basic-auth (`basic_auth_user` / `basic_auth_password` on
`SchemaRegistryConnection`) is accepted on the dataclass but the
runtime doesn't yet apply it — fails loud at TOML-emit time
pending a Rust-core follow-up.

### Dead-letter routing

Two layers:

- **App-level** — `dead_letter_topic` on the pipeline. Failed batch
  rows get produced to a separate Kafka topic (source backend must
  be Kafka — its FutureProducer is reused). Source offsets are
  committed only after the DLQ produce ack lands.
- **Broker-level** — RabbitMQ `nack_pending(requeue=False)` plus an
  `x-dead-letter-exchange` queue arg; Pub/Sub `nack_pending` plus a
  subscription `dead_letter_policy`.

```toml
[source]
kind = "kafka"
bootstrap_servers = "localhost:9092"
group_id = "ematix-flow"
dead_letter_topic = "events-failed"
```

### Multi-source fan-in

```toml
pipeline_name = "fanin"

[[sources]]
query = "orders"
kind = "kafka"
bootstrap_servers = "localhost:9092"

[[sources]]
query = "payments"
kind = "kafka"
bootstrap_servers = "localhost:9092"

[target]
kind = "postgres"
url = "postgres://localhost/wh"

[target.table]
schema = "public"
name = "events_union"
```

The pipeline reads from every source concurrently per iteration,
unions the batches at the Arrow level, then writes once. Each source
gets its own offset commit on success.

### Multi-target fan-out (streaming)

```toml
[[targets]]
kind = "postgres"
url = "postgres://localhost/wh"
[[targets.table]]
schema = "public"
name = "events_pg"

[[targets]]
kind = "delta_s3"
bucket = "ematix-archive"
prefix = "events"
```

Every batch writes to every target before offsets advance. Partial
write failure surfaces the first error; targets that already wrote
keep their data (at-least-once across the fan-out — duplicates
absorbed by idempotent targets).

### Object-store target options (Π.1.4 + Π.4)

Object-store targets accept per-format write *and* read options on
`Target`. Π.1.4 shipped Parquet compression + the basic CSV
write knobs; Π.4 added the full CSV/JSON read surface and the
remaining CSV write parity (`csv_quote`, `csv_escape`,
`csv_null_value`).

```python
from ematix_flow import (
    ObjectStoreLocalConnection, Target, run_streaming_pipeline,
)

lake = ObjectStoreLocalConnection(
    name="lake", path="/data/lake", format="parquet",
)

run_streaming_pipeline(
    name="events_archive",
    source=kafka_prod, source_query="events",
    targets=[
        Target(
            connection=lake,
            prefix="events/raw",
            parquet_compression="zstd",      # snappy / gzip / zstd / uncompressed
        ),
    ],
)
```

**CSV write options.** Defaults match Arrow: comma delimiter,
header on, double-quote quoting, doubled-quote escape, empty-string
null rendering.

```python
Target(
    connection=lake,
    prefix="events/csv",
    csv_delimiter=";",           # Π.1.4
    csv_header=False,            # Π.1.4
    csv_quote="'",               # Π.4e — single ASCII char
    csv_escape="\\",             # Π.4e — single ASCII char
    csv_null_value="\\N",        # Π.4e — how null cells render on write
)
```

**CSV read options** apply to schema inference *and* the row
decoder, so a file written with one dialect can be read back with
the same one:

```python
Target(
    connection=lake,
    prefix="events/csv",
    csv_read_options={
        "has_header": True,
        "delimiter": ",",        # single ASCII char
        "quote": '"',
        "escape": "\\",
        "comment": "#",          # ignore lines starting with `#`
        "null_regex": r"^(NA|NULL|\\N)$",
        "truncated_rows_ok": False,
        "schema_infer_max_records": 4096,
    },
)
```

**JSON / JSON-lines read options:**

```python
Target(
    connection=lake,
    prefix="events/json",
    json_read_options={
        "schema_infer_max_records": 4096,
        "batch_size": 8192,
    },
)
```

The typed-Python boundary catches shape mismatches early — setting
`parquet_compression` on a CSV target, or `csv_read_options` on a
Parquet target, raises immediately before TOML round-trip.

**Format auto-detection** for a file path or URL — useful when
building a connection from data dropped into a watch directory:

```python
from ematix_flow.connections import format_from_path

format_from_path("s3://bucket/year=2026/events.csv.gz")  # → "csv"
format_from_path("logs.ndjson")                          # → "json_lines"
format_from_path("data.parquet")                         # → "parquet"
format_from_path("unknown.xml")                          # → None
```

Recognized: `.parquet` / `.pq`, `.csv` / `.tsv`, `.json` /
`.jsonl` / `.ndjson`, `.orc`. The matcher strips `.gz` / `.bz2` /
`.zst` / `.lz4` / `.snappy` first so a `.csv.gz` URL still
classifies as CSV. URLs with query strings (`?download=true`) are
not stripped — strip them before calling if you need that case.

The TOML equivalent of the write block:

```toml
[target]
kind = "object_store_local"
path = "/data/lake"
format = "parquet"
prefix = "events/raw"
parquet_compression = "zstd"

[target.read_options.csv]
has_header  = true
delimiter   = 44                  # ord(',')
null_regex  = "^(NA|NULL)$"
schema_infer_max_records = 4096
```

Parquet readers handle compression transparently from file metadata,
so existing `read_arrow_stream` callers don't change. CSV/JSON read
options are honored by both schema inference and the row decoder.

---

## Surface 3: stream processing

Phase 39 layers stateful transforms onto the streaming pipeline. The
mental model:

```
sources → [optional SQL pre-stage] → [optional window/join] → target
                  ↑                            ↑
           [transform.sql]            [transform.window]
                                      [transform.join]
```

### SQL transform: filter + project + lookup-join (39.1–39.3)

The simplest mid-stream transform — filter and reshape rows before
they reach the target.

```python
from ematix_flow import Lookup, run_streaming_pipeline

run_streaming_pipeline(
    name="events-clean",
    source=KafkaConnection(name="src", bootstrap_servers="localhost:9092",
                           group_id="ematix-flow"),
    source_query="events",
    target=PostgresConnection(name="warehouse", url="postgres://localhost/wh"),
    target_table=("public", "events_enriched"),
    transform_sql="""
        SELECT
            s.user_id,
            s.event_type,
            s.amount,
            u.country,
            s._event_ts
        FROM source s
        LEFT JOIN users u ON s.user_id = u.id
        WHERE s.event_type IN ('click', 'view', 'purchase')
          AND s.amount > 0
    """,
    lookups={
        "users": Lookup(
            connection=PostgresConnection(name="lookups", url="postgres://localhost/refdata"),
            table="users",
            schema="public",
            refresh_interval_ms=300_000,   # re-load every 5 min
        ),
    },
)
```

`transform_sql` is compiled once via DataFusion at the first batch's
schema; subsequent batches reuse the cached plan. Lookups load from
any DB backend on startup. With `refresh_interval_ms`, a background
task reloads + atomically swaps the registered `MemTable`.

The reserved table name `source` references the streaming source.
Lookup names become other tables.

### Aggregating many source rows into one JSON-shaped target row

A common shape mismatch: the upstream is row-per-event (one row per
options contract per minute) but the target column holds a JSON
array of N events per group (one row per *minute*, with all
contracts under it). Other frameworks make you stage the rows,
GROUP BY in a second pipeline, then write the aggregate. ematix-flow
handles this in one pass because DataFusion has `array_agg` +
`named_struct`, and the Postgres write path serialises Arrow
`List<Struct<...>>` directly into a `JSONB` column.

```python
from ematix_flow import (
    ObjectStoreS3Connection,
    PostgresConnection,
    run_streaming_pipeline,
)

s3 = ObjectStoreS3Connection(
    name="raw",
    endpoint="https://files.example.com",
    bucket="market-data",
    region="us-east-1",
    access_key_id="${S3_KEY}",
    secret_access_key="${S3_SECRET}",
    format="csv",
)
warehouse = PostgresConnection(
    name="warehouse",
    url="postgres://localhost/marketdata",
)

run_streaming_pipeline(
    name="option-chain-snapshots",
    source=s3,
    source_query="raw/options/",                  # watched prefix
    target=warehouse,
    target_table=("marketdata", "option_chain_snapshots"),
    transform_sql="""
        SELECT
          date_trunc('minute', ts) AS minute,
          array_agg(named_struct(
            'strike', strike,
            'bid',    bid,
            'ask',    ask
          )) AS strikes_json
        FROM source
        WHERE underlying = 'SPXW' AND days_to_expiry = 0
        GROUP BY 1
    """,
)
```

The target column is a real JSONB:

```sql
CREATE TABLE marketdata.option_chain_snapshots (
  minute        TIMESTAMPTZ NOT NULL,
  strikes_json  JSONB NOT NULL
  -- example payload:
  -- [
  --   {"strike": 4500, "bid": 1.25, "ask": 1.30},
  --   {"strike": 4505, "bid": 0.75, "ask": 0.85},
  --   ...
  -- ]
);
```

**Type mapping.** `named_struct(field, value, ...)` produces an Arrow
`Struct`; `array_agg(struct)` produces a `List<Struct>`. The
PostgresBackend recognises a `List<...>` or `Struct<...>` column
whose Postgres destination is `JSON` (oid 114) or `JSONB` (oid 3802)
and recursively serialises:

- Primitives → JSON primitives (Int*, Float*, Bool, Utf8).
- `Timestamp(Microsecond, _)` → ISO-8601 UTC string.
- `Binary` → lowercase hex (matches Postgres `to_jsonb(bytea)`).
- `Float` NaN / Infinity → `null` (so a single bad row doesn't tank
  a 10k-row batch).

A `List` or `Struct` column targeting a non-JSON Postgres type is
rejected with a clear error pointing at the column name.

### Custom scalar UDFs (`@ematix_flow.udf`)

Functions DataFusion's stdlib doesn't cover (cumulative-normal CDF
for Black-Scholes deltas, custom hashing, financial day-count
conventions, …) register as **scalar UDFs**. Two surfaces:

**Python (recommended for most users).** Decorate a Python callable
with `@udf` and pass it to `run_streaming_pipeline(udfs=[...])`. The
decorator wraps the callable as a DataFusion `ScalarUDF` callable
from any `transform_sql`:

```python
import math

import numpy as np
import pyarrow as pa

from ematix_flow import run_streaming_pipeline, udf


@udf(args=("Float64", "Float64", "Float64", "Float64", "Float64"),
     returns="Float64")
def bs_call_delta(strike, spot, vol, rate, expiry):
    # All five inputs arrive as PyArrow Float64 Arrays once per
    # batch (typically thousands of rows). Convert once, do the
    # math vectorised, return a PyArrow Array.
    k = strike.to_numpy(zero_copy_only=False)
    s = spot.to_numpy(zero_copy_only=False)
    v = vol.to_numpy(zero_copy_only=False)
    r = rate.to_numpy(zero_copy_only=False)
    t = expiry.to_numpy(zero_copy_only=False)
    d1 = (np.log(s / k) + (r + 0.5 * v * v) * t) / (v * np.sqrt(t))
    cdf = 0.5 * (1.0 + np.vectorize(math.erf)(d1 / np.sqrt(2)))
    return pa.array(cdf, type=pa.float64())


run_streaming_pipeline(
    name="option-chain-with-greeks",
    source=s3, source_query="raw/options/",
    target=warehouse, target_table=("marketdata", "option_chain_snapshots"),
    transform_sql="""
        SELECT
          date_trunc('minute', ts) AS minute,
          array_agg(named_struct(
            'strike', strike,
            'bid',    bid,
            'ask',    ask,
            'delta',  bs_call_delta(strike, spot, vol, rate, expiry)
          )) AS strikes_json
        FROM source
        GROUP BY 1
    """,
    udfs=[bs_call_delta],
)
```

**Per-batch dispatch.** One PyO3 GIL acquisition + PyArrow
round-trip per *batch*, not per row. For a 10k-row batch the
overhead amortises across all rows; vectorise inside the callable
(`numpy` / `pyarrow.compute`) — per-row Python loops will be slow.

**Argument and return types** are DataFusion `DataType` strings:
`"Int8"` … `"Int64"`, `"UInt8"` … `"UInt64"`, `"Float32"`,
`"Float64"`, `"Boolean"`, `"Utf8"`, `"Binary"`. Other types raise
`ValueError` at decoration time. Mismatched call sites in SQL
surface at plan-compile time as DataFusion type errors. Two UDFs
registering under the same name is a config-load error — no silent
shadowing.

**The optional `name=` kwarg** lets you decouple the SQL-side name
from the Python identifier — useful when the Pythonic name (`erf`)
collides with a DataFusion built-in or you want to shorten a
namespaced helper:

```python
@udf(args=("Float64",), returns="Float64", name="black76_d1")
def _b76_d1(x):
    ...
```

**Pure-Rust (for callers who want to avoid the PyO3 round-trip).**
The same wiring exists at the Rust layer. Each UDF is an
`Arc<datafusion::logical_expr::ScalarUDF>` (re-exported as
`ematix_flow_core::transform::ScalarUDF` for ergonomics):

```rust
use std::sync::Arc;
use ematix_flow_core::transform::{DataFusionTransform, ScalarUDF};
use datafusion::logical_expr::{ScalarUDFImpl, Signature, Volatility};
// ... implement ScalarUDFImpl for BsDelta; then:
let bs_delta = Arc::new(ScalarUDF::from(BsDelta::new()));
let t = DataFusionTransform::new_with_lookups_and_udfs(
    "SELECT bs_delta(strike, spot, vol, rate, expiry) FROM source",
    input_schema,
    Vec::new(),       // lookups
    vec![bs_delta],   // UDFs
).await?;
```

The cached SQL plan binds to them at construction time, so dropping
the caller-side reference after registration is fine.

### Custom aggregate UDFs (`@ematix_flow.udaf`)

For per-group reductions DataFusion's stdlib doesn't ship —
volume-weighted average price, custom percentiles, distinct-by-
cardinality with custom merge semantics — register a Python
*class* via `@udaf` and pass it to
`run_streaming_pipeline(aggregate_udfs=[...])`:

```python
import pyarrow as pa
import pyarrow.compute as pc

from ematix_flow import run_streaming_pipeline, udaf


@udaf(
    args=("Float64", "Float64"),
    state=("Float64", "Float64"),   # running num + den
    returns="Float64",
    name="vwap",
)
class Vwap:
    def __init__(self):
        self.num = 0.0
        self.den = 0.0

    def update_batch(self, prices, qtys):
        # Per-batch fold. Both args are PyArrow Float64 Arrays.
        self.num += pc.sum(pc.multiply(prices, qtys)).as_py() or 0.0
        self.den += pc.sum(qtys).as_py() or 0.0

    def merge_batch(self, num_states, den_states):
        # Merge K partial-state accumulators into this one. Each arg
        # is a PyArrow Array of length K, one per declared `state` field.
        self.num += pc.sum(num_states).as_py() or 0.0
        self.den += pc.sum(den_states).as_py() or 0.0

    def evaluate(self):
        # Length-1 PyArrow Array of the declared `returns` type.
        if self.den == 0:
            return pa.array([None], type=pa.float64())
        return pa.array([self.num / self.den], type=pa.float64())

    def state(self):
        # One length-1 PyArrow Array per declared `state` field, in
        # declaration order. Same wire shape `merge_batch` consumes.
        return (
            pa.array([self.num], type=pa.float64()),
            pa.array([self.den], type=pa.float64()),
        )


run_streaming_pipeline(
    name="vwap-per-minute",
    source=kafka_quotes, source_query="quotes",
    target=warehouse, target_table=("marketdata", "vwap_per_minute"),
    transform_sql="""
        SELECT
          date_trunc('minute', ts) AS minute,
          vwap(price, qty)         AS vwap
        FROM source
        GROUP BY 1
    """,
    aggregate_udfs=[Vwap],
)
```

**The four required methods** map to DataFusion's `Accumulator`
trait:

- `__init__(self)` — identity state. Called once per group.
- `update_batch(self, *args)` — fold a batch's rows into state.
  Receives one PyArrow Array per declared `args` entry. Use
  `pyarrow.compute` / `numpy` for vectorised math — per-row
  Python loops will be slow.
- `merge_batch(self, *states)` — merge K partial-state
  accumulators into this one (parallel-execution fan-in).
  Receives one PyArrow Array per declared `state` field.
- `evaluate(self)` — final result as a length-1 PyArrow Array of
  the declared `returns` type.
- `state(self)` — intermediate state for shuffle as a tuple of
  length-1 PyArrow Arrays, one per declared `state` field.

**Per-batch dispatch.** Same amortisation story as `@udf`: the
GIL acquisition + PyArrow round-trip happen once per *batch*, so
vectorise inside the methods. The accumulator instance survives
across batches within the same group, so per-group state stays
in Python land.

**Type declarations.** `args` / `state` / `returns` are the same
DataFusion `DataType` strings as scalar UDFs (`"Int64"`,
`"Float64"`, `"Utf8"`, `"Boolean"`, etc.). Mismatched call sites
surface at plan-compile time; mismatched `evaluate()` return
dtype surfaces at first-emit with a pointer at the offending
method.

For aggregates whose per-group state genuinely doesn't fit the
Python round-trip cost — high-cardinality streaming aggregations
where the GIL contention dominates — the same `AggregateUDFImpl`
trait is reachable from pure Rust (mirroring the scalar-UDF
Rust path above).

### Tumbling window (39.4)

Bucketed aggregations — count events per user per minute, no
overlap.

```python
from ematix_flow import Aggregation, Window

run_streaming_pipeline(
    name="events-per-min",
    source=KafkaConnection(name="src", bootstrap_servers="localhost:9092",
                           group_id="ematix-flow"),
    source_query="events",
    target=PostgresConnection(name="warehouse", url="postgres://localhost/wh"),
    target_table=("public", "events_per_min"),
    transform_sql="SELECT user_id, amount, _event_ts FROM source",
    window=Window(
        kind="tumbling",
        duration_ms=60_000,
        group_by=("user_id",),
        max_groups_per_window=1_000_000,    # fail-loud cap
        aggregations=[
            Aggregation(agg="count", as_="n"),
            Aggregation(agg="sum",   column="amount", as_="amount_sum"),
            Aggregation(agg="avg",   column="amount", as_="amount_avg"),
            Aggregation(agg="count_distinct",
                        column="event_type",
                        as_="distinct_event_types",
                        # mode="approximate" (HLL+) is the default;
                        # mode="exact" requires max_distinct_values_per_group.
                        ),
        ],
    ),
)
```

Output schema:

```
window_start TIMESTAMP, window_end TIMESTAMP, user_id BIGINT,
n BIGINT, amount_sum BIGINT, amount_avg DOUBLE,
distinct_event_types BIGINT
```

A window emits when the pipeline's watermark crosses
`window_end`. Watermark = `min` over per-source `max(_event_ts)`,
excluding sources idle for `source_idleness_ms`. Idle ticks (no new
batches) also drive emit — windows fire even with no fresh input as
long as the watermark advances.

#### Hopping windows

`kind="hopping"` plus `hop_ms` — overlapping windows. Each row
contributes to every window whose `[start, end)` interval contains
its event-time.

```python
window=Window(
    kind="hopping",
    duration_ms=300_000,    # 5-min window
    hop_ms=60_000,          # advances every 1 min
    ...
)
```

#### Late-data handling

```python
window=Window(
    kind="tumbling",
    duration_ms=60_000,
    late_data="reopen",
    allowed_lateness_ms=30_000,  # retain state for 30s past window end
    ...
)
```

| Policy | Behavior |
|--|--|
| `"drop"` (default) | Late rows discarded at ingest. Lightest. Counter labeled `policy="drop"`. |
| `"reopen"` | Window state retained for `allowed_lateness_ms` past `window_end`; late arrivals re-aggregate; window re-emits with corrected aggregates. Requires update-style targets (DB MERGE, Delta MERGE). Counter labeled `policy="reopen"`. |
| `"dlq"` | Reserved. Not yet implemented. |

#### Watermark + the `_event_ts` column

Every streaming source backend injects an `_event_ts` column
(timestamp in microseconds) into every batch. Kafka uses the
broker-attached message timestamp; RabbitMQ uses
`AMQPProperties.timestamp`; Pub/Sub uses `publish_time`; Kinesis
uses `approximate_arrival_timestamp`. Override the column name with
`event_time_column="..."` on `Window`.

### Session window (39.5a)

Variable-length windows defined by gap-of-inactivity.

```python
from ematix_flow import StateStore

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
        gap_ms=300_000,                       # 5-min idle = session boundary
        max_session_duration_ms=86_400_000,   # 24h hard cap (mandatory)
        group_by=("user_id",),                # required non-empty
        max_groups_per_window=1_000_000,
        aggregations=[
            Aggregation(agg="count", as_="events"),
            Aggregation(agg="first", column="page", as_="entry_page"),
            Aggregation(agg="last",  column="page", as_="exit_page"),
        ],
    ),
    state_store=StateStore(
        kind="postgres",
        url="postgres://localhost/ematix_state",
    ),
)
```

**Session semantics in one paragraph.** Per group key, a session is
a maximal run of rows whose consecutive event-time gap is ≤
`gap_ms`. The first row past `gap_ms` opens a new session.
`max_session_duration_ms` is a hard ceiling — the cap fires even
under `Reopen` retention. Output adds a per-row `session_id`
column (`hash(group_key, start_ts)` as `UInt64`); `window_start` =
session start, `window_end` = `last_event_ts + gap_ms` (close
boundary).

**State persistence is mandatory.** Sessions can hold state for
hours; rebuilding from source replay isn't bounded. The framework
requires a `[state_store]` block. Today's options:

- `kind = "postgres"` — Postgres-backed, two tables under a
  configurable schema (`ematix_streaming_state` +
  `ematix_streaming_offsets`). Per-emit atomic state+offsets commit
  in a single transaction.
- `kind = "in_memory"` — process-local. Useful for tests; lossy on
  restart.

**Recovery on startup.** `StreamingPipeline::load_state(store)` calls
`StateStore::load(pipeline_name)`, decodes the recovered per-key
session blobs (postcard wire format), rehydrates the windowed
transform's session map, then applies `seek_to(offset_bytes)` to
each source so the next read resumes from the committed offset.

**Source-backend constraints.** PR 1 ships `seek_to` for Kafka only;
the CLI rejects session pipelines configured with non-Kafka sources.
Session pipelines also reject `count_distinct` aggregators today —
HLL+ sketches don't currently round-trip through postcard (see
[`docs/ROADMAP.md`](ROADMAP.md) P1.9).

#### Out-of-order session merging

Under `late_data="reopen"`, a late row arriving within `gap_ms` of
two existing sessions for the same key bridges them: their
accumulator states combine via per-aggregator `combine` ops (sum →
add, min/max → compare, avg → componentwise add of `(sum, count)`,
first/last → pick by event-time, HLL+ → `merge`). Merged sessions
are marked dirty and re-emitted on the next watermark advance.

### Stream-stream join (39.5b)

Keyed time-windowed inner (or outer) join across two `[[sources]]`.
Drivable from typed Python (P2.18) or raw TOML.

**Python (typed):**

```python
from ematix_flow import (
    Join, KafkaConnection, PostgresConnection,
    Source, StateStore, run_streaming_pipeline,
)

orders_kafka = KafkaConnection(
    name="orders_kafka", bootstrap_servers="localhost:9092",
    group_id="ematix-flow",
)
payments_kafka = KafkaConnection(
    name="payments_kafka", bootstrap_servers="localhost:9092",
    group_id="ematix-flow",
)

run_streaming_pipeline(
    name="orders-payments",
    sources=[
        Source(connection=orders_kafka,   query="orders"),
        Source(connection=payments_kafka, query="payments"),
    ],
    target=PostgresConnection(name="warehouse", url="postgres://localhost/wh"),
    target_table=("public", "orders_with_payments"),
    join=Join(
        left_source="orders",                 # matches sources[0].query
        right_source="payments",
        left_keys=("order_id",),
        right_keys=("order_id",),
        time_window_ms=300_000,               # ±5 min symmetric window
        # OR asymmetric:
        # min_delta_ms=0,  max_delta_ms=300_000,  # right MUST arrive after left
    ),
    state_store=StateStore(kind="postgres", url="postgres://localhost/ematix_state"),
)
```

The same shape works through `@ematix.streaming_pipeline(...)` for
schedule-based deployment.

**Equivalent TOML** (still supported via `flow consume`):

```toml
pipeline_name = "orders-payments"

[[sources]]
query = "orders"
kind = "kafka"
bootstrap_servers = "localhost:9092"
group_id = "ematix-flow"

[[sources]]
query = "payments"
kind = "kafka"
bootstrap_servers = "localhost:9092"
group_id = "ematix-flow"

[target]
kind = "postgres"
url = "postgres://localhost/wh"

[target.table]
schema = "public"
name = "orders_with_payments"

[transform]

[transform.join]
kind = "inner"                # or "left_outer" / "right_outer" / "full_outer"
left_source = "orders"
right_source = "payments"
left_keys = ["order_id"]
right_keys = ["order_id"]
time_window_ms = 300000
# late_data = "drop"          # default; "reopen" extends retention by allowed_lateness_ms

[state_store]
kind = "postgres"
url = "postgres://localhost/ematix_state"
```

**Join semantics in one paragraph.** A row from each side joins with
every row from the other side that shares the configured key and
whose event-time falls within `time_window_ms`. Inner join only.
Per-side per-key buffers retain rows until
`global_wm > entry.event_ts + time_window_ms + allowed_lateness_ms`,
then evict per-row. Output schema concatenates `left_<col>` +
`right_<col>` (prefix configurable).

**Each pair emits exactly once** — when the second-arriving side
processes the row it finds N matches in the opposite buffer and
emits N joined rows. The opposite-side buffer doesn't change at
that point; the new row is added to its own side's buffer.

**Cross-validation.**
- `[transform.window]` and `[transform.join]` are mutually
  exclusive.
- `[transform.join]` requires `[[sources]]` (multi-source form,
  not single `[source]`).
- `left_source` / `right_source` must each match the `query` field
  of one of the configured sources.
- `[transform.join]` requires `[state_store]`.

### Advanced transform + watermark knobs (Π.1)

Two production-relevant knobs were originally accepted only via
TOML; both are now drivable from the typed-Python surface and the
decorator:

**Per-batch error policy** — `transform_on_error="fail" |
"drop" | "dlq"`:

```python
run_streaming_pipeline(
    name="events-clean",
    source=kafka_prod, source_query="events",
    target=warehouse, target_table=("public", "events_clean"),
    transform_sql="SELECT user_id, _event_ts FROM source WHERE valid",
    transform_on_error="dlq",            # batch failures → DLQ topic
    dead_letter_topic="events-failed",
)
```

DataFusion executes per-batch, so the granularity is per-batch
(not per-row) — documented behavior. `"drop"` skips a failing
batch silently; `"dlq"` re-uses the existing `dead_letter_topic`
plumbing; `"fail"` aborts the run (default).

**Watermark tuning** — `Watermark(lateness_ms=, source_idleness_ms=)`:

```python
from ematix_flow import Watermark

run_streaming_pipeline(
    ...,
    watermark=Watermark(
        lateness_ms=5_000,            # per-source slack (default 0)
        source_idleness_ms=120_000,   # min-watermark drops idle sources after this (default 60s)
    ),
)
```

Watermarks auto-enable for windowed pipelines + stream-stream
joins; this kwarg lets you override the framework defaults without
hand-writing a `[watermark]` TOML block. Either field can be set
independently — omitted fields keep the Rust core's defaults.

The same kwargs apply on `@ematix.streaming_pipeline` — captured
once, rendered into the TOML the runtime parses.

---

## CDC source mode (Δ)

Phase Δ adds change-data-capture as a *consumption mode* on the
streaming pipeline. Instead of treating each Kafka batch as
"append every row to the target," the runtime decodes each
message as a CDC envelope (Debezium / Maxwell / a custom shape)
and applies the resulting `c` / `u` / `d` / `r` operation to a
**mirror** table.

End-to-end examples (the source half is identical; only the
target differs, so they share the connector + seed SQL but use
different Compose ports so they can run side-by-side):

- [`examples/cdc-debezium`](https://github.com/ryan-evans-git/ematix-flow/tree/main/examples/cdc-debezium)
  — Postgres → Debezium → Kafka → ematix-flow → **Postgres mirror**.
- [`examples/cdc-delta`](https://github.com/ryan-evans-git/ematix-flow/tree/main/examples/cdc-delta)
  — Postgres → Debezium → Kafka → ematix-flow → **Delta Lake (local)**.
  Demonstrates the Δ.X1 single-MERGE-per-batch executor + the
  Δ.X1.2 `[target.table].primary_key` field (required because
  Delta tables don't carry PK metadata that reflection can pick
  up).

(Absolute GitHub URLs because the rendered docs site only serves
`docs/`; relative links to `examples/` don't resolve in mkdocs-
material.)

### What it looks like

**TOML**:

```toml
pipeline_name = "cdc-mirror-customers"
source_query  = "dbz.public.customers"

[source]
kind              = "kafka"
bootstrap_servers = "localhost:9094"
group_id          = "ematix-flow-cdc"

[target]
kind = "postgres"
url  = "postgres://postgres:postgres@localhost:5434/target"

[target.table]
schema = "public"
name   = "customers"

[transform.cdc]
envelope  = "debezium"
key_field = "after.id"
```

**Python** (decorator surface):

```python
import ematix_flow as ematix
from ematix_flow import CDC

@ematix.connection
class warehouse:
    kind = "postgres"
    url  = "${EMATIX_FLOW_TARGET_DSN}"

@ematix.table(connection="warehouse")
class customers:
    schema = "public"
    name   = "customers"
    primary_key = ["id"]

@ematix.streaming_pipeline(
    name="cdc-mirror-customers",
    source_kind="kafka",
    source_query="dbz.public.customers",
    bootstrap_servers="localhost:9094",
    group_id="ematix-flow-cdc",
    target=customers,
    cdc=CDC(envelope="debezium", key_field="after.id"),
)
def mirror_customers(): ...
```

The two surfaces lower to the same `CdcConfig` struct in the
Rust core — pick whichever fits your workflow. `CDC(...)` is a
frozen dataclass; field-for-field equivalent to `[transform.cdc]`.

### Supported envelopes

| `envelope` | Defaults populate |
|---|---|
| `"debezium"` | `op_field="op"`, `before/after`, `source.ts_ms`, `op_map = {c → Create, u → Update, d → Delete, r → Read}` |
| `"maxwell"`  | `op_field="type"`, `data` (after) + `old` (before), `ts` (auto-scales seconds → ms), `op_map = {insert → Create, update → Update, delete → Delete}` |
| `"custom"`   | Every field path + `op_map` must be set explicitly; the validator names what's missing |

For Debezium / Maxwell, `key_field` is the only required
override since the PK column name is table-specific. Common
form: `after.<pk_col>`. The parser falls back to
`before.<pk_col>` for delete events whose `after` is null.

### What happens per batch

1. Source backend reads a Kafka batch.
2. Streaming runtime sees `[transform.cdc]` set → routes through
   `Backend::run_cdc` instead of the universal append path.
3. `events_from_batch` walks the batch and resolves each row to
   a `CdcEvent { op, key, ts_ms, before, after }`. Tombstones
   (`payload = null`) and parse errors counted as `skipped`.
4. The Postgres executor opens one transaction per batch; per
   event, in order:
   - **Idempotency gate**. `INSERT … ON CONFLICT DO UPDATE …
     WHERE existing.last_seen_ts_ms < EXCLUDED.last_seen_ts_ms
     RETURNING 1` against `ematix_flow.cdc_idempotency`. Empty
     RETURNING = redelivery; the executor skips the data write
     and bumps `idempotent_skipped`. Strict-monotonic per
     `(pipeline_name, pk)`. Events with `ts_ms = None` bypass
     the gate (no-idempotency mode).
   - **Schema-evolution check**. Keys in `after` not in the
     target's reflected column set go through the configured
     policy: `"skip"` (default) warns once per drift column per
     batch then lets Postgres's `jsonb_populate_record` discard
     the unknown key; `"fail"` returns an error and rolls back
     the whole batch.
   - **Apply**. UPSERT for `c`/`r`, UPDATE for `u`, DELETE for
     `d` (or column-flip when `delete_mode = "soft"`). All three
     paths use `jsonb_populate_record(NULL::<table>, $1::jsonb)`
     so type coercion is identical.
5. Single transaction commit; source offsets advance only after
   the target commit succeeds (at-least-once with executor-side
   idempotent suppression of redeliveries).

### Configurable knobs

| Field | Default | Meaning |
|---|---|---|
| `envelope` | required | `"debezium"` / `"maxwell"` / `"custom"` |
| `key_field` | required for canonical envelopes | JSON path to PK in the envelope (e.g. `"after.id"`) |
| `delete_mode` | `"hard"` | `"soft"` flips a configured `soft_delete_column` to NOW() instead of DELETE |
| `soft_delete_column` | — | Required when `delete_mode = "soft"` |
| `schema_evolution` | `"skip"` | `"fail"` aborts the batch on first unknown column |
| `out_of_order_tolerance_ms` | `5000` | Reserved for future warn-on-backwards window — the gate is strict-monotonic today |

### Declaring the target's primary key

CDC needs to know which columns make up the target table's
primary key — that's what the apply layer joins on. For
**Postgres** targets, `information_schema` surfaces PK info via
reflection automatically; you don't need to declare anything.

For **Delta Lake** (and any other target whose schema doesn't
carry PK constraints) you must declare the PK on the target.
Three equivalent paths:

```toml
# 1. TOML
[target.table]
schema      = "default"
name        = "customers"
primary_key = ["id"]
```

```python
# 2. Typed-Python multi-target
from ematix_flow.streaming import Target
targets = [
    Target(connection=lake, table=("default", "customers"), primary_key=["id"]),
]
```

```python
# 3. Typed-Python single-target legacy kwargs
run_streaming_pipeline(
    target=lake, target_table=("default", "customers"),
    target_primary_key=["id"],
    ...
)
```

The streaming runtime augments the reflected schema with this
declaration before dispatching to `Backend::run_cdc`. A
typo'd column name (one that doesn't exist in the live target)
fails loud at startup with a message naming the offending
column.

### Cross-validation

`[transform.cdc]` is mutually exclusive with:
- `[transform.window]` (CDC consumes envelopes per-event, windowing them would break the apply contract)
- `[transform.join]` (joins consume two flat streams, not envelopes)
- `[transform.sql]` (a SQL pre-stage projecting envelope columns would lose the original op-/before-/after-shape)

The CLI's `validate_transform_cdc` enforces this at config-load
so the runtime never sees an inconsistent combination.

### Metrics

Pipelines with `[transform.cdc]` set surface five extra Prometheus
counters under `pipeline=<name>`:

- `ematix_streaming_cdc_creates_total` — `c` + `r` ops applied.
- `ematix_streaming_cdc_updates_total` — `u` ops applied.
- `ematix_streaming_cdc_deletes_total` — `d` ops applied (hard or soft).
- `ematix_streaming_cdc_skipped_total` — tombstones + parse failures.
- `ematix_streaming_cdc_idempotent_skipped_total` — events
  rejected by the per-PK last-seen-ts gate (Kafka redeliveries).
  Watch this counter — a steady non-zero rate indicates upstream
  redelivery noise that the gate is absorbing.

### Multi-target reach

| Target | Status | Notes |
|---|---|---|
| **Postgres** | Shipped (Δ PR 3 + 4 + 5 + 5.5) | Full per-event apply with idempotency gate, schema-evolution detection, and streaming-runtime dispatch. |
| **Delta Lake** | Shipped (Δ.X1 PR 1 + Δ.X1.2 + Δ.X1.1) | Single-MERGE-per-batch via `DeltaOps::merge`; within-batch dedupe by newest ts; auto-schema-evolution under Skip policy. Δ.X1.1 added an opt-in `_cdc_last_ts BIGINT` column for between-batch idempotency on top of the natural in-batch dedupe. Streaming-runtime dispatch via `flow consume` works end-to-end — declare the PK on `[target.table].primary_key = [...]` (Delta tables don't carry PK constraints natively). |
| **DuckDB** | Shipped (Δ.X2) | Same per-event shape as Postgres; type coercion via `from_json(?, '<struct-spec>')`. In-memory + file-backed both work. |
| **SQLite** | Shipped (Δ.X2) | Same per-event shape as Postgres; type coercion via `json_extract(?1, '$.col')` per column. `main` schema only (SQLite has no first-class schemas; multi-DB ATTACH is a future enhancement). |
| **MySQL** | Shipped (Δ.X2) | Same per-event shape as Postgres; uses `INSERT ... ON DUPLICATE KEY UPDATE` and `IF(JSON_TYPE(...) = 'NULL', NULL, JSON_UNQUOTE(JSON_EXTRACT(...)))` to keep JSON null clean across all column types. No `RETURNING` — the idempotency gate reads `affected_rows()` (1=insert, 2=advance, 0=reject). |
| **Object stores** | Closed: deferred (Δ.X3) | Parquet / CSV / JSON / ORC files are immutable — per-event UPDATE / DELETE has no clean shape. Use **`DeltaS3Backend`** (already shipped under Δ.X1) for CDC into object storage; Delta sits on the same S3 / GCS / Azure blobs and gives you transactional MERGE for free. The append-only "event log + downstream window-by-PK" pattern remains buildable via the existing transform stage + ObjectStore target without first-class CDC support. |

Default `Backend::run_cdc` impl errors with a clear "not
implemented for this dialect" message on backends without a
concrete implementation, so misconfiguration fails fast.
[`docs/PHASE_DELTA_CDC_PLAN.md`](PHASE_DELTA_CDC_PLAN.md#phase-δ-extensions)
catalogues every extension's design + effort estimate.

---

## Operations

### Durable run history (RunLog)

Every `flow run-due` invocation appends a record per pipeline
firing — status, row counts, attempt count, error message, metrics
JSON — to the configured RunLog. Pick the backend with the
`--run-log <url>` flag (same URL form as `@ematix.connection`
DSNs).

| Scheme | Backend | Typical fit |
|---|---|---|
| `sqlite://path/to/run_log.db` | SQLite (default) | Single-process / single-host. |
| `memory://` | In-memory | Tests; lost on exit. |
| `postgres://user:pw@host/db` | Postgres | Multi-host cron fan-out. |
| `mysql://user:pw@host/db` | MySQL | Same shape as Postgres. |
| `duckdb://path/to/run_log.duckdb` | DuckDB | Local analytical inspection. |
| `s3://bucket/prefix?region=...` | S3 (AWS) | Lambda / serverless. |
| `azureblob://account/container/prefix` | Azure Blob | Azure Functions. |
| `gcs://bucket/prefix` | GCS | Cloud Run / Functions. |

```sh
flow run-due --module my_pipelines \
    --run-log postgres://flow:pw@logdb/flow_history
```

Postgres/MySQL backends auto-create the `ematix_flow` schema and
the run-log tables on first use; pass `create_tables=false` in the
URL query string to opt out (e.g. when DDL is managed by a
migration tool). When the configured location is unwritable
(read-only FS, missing credentials), `flow` warns and continues —
orchestration stays alive even with the durable layer down.

After-the-fact inspection:

```sh
flow runs list --run-log postgres://flow:pw@logdb/flow_history
flow status   --run-log postgres://flow:pw@logdb/flow_history
```

`flow status` reads the RunLog plus the in-process registry and
prints a per-pipeline table: last status, last error, attempt
count, next-due, and waiting-on-upstream state from the DAG.

The RunLog is also what makes the `retry` policy survive process
restarts — attempt state is reloaded from the backend at startup,
so a `max_attempts=5` policy doesn't reset just because the cron
worker rolled.

### Alerters

`--alerter <url>` (repeatable) attaches one or more event sinks
for pipeline failures and recoveries.

| Scheme | Effect |
|---|---|
| `stdout://` | Pretty lines on stderr. |
| `slack://hooks.slack.com/services/...` | POSTs to a Slack incoming webhook. |

```sh
flow run-due --module my_pipelines \
    --run-log postgres://... \
    --alerter slack://hooks.slack.com/services/T000/B000/XXXX \
    --alerter stdout://
```

Buggy alerters are fault-isolated — any exception is logged and
swallowed, never crashes the orchestrator. The recovery event
("pipeline X succeeded after N failed attempts") fires only when
the previous attempt-count was ≥ 1, so first-time successes don't
spam your channel.

### Metrics sinks

`--metrics <url>` exports per-pipeline run counters, durations,
and current attempt-state for declarative pipelines. This is
separate from the streaming-daemon `--metrics-port` (which serves
Prometheus for the long-running consumer).

| Scheme | Effect |
|---|---|
| `null://` | Drop everything (default). |
| `stdout://` | Pretty-print on flush. |
| `memory://` | In-process counters; readable from Python tests. |
| `prometheus://:9100` | `/metrics` endpoint on the given port. |
| `otlp://collector:4318` | OTel HTTP exporter. |

```sh
flow run-due --module my_pipelines \
    --metrics prometheus://:9100
```

Full operator-deployment recipes (per environment, with example
URLs and the right pyproject extras for each backend):
[`docs/DEPLOYMENT.md`](DEPLOYMENT.md).

### Central scheduler (`flow scheduler`)

`flow run-due` is the **cron-tick** model: an external scheduler
(cron, systemd timer, k8s CronJob) wakes the process once per
interval, fires whatever is due, and exits. State survives between
ticks via the RunLog.

`flow scheduler` is the **central-daemon** model: one long-running
process holds a leader lease, walks the DAG every
`--poll-interval` seconds, and hands eligible pipelines off to an
**Executor** that spawns the actual work somewhere else (a local
subprocess, a k8s `Job`, or a Lambda invocation). The scheduler
itself never runs your pipeline code — it just decides what should
fire next.

Pick the central daemon when you want:

- **Fan-out to disposable workers** instead of cramming every
  pipeline into one cron host. k8s `Job` or Lambda dispatch gives
  each pipeline its own pod / container.
- **Sub-minute reactions**, e.g. dispatch within seconds of an
  upstream succeeding. The cron model wakes at minute boundaries;
  `--poll-interval 5` reacts within 5 seconds.
- **One controller process** that's easy to observe and roll —
  alerts on dispatch failure, metrics from a single source.

```sh
flow scheduler \
    --module my_app.pipelines \
    --executor "k8s://flow?image=my-registry/flow:latest&service-account=flow" \
    --run-log-url postgres://flow:pw@db/flow_history \
    --alerter slack://hooks.slack.com/services/T/B/X \
    --metrics prometheus://:9100 \
    --poll-interval 10 \
    --lease-seconds 300
```

**How leader election works.** Multiple scheduler replicas can run
side-by-side for HA; on each tick they all try to `claim` a
reserved `_scheduler_singleton` row in the RunLog. Whichever one
wins the CAS is the leader for that tick; the others log "leader
is X" and sleep. A scheduler that crashes mid-tick loses its lease
after `--lease-seconds` (default 300), and the next tick a
replica takes over. The per-pipeline claim also CAS-protected, so
even a brief two-leader window during a network partition can't
double-dispatch.

**Executor URL schemes.**

| URL | Spawns | Install extra |
| --- | --- | --- |
| `subprocess://` | A `flow run` worker on the same host. Needs `flow` on `PATH`. | (none) |
| `subprocess+python://` | `python -m ematix_flow.cli run` via `sys.executable`. Best for tests and dev shells where the wheel isn't on `PATH`. | (none) |
| `k8s://<namespace>?image=<image>&service-account=<sa>` | A `batch/v1 Job` per pipeline (`backoffLimit: 0`, `restartPolicy: Never`). Image must have your `--module` importable. | `executor-k8s` |
| `lambda://<function-name>?qualifier=<alias>` | Async `Invoke(InvocationType="Event")`. The Lambda's handler invokes `flow run` against the shared RunLog. | `executor-lambda` |

**What workers do.** Each dispatched worker:

1. Imports `--module` (so `@register` fires).
2. Heartbeats the lease every `lease_seconds // 3` while running.
3. Runs the pipeline.
4. Records outcome to the same RunLog URL the scheduler reads from.
5. Releases the claim on exit.

If a worker dies mid-run, the heartbeat stops; on the next
scheduler tick the lease is observed as expired and the row
becomes claimable again. There is no separate "release" step.

**Migration from `flow run-due`.** The two models share the
RunLog, the alerter URLs, the metrics URL, and the
`@register(retry=...)` semantics — switching is a deployment
change, not a code change. A typical migration:

1. Keep your existing `flow run-due` cron running.
2. Stand up `flow scheduler` against the **same** RunLog. The
   leader lock means only one of (cron, scheduler) will actually
   dispatch each minute — whichever wins the per-pipeline CAS
   first. There's no double-fire.
3. Once you trust the daemon, remove the cron entry.

### Prometheus metrics

The CLI binary exposes `/metrics` on `--metrics-port`. Every
streaming pipeline registers:

- `ematix_streaming_rows_consumed_total{pipeline}` — pre-transform
  input row count.
- `ematix_streaming_rows_written_total{pipeline}` — post-transform
  output row count.
- `ematix_streaming_batches_total{pipeline}` — non-empty batches
  processed.
- `ematix_streaming_iterations_total{pipeline}` — total run-loop
  iterations (incl. idle).
- `ematix_streaming_idle_iterations_total{pipeline}` — empty-batch
  iterations.
- `ematix_streaming_errors_total{pipeline}` — surfaced errors.
- `ematix_streaming_dlq_writes_total{pipeline}` — DLQ row count.
- `ematix_streaming_global_watermark_micros{pipeline}` —
  pipeline's current watermark (when watermark machinery is
  enabled).
- `ematix_streaming_per_source_watermark_micros{pipeline,source}`
  — per-source watermark.

When a windowed transform is configured:

- `ematix_streaming_windows_emitted_total{pipeline}` — window
  emits (incl. force-emits for sessions).
- `ematix_streaming_windows_active{pipeline}` — currently open
  windows / sessions.
- `ematix_streaming_state_groups_total{pipeline}` — distinct
  group keys across all windows / sessions.
- `ematix_streaming_late_rows_dropped_total{pipeline,policy}` —
  late rows discarded, labeled by `policy = "drop" | "reopen"`.

### Restart-on-error supervisor

```sh
flow consume pipeline.toml \
    --metrics-port 9100 \
    --restart-on-error \
    --max-backoff-ms 30000 \
    --min-backoff-ms 1000 \
    --max-restarts 100        # or omit for unbounded
```

Exponential backoff with jitter; success resets the counter.

### Schema evolution

Streaming pipelines treat input schema as discovered at first batch.
A schema change mid-stream (new column from upstream Avro/Protobuf)
surfaces an error from the next `transform()` / `write_arrow_stream`
call. The supervisor restart re-discovers the schema and re-plans
DataFusion / re-builds the windowed transform. Lookup table refresh
follows the same pattern — drift causes an error at the next
`transform()`, supervisor restarts, fresh plan.

For declarative Postgres pipelines, target-table drift is detected
at startup via `ensure_table()`; if a column was added upstream the
framework runs `ALTER TABLE ADD COLUMN` (Phase 4 DDL planner).

### Dead-letter routing

App-level (Kafka source only):

```toml
[source]
kind = "kafka"
dead_letter_topic = "events-failed"
```

Broker-level (RabbitMQ):

```toml
[source]
kind = "rabbitmq"
amqp_url = "amqp://guest:guest@localhost:5672"
queue_args = { "x-dead-letter-exchange" = "ematix.dlx" }
```

Broker-level (Pub/Sub):

```toml
[source]
kind = "pubsub"
project_id = "my-project"
subscription = "events-sub"
dead_letter_policy = { topic = "events-dlq", max_delivery_attempts = 5 }
```

---

## Troubleshooting

### "missing field `source_id`" / "join transform requires …"

You're using a join transform but the pipeline isn't dispatching
per-source. Either:
- Use the multi-source `[[sources]]` form (single `[source]` won't
  work for joins).
- Make sure your TOML has `[transform.join]`, not
  `[transform.window]`.

### "source backend dialect Streaming { … } does not support seek_to"

You configured `[transform.window] kind = "session"` (or
`[transform.join]`) on a non-Kafka source. PR 1 ships `seek_to` for
Kafka only — see [`ROADMAP.md`](ROADMAP.md) P1.7. Fix today: switch
the source to Kafka, or remove the session/join.

### "session state persistence: count_distinct aggregators are not supported"

`HLL+` sketches and `HashSet`-backed exact-distinct sets don't
round-trip through postcard yet. Drop `count_distinct` from the
session window, or use a tumbling/hopping window where state isn't
persisted.

### "max_groups_per_window=N reached"

The fail-loud cap. Either bump `max_groups_per_window`, or narrow
the `group_by` to fewer distinct keys, or shorten `duration_ms` /
`gap_ms` so windows close faster.

### "late_data = \"reopen\" requires allowed_lateness_ms"

Set `allowed_lateness_ms` on the `Window`. Reopen retains state
that long past `window_end` for late arrivals.

### Watermark stuck

If `ematix_streaming_global_watermark_micros` doesn't advance:
- Check that `_event_ts` is being injected. For Kafka, the broker
  must attach a message timestamp (`enable.idempotence` in the
  producer guarantees this).
- Multi-source pipelines: the `min` is excluded only after a
  source has been idle for `source_idleness_ms` (default
  10_000). If one source is genuinely producing rows with
  stale event-times, the global watermark sticks at that source's
  max.

### "config parse error: [transform.window] kind = \"session\" requires …"

Read the message — it tells you exactly which field is missing.
Common ones: `gap_ms`, `max_session_duration_ms`, non-empty
`group_by`, or the missing `[state_store]` block.

### A pipeline restart produces duplicate rows

Expected for at-least-once with non-idempotent targets. Use:
- DB targets: `mode = "merge"` or `mode = "scd2"` keys on the
  `(group_keys..., session_id)` (or `(group_keys..., window_start)`
  for tumbling/hopping) so MERGE absorbs duplicates.
- Delta target: `mode = "merge"` on the same.
- Object-store: rotates by `prefix=`; use the partition path to
  bucket emits and dedup downstream.

---

## Interactive DataFrame work in Python

ematix-flow's Python surface is shaped around **declarative
pipelines** — `@ematix.streaming_pipeline` decorators, typed
`Source` / `Target` config, the `flow consume` CLI. That's the
right surface for "this pipeline runs every N seconds in
production" but not for "I'm exploring data in a notebook" or
"let me prototype a transform interactively before formalizing it".

For that interactive use case, **`datafusion-python` is the natural
complement**:

```sh
pip install datafusion
```

```python
from datafusion import SessionContext

ctx = SessionContext()
ctx.register_parquet("orders", "examples/tpch/data/sf1/orders.parquet")
df = ctx.sql("SELECT o_custkey, SUM(o_totalprice) AS total "
             "FROM orders GROUP BY o_custkey ORDER BY total DESC LIMIT 10")
print(df)              # pretty-print
arrow = df.to_arrow_table()  # → pyarrow for downstream tooling
pandas = df.to_pandas()      # if you want a DataFrame
```

It runs the same Apache DataFusion engine ematix-flow uses under
the hood (`crates/ematix-flow-core` pins `datafusion = "53.1"`;
`datafusion-python` tracks the same release line). So:

- A SQL query that works in a `[transform]` block in your
  pipeline TOML works identically in the interactive
  `SessionContext`. Useful for iteration: prototype the SQL
  interactively, then drop it into the pipeline.
- Schemas + type coercion + window functions + the dialect-
  translator-targeted SQL surface (Spark / DuckDB) all behave the
  same. What `datafusion-python` accepts, `LazySqlTransform`
  accepts.

**Not the same thing** (worth being explicit):

- `datafusion-python` ships its own DataFusion + Arrow as a wheel.
  It's a separate process-level install from ematix-flow's wheel.
  Mixing the two in one Python process is fine for normal use, but
  if you're passing PyArrow tables across the boundary be aware
  that they may go through one extra Arrow-IPC round-trip when
  ABIs don't perfectly align.
- ematix-flow's PyO3 layer (the `import ematix_flow` you already
  use for `@ematix.streaming_pipeline`) doesn't depend on
  `datafusion-python`. Installing one doesn't pull in the other.

## Where to read next

- **[`docs/ROADMAP.md`](ROADMAP.md)** — what's left to ship and the
  priority order.
- **[`docs/PHASE_39_4_WINDOWS.md`](PHASE_39_4_WINDOWS.md)** — full
  design for tumbling + hopping windows.
- **[`docs/PHASE_39_5_SESSIONS.md`](PHASE_39_5_SESSIONS.md)** — full
  design for session windows + `StateStore`.
- **[`docs/PHASE_39_5B_JOINS.md`](PHASE_39_5B_JOINS.md)** — full
  design for stream-stream join.
- **[`docs/SQL_TRANSFORMS_PLAN.md`](SQL_TRANSFORMS_PLAN.md)** —
  Phase 39 umbrella plus open design questions.
- **[`docs/MULTI_BACKEND_PLAN.md`](MULTI_BACKEND_PLAN.md)** — every
  backend's design + integration testing rationale.
- **[`docs/ERGONOMICS_PLAN.md`](ERGONOMICS_PLAN.md)** — declarative
  decorator API design.
