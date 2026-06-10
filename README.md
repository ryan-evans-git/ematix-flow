```
███████╗███╗   ███╗ █████╗ ████████╗██╗██╗  ██╗
██╔════╝████╗ ████║██╔══██╗╚══██╔══╝██║╚██╗██╔╝
█████╗  ██╔████╔██║███████║   ██║   ██║ ╚███╔╝
██╔══╝  ██║╚██╔╝██║██╔══██║   ██║   ██║ ██╔██╗
███████╗██║ ╚═╝ ██║██║  ██║   ██║   ██║██╔╝ ██╗
╚══════╝╚═╝     ╚═╝╚═╝  ╚═╝   ╚═╝   ╚═╝╚═╝  ╚═╝
```

# ematix-flow

**Declarative Python data pipelines. Rust + Apache Arrow under the hood.**

Move data between databases, files, and streams with one decorator. Cron
schedules, DAG dependencies, watermarks, schema evolution, restart-safe state,
and at-least-once delivery are all built in — no extra scheduler service to
deploy.

Project site: **[ematix.dev](https://ematix.dev)**.

```python
from ematix_flow import ematix, ManagedTable, Annotated, BigInt, Text, TimestampTZ, pk

# 1. Connection — which database. Credentials redacted in repr();
#    the framework looks this up by name from any pipeline.
@ematix.connection
class warehouse:
    kind = "postgres"
    url = "${WAREHOUSE_URL}"   # resolved from env at run time

# 2. Target table — which schema + which table. Column types are
#    annotated Python; the framework creates / migrates the table
#    on first run.
class Events(ManagedTable):
    __schema__ = "analytics"
    __tablename__ = "events"

    event_id: Annotated[BigInt, pk()]
    name: Text | None
    received_at: TimestampTZ

# 3. Pipeline — source SQL into the target table on a cron.
@ematix.pipeline(
    target=Events,
    target_connection="warehouse",
    schedule="*/5 * * * *",
    mode="append",
)
def ingest_events(conn):
    return "SELECT event_id, name, received_at FROM raw.events"
```

```sh
pip install ematix-flow
flow run-due --module my_pipelines    # cron-style; drop into systemd / cron / k8s CronJob
```

## Why ematix-flow

- **Fast.** TPC-H, 22 queries, single Apple M4 Max, measured at three
  scales through the production preset (fresh context per query, no
  bench-only tricks): ematix-flow takes **21 / 22** at SF=1 (**2.36×**
  DuckDB, **3.95×** Polars, **17×** single-node PySpark), **20 / 22** at
  SF=10 (**1.33×** DuckDB), and **15 / 22** at SF=100 (**1.30×** DuckDB)
  — still ahead on the geomean where the data spills out of cache. Full
  numbers, the losses included, and the reproducer in
  [Benchmarks](#benchmarks).
- **Scheduling + DAG, no service to operate.** Pipelines carry their own
  cron schedule and `depends_on=` edges (with cycle detection and exponential-
  backoff retries). Run `flow run-due` from cron, systemd, a k8s `CronJob`,
  GitHub Actions, or the bundled long-running scheduler — same code, same
  topological order, same retry semantics. Already on Airflow / Dagster /
  Prefect? Call `.sync()` directly.
- **Batteries included.** Postgres, MySQL, SQLite, DuckDB, Snowflake,
  BigQuery, Redshift, Kafka, RabbitMQ, Kinesis, Pub/Sub, S3, Delta Lake.
  Schema Registry + Avro / Protobuf. CDC source mode dispatches per-op
  transactionally to your existing target.
- **Operationally honest.** Restart-safe state, watermarks, at-least-once
  delivery, credential redaction, structured run history, Prometheus +
  OpenTelemetry metrics, Slack alerts.

> Status: **v0.4.0 on PyPI** as `ematix-flow` (alpha). All four surfaces —
> declarative pipelines, multi-backend, streaming, stream processing — are
> shipped end-to-end. The next release adds
> `@ematix.warehouse_pipeline` (cron + retries + DAG for warehouse
> sources/targets), AWS Glue Schema Registry, and timezone-aware cron
> schedules; see CHANGELOG for the in-flight slice plan.

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
12. [Benchmarks and comparisons](#benchmarks)
13. [What's shipped](#whats-shipped)
14. [Development](#development)
15. [License](#license)

---

<a id="install"></a>
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
| `spark` | PySpark interop helpers (`to_pyspark()` / `from_pyspark()`). Heavy — pulls in PySpark + its JDBC dependency. | `pip install "ematix-flow[spark]"` |
| `pyarrow` | Required for the streaming-backend `pyclass` wrappers (`KafkaBackend`, `KinesisBackend`, …) when you want batch-by-batch iteration in Python. | `pip install pyarrow` |

The `flow` binary, `run_pipeline`, and the typed-Python streaming
API work without any extras. To build from source, see
[Development](#development) at the bottom.

---

<a id="connections"></a>
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

`${VAR}` interpolation lets secrets stay out of the file. Bare
`${VAR}` resolves against `os.environ`; prefixed forms route to
pluggable resolvers — `${vault:secret/db#password}` (HashiCorp Vault),
`${aws:prod/db#password}` (AWS Secrets Manager), `${gcp:db-password}`
(GCP Secret Manager). Register additional resolvers via
`ematix_flow.secrets.register_resolver`.

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

Today's `kind = "schema_registry"` covers Confluent-style wire format
(Confluent SR, Apicurio Confluent-compat). **AWS Glue Schema Registry**
(separate framing: `0x03` header byte + UUID + compression byte) is
fully wired in v0.5.0 — use
`GlueSchemaRegistryConnection(kind = "glue_schema_registry", …)`,
and the Rust Kafka backend dispatches consumer + producer paths to
Glue automatically. End-to-end recipe (LocalStack quickstart +
production wiring + IAM template) at
[`docs/DEPLOYMENT.md` Recipe 10](docs/DEPLOYMENT.md#recipe-10--kafka--aws-glue-schema-registry).

---

<a id="backends"></a>
## Backends

Every source and target lives behind one `Backend` trait. Switch a
pipeline's target by changing one line.

| Backend | Batch source | Streaming source | Target | DDL planning | Strategy executors (append / merge / scd2 / truncate) | CDC target |
|---|:--:|:--:|:--:|:--:|:--:|:--:|
| Postgres | ✅ | — | ✅ | ✅ | ✅ (native + COPY BINARY) | ✅ |
| MySQL | ✅ | — | ✅ | ✅ | ✅ (`ON DUPLICATE KEY`) | ✅ |
| SQLite | ✅ | — | ✅ | ✅ | ✅ | ✅ |
| DuckDB | ✅ | — | ✅ | ✅ | ✅ | ✅ |
| Snowflake | ✅ | — | ✅ | n/a | append (`write_pandas`) + truncate + merge (staged `MERGE`) | — |
| BigQuery | ✅ | — | ✅ | n/a | append (`load_table_from_dataframe`) + truncate + merge (`MERGE INTO` from staging) | — |
| Redshift | ✅ | — | ✅ | n/a | append (S3 → `COPY`) + truncate + merge (`MERGE INTO` from staging) | — |
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
  Confluent SR or Apicurio. AWS Glue Schema Registry support lands
  in the next release.

### Cross-backend reads + writes

When source and target are on the same database, ematix-flow uses
an `INSERT … SELECT` fast path. When they differ, the framework
streams Apache Arrow batches between them — no row-by-row
serialization, no intermediate file roundtrip. Switching from
`Postgres → Postgres` to `Postgres → Delta Lake` is a one-line
change.

---

<a id="pipelines"></a>
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
flow status --module my_pipelines                # operator view
flow preview --module my_pipelines ingest_events # what would it do?
flow validate --module my_pipelines ingest_events # EXPLAIN against the DB
```

#### Dependencies + retry

Pipelines can declare upstream `depends_on` and a per-pipeline
`retry` policy. `flow run-due` honors both: it topologically
orders fires, skips downstream work when upstreams fail, and
applies exponential backoff between attempts.

```python
@ematix.pipeline(
    target=DailyRollups,
    target_connection="warehouse",
    schedule="0 2 * * *",
    mode="merge",
    depends_on=["ingest_events"],          # must succeed first today
    retry={"max_attempts": 5, "backoff_seconds": 30, "backoff_factor": 2.0},
)
def daily_rollups(conn):
    return "SELECT ... FROM analytics.events GROUP BY ..."
```

Cycles are detected at module load time. Attempt state survives
process restarts when a durable [Run history](#run-history)
backend is configured.

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

<a id="modes"></a>
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

<a id="scheduling"></a>
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

### Web UI (`flow web`)

For visual ops, `flow web` serves a local SPA over the same
RunLog. Lists pipelines + run history, shows per-run task graphs,
and exposes one-click **restart from failed step** / **resume from
watermark** / **rerun from beginning** / **pause** / **resume** on
in-flight runs.

```sh
pip install "ematix-flow[web]"
flow web --port 8080
# open http://127.0.0.1:8080/
```

Localhost-only by default. For remote access, pass `--token <value>`
to enable bearer-token auth on every `/api/*` route
(`/api/health` stays open for load-balancer probes); without a token,
binding a non-loopback address logs a warning — SSH tunnel or front
with a reverse proxy.

```sh
flow web --port 8080 --bind 0.0.0.0 --token "$EMATIX_FLOW_WEB_TOKEN"
# clients send: Authorization: Bearer $EMATIX_FLOW_WEB_TOKEN
```

The landing view is **Pipelines** — one card per pipeline with a
last-10-execution strip (teal = succeeded, red = failed,
amber-pulse = running; a retried-then-succeeded run counts as green).
Bar **height** is proportional to run duration, scaled per-pipeline
so variance is visible at a glance — a tall bar means that run took
unusually long. The right side shows "Next: `<UTC>`" for batch
pipelines or "LIVE STREAMING" for streaming. Batch cards have a
median-duration footer; streaming cards swap that for live throughput
(1m / 5m rolling) + average batch cycle, snapshotted from the
daemon's `/metrics` endpoint every ~30s:

![Pipelines view — batch median duration vs streaming live throughput](docs/screenshots/pipelines-view.png)

Clicking any square drills into the run-detail page, which renders
the task DAG live — solid teal for **succeeded**, pulsing amber
for **running**, dashed dim teal for **pending**, solid red for
**failed**:

![workflow DAG with parallel branch](docs/screenshots/workflow-dag.png)

Reading left-to-right: `extract_orders` (done) fans out into two
parallel branches — `transform_orders` (done) and `transform_payments`
(running) — both feeding `merge_payments` and ultimately
`load_warehouse`. Sibling steps at the same rank stack vertically
so parallelism is visible at a glance.

The action buttons map directly to scheduler-loop pickup: clicking
**Restart from step "merge_payments"** writes a `requested` row to
the RunLog with `extras["restart_from_step"]`; the next scheduler
tick claims it and the worker honors `EMATIX_FLOW_RESTART_FROM_STEP`
to resume the DAG from that node — upstream artifacts get reused
rather than recomputed.

A third top-level view, **DAG**, renders the cross-pipeline
`depends_on` graph: each node carries its schedule + timezone +
fan-out count, and topological-rank columns guarantee upstreams sit
left of their downstreams. Useful for spotting orphan pipelines,
unintended cycles, or fan-out hotspots before they bite at scheduler
tick time.

More screenshots + walkthrough: [ematix.dev/specs/04-web-ui-screenshots](https://ematix.dev/specs/04-web-ui-screenshots).

### Run history

Every run lands in the configured RunLog with a `run_id`, status,
row counts, attempt count, error message (if any), and metrics
JSON. Inspect via SQL, the backend's tooling, or `flow runs list`.

#### RunLog backends

Choose a backend via URL — same form for the CLI flag, the
`@ematix.connection` decorator, and `run_due_with_dag_detailed`.

| Scheme | Backend | Notes |
|---|---|---|
| `sqlite://path/to/run_log.db` | SQLite (default) | Single-process; zero config. |
| `memory://` | In-memory | Tests only; lost on exit. |
| `postgres://user:pw@host/db` | Postgres | Multi-host; auto-creates `ematix_flow` schema unless `create_tables=false`. |
| `mysql://user:pw@host/db` | MySQL | Same shape as Postgres. |
| `duckdb://path/to/run_log.duckdb` | DuckDB | Single-file analytical store. |
| `s3://bucket/prefix?region=...` | S3 (AWS) | JSONL append; good for serverless. |
| `azureblob://account/container/prefix` | Azure Blob | Append-block log. |
| `gcs://bucket/prefix` | GCS | JSONL append. |

```sh
flow run-due --module my_pipelines \
    --run-log postgres://flow:pw@logdb/flow_history \
    --alerter slack://hooks.slack.com/services/... \
    --metrics prometheus://:9100
```

When the configured RunLog location is unwritable (lambda
read-only FS, missing credentials), `flow` warns and continues —
orchestration stays alive even with the durable-history layer
down.

#### Alerters

`--alerter <url>` (repeatable) attaches one or more sinks for
failure / recovery events.

| Scheme | Effect |
|---|---|
| `stdout://` | Human-readable lines on stderr. |
| `slack://hooks.slack.com/services/...` | Posts to a Slack incoming webhook. |
| `email://user:pass@host:port?from=…&to=…&starttls=1` | Sends email via stdlib `smtplib` (default port 587 STARTTLS / 465 implicit SSL). |
| `pagerduty://<routing_key>?service=…&severity=…` | PagerDuty Events API v2 trigger/resolve; `dedup_key="<service>:<pipeline>"` auto-resolves on recovery. |

Buggy alerters are fault-isolated: any exception is logged and
swallowed, never crashes the orchestrator.

#### Metrics sinks

`--metrics <url>` exports per-pipeline run counts, durations, and
current attempt state.

| Scheme | Effect |
|---|---|
| `null://` | Drop everything (default). |
| `stdout://` | Pretty-print on flush. |
| `memory://` | In-process counters; readable from Python. |
| `prometheus://:9100` | `/metrics` endpoint on the given port. |
| `otlp://collector:4318` | OTel HTTP exporter. |

Full operator-deployment recipes (per environment, with example
URLs and the right pyproject extras): [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md).

---

<a id="streaming-pipelines"></a>
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

<a id="stream-processing"></a>
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

### Aggregating many source rows into one JSON-shaped target row

When the target schema isn't 1:1 with the source — e.g. an
`option_chain_snapshots(minute, strikes_json)` table where every
row carries a JSON array of N contracts for that minute —
DataFusion's `array_agg(named_struct(...))` does the pivot inside a
single `transform_sql` pass. The framework's Postgres write path
serializes Arrow `List<Struct<...>>` straight into a JSONB column,
so no staging table or two-stage pipeline is needed.

```python
run_streaming_pipeline(
    name="option-chain-snapshots",
    source=s3,                                # CSV files under a watched prefix
    source_query="raw/options/",
    target=warehouse,                         # postgres
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

The mirror table:
```sql
CREATE TABLE marketdata.option_chain_snapshots (
  minute        TIMESTAMPTZ NOT NULL,
  strikes_json  JSONB NOT NULL  -- [{"strike": 4500, "bid": 1.25, "ask": 1.30}, ...]
);
```

Same shape works for `MERGE` / `SCD2` / `Truncate` strategies via
the strategy executor — the JSONB column round-trips like any other
type.

For functions DataFusion's stdlib doesn't cover (cumulative-normal
CDF, custom hashing, day-count conventions), register a Python
scalar UDF — covered in the next section.

### Python UDFs (`@ematix_flow.udf`)

Wrap a Python callable as a DataFusion scalar UDF and call it from
`transform_sql`. Per-batch dispatch through PyArrow zero-copy: one
GIL acquisition + PyArrow round-trip per batch (typically thousands
of rows), so vectorised numpy / pyarrow.compute inside the callable
amortises the overhead.

```python
import math

import numpy as np
import pyarrow as pa

from ematix_flow import run_streaming_pipeline, udf


@udf(args=("Float64", "Float64", "Float64", "Float64", "Float64"),
     returns="Float64")
def bs_call_delta(strike, spot, vol, rate, expiry):
    # All five inputs arrive as PyArrow Float64 Arrays; convert
    # once, do the math vectorised, ship a PyArrow Array back.
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

Argument and return types are DataFusion `DataType` strings —
`"Int64"`, `"Float64"`, `"Utf8"`, `"Boolean"`, etc. Mismatched call
sites surface at plan-compile time as DataFusion type errors. Two
UDFs registering under the same name is a config-load error — no
silent shadowing.

### Python aggregate UDFs (`@ematix_flow.udaf`)

For per-group reductions DataFusion's stdlib doesn't ship (VWAP,
custom percentiles, distinct-by-cardinality), decorate a Python
*class* with `@udaf` and pass it as `aggregate_udfs=`. The class
must expose four methods — `update_batch`, `merge_batch`,
`evaluate`, `state` — mirroring DataFusion's `Accumulator` trait.
PyArrow zero-copy on the inputs, length-1 PyArrow Arrays on the
outputs.

```python
import pyarrow as pa
import pyarrow.compute as pc

from ematix_flow import run_streaming_pipeline, udaf


@udaf(args=("Float64", "Float64"),
      state=("Float64", "Float64"),
      returns="Float64", name="vwap")
class Vwap:
    def __init__(self):
        self.num = 0.0
        self.den = 0.0

    def update_batch(self, prices, qtys):
        self.num += pc.sum(pc.multiply(prices, qtys)).as_py() or 0.0
        self.den += pc.sum(qtys).as_py() or 0.0

    def merge_batch(self, num_states, den_states):
        self.num += pc.sum(num_states).as_py() or 0.0
        self.den += pc.sum(den_states).as_py() or 0.0

    def evaluate(self):
        if self.den == 0:
            return pa.array([None], type=pa.float64())
        return pa.array([self.num / self.den], type=pa.float64())

    def state(self):
        return (pa.array([self.num], type=pa.float64()),
                pa.array([self.den], type=pa.float64()))


run_streaming_pipeline(
    name="vwap-per-minute",
    source=kafka_quotes, source_query="quotes",
    target=warehouse, target_table=("marketdata", "vwap_per_minute"),
    transform_sql="""
      SELECT date_trunc('minute', ts) AS minute,
             vwap(price, qty)          AS vwap
      FROM source GROUP BY 1
    """,
    aggregate_udfs=[Vwap],
)
```

Per-batch dispatch (one GIL acquisition + PyArrow round-trip per
batch, accumulator instance survives across batches within a
group), vectorise with `pyarrow.compute` or `numpy` inside the
methods. See the user guide for the full state-shape contract +
the pure-Rust escape hatch when GIL contention dominates.

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
Supported targets: Postgres, Delta Lake (local + S3), DuckDB,
SQLite, MySQL.

**Raw object stores (S3 / GCS / Azure Parquet, CSV, JSON) are
intentionally not CDC targets** — those file formats are
immutable, so per-event UPDATE / DELETE has no clean shape. For
CDC into object storage, use `DeltaS3Backend`: Delta sits on top
of the same object stores and gives you transactional MERGE for
free. The append-only "event log" pattern (one row per change
event, materialise current state downstream) remains buildable
ad-hoc with the existing transform stage + ObjectStore target.

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

**Peer auto-detection.** `peers = [...]` accepts three schemes:

- `http://host:port` — static, unchanged.
- `dns://host:port` — resolves the A-record at startup and
  expands to every IP behind the name (good for headless services).
- `k8s://service.namespace:port` — sugar for
  `dns://service.namespace.svc.cluster.local:port`.

Mix and match freely. Static peers added by the user are kept as-is;
auto-detected peers are appended on top.

**Engine default — `engine = "auto"`.** When unspecified (or set to
`"auto"`), the runtime picks distributed if `peers` expands to ≥1 URL,
otherwise falls back to in-process with an info log. Existing
`engine = "single"` / `engine = "distributed"` selections are honored
verbatim.

---

<a id="configuration-reference"></a>
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

### Object-store file formats

Object-store backends support `parquet`, `csv`, `json_lines`, and
`orc`. The format is set on the connection and steers both the
reader (schema inference + decode) and the writer (per-format
encoder).

```python
from ematix_flow.connections import format_from_path

format_from_path("s3://bucket/year=2026/events.csv.gz")  # → "csv"
format_from_path("logs.ndjson")                          # → "json_lines"
format_from_path("data.parquet")                         # → "parquet"
```

Recognized extensions cover `.parquet` / `.pq`, `.csv` / `.tsv`,
`.json` / `.jsonl` / `.ndjson`, and `.orc`; the matcher strips
`.gz` / `.bz2` / `.zst` / `.lz4` / `.snappy` first.

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
    csv_delimiter=";",                # single ASCII char
    csv_header=False,
    csv_quote="'",                    # Π.4e — defaults to `"`
    csv_escape="\\",                  # defaults to doubled-quote
    csv_null_value="\\N",             # how null cells render on write
)
```

### Object-store read options

CSV decode and JSON-lines decode honor a matching set of
options. Schema inference and the row decoder both see the same
settings, so a file written with one dialect can be read back with
the same one.

```python
Target(
    connection=lake,
    prefix="events/csv",
    csv_read_options={
        "has_header": True,
        "delimiter": ",",
        "quote": '"',
        "escape": "\\",
        "comment": "#",
        "null_regex": r"^(NA|NULL|\\N)$",  # in addition to empty string
        "truncated_rows_ok": False,
        "schema_infer_max_records": 4096,
    },
    json_read_options={
        "schema_infer_max_records": 4096,
        "batch_size": 8192,
    },
)
```

The typed-Python boundary catches mis-shaped combos (e.g. setting
`parquet_compression` on a CSV target, or `csv_read_options` on a
Parquet target) before TOML round-trip.

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

<a id="cli"></a>
## CLI

```
flow list                # registered pipelines
flow run <name>          # one-shot
flow run-due             # cron-style fire of all due pipelines (DAG-aware)
flow scheduler --executor <url>   # long-running daemon: claim pipelines, dispatch to workers
flow status              # operator view: per-pipeline status / next-due / attempts
flow preview <name>      # dry-run, no commit
flow validate <name>     # EXPLAIN against the target
flow runs list           # recent runs from the configured RunLog
flow logs <run_id>       # tail captured stdout/stderr for a run (opt-in capture: EMATIX_FLOW_CAPTURE_LOGS=1)
flow connections list / check / set
flow transform list / run

flow init <dir>          # scaffold a runnable starter project (pipelines.py + connections.toml + Dockerfile + flow.service)
flow doctor              # connection-health probes per kind (pg / kafka / glue / s3 / snowflake / …); non-zero exit on any failure
flow secrets test [path] # resolve every ${…} in connections.toml + report per-secret OK/missing/error (no values printed)

flow consume <toml>      # streaming daemon (TOML form)
flow consume --module my_pipelines <name>   # typed-Python form
flow consume-list --module my_pipelines     # registered streaming pipelines
```

`flow run-due` is the cron-tick model (external scheduler fires the
process each minute). `flow scheduler` is the central-daemon model:
one long-running process holds a leader lease, walks the DAG every
`--poll-interval` seconds, and hands eligible pipelines to an
Executor (`subprocess://`, `k8s://<ns>?image=...`,
`lambda://<fn>`). See [USER_GUIDE.md § Central scheduler](docs/USER_GUIDE.md#central-scheduler-flow-scheduler)
and [DEPLOYMENT.md recipe 8](docs/DEPLOYMENT.md#recipe-8--central-scheduler-fan-out-to-workers).

`--module` points at any importable Python module.

Observability flags (work on `run-due`, `status`, `runs list`):

```
--run-log <url>     # any RunLog scheme (sqlite/postgres/mysql/duckdb/s3/azureblob/gcs/memory)
--alerter <url>    # repeatable; stdout:// / slack://... / email://... / pagerduty://...
--metrics  <url>   # null:// / stdout:// / memory:// / prometheus://:port / otlp://endpoint
--traces   <url>   # OTEL spans for every pipeline run: otel://stdout / otel+otlp+grpc://… / otel+otlp+http://…
```

Streaming-daemon flags: `--metrics-port` exposes Prometheus metrics;
`--restart-on-error --max-backoff-ms` enables the supervised-restart
loop.

---

<a id="python-api"></a>
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

<a id="performance-and-comparisons"></a>
<a id="benchmarks"></a>
## Benchmarks and comparisons

### TPC-H — SF=1 / SF=10 / SF=100, all 22 queries

Five engines, the same Apple M4 Max, the same Parquet files (2026-06-10
refresh). ematix-flow numbers are **production-faithful**: the harness
(`examples/tpch_preset_rebench.rs`) builds a fresh `SessionContext` per
measurement through the shipped preset — no plan cache, no context reuse
— and ematix-flow + DuckDB run in **isolated passes** (20 timed trials
after 3 warmups at SF=1 / SF=10; 5 after 1 at SF=100, medians). Polars
runs the in-process harness (SF=10 re-measured 2026-06-10, other columns
carried from 2026-05-29/31); PySpark is `local[*]` on the JVM; Postgres
14 is `EXPLAIN ANALYZE` Execution Time.

| Scale | ematix-flow wins | vs DuckDB | vs Polars | vs PySpark | vs Postgres |
|---|:--|--:|--:|--:|--:|
| **SF=1** (~1 GB, in-cache) | **21 / 22** | 2.36× | 3.95× | 17.1× | 15.7× |
| **SF=10** (~10 GB) | **20 / 22** | 1.33× | 4.00× | 12.0× | 21.7× |
| **SF=100** (~100 GB) | **15 / 22** | 1.30× | 4.92× | 8.4× | 58× † |

Geomean of competitor ÷ ematix-flow across the 22 queries (Polars n=21
at SF=10 / n=17 at SF=100; **†** Postgres ran 6 / 22 at SF=100 under a
90 s cap). The queries ematix-flow doesn't win are named, not hidden:
DuckDB takes Q05 (by 1.7%) + Q08 at SF=10 and Q10 / Q11 / Q16 / Q18 /
Q20 at SF=100; Polars edges Q15 by 0.1 ms at SF=1 and takes Q14 / Q15
at SF=100. ematix-flow leads the geomean at every scale. Full tables,
tabbed by scale, are on the docs site:
[ematix.dev/reference/benchmarks](https://ematix.dev/reference/benchmarks).
SF=1 in full (the README's headline scale):

| Query | ematix-flow | DuckDB | Polars | PySpark | Postgres |
|---|--:|--:|--:|--:|--:|
| Q01 | **18.6** | 47.8 | 38.6 | 167 | 411 |
| Q02 | **7.5** | 17.7 | 47.9 | 190 | 123 |
| Q03 | **13.2** | 32.9 | 46.5 | 257 | 163 |
| Q04 | **11.7** | 22.4 | 23.8 | 184 | 94.6 |
| Q05 | **16.7** | 31.6 | 8,949 | 335 | 226 |
| Q06 | **1.8** | 13.0 | 10.5 | 41.5 | 218 |
| Q07 | **25.7** | 32.7 | 118 | 260 | 1,262 |
| Q08 | **14.1** | 39.0 | 96.7 | 182 | 99.7 |
| Q09 | **18.6** | 55.2 | 47.5 | 583 | 820 |
| Q10 | **26.4** | 60.2 | 111 | 362 | 355 |
| Q11 | **5.7** | 9.6 | 8.87 | 119 | 36.3 |
| Q12 | **14.9** | 25.1 | 19.2 | 269 | 361 |
| Q13 | **9.7** | 138 | 118 | 684 | 871 |
| Q14 | **11.5** | 22.2 | 12.3 | 114 | 69.8 |
| Q15 | 11.3 | 13.8 | **11.2** | 127 | 140 |
| Q16 | **9.6** | 21.8 | 21.2 | 205 | 113 |
| Q17 | **16.0** | 25.3 | 39.0 | 233 | 398 |
| Q18 | **19.9** | 45.3 | 56.6 | 560 | 1,154 |
| Q19 | **17.8** | 34.4 | 105 | 86.0 | 31.9 |
| Q20 | **13.0** | 29.0 | 22.4 | 106 | 147 |
| Q21 | **34.9** | 74.6 | 721 | 628 | 609 |
| Q22 | **8.6** | 21.0 | 13.6 | 354 | 24.1 |

Median ms, fastest per row in **bold**; ematix-flow + DuckDB measured in
isolated passes through the production preset (fresh `SessionContext`
per query). Polars runs hand-translated `.polars.sql` variants where its
planner rejects the canonical shape (semantically identical); its Q05
SF=1 outlier (~9 s) is a known planner blowup. ematix-flow takes 21 of
22 at SF=1 (Polars edges Q15 by 0.1 ms), with the biggest margins on
the join-heavy queries (Q06 7.2×, Q13 14×, Q18 2.3×) where the
fused-aggregate + push-LeftSemi + runtime-bloom rules do the work.

Full methodology, per-engine reproducers, and the SF=10 / SF=100 tables
are in [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md).

### How the speedups land

Two layers do the work, both transparent at the SQL level:

1. **`EmatixFastParquetTableProvider`** — the default parquet scan,
   backed by [ematix-parquet 0.16.3](https://github.com/ryan-evans-git/ematix-parquet)
   (a hand-rolled decoder we ship as a sibling crate). Adds row-group-parallel
   decode, parallel per-page Snappy decompression, dict-preserved `Utf8View`,
   buffer-reuse on the hot path, and small-bit-width NEON+AVX2 SIMD kernels
   for RLE-dictionary indices. Replaces DataFusion's parquet-rs scan in-place
   — `register_dict_aware_parquet(ctx, name, path)` is the whole API.
2. **Physical-optimiser rules** — pattern-match canonical aggregate
   shapes on the default plan tree and rewrite the matching subtree
   to a specialised operator over the scan. No exec construction at
   the user's level — `SessionContext::sql(...)` is the whole API.

   | Rule | Shape it matches | Replaces with |
   |---|---|---|
   | `InjectFilterMultiAggRule` | `Aggregate(Final/Partial) → Projection(CSE) → Filter → scan` with a small-cardinality group-by | Template-specialised `FilterMultiAggSpec` (perfect-hash agg over typed-slice predicate eval) |
   | `InjectFilterSumRule`     | `Aggregate(Final, sum/sum-product) → CoalescePartitions → Aggregate(Partial) → Filter(N-AND chain) → scan` | `FusedFilterSumExec` |
   | `EnableDictGroupCountRule` | `count(*)` group-by on a dictionary-encoded string column | `DictGroupCountExec` (counts directly over dict keys) |

   Each rule fires on shape — no query-specific gating. When anything
   diverges (different aggregate, wrong column types, missing filter,
   extra wrappers we don't recognise) the rule passes the node through
   unchanged.

Register them like any other physical-optimiser rule:

```rust
use ematix_flow_core::dict_aggregate_rule::EnableDictGroupCountRule;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fused_aggregate_filter_multi_agg_rule::InjectFilterMultiAggRule;
use ematix_flow_core::fused_aggregate_filter_sum_rule::InjectFilterSumRule;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use std::sync::Arc;

let state = SessionStateBuilder::new()
    .with_config(SessionConfig::new().with_target_partitions(14))
    .with_default_features()
    .with_physical_optimizer_rule(Arc::new(EnableDictGroupCountRule))
    .with_physical_optimizer_rule(Arc::new(InjectFilterMultiAggRule))
    .with_physical_optimizer_rule(Arc::new(InjectFilterSumRule))
    .build();
let ctx = SessionContext::new_with_state(state);
for t in ["lineitem", "part" /* … */] {
    let prov = EmatixFastParquetTableProvider::try_new(format!("{t}.parquet"))?;
    ctx.register_table(t, Arc::new(prov))?;
}
// SQL goes through the fast scan and the matching rule fires
// automatically. No per-query code.
```

Reproduce the bench with:

```bash
cargo run --release -p ematix-flow-core \
    --example tpch_triangulation_bench --features triangulation
```

22/22 PASS on TPC-H SF=1; 103/103 PASS on the canonical Apache
Spark TPC-DS plan-time audit (Spark dialect → DataFusion via the
built-in translator).

Distributed batch SQL across multiple ematix-flow processes is
available via the bundled `flow-worker` peer mesh. Cross-host
scaling claims are honestly framed as deferred — there's no
cluster hardware in this project's runway.

### How it compares

| If you're running… | …ematix-flow replaces… |
|---|---|
| One-off pandas / SQL scripts | Adds correctness guarantees (watermarks, atomic state, schema evolution) without the operational weight of Airflow + Spark. |
| Airflow + dbt | Handles the load logic and streaming sources without a scheduler tier. Cron / k8s `CronJob` / GitHub Actions all fire `flow run-due` — no Airflow worker, no scheduler stub, no DAG plumbing. |
| Kafka Connect + Debezium + custom sinks | First-class CDC source mode dispatches per-op transactionally to your existing target. No separate connector tier to operate. |
| PySpark Structured Streaming (single-node) | Same SQL surface (DataFusion + Spark dialect translator). No cluster manager, no JVM startup tax, no driver/executor split — for single-node workloads the operational weight gap is the bigger win than any one query's wall-clock. |
| Polars `read_*` + custom load logic | Comparable per-query performance on shared workloads, plus the load tier on top — watermarks, atomic state, schema evolution, multi-target fan-out, CDC sources, streaming. |

---

<a id="whats-shipped"></a>
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
- 622 default Python tests + ~196 testcontainers-gated Python tests
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
→ **Operator deployment recipes:** [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md).
→ **Runnable examples:** [`examples/`](examples/) (one per strategy + streaming + windowed + session + join + CDC).
→ **Cutting a release:** [`docs/RELEASE.md`](docs/RELEASE.md).

---

<a id="development"></a>
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
make test                                 # both fast suites (no Docker)
make test-integration                     # Docker-gated; auto-cleans testcontainers after

# Or run them directly:
cargo test --workspace --lib              # default (no Docker)
cargo test --workspace -- --ignored       # Docker integration tests
                                          # (Kafka, RabbitMQ, Pub/Sub
                                          # emulator, Kinesis via
                                          # LocalStack, MinIO,
                                          # Schema Registry, etc.)

pytest                                    # default Python suite
pytest -m integration                     # full integration (Docker)
pytest -m spark                           # opt-in Spark E2E

# If a test run is SIGKILL'd or OOM'd before testcontainers' Drop
# fires, leaked containers + volumes can accumulate. The Makefile
# target prunes by label and won't touch unrelated containers:
make clean-testcontainers
```

See `make help` for the full target list (fmt / lint / security).

---

<a id="license"></a>
## License

Apache-2.0
