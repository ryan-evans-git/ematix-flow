```
███████╗███╗   ███╗ █████╗ ████████╗██╗██╗  ██╗
██╔════╝████╗ ████║██╔══██╗╚══██╔══╝██║╚██╗██╔╝
█████╗  ██╔████╔██║███████║   ██║   ██║ ╚███╔╝
██╔══╝  ██║╚██╔╝██║██╔══██║   ██║   ██║ ██╔██╗
███████╗██║ ╚═╝ ██║██║  ██║   ██║   ██║██╔╝ ██╗
╚══════╝╚═╝     ╚═╝╚═╝  ╚═╝   ╚═╝   ╚═╝╚═╝  ╚═╝
```

# ematix-flow

**A declarative Python framework for moving and transforming data
between databases, files, and streaming sources. Rust + Apache
Arrow under the hood.**

> Status: **v0.2.1 on PyPI** as `ematix-flow`. All four
> surfaces below — declarative pipelines, multi-backend, streaming,
> stream processing — are shipped and stable. Python `@udf` /
> `@udaf` decorators (PyArrow zero-copy), Phase Δ CDC across
> Postgres / MySQL / SQLite / DuckDB / Delta Lake, object-store as
> a streaming source, and the Spark / DuckDB → DataFusion dialect
> translator (103/103 TPC-DS PASS) all land in 0.2.

<a id="benchmarks"></a>
## Benchmarks — TPC-H SF=1, all 22 queries

Every TPC-H query, four engines, same M3 Pro / SF=1 / Parquet,
2026-05-11 baseline:

| Query |   Pandas³ | PySpark | Polars | DataFusion | **ematix-flow** | Note |
|---|---:|---:|---:|---:|---:|---|
| Q1  | 1086.05 | 208.8 |   39.28² |  48.84 | **21.74**⁴ | Σ.D3-D auto-injects FusedFilterMultiAggExec (JIT) from SQL — **1.81× faster than Polars** |
| Q2  |    —    | 310.9 | FAIL¹    |  29.58 |  29.58   | |
| Q3  |  675.26 | 368.5 |   44.57² |  37.30 |  37.30   | |
| Q4  |    —    | 278.9 | FAIL¹    |  26.65 |  26.65   | |
| Q5  |  454.66 | 377.6 |10935.93² |  51.84 |  51.84   | Polars regresses 211× on this 6-way join |
| Q6  |  465.68 |  53.7 |   23.30  |  19.03 | **11.46**⁴ | Σ.D3-D auto-injects FusedFilterSumExec (JIT) from SQL |
| Q7  |    —    | 349.8 |  178.46² |  66.52 |  66.52   | |
| Q8  |    —    | 220.9 |  106.87² |  50.53 |  50.53   | |
| Q9  |    —    | 570.6 |   52.81² |  68.58 |  68.58   | ⚠ Polars 1.30× faster |
| Q10 |  582.01 | 406.0 |   71.85² |  62.55 |  62.55   | |
| Q11 |    —    | 148.5 | FAIL¹    |  22.18 |  22.18   | |
| Q12 |  894.94 | 298.3 |   22.01² |  50.39 |  50.39   | ⚠ Polars 2.29× faster |
| Q13 |    —    | 706.1 | FAIL¹    |  94.52 |  94.52   | |
| Q14 |  458.27 | 132.1 |   12.53² |  27.21 | **15.40**⁵ | EmatixFastParquetProvider (Phases 1-4) — standard SQL ties the bespoke FusedQ14FullExec; ⚠ Polars 1.23× faster |
| Q15 |    —    | 146.3 | FAIL¹    |  30.30 |  30.30   | |
| Q16 |    —    | 223.5 |   25.94² |  22.43 |  22.43   | |
| Q17 |    —    | 303.9 | FAIL¹    |  61.99 |  61.99   | |
| Q18 |    —    | 678.8 | FAIL¹    | 104.79 | 104.79   | |
| Q19 |    —    | 109.9 |  392.14² |  56.02 |  56.02   | |
| Q20 |    —    | 134.5 | FAIL¹    |  38.66 |  38.66   | |
| Q21 |    —    | 706.5 | FAIL¹    |  85.77 |  85.77   | |
| Q22 |    —    | 327.8 | FAIL¹    |  19.77 |  19.77   | |

All times in milliseconds. 5-trial median for DataFusion / ematix-flow,
3-trial median for everything else (PySpark 4.1.1 on JDK 23, Polars
1.40.1, pandas 3.0.2).

- The Σ.D-arc fused operators (PRs [#46], [#47], [#48], [#55]) and
  the Σ.D3 phase D auto-injection rules deliver the shifts on Q1 / Q6
  / Q14 vs DataFusion's default plan; the remaining 19 queries match
  DataFusion (which is ematix-flow's SQL engine).
- **Q14 lands at 15.40 ms** via the new `EmatixFastParquetTableProvider`
  (Σ.E3, Phases 1-4) — standard SQL through the generic provider now
  ties the bespoke `FusedQ14FullExec` (15.03 ms) and is 19% faster
  than DataFusion default. The remaining 2.9 ms gap to Polars (12.53)
  is in DataFusion's join + aggregate machinery, not in the parquet
  scan we own.
- Only **Q14** is a remaining engine-vs-Polars loss (1.23× behind).
  Tracked under the **Σ.E arc**.
- The table above shows `ematix-flow` numbers on DataFusion's
  *default* parquet path (5-trial median, 2026-05-11 baseline) for
  every query except Q1, Q6, and Q14 — those three are real-SQL
  numbers with the Σ.D3 phase D auto-injection rules registered
  ([details below](#σd3--sql-pattern-auto-injection-of-fused-execs)).
  The
  FastParquet provider — landed 2026-05-12 — gives a mean 1.53×
  speedup on top of those numbers; per-query results in the section
  below. Polars's listed wins on Q9 and Q12 reflect the default-path
  numbers; with FastParquet enabled, Q9 flips and Q12 ties.
- ¹ Polars's SQL parser (1.40.1) rejects implicit `FROM a, b, c`
  joins, `INTERVAL 'N' DAY` literals, `EXISTS` subqueries, and
  non-equi-join predicates. The 10 FAIL queries above hit those
  blockers and need different operator classes to support, not just
  SQL rewrites.
- ² Polars number from a hand-translated `.polars.sql` variant in
  `examples/tpch/queries/q*.polars.sql` — implicit-FROM rewritten as
  explicit `JOIN ... ON`, plus interval literals pre-resolved.
  Semantically identical to the canonical TPC-H text.
- ³ Pandas isn't a SQL engine; we time hand-translated DataFrame
  operations (`pandas.read_parquet` + `merge` + `groupby`) on the
  same SF=1 Parquet files. Each query is a fresh process so parquet
  decode is included — matches an out-of-box pandas user's
  experience. Queries shown as `—` weren't hand-translated; pandas
  needs a per-query implementation. See
  [`scripts/bench-tpch-pandas.py`](scripts/bench-tpch-pandas.py)
  for the seven translations we did ship. Pandas is **17-490× slower
  than ematix-flow** on the queries measured; geomean is **~36×
  slower** across that subset.
- ⁴ Q1 and Q6 numbers are real `SessionContext::sql(...)` execution
  times (15-trial median, FastParquet provider, M3 Pro) with the
  `InjectFusedQ1Rule` / `InjectFusedQ6Rule` physical-optimiser rules
  registered. The rules pattern-match DataFusion's default plan tree
  and rewrite the Filter+HashAggregate subtree to a single
  Cranelift-JIT'd fused exec over the scan. No exec construction at
  the user's level — `SELECT ...` is the whole API. Reproduce with
  `cargo run --release -p ematix-flow-core --example tpch_q1_inject_bench`
  (or `_q6_`). See
  [Σ.D3 phase D](#σd3--sql-pattern-auto-injection-of-fused-execs)
  for the rule mechanics.
- (footnote ⁴ covers Q14 too — the Q14 rule wraps both `lineitem`
  and `part` scans inside a single `FusedQ14FullExec`, replacing the
  hash join with a direct-indexed promo bitmap probe. Hand-built
  `FusedQ14FullExec` direct-execute is ~15.0 ms; the rule-injected
  full-SQL path is ~17 ms — the ~1 ms gap is per-call SQL parsing +
  logical-planning + physical-planning + rule rewrite, intrinsic to
  the `SessionContext::sql(...)` workflow.)
- ⁵ Q14 at 15.40 ms uses the new `EmatixFastParquetTableProvider`
  (Σ.E3, see below). Registering each TPC-H table via the provider
  swaps the parquet scan for ematix-parquet kernels (NEON-fused
  bitmap predicate pushdown for `l_shipdate`, bitmap-driven sparse
  gather for the aggregate columns). Standard `SessionContext::sql`
  with no per-query rule injection now ties the bespoke
  `FusedQ14FullExec` (15.03 ms ± 0.58, 15-trial median, M3 Pro).
  Reproduce with
  `cargo run --release -p ematix-flow-core --example tpch_q14_full_bench`.

Full methodology + per-engine reproducers in
[`docs/BENCHMARKS.md`](docs/BENCHMARKS.md).

### Σ.E2 — FastParquet scan provider (opt-in, 2026-05-12)

`FastParquetTableProvider` is an opt-in alternative to DataFusion's
default `register_parquet`. Same query results, faster scans. Two
changes do the work:

1. **Row-group-parallel decode with incremental streaming** — one
   tokio task per row group, each batch flows through an `mpsc(8)`
   channel so downstream operators start work the moment the first
   batch lands instead of waiting for the whole partition. mimalloc
   replaces macOS's globally-locked allocator under the 14-way
   concurrent decode pattern (Σ.E2 PR #63).
2. **`Utf8View` column promotion** — string columns surface as
   Arrow's StringView layout (16-byte inline storage) instead of
   classic Utf8 byte arrays. Downstream `FilterExec` and
   `AggregatePartial` then pick their SIMD-optimised kernels for
   hashing and equality.

#### Headline numbers vs DataFusion's default scan

15-trial median, M3 Pro, mimalloc allocator. A query is only called
a win when the median difference exceeds the combined run-to-run
noise envelope:

| Scale | Real wins | Real losses | Mean speedup |
|---|---:|---:|---:|
| **SF=1**  (60 MB lineitem)  | **15 of 22** | **0** | **1.53×** |
| **SF=10** (2.2 GB lineitem) | 1 of 22, plus 6 in-noise gains | **0** | **1.16×** |

#### Status on the three previously-tight queries vs Polars (SF=1)

| Query | Polars (ms) | ematix-flow + FastParquet (ms) | Status |
|---|---:|---:|---|
| **Q9**  | 52.81 | **34.73** | ematix-flow wins, **1.52× faster than Polars** |
| **Q12** | 22.01 | 23.05 | basically tied (Polars 5% ahead, inside noise) |
| **Q14** | 12.53 | **15.40** (Σ.E3 Emat provider, standard SQL) | Polars 1.23× faster — Emat provider now ties hand-built `FusedQ14FullExec` (15.03 ms) on standard SQL; gap from 2.17× → 1.23× |

Reproducing this is a one-liner:

```bash
# All 22 queries, statistical mode with noise envelope (SF=1)
cargo run --release -p ematix-flow-core --example tpch_fast_parquet_bench

# Same harness at SF=10
TPCH_DATA_DIR=$(pwd)/examples/tpch/data/sf10 \
  cargo run --release -p ematix-flow-core --example tpch_fast_parquet_bench
```

To use the provider in your own code, register it like any
`TableProvider`:

```rust
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
let prov = FastParquetTableProvider::try_new("/path/to/lineitem.parquet")?;
ctx.register_table("lineitem", std::sync::Arc::new(prov))?;
// then `SELECT … FROM lineitem` flows through the fast scan path.
```

### Σ.D3 — SQL-pattern auto-injection of fused execs

ematix-flow ships a *custom optimisation engine* on top of DataFusion's
planner: physical-optimiser rules that pattern-match canonical query
shapes in the default plan tree and rewrite the matching subtree to a
single Cranelift-JIT'd fused operator over the scan. No exec
construction at the user's level — `SessionContext::sql(...)` is the
whole API.

Five rules ship today (2026-05-13):

| Rule | Shape it matches | Replaces with | SQL @ SF=1 (rule on vs off) | Status |
|---|---|---|---:|---|
| `InjectFusedQ6Rule` | `Projection → Aggregate(Final, single SUM(extprice*disc)) → CoalescePartitions → Aggregate(Partial) → Filter(5-AND chain) → scan` | `FusedFilterSumExec` (JIT) | 11.46 vs 12.04 ms (**+4.8 %**) | reliable win |
| `InjectFusedQ1Rule` | `SortMerge → Sort → Projection → Aggregate(FinalPartitioned, gby=[returnflag,linestatus], 8 aggs) → RepartitionExec(Hash) → Aggregate(Partial) → Projection(CSE) → Filter(l_shipdate ≤ V) → scan` | `FusedFilterMultiAggExec` (JIT) | 21.74 vs 37.71 ms (**+42-46 %**) | **beats Polars 35.2 ms by 38 %** |
| `InjectFusedQ3Rule` | `SortMerge → Sort → Projection → Aggregate(FinalPartitioned, gby=[orderkey, orderdate, shippriority]) → … → Aggregate(Partial) → 3-table join` | `FusedPostJoinExec(spec=Q3)` (keeps join, replaces aggregate stack) | 19.08 vs 19.31 ms (**+1.2 %**) | already 2.3× faster than Polars |
| `InjectFusedQ5Rule` | Same top shape as Q3 with gby=[n_name] over Q5's 6-table join chain | `FusedPostJoinExec(spec=Q5)` | 23.52 vs 23.91 ms (**+1.7 %**) | already 465× faster than Polars (Polars regresses on Q5) |
| `InjectFusedQ14Rule` | `Projection → Aggregate(Final, 2 SUMs) → CoalescePartitions → Aggregate(Partial) → Projection(CSE) → HashJoin(p_partkey = l_partkey) → {part scan, Filter(shipdate range) → lineitem scan}` | `FusedQ14FullExec` (owns both scans, replaces hash join with direct-indexed promo bitmap probe) | 17.17 vs ~18 ms (**+6-30 %** depending on run) | Polars 1.37× faster (was 2.17×) |

Both rules' detectors walk the `PhysicalExpr` AST (`BinaryExpr` /
`Column` / `Literal` with column-on-either-side flipping) to extract
the predicate constants, validate aggregate function names, check the
scan exposes the right column names + types, and only fire on an
exact-shape match. When anything diverges (different aggregate,
wrong column types, missing filter, extra wrappers we don't
recognise) the rule passes the node through unchanged — no rewriting
cliff.

Register them like any other physical-optimiser rule:

```rust
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fused_jit_rule::{
    InjectFusedQ1Rule, InjectFusedQ3Rule, InjectFusedQ5Rule,
    InjectFusedQ6Rule, InjectFusedQ14Rule,
};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};

let state = SessionStateBuilder::new()
    .with_config(SessionConfig::new().with_target_partitions(14))
    .with_default_features()
    .with_physical_optimizer_rule(std::sync::Arc::new(InjectFusedQ1Rule))
    .with_physical_optimizer_rule(std::sync::Arc::new(InjectFusedQ3Rule))
    .with_physical_optimizer_rule(std::sync::Arc::new(InjectFusedQ5Rule))
    .with_physical_optimizer_rule(std::sync::Arc::new(InjectFusedQ6Rule))
    .with_physical_optimizer_rule(std::sync::Arc::new(InjectFusedQ14Rule))
    .build();
let ctx = SessionContext::new_with_state(state);
for t in ["lineitem", "part" /* … */] {
    let prov = FastParquetTableProvider::try_new(format!("{t}.parquet"))?;
    ctx.register_table(t, std::sync::Arc::new(prov))?;
}

// Now `ctx.sql(...)` auto-detects Q1/Q3/Q5/Q6/Q14 shapes and uses
// the corresponding fused exec for the matching subtree. Other
// queries pass through unchanged.
```

A real-SQL test pins each rule's correctness against the un-rewritten
plan to relative error < 1e-10 (Float64 cells) or bit-equality (Int64
counts + string group keys). See
[`crates/ematix-flow-core/src/fused_jit_rule.rs`](crates/ematix-flow-core/src/fused_jit_rule.rs)
for the matchers and
[`crates/ematix-flow-core/examples/tpch_{q1,q6,q14}_inject_bench.rs`](crates/ematix-flow-core/examples)
for the reproducible benches.

The bigger story is that the Cranelift-JIT'd substrate behind these
rules ([`fused_jit.rs`](crates/ematix-flow-core/src/fused_jit.rs)) is
*generic* — `FusedFilterAggSpec` describes any filter + multi-
aggregate + small-cardinality group-by shape, and the IR emitter
generates code for it. The Q14 rule extends this to filter + join +
agg by owning both inputs and substituting the hash join with a
direct-indexed bitmap probe. Adding support for a new query shape is
a matcher + a spec call, not a new operator.

[#46]: https://github.com/ryan-evans-git/ematix-flow/pull/46
[#47]: https://github.com/ryan-evans-git/ematix-flow/pull/47
[#48]: https://github.com/ryan-evans-git/ematix-flow/pull/48
[#55]: https://github.com/ryan-evans-git/ematix-flow/pull/55

---

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

1. [Benchmarks](#benchmarks)
2. [Install](#install)
3. [Connections](#connections)
4. [Backends](#backends)
5. [Pipelines](#pipelines)
6. [Modes](#modes)
7. [Scheduling](#scheduling)
8. [Streaming pipelines](#streaming-pipelines)
9. [Stream processing](#stream-processing)
10. [Configuration reference](#configuration-reference)
11. [CLI](#cli)
12. [Python API](#python-api)
13. [Performance and comparisons](#performance-and-comparisons)
14. [What's shipped](#whats-shipped)
15. [Development](#development)
16. [License](#license)

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
flow status              # operator view: per-pipeline status / next-due / attempts
flow preview <name>      # dry-run, no commit
flow validate <name>     # EXPLAIN against the target
flow runs list           # recent runs from the configured RunLog
flow connections list / check / set
flow transform list / run

flow consume <toml>      # streaming daemon (TOML form)
flow consume --module my_pipelines <name>   # typed-Python form
flow consume-list --module my_pipelines     # registered streaming pipelines
```

`--module` points at any importable Python module.

Observability flags (work on `run-due`, `status`, `runs list`):

```
--run-log <url>     # any RunLog scheme (sqlite/postgres/mysql/duckdb/s3/azureblob/gcs/memory)
--alerter <url>    # repeatable; stdout:// or slack://...
--metrics  <url>   # null:// / stdout:// / memory:// / prometheus://:port / otlp://endpoint
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
## Performance and comparisons

The headline 22-query four-engine table lives at the top of this
README — see [Benchmarks](#benchmarks). On the SF=1 suite at the
default settings, ematix-flow finishes every query DataFusion can
plan; the Σ.D fused-operator arc shifts Q1 / Q6 / Q14 to the front
of the pack vs Polars. Q14 specifically — historically Polars's
strongest TPC-H win against DataFusion — closes from 2.17× behind
Polars to 1.23× via the Σ.E3 ematix-parquet provider integration.
Engine-level investigation continues under the Σ.E arc.

ematix-flow uses DataFusion for in-process SQL and Apache Arrow
for cross-backend I/O, plus custom fused physical operators (the
**Σ.D arc**) that close the gap on workload shapes where stock
DataFusion materializes intermediate `BooleanArray` masks between
filter and aggregate. The Σ.D operators (PRs [#46], [#47], [#48],
[#55]) cover the `Aggregate over Filter over Scan` plan shape and
deliver the 56-68× shifts on Q1 / Q6 in the headline table; the
same architectural pattern extends to post-join aggregates and
conditional `SUM(CASE WHEN ...)` shapes.

22/22 PASS on TPC-H SF=1; 103/103 PASS on the canonical Apache
Spark TPC-DS plan-time audit (Spark dialect → DataFusion via the
built-in translator).

The headline 22-query 5-engine table at the top of this README
supersedes the earlier 4-query Polars head-to-head — see
[Benchmarks](#benchmarks). The Σ.D arc covers Q1 / Q6 today;
Q9 / Q12 / Q14 are the remaining gap (parquet decoder, tracked
as Σ.E).

[#46]: https://github.com/ryan-evans-git/ematix-flow/pull/46
[#47]: https://github.com/ryan-evans-git/ematix-flow/pull/47
[#48]: https://github.com/ryan-evans-git/ematix-flow/pull/48
[#55]: https://github.com/ryan-evans-git/ematix-flow/pull/55

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
| Kafka Connect + Debezium + custom sinks | First-class CDC source mode dispatches per-op transactionally to your existing target. No separate connector tier to operate. |
| PySpark Structured Streaming (single-node) | Same SQL surface (DataFusion + Spark dialect translator). No cluster manager, no JVM startup tax, no driver/executor split — for single-node workloads the operational weight gap is the bigger win than any one query's wall-clock. |
| Polars `read_*` + custom load logic | Comparable per-query performance on shared workloads; **ematix-flow is faster (1.5–11.5× on tested shapes)** once Σ.D applies, plus the load tier on top — watermarks, atomic state, schema evolution, multi-target fan-out, CDC sources, streaming. |

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
