# Phase 39 — SQL transforms layer (DataFusion)

**Status:** design — not yet implemented.

**Goal:** add an opt-in SQL transform between `source.read_arrow_stream`
and `target.write_arrow_stream` so streaming pipelines can filter,
project, enrich, and (eventually) aggregate mid-stream — without
dragging down the no-transform fast path.

## Why DataFusion

DataFusion is already in the dep tree (transitively, via
`deltalake-core`'s MERGE planner). Its sweet spot is Arrow-in /
Arrow-out SQL execution, which is exactly the shape we need:

- No ser/de overhead inside the transform — `RecordBatch` flows
  in, `RecordBatch` flows out.
- Vectorized per-column kernels; per-row cost amortizes.
- Rich SQL surface: filter / project / cast / join /
  aggregations / window functions.
- Streaming-friendly: plans can be re-executed against successive
  batches; no need to re-parse SQL.

## Non-goals (for Phase 39)

- **Replacing the per-DB hand-written SQL** in
  `strategy/{append,merge,scd2,truncate}`. Those compose into
  PG/MySQL/SQLite/DuckDB-native plans (with `RETURNING`, `ON
  CONFLICT`, `MERGE`, LATERAL joins) that DataFusion can't
  express. Same-DB load paths stay unchanged.
- **Routing reads through DataFusion** for cross-backend
  Arrow IO. The existing `read_arrow_stream` pulls native
  bytes; a DataFusion sandwich would add a copy.
- **Becoming a full SQL compatibility layer**. The transform
  speaks DataFusion's SQL, not Postgres-superset SQL. We'll
  document the dialect's limits.

## Architecture

```
                read_arrow_stream
source backend  ───────────────▶  RecordBatch
                                       │
                                       ▼
                          ┌──────────────────────────┐
                          │  BatchTransform (opt.)   │  <── new in Phase 39
                          │  DataFusion plan, lookups│
                          └──────────────────────────┘
                                       │
                                       ▼
                                  RecordBatch
                                       │
                                       ▼ write_arrow_stream
                                target backend
```

### `BatchTransform` trait

```rust
pub trait BatchTransform: Send + Sync {
    /// Returns the input schema this transform expects. Used by
    /// the pipeline to validate the source's output up front.
    fn input_schema(&self) -> SchemaRef;

    /// Returns the output schema. Used by the pipeline to verify
    /// the target's column expectations match.
    fn output_schema(&self) -> SchemaRef;

    /// Apply the transform to a single input batch. May produce
    /// 0..N output batches (a filter that drops everything → 0;
    /// a window aggregation that buffers across calls → 0 until
    /// the window fires, then 1+).
    fn transform(&self, input: RecordBatch) -> Result<Vec<RecordBatch>, BackendError>;

    /// Hook for time-driven emission (windows that fire on a
    /// timer, not a row). Default: no-op.
    fn on_idle_tick(&self) -> Result<Vec<RecordBatch>, BackendError> {
        Ok(Vec::new())
    }
}
```

### `DataFusionTransform`

The reference implementation. Compiles SQL once at construction
time:

1. Build a fresh `SessionContext`.
2. Register a virtual `MemTable` named `source` with the input
   schema, populated lazily one batch at a time.
3. For each lookup table (Phase 39.2+), register another
   `MemTable` populated at construction time.
4. `ctx.sql(transform_sql)` → `LogicalPlan` → `PhysicalPlan`.
5. Hold the plan; `transform()` swaps the source's
   single-batch contents and re-executes the plan.

Plan-cache hit per call; no re-parse.

### Pipeline integration

`StreamingPipelineConfig` grows one optional field:

```rust
pub struct StreamingPipelineConfig {
    // ... existing fields ...
    pub transform: Option<Arc<dyn BatchTransform>>,
}
```

In `StreamingPipeline::run`, the read/write path becomes:

```rust
let stream = source.read_arrow_stream(...).await?;
let batches: Vec<RecordBatch> = stream.try_collect().await?;

let batches = match &self.config.transform {
    Some(t) => {
        let mut out = Vec::new();
        for b in batches {
            out.extend(t.transform(b)?);
        }
        out
    }
    None => batches,  // Zero-transform fast path — no overhead.
};

let stream = futures_util::stream::iter(batches.into_iter().map(Ok));
target.write_arrow_stream(&target_table, Box::pin(stream), Append).await?;
```

The `None` arm is bit-identical to today's path. **Existing
deployments see zero overhead.**

### TOML extension

```toml
pipeline_name = "events-clean"
source_query = "events"

[source]
kind = "kafka"
bootstrap_servers = "localhost:9092"
group_id = "ematix-flow"

[transform]
sql = """
    SELECT
        user_id,
        event_type,
        ts,
        json_extract_path_text(payload, 'page') AS page
    FROM source
    WHERE event_type IN ('click', 'view')
"""

# Phase 39.2+: static lookup tables
[transform.lookups.users]
kind = "postgres"
url = "postgres://localhost/mydb"
schema = "public"
table = "users"
refresh_interval_ms = 300000  # 5 min; Phase 39.3+

[target]
kind = "postgres"
url = "postgres://localhost/warehouse"

[target.table]
schema = "public"
name = "events_clean"
```

## Performance design

The user-stated requirement: **"can run without dragging down
performance too much."** This is taken seriously. Three principles:

### 1. The zero-transform path is bit-identical

Today's pipelines have `transform = None`. The match arm in
`StreamingPipeline::run` skips DataFusion entirely. No allocations,
no plan, no SessionContext. Latency unchanged.

### 2. Plan compilation is amortized over the pipeline's lifetime

A streaming pipeline runs for hours or days. Plan compilation
costs ~10-100ms (single-table SQL). Per-batch cost is the
plan execution, which on a 1000-row batch is sub-ms for
filter/project SQL.

### 3. Trivial transforms bypass DataFusion entirely

If the SQL is structurally `SELECT col1, col2 FROM source` (no
expressions, no `WHERE`, no joins, no aggregates), the
transform is a `RecordBatch::project(...)` plus optional
column rename. No DataFusion involved. We detect this at
construction time by inspecting the parsed `LogicalPlan`.

### 4. Lookups are MemTables, loaded once

A 10K-row reference table is ~MB; loaded into a DataFusion
`MemTable` at construction; `JOIN`s reference it without
re-reading. Refreshing lookups (Phase 39.3) reload on a timer
in the background, double-buffered so the in-flight plan
never sees a torn read.

### 5. Bounded state for window aggregations

Tumbling/hopping window state is bounded by the window
duration × event rate. We document a per-window memory cap
and emit a warning when approached. Session windows have
unbounded worst-case state; they're Phase 39.5 with explicit
TTL config.

### Benchmark targets (acceptance criteria for Phase 39.1)

| Workload | Target |
|--|--|
| 1000-row batch, identity transform (`SELECT *`) | <5% overhead vs zero-transform |
| 1000-row batch, single-column filter | <10% overhead vs hand-written filter on `RecordBatch` |
| End-to-end Kafka→PG, simple project+filter | ≥80% throughput of zero-transform baseline |
| Plan compilation (single-source SQL) | <100ms |
| MemTable load (10K rows, 10 columns) | <100ms |

If these targets aren't met, Phase 39.1 ships behind a feature
flag and the trivial-transform bypass widens until they are.

## Phasing

| Phase | Scope | Risk |
|--|--|--|
| **39.1** | `BatchTransform` trait + `DataFusionTransform` for filter / project / cast. No joins, no aggregates. TOML wiring. Bench harness. | Low. The trivial-transform bypass plus zero-transform-default keeps the blast radius small. |
| **39.2** | Static lookup tables loaded from any DB backend. Joins enabled. | Medium. Lookup-load failure modes need careful error handling (fail fast at construction, not mid-pipeline). |
| **39.3** | Refreshing lookups (configurable interval, double-buffered). | Medium. Refresh coordination + plan rebuild on schema change. |
| **39.4** | Tumbling-window aggregations. Bounded state with documented memory cap. Idle-tick emission. | High. Window semantics + state management + crash recovery (committed offsets after windowed emit). |
| **39.5** | Session windows. Stateful joins. | High. Memory-unbounded worst case; needs explicit TTL + spill-to-disk. May be deferred indefinitely if 39.4 covers most use cases. |

## Open design questions

- **Schema inference vs declared:** does the user declare the
  output schema in TOML, or do we infer from the DataFusion
  plan? Inference is friendlier; declaration is easier to
  validate at config-load time. Probably: infer + offer a
  declared override.

- **Per-row error handling:** if a row fails type-casting in
  the transform (e.g. `CAST('abc' AS INTEGER)`), do we drop,
  DLQ, or fail the batch? Default to fail-the-batch; offer
  `on_error = "drop" | "dlq" | "fail"` in TOML.

- **Cross-batch state for windows + at-least-once:** if a
  window fires after a target write but before commit, and
  the process crashes, we need to either re-emit the window
  on restart (idempotent target required) or persist window
  state. Phase 39.4 will pick: probably persist state to the
  same checkpoint store as offsets.

- **Transform overhead in `pending_*` counters:** if the
  transform drops 90% of rows, the source's pending offsets
  cover 100% of input. The streaming pipeline's metrics
  should report both `rows_in` and `rows_out` so the drop
  rate is observable.

## What this doesn't change

- **Same-DB load paths** still use the dialect-specific SQL
  in `strategy/{append,merge,scd2,truncate}.rs`. The
  transform layer is for cross-backend or stream→DB
  pipelines where there's a real Arrow-shape boundary anyway.
- **Streaming backends without ack/checkpoint coordination**
  (e.g. RabbitMQ, Pub/Sub, Kinesis with in-memory checkpoints
  today) keep their existing semantics. The transform is
  inserted between read and write; commit/ack semantics are
  unchanged.
- **DataFusion's arrow ABI version.** It's already pinned via
  the `deltalake` dep tree. Phase 39 doesn't move the pin;
  it just lets us use what's already there.

## When this lands

After:
- The CLI track stabilizes (CLI.1–.3 + the S3/ObjectStore
  follow-up are already in)
- A benchmark harness exists (Phase 39.1 ships with one or
  not at all)
- A real user pipeline asks for a transform — driven by an
  actual need rather than feature creep

The current pipeline shape (Kafka → DB unchanged) is honest
about what it does. Adding transforms is genuinely useful but
also genuinely scope-creepy. Wait for a concrete requirement.
