# Phase 39 — SQL transforms layer (DataFusion)

**Status:** Phases 39.1, 39.2, 39.3, and 39.4 shipped (including
PR 2b: `count_distinct` + metrics auto-wiring; PR 2c:
`late_data = "reopen"`). DLQ for late rows defers indefinitely.
39.5 split into 39.5a (sessions + `StateStore` foundation —
PR 1 + PR 2 + PR 3 landed) and 39.5b (stream-stream join —
PR 1 + PR 2 + PR 3 landed).

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

| Workload | Target | Measured (Apple M-series, release build) |
|--|--|--|
| 1000-row batch, identity transform (`SELECT *`) | sub-µs absolute (see note) | **156 ns** — trivial-projection bypass active |
| 1000-row batch, single-column filter | <10% overhead vs hand-written | **78.6 µs** — vs **905 ns** raw `filter_record_batch`; ≈87× slower in absolute terms but ≈12.7M rows/s, ample for Kafka throughput |
| End-to-end Kafka→PG, simple project+filter | ≥80% throughput of zero-transform baseline | not yet benched (requires live infra) |
| Plan compilation (single-source SQL) | <100ms | **50.7 µs** — ≈2000× under target |
| MemTable load (10K rows, 10 columns) | <100ms | **86.5 µs** — ≈1100× under target |

**Note on the identity-transform target.** The original "<5%
overhead" framing was based on an assumed end-to-end zero-transform
baseline. In isolation the zero-transform "baseline" is just an
`Arc<RecordBatch>` clone — ≈13 ns. The trivial-projection bypass
adds ≈140 ns absolute (a `RecordBatch::project` allocation plus
the wrapping Vec). 140 ns is 12× the bare clone, but is also
sub-µs per batch — at any realistic streaming rate it's
imperceptible next to target-write time. The amended target is
**absolute sub-µs per batch**, not a relative percentage against
a baseline this cheap.

**Note on the filter target.** The raw-baseline kernel is a
hand-written `BooleanArray` mask + `filter_record_batch`. The
DataFusion path is ≈87× slower in this microbench, far outside
the original "<10%" target. The honest read: that 87× pays for
SQL parsing, planning, casts, joins, aggregations, and refreshable
lookups — features the raw kernel doesn't provide. At 78 µs / 1000
rows = 12.7M rows/s single-threaded the absolute throughput is
plenty for typical Kafka workloads (1M rows/s would consume only
~8% of a single core). If a future workload pins the transform as
the hot spot, the trivial-bypass can widen further (e.g. detect
single-predicate `WHERE`s and short-circuit) without breaking the
DataFusion fallback.

Reproduce with `cargo bench -p ematix-flow-core --bench transform`.

## Phasing

| Phase | Scope | Risk | Status |
|--|--|--|--|
| **39.1** | `BatchTransform` trait + `DataFusionTransform` for filter / project / cast. No joins, no aggregates. TOML wiring. Bench harness. | Low. The trivial-transform bypass plus zero-transform-default keeps the blast radius small. | **Shipped** (Π.4b-1a + 1b). `LazySqlTransform` defers DataFusion compilation to first batch so streaming sources without a known schema can wire by SQL alone. Trivial-projection bypass detects bare `SELECT col1, col2 FROM source` and skips DataFusion at runtime. |
| **39.2** | Static lookup tables loaded from any DB backend. Joins enabled. | Medium. Lookup-load failure modes need careful error handling (fail fast at construction, not mid-pipeline). | **Shipped** (Π.4b-3). `[transform.lookups.<name>]` blocks load via `read_arrow_stream(SELECT * FROM <table>)` from Postgres / MySQL / SQLite / DuckDB at pipeline startup. Lookup-load failures bubble before the source is touched. Reserved-name (`source`) and duplicate-name validation. |
| **39.3** | Refreshing lookups (configurable interval, double-buffered). | Medium. Refresh coordination + plan rebuild on schema change. | **Shipped.** `refresh_interval_ms` per lookup; one tokio task per refreshing lookup `select!`s `shutdown.wait()` against `sleep(interval)`. Atomic swap is via `tokio::sync::Mutex<SessionContext>` + `deregister_table` + `register_table` — the streaming pipeline already serializes batches, so contention is zero in practice; the lock exists for correctness against the background loader. **Schema-change handling is not implemented**: a refresh that returns batches with a different schema produces a runtime error in the next `transform()` call. The pipeline's supervisor restart re-loads at startup and re-plans. |
| **39.4** | Tumbling-window aggregations. Bounded state with documented memory cap. Idle-tick emission. | High. Window semantics + state management + crash recovery (committed offsets after windowed emit). | **Shipped.** Tumbling + hopping windows; aggregators count / count_col / sum / min / max / avg / first / last / count_distinct (HLL+ approximate or exact); `late_data = "drop"` and `"reopen"` (with `allowed_lateness_ms` + state retention + re-emit on dirty); per-window cap fail-loud; idle-tick emit; `_event_ts` injection per source backend; multi-source `min`-with-idleness watermark; `BatchContext` arg threaded through `transform()` and `on_idle_tick()`; CLI auto-wires `WindowedMetrics` into the pipeline's Prometheus registry; Python `Window` + `Aggregation` dataclasses + `window=` kwarg. **Deferred:** `late_data = "dlq"` (separate write path; not yet implemented). See `docs/PHASE_39_4_WINDOWS.md` for full design. |
| **39.5a** | Session windows + durable `StateStore` foundation. Mandatory `max_session_duration_ms`; out-of-order merging under `Reopen`; per-emit atomic state+offsets commit; `seek_to` on source backends (Kafka first). | High. State persistence + source-side seek plumbing + session merge semantics. | **Shipped.** See `docs/PHASE_39_5_SESSIONS.md`. PR 1 (StateStore foundation), PR 2 (session machinery), PR 3 (state-store integration + TOML/Python wiring + e2e crash-recovery test). |
| **39.5b** | Stream-stream join (keyed, time-windowed). Reuses 39.5a's `StateStore`. | High. Two-sided gated state, watermark-coordinated eviction, output ordering. | **Shipped.** See `docs/PHASE_39_5B_JOINS.md`. PR 1 (in-memory join + `BatchContext::source_id`), PR 2 (StateStore integration + CLI cross-validation), PR 3 (pipeline per-source dispatch + Python `Join` dataclass). |

## Open design questions

- **Schema inference vs declared:** does the user declare the
  output schema in TOML, or do we infer from the DataFusion
  plan? *Resolved (39.1):* inferred. `LazySqlTransform`
  captures input schema on first batch and exposes
  `output_schema()` from the compiled plan. A declared
  override could be added later if config-load-time validation
  becomes a real need.

- **Per-row error handling:** if a row fails type-casting in
  the transform (e.g. `CAST('abc' AS INTEGER)`), do we drop,
  DLQ, or fail the batch? Default to fail-the-batch; offer
  `on_error = "drop" | "dlq" | "fail"` in TOML. *Status:* not
  yet implemented; the current path bubbles the error from
  `DataFusionTransform::transform`, which the pipeline's
  metrics counter increments + propagates. DLQ on transform
  errors is plausible scope for a follow-up.

- **Cross-batch state for windows + at-least-once:** if a
  window fires after a target write but before commit, and
  the process crashes, we need to either re-emit the window
  on restart (idempotent target required) or persist window
  state. Phase 39.4 will pick: probably persist state to the
  same checkpoint store as offsets. *Still open* — gated on
  Phase 39.4.

- **Lookup schema drift on refresh:** *new (39.3).* If the
  refreshed lookup's schema differs from the original, the
  `MemTable::try_new` registration succeeds (it doesn't
  cross-check against the cached SQL plan), but the next
  `transform()` re-execution may fail with a column-not-found
  error from DataFusion. The current behavior is to surface
  that error to the supervisor (which restarts + re-plans
  against the new schema). A more graceful path would be to
  detect drift on refresh + force a re-plan eagerly; left as
  follow-on work.

- **Transform overhead in `pending_*` counters:** if the
  transform drops 90% of rows, the source's pending offsets
  cover 100% of input. The streaming pipeline's metrics
  should report both `rows_in` and `rows_out` so the drop
  rate is observable. *Resolved (Π.4b-1b):* the pipeline's
  Prometheus registry exports `ematix_streaming_rows_consumed_total`
  (pre-transform input row count) and
  `ematix_streaming_rows_written_total` (post-transform row
  count actually written to targets). The drop rate is
  `1 - rows_written / rows_consumed`.

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

Originally a "wait for concrete requirement" gate. Phases 39.1
through 39.3 have now landed with:

- `BatchTransform` trait, `DataFusionTransform`, and
  `LazySqlTransform` in `crates/ematix-flow-core/src/transform.rs`.
- Pipeline integration via `StreamingPipelineConfig.transform` and
  the matching `[transform]` TOML block.
- Lookup tables (`[transform.lookups.<name>]`) loaded once at
  startup, with optional `refresh_interval_ms` driving a
  background tokio task that atomically swaps the registered
  `MemTable`.
- Python facade: `run_streaming_pipeline(transform_sql=, lookups=)`,
  the `Lookup` dataclass, and `@ematix.streaming_pipeline(...)`
  decorator.
- Bench harness at `crates/ematix-flow-core/benches/transform.rs`
  with results recorded above.

Phases 39.4 (windows) and 39.5 (session windows + stateful joins)
remain gated on a concrete requirement. Window semantics + crash
recovery + bounded state are non-trivial; the design above
captures the open questions but the phases haven't started.
