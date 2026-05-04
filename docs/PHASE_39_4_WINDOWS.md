# Phase 39.4 — Tumbling + hopping window aggregations

**Status:** design 2026-05; PR 1+2+3 (initial windowing + Python +
bench), PR 2b (`count_distinct` + CLI metrics auto-wiring), and
PR 2c (`late_data = "reopen"`) all shipped 2026-05.

Remaining follow-on:
- **`late_data = "dlq"`** for late rows. Defers indefinitely — needs
  a separate write path (the existing target-write-failure DLQ
  can't be reused since late rows haven't failed any write) and
  the windowed transform doesn't have access to the source
  backend's writer. Reserved schema; CLI rejects with a clear
  error until implemented.

**Goal.** Add stateful, time-windowed aggregations on top of the
SQL transform layer (Phase 39.1–39.3) so streaming pipelines can
emit per-window summaries (counts, sums, distinct counts, etc.)
without a separate batch job.

This phase ships:
- Tumbling and hopping windows; sliding deferred indefinitely.
- Event-time semantics with processing-time as a thin shorthand.
- Bounded-delay watermarks; `min`-aggregated across multi-source
  pipelines; per-source idleness fallback.
- At-least-once correctness with an idempotent target requirement.
- Fixed-width aggregators (count, sum, min, max, avg, first, last)
  plus HLL+-approximate `count_distinct` (with `exact` opt-in).
- All in-memory state. Durable state, Kafka EOS coordination, and
  stream-stream join all defer.

## Non-goals

- **Stream-stream join.** Joining two `[[sources]]` on a key within
  a time interval. Substantially harder; lands in 39.5.
- **Session windows.** Variable-duration windows triggered by gaps
  in event time. Lands in 39.5.
- **Sliding windows** (per-row windows). State is unbounded without
  per-row eviction; deferred indefinitely.
- **Persisted state.** Durable checkpoints to RocksDB / sled / etc.
  Reserved as 39.4c, gated on a real user need.
- **Replacing existing aggregation paths.** The per-DB hand-written
  SQL in `strategy/{merge,scd2}.rs` stays exactly as it is. The
  windowed transform is for streaming sources where the aggregation
  semantically *is* time-windowed.
- **DataFusion-native `TUMBLE`/`HOP` SQL syntax.** DataFusion 53.x
  doesn't ship time-window table functions; contributing them
  upstream is multi-month. We use a config block + a separate
  `BatchTransform` impl instead.

## Architecture

```text
                read_arrow_stream
source backend  ──────────────▶  RecordBatch (with `_event_ts` column)
   (per source)                     │
                                    ▼
                          ┌──────────────────────────┐
                          │  pipeline watermark loop │
                          │  per-source max(_ts)     │
                          │  global_wm = min(...)    │
                          └──────────────────────────┘
                                    │ BatchContext { global_wm }
                                    ▼
                          ┌──────────────────────────┐
                          │  WindowedAggregateXform  │  (this phase)
                          │  ┌────────────────────┐  │
                          │  │  inner Lazy SQL    │  │  (39.1–39.3)
                          │  │  filter / project  │  │
                          │  │  cast / lookup ⨝   │  │
                          │  └────────────────────┘  │
                          │  state: HashMap<         │
                          │    (window, group), acc> │
                          │  emit: window_end ≤ wm   │
                          └──────────────────────────┘
                                    │
                                    ▼
                              RecordBatch
                                    │ write_arrow_stream
                                    ▼
                              target backend
                              (idempotent: MERGE / UPSERT)
```

The windowed transform plugs in **between** `LazySqlTransform`
(Phase 39.1–39.3) and the target write. It is a separate
`BatchTransform` impl, not a DataFusion plan — windowed state is
fundamentally incompatible with DataFusion's per-batch re-execute
model.

**Composition.** `WindowedAggregateTransform` inner-wraps an
optional `LazySqlTransform`. The pipeline's
`config.transform: Option<Arc<dyn BatchTransform>>` field holds
the outer windowed transform; the windowed transform's
constructor takes the SQL pre-stage as an inner. The pipeline's
hot loop doesn't need to know about the composition.

## Time model

Per-pipeline knob `time_mode`:

| Value | Meaning |
| --- | --- |
| `"event"` (default) | Use the event timestamp from the message. Deterministic on replay. |
| `"processing"` | Synthesize `_event_ts = read_arrow_stream.arrival_time` at read. Non-deterministic on replay; trivial to implement. |

Both modes compile to the same machinery. `"processing"` is just
`"event"` with a synthesized timestamp column.

### `_event_ts` column

Streaming source backends inject `_event_ts` into every batch:

| Source backend | Source field |
| --- | --- |
| Kafka | `message.timestamp` |
| Pub/Sub | `publish_time` |
| Kinesis | `ApproximateArrivalTimestamp` |
| RabbitMQ | processing-time fallback (no native message timestamp) |

Type: `Timestamp(Microsecond, Some("UTC"))`.

Per-pipeline override: `event_time_column = "ts"` to use a
user-decoded payload column instead of the envelope. Naming
collision (the user's payload already has `_event_ts`) is a
config-time error.

For `time_mode = "processing"`, sources still inject `_event_ts`
but its value is the read-clock arrival time, not the envelope.

## Watermarks

Per-source: `wm_i = max(_event_ts_seen_i) − lateness_ms`. Monotone
— a late row never causes the watermark to recede.

Multi-source aggregation:

```text
global_wm = min(wm_i over non-idle sources)
```

A source is "non-idle" if `now − last_arrival_i ≤ source_idleness_ms`.
Idle sources are excluded from the `min` so a quiet sibling source
doesn't stall every active window. If **all** sources are idle, the
`global_wm` does not advance.

A window emits when `global_wm ≥ window_end`.

**Single-source idle pipelines stall by design.** A quiet topic
with `lateness_ms = 30s` holds open windows until either data
resumes or the user restarts. There is no wall-clock fallback;
this is the trade for replay determinism. Operators worried about
quiet topics should set `lateness_ms` low or accept the stall.

## Trait surface

`BatchTransform` evolves to take a `BatchContext` on each per-data
call:

```rust
#[derive(Debug, Clone, Default)]
pub struct BatchContext {
    /// Pipeline's current watermark (the min over per-source
    /// watermarks, excluding idle sources). `None` for pipelines
    /// without time-aware transforms — existing transforms ignore
    /// this field anyway.
    pub global_wm: Option<i64>,
    // Reserved for 39.5: arrival_time, source_idx, retraction signals.
}

#[async_trait]
pub trait BatchTransform: Send + Sync + std::fmt::Debug {
    fn input_schema(&self) -> SchemaRef;
    fn output_schema(&self) -> SchemaRef;

    async fn transform(
        &self,
        input: RecordBatch,
        ctx: &BatchContext,
    ) -> Result<Vec<RecordBatch>, BackendError>;

    async fn on_idle_tick(
        &self,
        ctx: &BatchContext,
    ) -> Result<Vec<RecordBatch>, BackendError> {
        Ok(Vec::new())
    }

    async fn refresh_lookup(
        &self,
        name: &str,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<(), BackendError> { /* unchanged */ }
}
```

`DataFusionTransform` and `LazySqlTransform` accept `ctx` and
ignore it. All test call sites + the streaming pipeline update
for the new signature in PR 1.

There is **no separate `advance_watermark` method**. Watermark-
driven window emit happens inside `transform()` and
`on_idle_tick()` — both check `ctx.global_wm` and emit any
windows with `end ≤ global_wm`. Single emit path; the pipeline
combines emits with regular transform output.

## Pipeline loop

```text
loop:
  if shutdown: break

  for each (source_idx, query):
    batches_i = source.read_arrow_stream(query)
    for b in batches_i:
      pipeline.update_max_event_ts(source_idx, b._event_ts.max())
      pipeline.update_last_arrival(source_idx, now())

  global_wm = pipeline.compute_global_watermark()
  ctx       = BatchContext { global_wm: Some(global_wm) }
  all_batches = concat(batches_i)

  if all_batches.is_empty():
    emits = transform.on_idle_tick(&ctx)
    if non-empty: target.write(emits) + commit_offsets()
    sleep(idle_pause_ms)
    continue

  transformed = transform.transform(all_batches, &ctx)
  target.write(transformed) + commit_offsets()
```

Pipeline gains: per-source `max_event_ts_seen` + `last_arrival_time`
state, `_event_ts` column extraction, `global_wm` computation.

`_event_ts` validation: when the user has a windowed transform but
the first batch from a source is missing the `_event_ts` column
(e.g., user picked `time_mode = "event"` on a RabbitMQ source
that doesn't inject it), the pipeline fails loudly on first batch
rather than silently producing wrong watermarks.

## Window state model

Internal to `WindowedAggregateTransform`:

```rust
pub struct WindowedAggregateTransform {
    // ... config ...
    inner_sql: Option<Arc<LazySqlTransform>>,
    state: tokio::sync::Mutex<WindowState>,
}

struct WindowState {
    // (window_start_micros, group_key) → accumulator-set
    windows: HashMap<(i64, GroupKey), AccumulatorSet>,
    // Per-window metadata: when state was first opened, distinct-key count,
    // metrics handles.
    window_meta: HashMap<i64, WindowMeta>,
}
```

Window-id from `_event_ts`:

| Kind | window_starts for row at event_ts |
| --- | --- |
| tumbling | `floor(event_ts / duration_ms) * duration_ms` (one window per row) |
| hopping  | every multiple of `hop_ms` within `[event_ts − duration_ms + hop_ms, event_ts]` (up to `ceil(duration_ms / hop_ms)` windows per row) |

State is dropped when a window emits, **unless**
`late_data = "reopen"`, in which case state is held for
`allowed_lateness_ms` past `window_end`.

## Aggregators

Fixed-width per group:

| Agg | State per group | NULL handling |
|---|---|---|
| `count(*)` | `i64` | counts all rows |
| `count(col)` | `i64` | skips nulls |
| `sum(col)` | `i64` / `f64` (type-promoted) | skips nulls |
| `min(col)` | column-width | skips nulls |
| `max(col)` | column-width | skips nulls |
| `avg(col)` | `(sum, count)` — 16 bytes for numeric | skips nulls |
| `first(col)` | `(min_event_ts, value)` | skips nulls in value |
| `last(col)` | `(max_event_ts, value)` | skips nulls in value |

Variable-width opt-in:

| Agg | Mode | State per group | Notes |
|---|---|---|---|
| `count_distinct(col)` | `"approximate"` (default) | HLL+ sketch (~16 KB at p=14) | ~0.81% standard error |
| `count_distinct(col)` | `"exact"` | `HashSet<value>` | requires `max_distinct_values_per_group`; fail-loud cap |

NULL handling matches SQL standard — `COUNT(*)` includes NULL
rows; `COUNT(col)` and aggregations on `col` skip nulls.

## Late data

When a row arrives with `_event_ts` such that the window
containing it has already emitted (i.e., `_event_ts < global_wm`),
three policies:

| Policy | Behavior |
|---|---|
| `"drop"` (default) | `late_rows_dropped_total++`; row is discarded |
| `"dlq"` | route to `dead_letter_topic`; row's source bytes preserved (Kafka source only) |
| `"reopen"` | hold window state past close for `allowed_lateness_ms` (default = `lateness_ms`); late arrivals re-aggregate; window re-emits with corrected aggregates; past-budget rows then drop |

`"reopen"` requires update-style targets (DB MERGE, Delta MERGE)
since prior emits are superseded. Append-only sinks see N rows
for the same window when reopens fire.

`"dlq"` for late data overlaps with the existing target-failure
DLQ path. Both produce to the same topic. Downstream consumers
must demux if they care about the distinction.

## Memory caps

- Per-window cap: **`max_groups_per_window`** (count of distinct
  group keys). Mandatory.
- Per-window cap with exact `count_distinct`: also
  **`max_distinct_values_per_group`**. Mandatory when
  `mode = "exact"`.

**Cap-hit behavior in 39.4 MVP: fail-loud only.** A clear error:

```
"window state cap hit: max_groups_per_window=1000000 reached on
 window [10:00:00Z, 10:01:00Z) for pipeline events-clean"
```

The schema reserves
`on_state_cap = "fail" | "drop_new_groups" | "backpressure"` for
follow-on phases. Routine cap-hits mean the config is wrong; the
fail-loud path makes that visible.

**State sizing.** Total state per pipeline ≈

```
active_windows × max_groups_per_window × per_group_state
```

where

```
active_windows ≈ 1                                            # tumbling, no reopen
active_windows ≈ ceil(duration_ms / hop_ms)                   # hopping, no reopen
active_windows ≈ ceil((duration_ms + lateness)/hop_ms)        # + reopen lateness
```

For HLL approximate `count_distinct`: per-group state is ~16 KB.
With `max_groups_per_window = 1M`, that's 16 GB per active window.
The pipeline logs a warning at startup whenever the computed
upper-bound state exceeds 1 GB.

## SQL surface

Hybrid: `[transform] sql=...` for the SQL pre-stage (filter /
project / cast / lookup-join — all the 39.1–39.3 machinery),
`[transform.window]` for the window + aggregations.

```toml
[transform]
sql = """
  SELECT user_id, amount, page_url, _event_ts
  FROM source
  WHERE event_type = 'click'
"""

[transform.window]
kind                  = "tumbling"          # "tumbling" | "hopping"
duration_ms           = 60_000
# hop_ms              = 30_000              # required when kind = "hopping"; <= duration_ms
event_time_column     = "_event_ts"         # default; user override permitted
time_mode             = "event"             # "event" (default) | "processing"
lateness_ms           = 30_000
source_idleness_ms    = 60_000              # default
group_by              = ["user_id"]
late_data             = "drop"              # "drop" | "dlq" | "reopen"
# allowed_lateness_ms = 30_000              # required when late_data = "reopen"
max_groups_per_window = 1_000_000
crash_recovery        = "at_least_once"     # MVP: only this value accepted
on_state_cap          = "fail"              # MVP: only this value accepted

# Optional renames for the canonical window columns:
window_start_column   = "window_start"      # default
window_end_column     = "window_end"        # default

[[transform.window.aggregations]]
agg = "count"
as  = "click_count"

[[transform.window.aggregations]]
agg    = "sum"
column = "amount"
as     = "amount_sum"

[[transform.window.aggregations]]
agg    = "count_distinct"
column = "page_url"
mode   = "approximate"                      # "approximate" (default) | "exact"
as     = "unique_pages"
# max_distinct_values_per_group = 100_000   # required when mode = "exact"
```

## Output schema

Output columns, in order:

1. `window_start: Timestamp(Microsecond, "UTC")` — inclusive
2. `window_end:   Timestamp(Microsecond, "UTC")` — exclusive
3. `<group_by columns>` in declared order
4. `<aggregation as aliases>` in declared order

Aggregation `as` aliases are required (no auto-derivation).
Conflicts (alias matches a `group_by` column or another alias)
are config-time errors.

## Crash recovery

39.4 MVP: **`crash_recovery = "at_least_once"`** + idempotent
target (required, documented).

On process restart:
1. Source consumers replay from last committed offsets.
2. Window state is rebuilt from replay; in-flight windows redo
   their emits when the watermark crosses each end.
3. The idempotent target — DB MERGE on a natural key including
   `(window_start, group_keys...)` — absorbs the duplicate emits.

Append-only targets (Kafka, raw object-store writes) without
natural-key dedup will produce duplicate window rows on restart.
Pipeline startup logs a warning when the target shape isn't
idempotent-friendly.

The schema reserves:
- `crash_recovery = "kafka_eos"` — 39.4b. Extends
  `KafkaToKafkaEosPipeline`'s transactional coordination to
  windowed emits. Sized at ~+1 week. Only meaningful for
  Kafka source + Kafka target.
- `crash_recovery = "persisted"` — 39.4c. Durable state
  checkpoint (RocksDB / sled / sqlite-as-state-store). Sized
  at ~+3–4 weeks. Gated on a real user need; until that need
  surfaces, this stays a reserved enum value.

## Prometheus metrics

| Metric | Type | Labels |
|---|---|---|
| `ematix_streaming_windows_active` | gauge | `pipeline` |
| `ematix_streaming_windows_emitted_total` | counter | `pipeline`, `kind` |
| `ematix_streaming_late_rows_dropped_total` | counter | `pipeline`, `policy` |
| `ematix_streaming_state_groups_total` | gauge | `pipeline` |
| `ematix_streaming_watermark_seconds` | gauge | `pipeline`, `source` |

`watermark_seconds` reports the per-source watermark and the
global watermark (with `source = "_global"`) so dashboards can
spot stuck or laggy sources.

## Python facade

`Window` and `Aggregation` dataclasses mirror the existing
`Lookup` shape:

```python
from ematix_flow import ematix
from ematix_flow.streaming import Window, Aggregation

@ematix.streaming_pipeline(
    name="user-clicks-per-minute",
    source=kafka_prod, source_query="events",
    target=warehouse, target_table=("public", "user_clicks_1min"),
    transform_sql="""
        SELECT user_id, page_url, _event_ts
        FROM source
        WHERE event_type = 'click'
    """,
    window=Window(
        kind="tumbling",
        duration_ms=60_000,
        event_time_column="_event_ts",
        lateness_ms=30_000,
        group_by=["user_id"],
        late_data="drop",
        max_groups_per_window=1_000_000,
        crash_recovery="at_least_once",
        aggregations=[
            Aggregation(agg="count", as_="click_count"),
            Aggregation(
                agg="count_distinct",
                column="page_url",
                mode="approximate",
                as_="unique_pages",
            ),
        ],
    ),
)
def user_clicks_per_minute():
    pass
```

The Python facade serializes `Window` and `Aggregation` into
`[transform.window]` and `[[transform.window.aggregations]]`
TOML blocks, then delegates to the existing
`run_pipeline_from_toml_str` Rust entry point. Same TOML-as-IPC
shape used by the rest of `streaming.py`.

`@ematix.streaming_pipeline(window=Window(...))` is supported
identically to the existing kwarg surface.

## Phasing (internal sub-PRs)

| PR | Scope |
| --- | --- |
| **PR 1 — trait + watermark plumbing** | `BatchContext` refactor; `transform`/`on_idle_tick` signature changes; pipeline-level per-source `max_event_ts_seen` + `last_arrival_time` state; `_event_ts` extraction; `min`-with-idle-fallback `global_wm` computation; `ematix_streaming_watermark_seconds` metric. **No windowed transform yet.** Existing tests + impls (`DataFusionTransform`, `LazySqlTransform`) updated for the new signature. The full 387-test suite stays green. |
| **PR 2 — `WindowedAggregateTransform`** | Tumbling + hopping windows. Aggregators: count / sum / min / max / avg / first / last / count_distinct (HLL+ approximate + exact opt-in). Late-data policies (drop / dlq / reopen). Memory cap fail-loud. Composition over `LazySqlTransform`. Config-block parsing in CLI. Other Prometheus metrics from the table above. End-to-end CLI integration test using the existing TestBackend pattern with a fixed timeline of `_event_ts` values. |
| **PR 3 — Python facade + bench + docs** | `Window`, `Aggregation` dataclasses; `window=` kwarg on `run_streaming_pipeline`; `@ematix.streaming_pipeline(window=...)` decorator support; TOML emitters for `[transform.window]` + `[[transform.window.aggregations]]`. Bench harness gains `bench_windowed_tumbling` (1000 rows / 100 distinct keys / 1-minute windows). `SQL_TRANSFORMS_PLAN.md` + this doc updated to mark 39.4 shipped with measured numbers. |

## What this doesn't change

- **Existing same-DB load paths** (`strategy/{append,merge,scd2,truncate}.rs`)
  are untouched. Windowed transforms don't go through them.
- **Streaming backends without ack/checkpoint coordination**
  (RabbitMQ, Pub/Sub with in-memory checkpoints) keep their
  existing semantics. The watermark machinery and window emit
  layer is wholly between read and write.
- **DataFusion's arrow ABI version.** No version bumps needed
  for 39.4 — we don't use DataFusion for windowing.

## Open questions deferred to follow-on phases

- **Per-source idleness timer default** (`source_idleness_ms`).
  Default `60_000` (1 minute) is reasonable but arbitrary.
  Calibrate after first production user pipeline.
- **HLL precision tuning** (`hll_precision`). Default `p = 14`
  matches Druid / Snowflake. Configurable as a follow-on if a
  workload genuinely cares.
- **Per-row error handling** for windowed emits (e.g., invalid
  timestamp casts inside a row). Bubbles up; supervisor restart
  + idempotent target. Same `on_error = "drop" | "dlq" | "fail"`
  hook from the parent SQL transform layer applies; no
  windowed-specific behavior.
- **Schema drift mid-pipeline.** The windowed transform's input
  schema is captured at first batch, like `LazySqlTransform`.
  A schema change mid-stream produces a transform error and
  supervisor restart. Acceptable for MVP.
- **Watermark holdback for known-late sources.** A source
  declared as "always 5 minutes behind" could publish a
  per-source `lateness_ms` override. Reserved schema; not in
  MVP.
- **Stream-stream join** (39.5) and **session windows** (39.5).
  Designed in their own doc when those phases start.

## When this lands

PR 1 (trait + watermark plumbing), PR 2 (WindowedAggregateTransform),
and PR 3 (Python facade + bench + docs) all shipped 2026-05.

## Bench numbers (PR 3)

`cargo bench -p ematix-flow-core --bench transform`. Apple M-series,
release build:

| Bench | Result |
| --- | --- |
| `windowed_tumbling_1000_rows_100_keys_ingest` (no emit) | 80 µs |
| `windowed_tumbling_1000_rows_100_keys_ingest_and_emit` (one full window) | 83 µs |

≈80 ns per row for the windowed hot path, in the same order as the
DataFusion filter path; emit cost (building output batch + finalizing
100 accumulators) adds ~3 µs.
