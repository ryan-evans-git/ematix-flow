# Phase 39.5b — Stream-stream join

**Status:** PR 1 + PR 2 + PR 3 landed (2026-05). Implementation
complete; remaining work in "Deferred for follow-up" at the bottom.

**Goal.** Keyed, time-windowed inner join between two `[[sources]]`
on top of 39.5a's `StateStore`. A row from each side joins with
every row from the other side that shares the configured key and
whose event-time falls within `time_window_ms`.

This phase ships:
- `kind = "stream_stream_join"` transform variant.
- Per-side, per-key buffers with watermark-driven eviction.
- Symmetric inner-join semantics: every (left, right) match
  emits one output row.
- `StateStore` integration: per-emit atomic state+offsets commit;
  recovery rehydrates both side buffers on restart.
- `BatchContext::source_id` so the transform routes incoming
  batches to the correct side.
- Python `Join` dataclass + TOML wiring.

## Non-goals

- **LEFT / RIGHT / FULL OUTER joins.** Inner only in this phase.
  Outer joins need explicit "no match landed before window expired"
  emit logic — separate follow-up.
- **N-way joins.** Two sides only. N-way reduces to chained
  binary joins; no first-class support here.
- **Aggregation downstream of the join.** A windowed aggregate over
  join output works (chain a session window after the join's
  output), but the join itself doesn't aggregate.
- **Stream-table (lookup) joins.** Already covered by Phase 39.3
  static `[transform.lookups]`. This phase is strictly stream-stream.
- **Asymmetric time windows.** `time_window_ms` is symmetric:
  `|left.ts - right.ts| ≤ time_window_ms`. Asymmetric (e.g. "right
  must arrive after left within X seconds") is a follow-up.

## Architecture

```text
sources                    transform                      target
─────────                 ─────────────                  ──────────
[[sources]] left          ┌──────────────────────────┐
   ├── batches ──┐        │  TimeWindowedJoin        │
   │            ├───────▶ │  ┌────────────────────┐  │
[[sources]] right          │  │ left_buf:           │  │
   └── batches ──┘        │  │   K → [(ts, row)…] │  │
                          │  │ right_buf: same    │  │
                          │  │ ingest left/right  │  │
                          │  │  → match against   │  │
                          │  │     opposite buf   │  │
                          │  │  → emit joined row │  │
                          │  │ evict on watermark │  │
                          │  └────────────────────┘  │
                          └──────────────────────────┘
                                    │
                                    ▼
                              RecordBatch (joined)
                                    │
                                    ▼
                          ┌──────────────────────────┐
                          │  StateStore (Postgres)   │
                          │  per-emit atomic commit  │
                          │  per-key per-side blobs  │
                          └──────────────────────────┘
```

## Join semantics

### Match condition

A left row `L` and a right row `R` join iff:
1. `L[left_keys] = R[right_keys]` (per-column equality, NULL=NULL not
   equal — matches SQL `INNER JOIN ... ON` semantics).
2. `|L.event_ts - R.event_ts| ≤ time_window_ms`.

### Per-key required

Both `left_keys` and `right_keys` must be non-empty and have equal
length. Empty keys would join every left row against every right
row — a cross join with time gating. Reject at config-load.

### Emit on arrival

A new row arriving on either side:
1. Looks up the same key in the **opposite** side's buffer.
2. Filters those entries by `|new.ts - existing.ts| ≤ time_window_ms`.
3. Emits one output row per match.
4. Inserts itself into its own side's buffer.

This means each (L, R) match emits exactly once: when whichever of
the pair arrives second is processed.

### Output schema

The output schema concatenates:
- All `[transform.join.output_left_columns]` (default: every left
  column, with prefix `left_`).
- All `[transform.join.output_right_columns]` (default: every right
  column, with prefix `right_`).

The output is a flat `RecordBatch`; downstream transforms (windowed
aggregation, etc.) treat it like any batch.

### Watermark-driven retention

Once `global_wm > buffer_row.event_ts + time_window_ms +
allowed_lateness_ms`, no more matches for that row are possible
(the opposite side's events that could match are guaranteed to be
in the past per the watermark guarantee). Evict the row.

Eviction is per-row, not per-key — a key with one stale and one
fresh row keeps the fresh entry in the buffer.

### Late-data handling

Same shape as session windows:
- `Drop` (default): rows past
  `wm > row.event_ts + time_window_ms + allowed_lateness_ms` are
  dropped at ingest. (lateness=0 under Drop.)
- `Reopen` (PR 2 follow-up if requested): retain late rows up to
  `allowed_lateness_ms`; emit retroactive matches. The first 39.5b
  PR ships **Drop only**.

## StateStore integration

### Per-side, per-key blobs

Two `StateKey`s per join key, distinguished by a side prefix:
- `b"L\0" + encode_key(K)` — left buffer for K.
- `b"R\0" + encode_key(K)` — right buffer for K.

Each blob is a postcard-encoded `Vec<JoinedRowBlob>` per side.
`JoinedRowBlob` carries `(event_ts, row_data)` where `row_data`
is the IPC-serialized single-row `RecordBatch`.

### Commit / recover

Mirrors session machinery exactly:
- `take_join_state_commit() -> (upserts, deletes)` drains
  `dirty_keys + evicted_keys` and produces per-side blobs.
- `recover_join_state(state_by_key)` decodes the side prefix,
  routes to the correct buffer.

### `state_version`

The join blob format starts at `state_version = 1` for 39.5b. The
existing `MigrationChain` (PR 1, slice 1.3) registers no migrations
yet — the layout is stable from day one. Future shape changes go
through the registered migration chain.

### Session vs. join blobs in the same store

Both kinds use the same `ematix_streaming_state` table. The
`group_key` BYTEA column is opaque — sessions store
`encode_group_key(...)` directly; joins store `b"L\0" + ...` /
`b"R\0" + ...`. A pipeline configured with a session window cannot
also be configured with a join (transform is single-purpose),
so the blob namespaces don't overlap inside a single pipeline name.

## API surface

### `BatchContext::source_id`

The pipeline loop currently merges all sources' batches before
invoking `transform`. For joins, source identity is essential.

`BatchContext` gains a new field:

```rust
pub struct BatchContext {
    pub global_wm: Option<i64>,
    pub source_id: Option<String>,  // new in 39.5b
}
```

The pipeline calls `transform.transform(batch, ctx)` once per
*per-source* batch (instead of merging first), passing the
source's `query` field as `source_id`. Existing transforms ignore
the field.

### Python

```python
ematix.run_streaming_pipeline(
    name="orders-payments-join",
    sources=[
        Source(name="orders", connection=kafka_conn, query="orders"),
        Source(name="payments", connection=kafka_conn, query="payments"),
    ],
    target=...,
    join=Join(
        left_source="orders",
        right_source="payments",
        left_keys=["order_id"],
        right_keys=["order_id"],
        time_window_ms=300_000,           # 5 min
        event_time_column="_event_ts",
        late_data="drop",
    ),
    state_store=StateStore(kind="postgres", url="..."),
)
```

### TOML

```toml
[transform.join]
kind = "stream_stream_join"
left_source = "orders"
right_source = "payments"
left_keys = ["order_id"]
right_keys = ["order_id"]
time_window_ms = 300000
event_time_column = "_event_ts"
late_data = "drop"
```

`[transform.window]` and `[transform.join]` are mutually exclusive;
config-load rejects pipelines that set both.

## PR sequencing

| PR | Scope |
|---|---|
| **PR 1** | `JoinConfig` / `JoinKind::Inner` + `TimeWindowedJoinTransform` (in-memory state, no `StateStore` yet). `BatchContext::source_id`. Pipeline routing. Output-batch builder. Drop-only late-data. Unit tests cover the match algebra + watermark eviction. |
| **PR 2** | `StateStore` integration: per-emit commit (state + offsets atomic), recovery flow on startup. End-to-end Postgres+Kafka crash-recovery test. CLI `[transform.join]` + cross-validation. |
| **PR 3** | Python `Join` dataclass + decorator support. TOML emitter. |

PR 1 is a usable in-memory stream-stream join — same caveat as
39.4 windows: state lost on restart, idempotent target absorbs
duplicates from source replay.

## Open design questions

- **Reopen for joins.** Late rows past their original window
  budget could still match retained opposite-side rows. Useful for
  pipelines tolerant of out-of-order data; complex to implement
  (need to track which matches have already been emitted to avoid
  duplicates). Deferred to a 39.5b PR 4 if user demand surfaces.
- **State growth on hot keys.** A join key with many same-side
  events accumulates buffer entries until eviction. Documented as
  the operator's responsibility (downstream rate, lateness budget,
  state-store byte-budget). No first-class limit yet — lift the
  config-load warning thresholds in concert with sessions
  (PHASE_39_5_SESSIONS open question).
- **Join column name collisions.** Default `left_` / `right_`
  prefixes avoid collisions; user can override. Reject if any
  configured output column name collides.
- **Output ordering.** Per-emit-call ordering is left-row-first,
  right-row-first ordering: ingest left rows in batch order,
  emitting matches against right buffer; then ingest right rows.
  Determinism within a single batch processing call; cross-batch
  ordering follows source delivery order (already non-deterministic
  for multi-source pipelines).

## When this lands

After 39.5a ships. PR 1 starts immediately; PR 2 after PR 1
merges; PR 3 after PR 2 (Python bindings come last so the Rust
side is fully mergeable without them).

## Deferred for follow-up

- **Outer joins** (`LEFT` / `RIGHT` / `FULL`). Need explicit
  emit-on-no-match logic at the eviction boundary. Future PR.
- **`late_data = "reopen"` for joins.** Late rows past the
  original window budget could still match retained opposite-side
  rows; needs duplicate-emit suppression. Future PR.
- **End-to-end testcontainers test** (Postgres + Kafka + join).
  PR 1 + PR 2 cover the algebra and serialization with unit tests
  (18 covering the join + 5 for the StateStore integration); the
  full crash-recovery e2e is the obvious next step but mirrors
  the structure of `session_pipeline_crash_recovers_committed_state`
  exactly.
- **Symmetric-windowed-join optimizations.** Current PR 1 stores
  per-row owned scalars; for high-throughput pipelines, switching
  to columnar Arrow storage (single-row `RecordBatch` references
  with row indices) would cut memory.
- **Asymmetric time windows** (`right must arrive after left
  within X ms`). One-sided variants are useful for late-arrival
  detection. Future PR.
- **Hot-key state-size warnings.** Same hooks as sessions; no
  first-class limit yet.
