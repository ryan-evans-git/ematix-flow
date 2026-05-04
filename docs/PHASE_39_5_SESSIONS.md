# Phase 39.5a — Session windows + StateStore foundation

**Status:** PR 1 + PR 2 + PR 3 landed (2026-05). Phase 39.5a is
implementation-complete; remaining work is documented at the bottom
under "Deferred for follow-up". Splits Phase 39.5 from the original
SQL-transforms plan into two sub-phases:

- **39.5a (this doc)** — session windows + the durable `StateStore`
  foundation that sessions and stream-stream join both depend on.
- **39.5b** — stream-stream join. Reuses everything in 39.5a;
  separate design doc when 39.5a lands.

**Goal.** Add session-window aggregations on top of 39.4's tumbling
+ hopping machinery, plus durable per-pipeline state so sessions
(and 39.5b joins) can recover deterministically across restarts
without unbounded source replay.

This phase ships:
- Session windows with gap-based boundaries and per-key grouping.
- Mandatory `max_session_duration_ms` cap to bound state.
- Out-of-order session merging under `late_data = "reopen"`.
- All nine 39.4 aggregators with `combine` ops for session merge.
- `StateStore` trait + `InMemoryStateStore` + `PostgresStateStore`.
- Per-emit atomic state+offset commits; dirty-only periodic
  checkpoint floor (default 60s).
- Forward-only state migrations across `state_version` bumps.
- Source-backend `seek_to(offset_bytes)` plumbing; Kafka ships in
  this phase, others land as user demand surfaces.

## Non-goals

- **Stream-stream join.** Keyed join across two `[[sources]]` with
  bounded time interval. Lands in 39.5b on top of 39.5a's
  `StateStore`.
- **Retrofitting 39.4 to use `StateStore`.** Tumbling and hopping
  windows keep their in-memory state model. Crash recovery for
  39.4 stays "supervisor restart + idempotent target." Future
  work; not part of 39.5a.
- **Sliding windows.** Same reasoning as 39.4: per-row windows are
  unbounded without per-row eviction. Deferred indefinitely.
- **Spill-to-disk for session state.** State is bounded by
  `max_session_duration_ms × group_cardinality`; we document a
  per-pipeline state-size warning and cap, not a spill mechanism.
- **`late_data = "dlq"` for sessions.** Same reasoning as 39.4:
  needs a separate write path the transform doesn't have access
  to. Reserved schema; CLI rejects with a clear error.
- **`seek_to` for every source backend.** Trait gains a
  default-error method; only Kafka implements it in 39.5a.
  Pipelines that configure sessions or joins on a source that
  doesn't support `seek_to` fail at config-load with a clear
  error. Other backends (file-based, S3, RabbitMQ) get retrofits
  as separate follow-up PRs.

## Architecture

```text
                read_arrow_stream  ◀──── seek_to(offset) on startup
source backend  ──────────────▶  RecordBatch (with `_event_ts`)
   (per source)                     │
                                    ▼
                          ┌──────────────────────────┐
                          │  pipeline watermark loop │  (39.4)
                          │  per-source max(_ts)     │
                          │  global_wm = min(...)    │
                          └──────────────────────────┘
                                    │ BatchContext { global_wm }
                                    ▼
                          ┌──────────────────────────┐
                          │  WindowedAggregateXform  │  (39.4 + 39.5a)
                          │  ┌────────────────────┐  │
                          │  │  inner Lazy SQL    │  │
                          │  └────────────────────┘  │
                          │  state: HashMap<         │
                          │    GroupKey,             │
                          │    Vec<SessionState>>    │  ← new in 39.5a
                          │  emit: wm > last_evt +   │
                          │        gap + lateness    │
                          └──────────────────────────┘
                                    │
                                    ▼
                              RecordBatch
                                    │
                                    ▼
                          ┌──────────────────────────┐
                          │  StateStore (Postgres)   │  ← new in 39.5a
                          │  per-emit atomic commit  │
                          │  state + offsets in one  │
                          │  transaction             │
                          └──────────────────────────┘
                                    │
                                    ▼ write_arrow_stream
                              target backend
                              (idempotent: MERGE / UPSERT)
```

The state store is the new component. The windowed transform's
state model gains a `Session` variant alongside tumbling/hopping
but otherwise reuses 39.4's `emit_ready` machinery.

## Session semantics

### Definition

For a given group key, a session is a maximal run of rows whose
consecutive event-time gap is `≤ gap_ms`. The first row past
`gap_ms` of the prior row's event-time starts a new session.

### Per-key required

`group_by` must be non-empty. Empty `group_by` (a single global
session for the entire stream) is rejected at config-load — global
sessions create a single hot-spot session that never closes and
defeat the per-key state-bounding model.

### Out-of-order merging (under `Reopen`)

If a late row arrives within `gap_ms` of two existing sessions for
the same key, the sessions merge into one. Aggregators implement a
`combine(other)` op so two accumulator states can be unioned:

| Aggregator | `combine` |
|---|---|
| `count` / `count_col` / `sum` | add |
| `min` / `max` | compare |
| `avg` | add `(sum, count)` componentwise |
| `first` / `last` | pick by `min(event_ts)` / `max(event_ts)` |
| `count_distinct` (HLL+) | `HyperLogLogPlus::merge` |
| `count_distinct` (exact) | set union; cap-violation fails loud |

Under `Drop` policy, late rows past `allowed_lateness_ms` are
dropped before reaching merge logic; merging only matters under
`Reopen`. Merged sessions are marked dirty and re-emitted.

### `max_session_duration_ms` (mandatory)

Required at config-load. When `event_ts - session.start_ts ≥
max_session_duration_ms`, the existing session force-emits and a
fresh session opens for that row. Force-emitted sessions evict
state immediately, even under `Reopen` — the cap is a hard
ceiling regardless of late arrivals. Bridging the cap with a late
row would defeat the bound; we don't do it.

### Emission

A session emits when:

1. **Watermark completes the session.** `global_wm > session.last_event_ts + gap_ms + allowed_lateness_ms`. No row inside the gap window can ever arrive again.
2. **Force-emit on duration cap.** Pre-row check; emits before processing the boundary-crossing row.

Under `Drop`: state evicts on emit.

Under `Reopen`: state retained until
`global_wm > session.last_event_ts + gap_ms + allowed_lateness_ms`
(same deadline as the emit trigger). Late arrivals before that
deadline merge and re-emit; after, evict.

### Output schema

```text
[group_keys..., window_start, window_end, session_id, agg_cols...]
```

- `window_start` / `window_end` — match 39.4 column names so
  downstream consumers don't branch on window kind.
- `session_id` — `hash(group_key, window_start)` as `UInt64`.
  Always emitted. Under `Reopen` a session's `window_start` may
  shift backward (late row at `t=5` extending `[10,20]` to
  `[5,20]`), so `session_id` shifts too. Idempotent target writes
  `(group_keys..., session_id)` as the de-dup key — same contract
  as 39.4.

## StateStore

### Why now

39.4 punted state persistence: in-flight window state lives in
memory, supervisor restart re-builds from source replay, idempotent
target absorbs duplicate writes. That works for tumbling/hopping
windows where state is bounded by window width × group count and
typical replay distance is one window.

Sessions and stream-stream joins break the model. Both can hold
state for hours (idle-key sessions; one-sided join state waiting
for matches). Replay distance for "rebuild state from source"
becomes unbounded. State persistence is required.

### Trait shape

Single-shot atomic commit:

```rust
#[async_trait]
pub trait StateStore: Send + Sync + std::fmt::Debug {
    async fn load(
        &self,
        pipeline: &str,
    ) -> Result<RecoveredState, BackendError>;

    async fn commit(
        &self,
        pipeline: &str,
        snapshot: CommitSnapshot,
    ) -> Result<(), BackendError>;
}

pub struct RecoveredState {
    pub state_by_key: HashMap<GroupKey, Vec<u8>>,   // postcard blobs
    pub offsets: HashMap<SourceId, Vec<u8>>,
    pub state_version: u32,
}

pub struct CommitSnapshot {
    pub state_upserts: Vec<(GroupKey, Vec<u8>)>,
    pub state_deletes: Vec<GroupKey>,
    pub offsets: HashMap<SourceId, Vec<u8>>,
}
```

Backend opens a transaction inside `commit`, applies all changes,
commits, returns. Pipeline never sees the transaction handle.

### Postgres schema

```sql
CREATE TABLE ematix_streaming_state (
    pipeline_name   TEXT      NOT NULL,
    group_key       BYTEA     NOT NULL,         -- serialized GroupKey
    state_blob      BYTEA     NOT NULL,         -- postcard Vec<SessionState>
    state_version   INT       NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (pipeline_name, group_key)
);

CREATE TABLE ematix_streaming_offsets (
    pipeline_name   TEXT      NOT NULL,
    source_id       TEXT      NOT NULL,
    offset_bytes    BYTEA     NOT NULL,
    state_version   INT       NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (pipeline_name, source_id)
);
```

Both tables update inside the same transaction in `commit`.

One row per `(pipeline, group_key)`, *not* per session. The
`state_blob` holds the (typically 1-element) `Vec<SessionState>`
of active sessions for that key — usually one, occasionally more
under `Reopen` retention.

### Cadence

**Per-emit atomic commit.** When `emit_ready` produces output:

1. Target write (idempotent).
2. Build `CommitSnapshot` from changed keys + new source offsets.
3. `StateStore.commit(pipeline, snapshot)` — single transaction.

Crash between (1) and (3) re-runs the emit on restart; idempotent
target absorbs duplicate.

**Dirty-only periodic checkpoint.** Default `checkpoint_interval_ms
= 60_000`. Even with no emits, a 60s ticker fires `commit` with
the keys whose accumulators changed since last commit + current
offsets. Bounds replay-on-restart to ≤60s of source data.

Pipelines with steady emit cadence (active sessions firing on gap)
rarely hit the periodic floor; the floor matters for idle-but-not-
empty stretches and for 39.5b joins waiting on match arrivals.

### Source offsets

`StateStore` is the single source of truth for source offsets.
Source backends gain a `seek_to(&self, offset_bytes: &[u8])` method
called once at pipeline start with the offset loaded from
`StateStore`. Native source-side commit (`Kafka.commit_offsets`)
becomes advisory — pipelines using `StateStore` skip it.

Default trait impl returns `Err("backend does not support state
persistence")`. Pipelines with sessions or joins fail at
config-load if any source backend hasn't overridden it.

In 39.5a, only Kafka implements `seek_to` (via `assign + seek`).
Other backends are reserved as follow-up PRs.

### Serialization

`postcard` for the `state_blob`. Compact (within a few % of
bincode), schema-stable (explicit append-only field rules), well-
supported in the Rust ecosystem. The `state_version` column gates
genuine shape changes; minor field additions don't force version
bumps.

### Migrations

Forward-only auto-migrate. On `load`, deserialize as the stored
version, run a migration chain
(`StateV1 → StateV2 → StateV3`) up to current. Each migration
is a pure function in `state_migrations.rs` that lives forever
(small constant cost per migration).

```rust
pub fn migrate(
    blob: &[u8],
    from: u32,
    to: u32,
) -> Result<Vec<u8>, MigrationError>;
```

A migration that genuinely can't be done returns
`MigrationError::Unsupported`, halting the pipeline with a clear
message. Auto-migrate is the default; fail-loud is the escape
hatch.

### Opt-in

`StateStore` is required only when the pipeline configures sessions
or stream-stream joins. 39.4 tumbling/hopping pipelines stay on
the in-memory model — no behavior change for existing deployments.

Config-load fails if `[transform.window]` has `kind = "session"`
without a corresponding `[state_store]` block.

## API surface

### Python

`Window` dataclass extended with `kind="session"`:

```python
ematix.run_streaming_pipeline(
    sources=[...],
    target=...,
    window=Window(
        kind="session",
        gap_ms=30_000,
        max_session_duration_ms=7_200_000,   # 2h hard cap
        group_by=["user_id"],
        late_data="reopen",
        allowed_lateness_ms=60_000,
        aggregations=[
            Aggregation(kind="count", output_name="events"),
            Aggregation(kind="first", input_column="page",
                        output_name="first_page"),
        ],
    ),
    state_store=StateStore(
        kind="postgres",
        url="postgres://localhost/ematix_state",
        schema="public",
        checkpoint_interval_ms=60_000,
    ),
)
```

Validation at config-load:
- `kind="session"` requires `gap_ms` and `max_session_duration_ms`.
- `kind="session"` rejects `width_ms` / `hop_ms`.
- `group_by` must be non-empty.
- `state_store=` required when `kind="session"`.

### TOML

```toml
[transform.window]
kind = "session"
gap_ms = 30000
max_session_duration_ms = 7200000
group_by = ["user_id"]
late_data = "reopen"
allowed_lateness_ms = 60000

[[transform.window.aggregations]]
kind = "count"
output_name = "events"

[[transform.window.aggregations]]
kind = "first"
input_column = "page"
output_name = "first_page"

[state_store]
kind = "postgres"
url = "postgres://localhost/ematix_state"
schema = "public"
checkpoint_interval_ms = 60000
```

`[state_store]` is a top-level block (not nested under
`[transform]`) because 39.5b will introduce stateful constructs
outside the windowed transform that share the same backend.

## PR sequencing

| PR | Scope |
|---|---|
| **PR 1** | `StateStore` trait + `InMemoryStateStore` + `PostgresStateStore` (postcard, version migrations, schema bootstrap) + `[state_store]` TOML + Kafka `seek_to` impl + pipeline `state-load` on startup. No session machinery yet — tested via `MockSessionState` fixture against the trait contract. |
| **PR 2** | `WindowKind::Session` + gap detection + merge-under-Reopen + force-emit on duration cap + per-aggregator `combine` ops + `session_id` column + watermark-driven emit and retention. Pure in-memory; uses 39.4's `emit_ready` machinery. Unit tests cover semantics; no `StateStore` integration yet. |
| **PR 3** | Session ↔ `StateStore` integration: per-emit `commit` calls, dirty-only periodic checkpoint, recovery flow on startup, end-to-end Postgres+Kafka crash-recovery tests via testcontainers. Python `Window(kind="session")` + `StateStore` dataclass + TOML wiring + decorator support. |

PR 1 is mergeable standalone — 39.5b will reuse it without
changes. PR 2 is mergeable without PR 3 (sessions work in-memory,
same crash-recovery posture as 39.4 until PR 3 lands).

## Open design questions

- **Schema-change for `_event_ts` mid-pipeline.** Same as 39.4 —
  source-injected; rejected at first batch if missing.

- **State-size warning thresholds.** A pipeline with 10M active
  group keys × ~200B/state_blob = ~2GB resident. Worth a
  config-load warning at `max_groups × estimated_blob_size > X`?
  Defer to PR 3 ops review.

- **`seek_to` semantics for sources with replication or partition
  reassignment.** Kafka rebalance mid-stream triggers
  `assign+seek` again with new partition set; current offset
  loaded from `StateStore` may not cover new partitions. Out of
  scope for PR 1; flagged for follow-up.

- **`InMemoryStateStore` as a production option.** Tests use it;
  ops sometimes want it for "I don't care about persistence,
  give me a no-op." Document as `kind = "in_memory"` in TOML
  with a loud-warning at config-load. Decision deferred to PR 1.

- **Merging sessions across the `max_session_duration_ms`
  boundary.** Confirmed *no* — the cap is a hard ceiling regardless
  of late arrivals. Documented above; flagged here for visibility.

## When this lands

Gated on the design review (this doc). PR 1 starts immediately
after; PR 2 after PR 1 merges; PR 3 after PR 2.

39.5b (stream-stream join) gets its own design pass after 39.5a
ships.

## Deferred for follow-up

- **`count_distinct` in stateful sessions.** PR 3 ships postcard
  serialization for the 7 base aggregators (`count`, `count_col`,
  `sum`, `min`, `max`, `avg`, `first`, `last`). HLL+ sketches and
  `HashSet`-backed exact-distinct sets don't currently round-trip
  through postcard; the CLI rejects pipelines that mix
  `count_distinct` with `kind = "session"` + `[state_store]` at
  config-load. Lifting the limit needs either custom HLL+ ser/de or
  a recompute-from-zero retention strategy.
- **`seek_to` for non-Kafka sources.** Default trait impl returns
  "not supported"; only Kafka overrides in PR 1. The CLI rejects
  session pipelines whose source is not Kafka. Adding Pub/Sub /
  Kinesis / RabbitMQ / object-store seek_to is mechanical per
  backend but each lands as its own follow-up PR.
- **Periodic dirty-only checkpoint ticker.** PR 3 commits state on
  every emit (per-emit cadence). The 60s dirty-only floor for
  idle-but-active pipelines was scoped in but deferred — current
  pipelines stay durable through the per-emit path; the floor
  matters only when sessions sit dirty without emitting (long
  retention windows on a sparse source). Configured via
  `[state_store] checkpoint_interval_ms` (default 60_000); the value
  is parsed and validated but the ticker itself is a follow-up.
- **`InMemoryStateStore` as a documented production option.**
  Implementation accepts `kind = "in_memory"` and works end-to-end;
  the loud config-load warning (per design doc) is not yet emitted.
- **State-size warning thresholds.** Per design doc open question;
  to revisit after first production deployment.
