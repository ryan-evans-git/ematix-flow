# Phase Δ — CDC source mode (Debezium / Maxwell / custom envelope)

**Status:** drafted, not yet started. Lives under
[`docs/ROADMAP.md`](ROADMAP.md) item 34 (P2 — feature extensions).
~3–4 weeks of focused work, single dev. Built entirely on
already-shipped pieces: Kafka source with SR-aware Avro/Protobuf,
mid-stream SQL transforms, the per-key `StateStore`, and
Postgres / Delta / object-store strategy executors. No new
backend; no breaking change to the connector trait.

**Goal.** Make ematix-flow first-class for the "Debezium →
target" pattern: a Postgres source emits change events to Kafka
via Debezium, ematix-flow consumes that topic and applies the
events to a downstream Postgres / Delta / object-store target
with the right semantics (`INSERT` on creates, `UPDATE` on
updates, `DELETE` or soft-delete on deletes). Idempotent across
Kafka redelivery; schema-aware; configurable in either Python or
TOML.

---

## Why this lives in scope

The "ingest a CDC topic and apply changes to a mirror table" is
a workflow ematix-flow's existing primitives almost handle —
Kafka source, SR Avro decode, mid-stream SQL transforms, and
strategy executors that know how to merge are all there. What's
missing is the orchestration layer that:

1. Recognises CDC envelopes (Debezium / Maxwell / generic).
2. Dispatches per-op to the right strategy (insert / update /
   delete).
3. Skips tombstones (Debezium emits a null-payload record after
   the `d` event).
4. Deduplicates redeliveries by tracking last-seen
   `source.ts_ms` per PK in the StateStore.

Today users *can* hand-roll this: configure a Kafka source with
SR Avro, write a SQL transform that splits the
`before`/`after`/`op` envelope and adds a synthesised `__op`
column, then run the target with `mode = "merge"` plus a custom
post-load SQL that issues DELETEs where `__op = 'd'`. It works
for happy paths but the user owns:

- envelope parsing,
- tombstone filtering,
- idempotency state,
- delete-vs-soft-delete dispatch,
- schema evolution.

Each of those is a footgun. First-class CDC is the right
abstraction.

---

## API surface — decorator AND TOML, peer-equivalent

Both forms compile to the same `CdcConfig` Rust struct + the
same per-op dispatch in the strategy executor. Pick whichever
fits the workflow; there is no performance difference.

### Decorator (Python)

```python
from typing import Annotated
from ematix_flow import (
    ematix, pk, CDC,
    KafkaConnection, PostgresConnection,
    register_connection,
)
from ematix_flow.types import BigInt, String, Text, TimestampTZ

@ematix.connection
class kafka_prod:
    kind = "kafka"
    bootstrap_servers = "${KAFKA_BOOTSTRAP}"
    schema_registry_url = "${SR_URL}"

@ematix.connection
class warehouse:
    kind = "postgres"
    url = "${WAREHOUSE_DSN}"

@ematix.table(schema="mirror")
class CustomerMirror:
    customer_id: Annotated[BigInt, pk()]
    email: String[256] | None
    name: Text | None
    updated_at: TimestampTZ

@ematix.streaming_pipeline(
    source="kafka_prod",
    source_query="dbserver1.public.customers",   # Debezium topic
    target=CustomerMirror,
    target_connection="warehouse",
    cdc=CDC(envelope="debezium"),                # ← all you need
)
def mirror_customers():
    pass
```

`CDC(envelope="debezium")` carries the canonical Debezium field
mapping (`op`, `before`, `after`, `source.ts_ms`,
`after.<pk_col>`). For non-Debezium envelopes, override fields:

```python
cdc=CDC(
    envelope="custom",
    op_field="action",
    before_field="old_state",
    after_field="new_state",
    key_field="new_state.id",
    ts_field="changed_at_ms",
    op_map={"INSERT": "c", "UPDATE": "u", "DELETE": "d"},
    delete_mode="soft",          # or "hard" (default)
    soft_delete_column="deleted_at",
)
```

### TOML (config-as-data)

```toml
pipeline_name = "mirror_customers"
source_query = "dbserver1.public.customers"

[source]
kind = "kafka"
bootstrap_servers = "${KAFKA_BOOTSTRAP}"
schema_registry_url = "${SR_URL}"

[target]
kind = "postgres"
url = "${WAREHOUSE_DSN}"

[target.table]
schema = "mirror"
name = "customer_mirror"

[transform.cdc]
envelope = "debezium"

# Override fields for non-Debezium envelopes:
# envelope = "custom"
# op_field = "action"
# before_field = "old_state"
# after_field = "new_state"
# key_field = "new_state.id"
# ts_field = "changed_at_ms"
# op_map = { INSERT = "c", UPDATE = "u", DELETE = "d" }
# delete_mode = "soft"
# soft_delete_column = "deleted_at"
```

Both surfaces are validated at config-load (same as today's
Backend trait round-trip). A hand-curated YAML, an
auto-generated config from a UI, and a Python decorator block
all converge on the same internal representation.

---

## Internal design

### `CdcConfig` — one source of truth

```rust
pub struct CdcConfig {
    pub envelope: EnvelopeKind,                  // Debezium | Maxwell | Custom
    pub op_field: String,                        // path within the row
    pub before_field: Option<String>,
    pub after_field: String,
    pub key_field: String,                       // PK extraction path
    pub ts_field: Option<String>,                // for idempotency + watermark
    pub op_map: HashMap<String, CdcOp>,          // string → c / u / d / r
    pub delete_mode: DeleteMode,                 // Hard | Soft { column }
    pub schema_evolution: SchemaEvolutionPolicy, // Skip | Fail | Warn (default)
}
```

Carried through `BackendConfig` so a serialized pipeline
round-trips bit-equivalent to its constructor. Derives
`Serialize + Deserialize`. The decorator and TOML paths both
produce one of these.

### Strategy executor: per-op dispatch

Today's strategy executors (`run_append`, `run_truncate`,
`run_merge`, `run_scd2`) live behind the `Backend` trait. Phase
Δ adds `run_cdc`:

```rust
async fn run_cdc(
    &self,
    spec: &TableSpec,
    batch: RecordBatch,            // one Kafka poll's worth of events
    cdc: &CdcConfig,
    state_store: &dyn StateStore,
    pipeline_name: &str,
) -> Result<CdcRunResult, BackendError>;
```

Per-batch flow:

1. Walk rows; for each row:
   - Read `op_field` → `CdcOp` via `op_map`.
   - Read `key_field` → primary-key value(s).
   - Read `ts_field` (if configured) → idempotency timestamp.
   - Idempotency check: load last-seen ts for this PK from
     StateStore; skip if `incoming.ts <= stored.ts`.
2. Group rows by op (so we can issue one INSERT-batch, one
   UPDATE-batch, one DELETE-batch per poll).
3. Execute:
   - `Create` / `Read` (snapshot) → INSERT INTO target (UPSERT
     on PK conflict; users can opt out with `on_conflict =
     "fail"`).
   - `Update` → UPDATE target SET … WHERE pk = … (per-row
     parameterised).
   - `Delete` → DELETE WHERE pk = … (or `UPDATE … SET
     <soft_delete_column> = NOW()` if `delete_mode = "soft"`).
4. Tombstones (next record after a `d` with null payload) → skip.
5. Atomic: state-store `last_seen_ts[pk]` updates commit in the
   same transaction as the data writes (matches the existing
   "atomic state + offset commit" guarantee for windows / joins).

### Idempotency — using the existing StateStore

The `StateStore` trait already supports per-key blob storage
with postcard wire format (Phase 39.5a). Phase Δ adds a small
typed payload:

```rust
struct CdcKeyState {
    last_seen_ts_ms: i64,
}
```

…stored under prefix `cdc::<pipeline_name>::<pk>`. Crash-recovery
identical to the windows / joins path: re-load on startup, skip
already-applied events.

### Schema evolution — best-effort first cut

Three policies, configurable via `schema_evolution`:

- `Skip` (default). Detect columns in `after` not present in the
  target schema → warn once per unknown column, omit from the
  applied UPDATE/INSERT. Keeps the pipeline running through
  upstream schema changes without manual intervention; user
  picks up the new column on the next deploy with an updated
  `@ematix.table` definition.
- `Fail`. Raise on first unknown column — strict mode for users
  who want to gate releases on schema sync.
- `AlterTable` *(deferred follow-up)*. Issue `ALTER TABLE
  ... ADD COLUMN ...` against the target. Postgres / DuckDB
  support is straightforward; Delta needs a separate code path
  (DataFusion has the ALTER but Delta's transaction log doesn't
  always); object-store targets fundamentally don't support it.

---

## PR breakdown

Six PRs, scoped so each is independently reviewable + ships
incremental value:

### PR 1 — `CdcConfig` scaffolding (3 days)

- Add `CdcConfig` struct in `ematix-flow-core` with serde
  derives.
- Wire into `BackendConfig::Cdc` variant or as a sub-field of
  the pipeline config (TBD; see open question 1).
- TOML parsing: `[transform.cdc]` block in
  `crates/ematix-flow-cli/src/lib.rs`.
- Python: `CDC(envelope="...")` dataclass in
  `python/ematix_flow/cdc.py`; export from package root;
  `@ematix.streaming_pipeline(cdc=...)` accepts it.
- Round-trip tests in `tests/backend_config_scaffold.rs`.
- No execution path yet — just config plumbing.

### PR 2 — Debezium + Maxwell envelope parsers (1 wk)

- `EnvelopeKind::Debezium` carries the canonical field map
  (`op` / `before` / `after` / `source.ts_ms` / `after.<pk>`).
- `EnvelopeKind::Maxwell` similar
  (`type` / `data` / `old` / `ts` / `data.<pk>`).
- `EnvelopeKind::Custom` uses the explicit field paths from the
  config.
- Unit tests: feed canonical Debezium / Maxwell sample payloads
  through the parser, assert correct op + before/after/key/ts
  extraction.

### PR 3 — `run_cdc` strategy executor (1 wk)

- `Backend::run_cdc` trait method (default impl: `NotImplementedYet`).
- Concrete impl on `PostgresBackend` first — single-DB,
  exercises the full per-op dispatch.
- Tombstone handling.
- One integration test: small Kafka topic, Debezium-shaped
  payloads, Postgres target → assert inserts, updates, deletes
  apply correctly.

### PR 4 — idempotency gate (3–4 days) — **shipped**

Per-PK last-seen-`ts_ms` admission gate, atomic with the data
write. Implementation diverged from the original "via StateStore"
sketch — the existing `StateStore::commit` opens its own
transaction internally, so it can't share a Postgres transaction
with the data writes the gate is supposed to be atomic with. CDC
state is also a different shape from windowed/session/join state
(a single `i64` per key, not an arbitrary postcard blob), so
re-using the durable layer would have meant trait surgery + a
weaker correctness story. Landed shape:

- New table `ematix_flow.cdc_idempotency (pipeline_name, pk_json,
  last_seen_ts_ms)` lazy-created via `ensure_meta_schema()`.
- Single-round-trip gate: `INSERT … ON CONFLICT DO UPDATE …
  WHERE existing.last_seen_ts_ms < EXCLUDED.last_seen_ts_ms
  RETURNING 1`. RETURNING-clause non-emptiness is the admission
  verdict — empty result = redelivery, skip the data write.
- Gate writes + data writes share the executor's per-batch
  Postgres transaction → atomic across the entire batch. A crash
  at any point leaves gate + target consistent.
- In-batch cache (`HashMap<canonical_pk, last_admitted_ts>`) so
  multiple events for the same PK in the same batch don't
  re-hit the gate.
- Events with `ts_ms = None` bypass the gate (documented
  no-idempotency mode — user opted out by not configuring a
  `ts_field`).
- New `CdcRunResult.idempotent_skipped: i64` counter so
  redeliveries are visible in metrics, not silent.

Tests landed (testcontainers, `--ignored`):
- `cdc_postgres_redelivery_is_idempotent` — apply same 3-event
  batch twice → second run reports `idempotent_skipped = 3`.
- `cdc_postgres_idempotency_survives_pool_restart` — apply,
  drop the entire pool, reconnect, replay → second apply blocks
  on durable on-disk state.
- `cdc_postgres_idempotency_keyed_per_pipeline` — same PK +
  ts_ms under two pipeline names is two distinct gate entries.

Deferred to PR 5 + later: cross-target idempotency (Delta MERGE
is naturally idempotent on equal post-images; the per-PK ts gate
is a Postgres-only gain today). Out-of-order tolerance window
(`out_of_order_tolerance_ms`) is configured + accepted but not
yet wired — PR 4's gate is strict-monotonic; PR 5 will add the
warn-on-backwards path.

### PR 5 — schema-evolution detection (3 days) — **shipped**

Drift check between the idempotency gate and the data dispatch:
walk the `after` payload's keys against the target's declared
columns, dispatch on `SchemaEvolutionPolicy`. Postgres's
`jsonb_populate_record` already discards unknown JSON keys, so
`Skip`'s "omit and apply" half is free — what PR 5 adds is the
warn-once log line so silent column drops are visible to
operators.

- `SchemaEvolutionPolicy::Skip` (default): `tracing::warn!` once
  per (column, batch) pair under `target = "ematix_flow::cdc"`,
  with `pipeline` + `column` structured fields. Row applies; the
  unknown column is dropped by Postgres's coercion path.
- `SchemaEvolutionPolicy::Fail`: returns `PgError::Other` with a
  message naming the offending column + the policy + a hint
  ("add the column to the target table or switch to Skip"). The
  outer transaction is rolled back via `Drop`, so any earlier
  events in the same batch are not persisted — strict mode is
  truly all-or-nothing.
- The per-batch HashSet of warned-columns means a batch with 1000
  events under one drift column produces one warn line, not
  1000. Across batches the warn fires once per batch (acceptable
  signal-to-noise; can refine to per-pipeline-lifetime if real-
  world log volume becomes a problem).
- `AlterTable` policy still deferred. Per-target ALTER plumbing
  varies enough by backend that bundling it into PR 5 would have
  doubled the surface — Δ.X1 (Delta) doesn't even use
  `ALTER TABLE` syntax.

Tests landed (testcontainers, `--ignored`):
- `cdc_postgres_schema_skip_keeps_pipeline_running` — `phone`
  column in `after` not on target → row applies, only declared
  columns persisted.
- `cdc_postgres_schema_fail_aborts_batch` — first event clean,
  second event drifts; whole batch rolled back, target stays
  empty, error message names `phone`.

### PR 6 — docs + Debezium-via-testcontainers example (3–4 days)

- `examples/cdc-debezium/`: docker-compose with Postgres source
  + Kafka + Schema Registry + Debezium connector + ematix-flow
  consumer + Postgres target. End-to-end: write to source PG,
  observe rows propagate to target PG.
- `docs/USER_GUIDE.md` section: "CDC source mode (Δ)".
- README mention in the "What's in it" section.
- CHANGELOG entry under `[Unreleased]`.

---

## Acceptance criteria

- Single-pipeline Debezium → Postgres with all four ops
  (`c` / `u` / `d` / `r`) applies correctly under Kafka redelivery.
- Tombstone records skipped, no errors.
- Soft-delete mode routes `d` to a column flip without DELETE
  firing.
- Custom envelope works against a Maxwell-style payload (proves
  the parser is generic).
- Crash mid-batch → restart → no double-apply, no missed event.
- Schema evolution (`Skip` policy): new column in `after` →
  warning, no row drops.
- Both decorator and TOML form parse + execute identically.

---

## Open questions (lock during PR 1)

1. **`BackendConfig` placement.** Does `CdcConfig` live as a
   field on the streaming pipeline config (alongside
   `transform.window` / `transform.join`), or as a new
   `BackendConfig::Cdc` variant? **Lean: pipeline-level field**,
   following the existing transform-block pattern. CDC is a
   *mode* of applying changes, not a different backend.
2. **Multi-table topic shape.** Debezium can publish all tables
   from a server to one topic with the table name in the
   payload. Initial scope: one pipeline per table (matches the
   default Debezium-per-table topic config). Multi-table
   demuxing is a follow-up.
3. **Out-of-order events.** Kafka guarantees per-partition
   order, and Debezium partitions by PK by default — so events
   for a given key arrive in order. We document this assumption
   + add a `tracing::warn!` if a per-key ts goes backwards by
   more than `out_of_order_tolerance_ms` (configurable; default
   `5000`).
4. ~~**Cross-DB `run_cdc`.**~~ **Resolved during planning.** PR 3
   lands Postgres-target only. Other targets follow per-backend;
   per-target effort + design tradeoffs documented in the
   "Phase Δ extensions" section below (Delta is highest-leverage
   next; SQL targets are easy ports; object stores are different
   problem shape).

---

## Out of scope / deferred

- **Outbound CDC** — emitting change events *from* an
  ematix-flow target. Different problem; not in Phase Δ.
- **`AlterTable` schema-evolution policy** — needs per-target
  ALTER plumbing; lands as a follow-up if/when demand surfaces.
- **Multi-table demux from one Kafka topic.** Debezium-per-table
  topics are the default; multi-table topic support waits for a
  user with that constraint.
- **Snapshot-vs-streaming cutover.** The `r` (read snapshot) op
  is treated identically to `c` (insert) on first cut — same
  shape, the snapshot just happens to predate streaming. A
  user-controlled cutover (start streaming only after snapshot
  completes) would need offset coordination with Debezium's
  snapshot phase; deferred.
- **Filtering at the CDC layer** (e.g. "only mirror updates,
  drop deletes"). Today's mid-stream SQL transform can do this
  cleanly via `WHERE op != 'd'` on the post-CDC envelope; no
  separate filter knob needed in `CdcConfig`.

---

## Phase Δ extensions — multi-target follow-ups

PR 3 ships Postgres-only. The trait method [`Backend::run_cdc`]
defaults to a clear "not yet implemented for this dialect" error
on every other backend, so existing pipelines don't break — they
just can't use `mode = "cdc"` against non-Postgres targets until
the corresponding extension lands.

This section catalogues the per-target follow-ups, ordered by
leverage. Each is a separate PR; none are blocked on each other.
The 6-PR core plan above completes Phase Δ for the Postgres
target; everything below is the multi-target build-out.

### Δ.X1 — Delta Lake target — **shipped (PR 1)**

Single-MERGE-per-batch CDC apply on `DeltaBackend`. Real-world
verified: 4 unit tests against tempdir-rooted Delta tables cover
multi-op batches, within-batch dedupe, soft-delete, and the Fail
schema-evolution policy.

**Shape that landed.** One `deltalake::DeltaOps::merge` call per
batch with three clauses, registered in this order so the
delete branch wins on `__op = 'd'`:

```rust
table.merge(df, "target.<pk> = source.<pk>")
    .with_source_alias("source")
    .with_target_alias("target")
    .with_merge_schema(skip_policy)             // Skip → auto-evolve
    .when_matched_delete(|d| d.predicate("source.__op = 'd'"))?
    .when_matched_update(|u| u.predicate("source.__op != 'd'")
        .update("col1", "source.col1")...)?     // c / u / r overwrite
    .when_not_matched_insert(|i| i.predicate("source.__op != 'd'")
        .set("col1", "source.col1")...)?        // c / u / r insert
```

For soft-delete the first clause flips to `when_matched_update`
with `predicate("source.__op = 'd'").update(soft_col,
"current_timestamp()")` and the hard-delete clause is omitted —
verified via `delta_cdc_soft_delete_flips_column`.

**Source RecordBatch.** Built via Arrow's NDJSON reader against
an explicit schema (every spec column made nullable + a non-null
`__op` Utf8 column). For c/u/r events the row pulls from
`event.after`; for d events the row carries only the PK from
`event.key` and nulls elsewhere. NULL columns on delete rows are
never persisted — `when_matched_delete` consumes them and the
NOT MATCHED INSERT branch is gated by `__op != 'd'`.

**Within-batch ordering.** Pre-MERGE dedupe by primary key,
keeping the event with the highest `ts_ms`. A `c` followed by a
`u` for the same PK in one batch collapses to the post-image of
the `u`. Test: `delta_cdc_within_batch_dedupes_by_newest_ts`.

**Schema evolution.** `Skip` → `with_merge_schema(true)`; Delta
auto-evolves (cleaner than Postgres's "warn + drop"). `Fail` →
pre-flight column comparison against the spec, returns an error
that names the offending column. Tests: `delta_cdc_schema_fail_aborts`.

**Between-batch idempotency — the residual gap.** PR 1 relies on
MERGE's natural idempotency on equal post-images:

- INSERT/UPDATE with equal post-image: row content unchanged → no-op.
- DELETE of an absent row: when_matched_delete doesn't fire → no-op.

The dangerous case PR 1 doesn't cover: an *older* event in a
*later* batch clobbering newer state. Requires either a per-row
`_cdc_last_ts` hidden column on the target (invasive — changes
the user's mirror schema) or a sidecar Delta table (two MERGEs,
not atomic). Tracked as **Δ.X1.1**. Non-issue when the source's
per-PK ordering is preserved — Kafka with Debezium's default
partition-by-PK gives this for free.

**Streaming-runtime dispatch — fully wired (Δ.X1.2 shipped).**
`Backend::run_cdc` and `Backend::reflect_table_spec` both work
end-to-end via `flow consume`. Δ.X1.2 added a side channel for
PK info instead of going through Delta's metadata JSON:

- New `[target.table].primary_key = ["id"]` TOML field on
  `TableSpecConfig`. Parses on every backend with a `table` —
  Postgres, MySQL, SQLite, DuckDB, DeltaLocal, DeltaS3.
- `PipelineCliConfig::target_primary_keys()` returns one PK list
  per target (parallel to `targets()`); the streaming-config
  lowering threads it into the new
  `StreamingPipelineConfig.target_primary_keys` field.
- `StreamingPipeline::ensure_cdc_target_specs` augments the
  reflected spec by flipping `primary_key = true` on columns
  named in the user's declaration. A typo'd column name fails
  loud — the augmentation step validates declared columns
  against the reflected schema and returns a clear error
  pointing at the TOML / decorator field.
- Python: new `Target(primary_key=[...])` field on the streaming
  dataclass + new `target_primary_key=[...]` kwarg on
  `run_streaming_pipeline` + render emitter writes the PK list
  into `[target.table].primary_key`. Both single- and multi-
  target paths covered.

Postgres CDC is unaffected: `information_schema` already
surfaces PK info, so `target_primary_keys` is empty by default
and the reflection path Just Works. Users can still declare PKs
in TOML for documentation purposes; the augmentation step
matches existing PK flags rather than overriding them.

**Coverage that landed.**
- `delta_cdc_inserts_updates_deletes_across_batches` — c/u/d each
  observable via the per-clause MergeMetrics counters.
- `delta_cdc_within_batch_dedupes_by_newest_ts` — three sequential
  ops on the same PK collapse to the highest-ts post-image.
- `delta_cdc_soft_delete_flips_column` — soft-delete flips the
  configured column instead of removing the row; reported as
  `updates`, not `deletes`.
- `delta_cdc_schema_fail_aborts` — Fail policy errors on first
  unknown `after`-payload key with the column name in the message.

**Numeric column types.** `ColumnType::Numeric { .. }` is
deferred — the source-batch path needs a decimal-aware Arrow
encoder beyond what's wired today. Workaround: model decimals as
`Double` or `Text` on the mirror schema. Documented in the error
the executor returns when Numeric columns appear.

### Δ.X2 — DuckDB / SQLite / MySQL targets — **shipped**

| Target | PR | Type-coercion primitive |
|---|---|---|
| DuckDB | #16 | `SELECT s.* FROM (SELECT from_json(?, '<spec>') AS s)` (subquery wrapping; DuckDB's parser refuses `(from_json(...)).*` directly before `ON CONFLICT`) |
| SQLite | #17 | `json_extract(?1, '$.col')` per column with the `?1` indexed-parameter form so a single JSON bind serves every reference |
| MySQL  | #18 | `IF(JSON_TYPE(JSON_EXTRACT(:json, '$.col')) = 'NULL', NULL, JSON_UNQUOTE(JSON_EXTRACT(:json, '$.col')))` per column — JSON-null guard keeps NULL semantics clean across nullable text + numeric columns |

The per-backend executors all share the PG layout: lazy-bootstrap
`<meta>.cdc_idempotency`, single-PK pre-flight, schema-evolution
`Fail` pre-check, transactional per-event apply with an in-batch
gate cache, and soft-delete dispatched through `UPDATE` with the
affected rows accounting under `updates`. The MySQL gate diverges
from the PG / DuckDB / SQLite shape in one place: no `RETURNING`,
so the gate reads `affected_rows()` after a conditional
`ON DUPLICATE KEY UPDATE` and treats `0` as reject.

The original plan below predicted a per-column type-dispatch
table; in practice the JSON-extract primitives above handled
every column type cleanly via implicit casts — the dispatch table
was never needed. Kept here for historical reference.

Same architectural shape as PostgresBackend's `run_cdc` — single
JSON parameter per event, dialect-specific UPSERT / UPDATE /
DELETE templates. The only differences:

| Target | UPSERT syntax | JSON-to-row helper | Notes |
|---|---|---|---|
| **DuckDB** | `INSERT … ON CONFLICT (pk) DO UPDATE SET …` (identical to Postgres) | `json_extract` per column, or `STRUCT_PACK` | No `jsonb_populate_record` analog — bind per-column. |
| **SQLite** | `INSERT … ON CONFLICT(pk) DO UPDATE SET …` (3.24+) | `json_extract` per column | Single-writer transactions; no concurrent CDC pipelines per file. |
| **MySQL** | `INSERT … ON DUPLICATE KEY UPDATE …` | `JSON_EXTRACT` per column, or `JSON_TABLE` | Different keyword, same semantics. PK type strictness is stricter than Postgres. |

Each port is ~1-2 days: lift the SQL templates, swap the
identifier-quoting helper, swap the JSON extraction primitive.
Same `Backend::run_cdc` trait method, three new concrete impls.
Tests follow the same `tests/integration_*.rs` testcontainers
pattern that Postgres uses.

**Per-column type dispatch.** Without `jsonb_populate_record` we
hand-roll a column-type → Rust-type binding table:

| Column type | Rust binding |
|---|---|
| `BIGINT` / `INTEGER` | `i64` |
| `TEXT` / `VARCHAR` | `&str` |
| `BOOLEAN` | `bool` |
| `TIMESTAMPTZ` | `chrono::DateTime<Utc>` |
| `NUMERIC` | `rust_decimal::Decimal` |
| `JSONB` | `serde_json::Value` |
| `BYTEA` | `&[u8]` |

The dispatch table is shared between DuckDB / SQLite / MySQL —
one helper module under `crates/ematix-flow-core/src/cdc/`. ~80
lines, written once, used three times.

### Δ.X3 — Object stores *(different problem shape; recommend deferring)*

Object stores can't UPDATE or DELETE in place — Parquet / CSV /
JSON / ORC files are immutable. Three honest options if a user
wants CDC into object storage:

1. **Append-only event log.** Write each CDC event as a row with
   synthesized `__op`, `__source_ts_ms` columns. Live state
   computed downstream via window-by-PK. Cheap to implement
   (~2 days), pushes complexity to readers + costs full-table
   scan to materialize current state.
2. **Periodic compaction.** Stream events to per-partition
   files, compact on a schedule into a current-state snapshot.
   Adds an orchestration layer (~1 week); fights with object
   store's immutability rather than embracing it.
3. **Defer.** Tell users who want CDC → object store that Delta
   on S3 (Δ.X1 above) is the right answer — Delta sits on
   object stores anyway and gives them transactional MERGE for
   free.

**Recommendation.** Default to (3). Land an "object store
limitation" callout in the docs alongside the Δ.X1 Delta target;
revisit if a user with a concrete (1) or (2) workflow shows up.
The "append-only event log" pattern is easy enough to build
ad-hoc with the existing transform pre-stage + object-store
target without needing first-class CDC support.

### Δ.X4 — Streaming targets *(out of scope; outbound CDC)*

CDC into a Kafka / RabbitMQ / Pub/Sub / Kinesis target = re-emit
change events. That's outbound CDC — different problem entirely;
explicitly out of scope per the "Out of scope / deferred"
section above. If that need surfaces it warrants its own phase
plan (Phase Δ-out / Φ); not a Phase Δ extension.

### Δ.X5 — Distributed batch SQL target *(N/A)*

Σ.B's `DistributedBackend` is a batch SQL executor over peer
ematix-flow workers — not a row-storage target. CDC + distributed
batch don't compose meaningfully. The trait method's default
"not yet implemented" error is the right behaviour.

### Suggested ordering

1. **Δ.X1 Delta** — highest user-value next step. CDC → Delta
   is a real analytics pattern; native MERGE is the cleanest fit.
2. **Δ.X2 SQL ports** — pick whichever target a user asks for
   first. Each is ~1-2 days; the per-column dispatch table is
   shared so the second + third are cheaper than the first.
3. **Δ.X3 Object stores** — defer until demand surfaces.
4. **Δ.X4 / Δ.X5** — out of scope; revisit if/when the ground
   shifts.

After Δ.X1 + Δ.X2 land, every SQL target ematix-flow supports +
Delta would be CDC-capable. That covers the realistic set of
"what target do users actually want their CDC events written to"
(Postgres mirrors, Delta lakehouses, occasional MySQL / SQLite /
DuckDB analytics tables). The coverage gap closes ~95% with
roughly two weeks of total effort.

---

## References

- [Debezium event structure](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-events)
- [Maxwell envelope format](https://maxwells-daemon.io/dataformat/)
- `docs/PHASE_39_5_SESSIONS.md` — StateStore design + atomic
  commit pattern that Phase Δ reuses.
- `docs/PHASE_SIGMA_B_TRAIT_SPIKE.md` — connector-trait shape
  (Phase Δ's `run_cdc` follows the same template as the
  existing `run_merge` etc.).
- `docs/UNIFIED_PIPELINE_API.md` — Π-series unified-API patterns
  that the `CDC(...)` typed-Python knob inherits.
