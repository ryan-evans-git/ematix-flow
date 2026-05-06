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

### PR 4 — idempotency via StateStore (3–4 days)

- `cdc::<pipeline>::<pk>` prefix; `CdcKeyState` payload;
  postcard ser/de.
- StateStore reads on first-seen-key (cold-cache), in-memory
  cache for the lifetime of the pipeline run.
- Atomic commit: state writes + data writes in one transaction
  (Postgres) or two-phase (Delta / object-store, follow-up).
- Crash-recovery integration test: kill mid-batch, restart,
  assert no double-apply.

### PR 5 — schema evolution detection (3 days)

- `SchemaEvolutionPolicy::Skip` default — warn once per unknown
  column, omit from applied DDL.
- `Fail` policy for strict deployments.
- Open: `AlterTable` policy — Postgres only on first cut;
  Delta + object-store deferred.
- Tests: introduce a new column in the `after` payload mid-test,
  assert Skip warns + omits, Fail errors out.

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
4. **Cross-DB `run_cdc`.** PR 3 lands Postgres-target only.
   Delta / object-store / DuckDB / SQLite as follow-ups —
   straightforward for DBs that support DELETE / UPDATE; Delta
   uses MERGE; object-stores need a Hive-style overwrite-by-key
   pattern (limited usefulness, may stay deferred).

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
