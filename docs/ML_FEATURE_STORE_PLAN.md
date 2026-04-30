# ematix-flow — ML / Feature-Store Extension Plan

A planning doc parallel to `docs/IMPLEMENTATION_PLAN.md`. Maps ML feature-store
concepts onto existing ematix-flow primitives, surfaces the optional parameters
each FS use case needs, and proposes a phased extension (Phases 15–20) on top of
the v0.1 core.

This doc is not a commitment — it's a design space. Some pieces (event-time
SCD2, TTL) are clearly worth doing; others (online store, training-dataset
builder) may end up out of scope or done by a sibling library.

---

## 1. What an ML feature store actually does

A feature store provides three things on top of "tables full of derived
columns":

1. **Point-in-time correct training data.** Given a spine of
   `(entity_id, prediction_timestamp)` pairs, produce a wide table where each
   feature column reflects what was *known* at the prediction timestamp — not
   what the feature is *now*. Time-travel.
2. **Low-latency current lookups for inference.** Given an entity_id, return
   the latest feature values fast enough for a request-time inference call.
3. **Lifecycle management.** Versioning, TTL/expiry, freshness SLAs, lineage,
   backfills, and retraining-time reproducibility.

Almost every concept here maps to something we already have.

---

## 2. Mapping FS needs onto current ematix-flow

| Feature-store concern | ematix-flow primitive | Gap? |
|---|---|---|
| Compute features from raw data | `pipeline.sync(target, source, mode=...)` | No — already supports append/merge/scd2 |
| Time-travel history | SCD2 (`valid_from`/`valid_to`/`is_current`/`row_hash`) | No — Phase 8 |
| Idempotent re-computation | merge/scd2 idempotency | No — Phase 7/8 |
| Incremental backfills | watermarks via `incremental_column` | No — Phase 10 |
| Schema evolution detection | `compare_table` drift | No — Phase 4 |
| Cross-DB (read OLTP, write feature DB) | cross-DB COPY binary path | No — Phase 5+ |
| Run observability | `ematix_flow.run_history` | Partial — see §6.2 |
| Entity-key lookups | natural-key PK on target | Mostly |
| Soft deletes / tombstones | `handle_deletes='soft'` (SCD2) | No — Phase 11 |
| Scheduled materialization | `flow run-due` cron + `@register` | No — Phase 12 |
| **Event timestamps distinct from load time** | — | **Yes — §3.1** |
| **Per-feature TTL / expiry** | — | **Yes — §3.2** |
| **Point-in-time query helper** | (raw SQL works) | Convenience — §3.3 |
| **Online lookup store** | (raw SQL works on SCD2) | Performance — §3.4 |
| **Feature-view metadata catalog** | — | **Yes — §3.5** |
| **Training-dataset builder (asof joins)** | — | **Yes — §3.6** |

Roughly 60% of an FS is already shipped. The rest is additive.

---

## 3. Gaps and proposed parameters

### 3.1 Event-time SCD2

Today SCD2 uses `valid_from = now()` — the load timestamp. For features, the
right thing is usually `valid_from = source.event_timestamp` so a feature value
reflects when the underlying event happened, not when ematix-flow ingested it.

**Proposed parameter on `pipeline.sync`:**

```python
event_timestamp_column: str | None = None
```

When set in `mode='scd2'`, `valid_from` is taken from this source column instead
of `now()`. The augment continues to add the same SCD2 metadata; only the
INSERT's `valid_from` value changes. Same semantic when closing out previous
versions: `valid_to = new_row.event_timestamp` rather than `now()`.

**Edge cases that drive the design:**

- Late-arriving data. A new row's event_ts may be older than the current
  version's event_ts. The SCD2 plan must detect and order-correct: insert the
  late row in the right position in the timeline, possibly closing out a
  previous version with a newer event_ts. The UPDATE close-out becomes
  `valid_to = LEAST(valid_to_now, late_event_ts)` rather than just `now()`.
- Out-of-order tied event_ts values. Tiebreaker on a synthetic ordinal
  (`_loaded_at`).

**Out of scope for v0.1:** late-arrival reordering. v0.1's event-time SCD2
should reject or warn on out-of-order arrivals; full late-arrival support is a
later phase.

### 3.2 Per-feature TTL / expiry

Features often have a validity window: "user's last 7-day spend" is
meaningless after 7 days have passed without a fresh compute. After expiry,
the row should either be tombstoned or the column should fall back to a default
in queries.

**Proposed parameter on `pipeline.sync`:**

```python
ttl: timedelta | None = None
```

Implementation options (pick one in Phase 16):

- **Tombstone TTL.** Each sync runs a post-step:
  `UPDATE target SET is_current=false, valid_to=now() WHERE is_current AND valid_from < now() - ttl`.
  Simple; works only for SCD2 mode.
- **Query-time TTL.** Don't mutate target. Provide query helpers that filter
  out expired rows. More flexible but pushes complexity to consumers.

Recommendation: **tombstone TTL** for v0.1 simplicity. Query-time fallbacks can
layer on later.

### 3.3 Point-in-time query helper

The SQL is mechanical:

```sql
SELECT cols FROM feature_view
WHERE entity_id = $entity_id
  AND valid_from <= $event_ts
  AND (valid_to IS NULL OR valid_to > $event_ts)
ORDER BY valid_from DESC
LIMIT 1;
```

**Proposed API:**

```python
class UserFeatures(FeatureView):
    __schema__ = "features"
    __tablename__ = "user_features_v1"
    user_id = Column(BigInt(), primary_key=True)
    total_spend = Column(Numeric(12, 2))

# inference-time historical lookup
row = UserFeatures.point_in_time(conn, entity_keys={"user_id": 100}, as_of=ts)

# training-time bulk lookup
df = UserFeatures.historical_features(
    conn,
    spine=[("user_id", "event_ts"), ...],  # entity-spine table or list
)
```

These are pure-SQL helpers that build the right query. Phase 18.

### 3.4 Online (current-snapshot) store

For request-time inference, scanning SCD2 history is overkill. With a partial
index `WHERE is_current` (Phase 8 deferred this — should land here) and a
covering index on `(entity_keys) WHERE is_current`, lookups are already fast
on Postgres alone. Two options for the "online" abstraction:

- **Materialized view** `WHERE is_current`, refreshed at the end of each sync.
- **Separate "current" table** maintained by triggers or post-step DML.

For Phase 19 v0.1: the partial-index path is enough. A `MATERIALIZED VIEW` is
a one-line DDL we can emit; `online=True` on `FeatureView` opt-ins. Fancy
sub-second-fresh online store via Redis / a dedicated kv layer is post-v0.1.

### 3.5 Feature-view metadata catalog

A new lazy table `ematix_flow.feature_views` mirroring how `run_history` works:

```sql
CREATE TABLE ematix_flow.feature_views (
    name TEXT PRIMARY KEY,                    -- {schema}.{table}.{version}
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    feature_version TEXT NOT NULL,            -- 'v1', 'v2', ...
    entity_keys TEXT[] NOT NULL,
    event_timestamp_column TEXT,              -- nullable: only set if event-time SCD2
    ttl_seconds BIGINT,                       -- nullable
    description TEXT,
    owner TEXT,
    freshness_sla_seconds BIGINT,             -- alert if no successful sync in this window
    last_synced_at TIMESTAMPTZ,
    fingerprint TEXT NOT NULL,                -- TableSpec fingerprint at register-time
    registered_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
```

Populated when a `FeatureView` subclass is imported and registered. Updated
after each successful sync.

### 3.6 Training-dataset builder

Given a "spine" of `(entity_keys, prediction_timestamp)` pairs and a list of
feature views to enrich with, produce a wide DataFrame.

The SQL pattern is `LEFT JOIN LATERAL ... ORDER BY ... LIMIT 1` per feature
view per row. For Postgres this is the standard "asof join" idiom.

Likely v0.2 — depends on whether the API surface is a Python helper that
returns a `pandas.DataFrame` (adds a numpy/pandas dep), a SQL string the user
runs themselves, or a table-creation step.

---

## 4. Proposed `FeatureView` shape

Sketch:

```python
class UserFeatures(FeatureView):
    __schema__ = "features"
    __tablename__ = "user_features"
    __feature_version__ = "v1"

    # entity keys = primary_key columns
    user_id = Column(BigInt(), nullable=False, primary_key=True)

    # feature columns
    total_purchases = Column(BigInt())
    avg_order_value = Column(Numeric(10, 2))
    last_purchase_at = Column(TimestampTZ())

    # FS-specific knobs (all optional)
    __event_timestamp_column__ = "last_purchase_at"
    __ttl__ = timedelta(days=30)
    __freshness_sla__ = timedelta(hours=4)
    __online__ = True            # maintain WHERE is_current materialized view
    __description__ = "Per-user purchase aggregates"
    __owner__ = "growth-ml@example.com"
```

Where `FeatureView` extends `ManagedTable` and adds:

- An `__init_subclass__` that:
  - Validates the FS dunders.
  - Registers the view in a process-local registry.
  - Defaults `mode='scd2'` (point-in-time correct by default).
- Class methods: `point_in_time(...)`, `historical_features(...)`,
  `online_features(...)`, `freshness(...)`.

Backwards-compat: a plain `ManagedTable` keeps working. `FeatureView` is purely
additive.

---

## 5. Where new parameters live

Three places gain optional kwargs:

1. **`ManagedTable` dunders** (declared on the class):
   - `__event_timestamp_column__`
   - `__ttl__`
   - `__feature_version__`
   - `__online__`
   - `__freshness_sla__`
   - `__description__`, `__owner__` (metadata-only)

2. **`pipeline.sync(...)` kwargs** (passed at sync time):
   - `event_timestamp_column: str | None`
   - `ttl: timedelta | None`
   - These override class-level defaults so the same target can sync with
     different windows for backfill vs steady-state.

3. **Future `pipeline.sync(...)` kwargs (later phases):**
   - `feature_set: tuple[str, ...]` — restrict which feature columns this
     sync touches (partial materialization).
   - `backfill_window: tuple[datetime, datetime]` — range to backfill;
     enforces `event_timestamp_column` between the bounds.

Validation rules across kwargs:

- `event_timestamp_column` is only meaningful with `mode='scd2'` (and
  optionally `mode='merge'` for last-write-wins by event time).
- `ttl` requires `mode='scd2'`.
- `online=True` requires `mode='scd2'` (current snapshot needs `is_current`).
- `feature_set` is mutually exclusive with `update_columns` (overlapping
  conventions).

---

## 6. Phased rollout

Each phase ends with green tests + a real Postgres demo. Same TDD discipline as
Phases 0–13.

### Phase 15 — Event-time SCD2 (≈1d)

- Plumb `event_timestamp_column` through `pipeline.sync` → `Connection.run_scd2`.
- `plan_scd2` accepts an optional event-time column expression; falls back to
  `now()` when absent.
- Reject out-of-order arrivals with a clear error (full reorder support is
  later).
- Tests: load an event with explicit ts; close-out uses ts; idempotency under
  same source state.

### Phase 16 — TTL / expiry (≈0.5d)

- `ttl` kwarg on `pipeline.sync` → optional post-step UPDATE that closes out
  current versions older than `ttl`.
- Same atomicity guarantees as `handle_deletes='soft'` — runs in the load tx.
- Tests: row inserted at T-10d with ttl=7d → closed on next sync at T+0.

### Phase 17 — `FeatureView` declarative class (≈1–2d)

- `FeatureView(ManagedTable)` base with the dunders listed in §4.
- Process-local registry of feature views.
- Lazy-create `ematix_flow.feature_views` and UPSERT a row per registration.
- `pipeline.sync` short-circuits to set `mode='scd2'` and read FS dunders when
  the target is a `FeatureView`, unless overridden.
- Tests: declare a FV; spec round-trips; metadata table populated; default
  mode is scd2.

### Phase 18 — Point-in-time query API (≈1–2d)

- `UserFeatures.point_in_time(conn, entity_keys, as_of_timestamp)` returns a
  single row dict.
- `UserFeatures.historical_features(conn, spine_table, feature_columns)`
  returns a `Cursor` (rows iterator) — no pandas dep at this stage.
- Tests: PIT query against a 3-version SCD2 history returns the right
  version for each `as_of`.

### Phase 19 — Online (`is_current`) snapshot (≈1d)

- `__online__ = True` triggers a `CREATE MATERIALIZED VIEW IF NOT EXISTS
  schema.tablename__online AS SELECT * FROM schema.tablename WHERE
  is_current` at ensure time.
- Plus a partial index `ON schema.tablename (entity_keys...) WHERE is_current`
  (the Phase 8 deferred index — finally lands here).
- Refresh at end of sync (`REFRESH MATERIALIZED VIEW CONCURRENTLY ...`).
- `UserFeatures.online_features(conn, entity_keys)` reads the MV.
- Tests: MV stays in sync after multi-sync workflows; lookup latency check.

### Phase 20 — Training-dataset builder + freshness monitoring (≈1–2d)

- `historical_features(spine: pyarrow.Table | list[dict] | sql_table_name)`
  builds the asof-join SQL, runs it, returns Arrow.
- `flow features status --module my_features` lists all FVs and freshness
  status (last_synced_at vs SLA).
- Tests: spine of 100 entity-ts pairs across 3 feature views produces the
  correct wide table; freshness command reports stale/healthy correctly.

---

## 7. Open questions

- **Naming.** `FeatureView` (Feast vibe), `FeatureGroup` (SageMaker vibe), or
  `FeatureTable` (Tecton vibe)? Recommend `FeatureView` — most widely
  understood.
- **DataFrame return type.** `pyarrow.Table`? `pandas.DataFrame`? Optional?
  Pyarrow is the lighter dep; pandas integration is a soft dep.
- **Where do features live physically?** A separate `features` schema, the
  user's `__schema__`, or auto-namespaced as `features_v1`/`features_v2`? Keep
  it user-driven via `__schema__`.
- **Online store backend.** Postgres MV is fine for v0.1. Redis / DynamoDB
  adapters via a separate `ematix-flow-online-redis` package later.
- **Feature lineage.** Track which sources flowed into each feature column?
  Probably out of scope until requested.
- **Drift monitoring.** Value-distribution drift detection on feature columns
  is a different product (often Evidently / WhyLabs); ematix-flow stays
  focused on plumbing.

---

## 8. Out of scope (for now)

- Streaming feature updates (Kafka → online store). v0.1 is batch + cron.
- Embedding stores (vector indexes). pgvector layered on top would work, but
  not a core concern.
- Full retraining-time reproducibility (snapshots of feature definitions at
  training time). Possibly via the existing `fingerprint` mechanism + a
  `feature_view_versions` table later.

---

## 9. Summary

The core insight: **SCD2 already gives us the hard part of a feature store**
(point-in-time correctness). Phases 15–18 add the small ergonomic pieces
(event-time, TTL, declarative FV class, query helpers). Phases 19–20 are
performance + UX polish that can land independently.

No part of this requires throwing away the v0.1 architecture. It's all
additive optional parameters on top of the existing target/source/sync model.
