# Iceberg backend — deferral status + path forward

**Status:** still deferred (re-checked 2026-05-01).

## What I checked

- `iceberg = "0.9.0"` (released 2026-03-19, ~6 weeks before this
  re-check) still pins arrow-array, arrow-schema, arrow-buffer,
  arrow-cast, arrow-ord, arrow-string, arrow-select,
  arrow-arith, and parquet to **`^57.1`**.
- Workspace `arrow-*` crates are pinned to **58** (Phase 35a)
  to keep the ABI consistent across deltalake / parquet /
  object_store / duckdb / orc-rust / arrow-csv / arrow-json /
  arrow-cast.
- Iceberg release cadence: 0.7 (Oct 2025) → 0.8 (Jan 2026) →
  0.9 (Mar 2026) → 0.10 likely Q3 2026. No explicit
  arrow-58-bump milestone has been announced upstream as of
  this check.

The core blocker (single arrow ABI per process) hasn't moved.

## Why mixing arrows in-process is the wrong call

Cargo will happily resolve two arrow-array versions if we let
it (one for iceberg's tree, one for the workspace). The runtime
isn't broken — each crate uses its own arrow types — but:

  - We'd lose the "convert RecordBatch from PG / Kafka /
    Delta / Parquet without a single re-encoding" win that
    Phase 35a fought for.
  - Every per-row Arrow operation in iceberg-bound code paths
    pays a copy at the boundary (arrow-58 RecordBatch →
    arrow-57 RecordBatch and back).
  - Two arrow versions means two Schema types, two DataType
    enums, two Buffer types — not interchangeable, even when
    they encode the same bytes.

So the binary-compat trick isn't a real escape.

## Three options

### 1. Wait for `iceberg` to ship arrow-58

- Cost: zero.
- Risk: no eta. Watch upstream issue tracker for the bump.
- Re-check trigger: a new iceberg release that picks up
  arrow 58.

### 2. Vendor + bump iceberg ourselves (`crates/iceberg-arrow58/`)

- Clone the iceberg 0.9.0 source as a workspace member, bump
  every `arrow-* = "57"` to `"58"`, fix the API drift.
- Surface the iceberg crate exposes that we'd touch:
  - 138 source files
  - ~373 call sites against arrow / parquet
  - ~52 of those are concrete `arrow_array::Array` /
    `RecordBatch` / `arrow_schema::DataType` / `Field` —
    the API surface most likely to have changed
  - ~87 call sites against parquet (parquet::file::*,
    parquet::arrow::*)
- arrow 57 → 58 known breaking changes that affect this
  surface:
  - `RecordBatchIterator` schema-ownership signature
  - `ArrowWriter::into_inner` (gained in parquet 58 — used by
    orc-rust 0.8 to drop the `Arc<Mutex<Vec<u8>>>` wrapper;
    iceberg's parquet write path may need similar simplification)
  - Various method renames in `arrow-cast` (most fixable
    mechanically)
- Time estimate: 1-2 days for the bump + tests.
- Ongoing cost: forward-port upstream changes manually
  every iceberg release — typically a few hours per release,
  but bug fixes from upstream don't free-flow.
- Ecosystem cost: our binary distributes our fork. If a user
  hits an upstream fix, we have to backport it.

### 3. Implement a minimal Iceberg-write backend ourselves

- Stop using the iceberg crate. Speak the Iceberg spec
  directly.
- What we'd need:
  - Parquet writer (✓ already in workspace).
  - Manifest builder (Avro-encoded; spec at
    iceberg.apache.org/spec).
  - Manifest-list builder (Avro).
  - Snapshot creator + table-metadata.json updater.
  - Catalog: file-system catalog only for v1 (a
    `metadata.json` file in the table directory is the source
    of truth; `version-hint.txt` points at the latest).
- Read path can be deferred — same-Iceberg-target Spark / Trino
  / Snowflake / Athena already read Iceberg natively, so a
  write-only backend covers most use cases.
- Time estimate: 2-3 weeks for write-only minimal v2 spec.
- Risk: spec compliance. Iceberg v3 is in development; we'd
  need to track it.
- Why not v1 of the spec: v1 doesn't support equality deletes
  or row-level positional deletes; v2 is the practical
  baseline.

## Recommendation: don't fork, don't reimplement

**For the typical "transactional table on object storage" use
case, our existing Delta backend (Phase 35) is feature-equivalent
and already production-ready in the workspace.**

  - Both formats: ACID transactions, time travel, schema
    evolution, partitioning, MERGE.
  - Both ecosystems: Spark / Trino / Snowflake / Athena /
    Dremio read either format natively.
  - Delta has the additional advantage of a Rust-native crate
    (`deltalake-core`) that's tracking arrow 58 directly.

**The case for Iceberg specifically over Delta is ecosystem
lock-in:**

  - Snowflake's Iceberg integration is more mature than its
    Delta one (as of mid-2026).
  - Some legacy data platforms (e.g. some Hadoop-era setups)
    standardized on Iceberg before Delta was viable.
  - A user shop that already runs Iceberg has tooling +
    catalogs + monitoring + pipelines built around it.

If that lock-in is the constraint, the right answer is **wait
for iceberg 0.10+** rather than carry a fork. The cost-vs-value
math:

  - **Wait:** 0 work, ~3 month delay, zero maintenance
    burden.
  - **Fork (option 2):** 1-2 days work, multi-month maintenance
    burden until upstream catches up, fragility against
    Iceberg's evolution.
  - **Reimplement (option 3):** 2-3 weeks work, ongoing
    spec-tracking burden, single-format scope (we'd be
    chasing a moving spec for one backend that has a
    well-supported alternative).

The ratio of marginal value (Iceberg vs Delta for our users) to
marginal cost (any of options 2 or 3) doesn't justify the work
right now.

## When to revisit

- A user explicitly asks for Iceberg compatibility (not just
  "transactional table on S3"). Delta-vs-Iceberg matters when
  the downstream consumer is fixed.
- iceberg 0.10+ ships and the arrow pin is on 58 (likely
  Q3 2026 based on release cadence).
- Snowflake / another major engine drops Delta support but
  keeps Iceberg, making Iceberg a hard requirement for that
  ecosystem.

## What I'd build if forced (option 3 sketch)

If a user need surfaces and waiting isn't acceptable, the
minimum viable Iceberg-write backend looks like:

```rust
pub struct IcebergBackend {
    table_root: Url,             // s3://bucket/path or file:///...
    catalog: IcebergCatalog,     // FsCatalog for v1; RestCatalog later
    object_store: Arc<dyn ObjectStore>,
}

impl Backend for IcebergBackend {
    async fn write_arrow_stream(&self, target, stream, mode) -> Result<u64> {
        // 1. Buffer the Arrow stream into one or more parquet files
        //    written under `<table>/data/...`.
        // 2. Build a manifest Avro file listing those data files
        //    + their stats (bounds, null counts, value counts).
        // 3. Build / extend the manifest list.
        // 4. Allocate the next snapshot id.
        // 5. Write a new `metadata.json` referencing the new
        //    snapshot.
        // 6. Atomic-rename `version-hint.txt` to point at it.
    }

    // Read deferred: we trust downstream Iceberg-aware engines.
}
```

This is ~2-3 weeks of careful spec-following. It's only worth
doing when Delta is genuinely off the table for a real user.

## Decision

**Continue to defer.** Watch for iceberg 0.10. Re-check this
doc when a user asks for Iceberg specifically.

The Delta backend (Phase 35) covers the same use case today.
