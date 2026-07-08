# Sidecar indexes in ematix-flow — design & plan

Status: **draft / pre-implementation.** Branch: `feat/sidecar-indexes`.

## What this is

`ematix-parquet` ships **sidecar indexes**: a `.parquet.idx` file built next
to any existing `.parquet` that lets a reader skip-decompress whole row groups
and masked-decode only the rows matching a predicate — Postgres-style indexing
on Parquet **without rewriting the source file**. Four index types:

| Index | Source types | Query |
|---|---|---|
| Sorted | `INT32` / `INT64` / `BYTE_ARRAY` | eq + range |
| Per-page Bloom | `INT64` | eq |
| Composite leading-prefix | `INT64 × INT64` | eq + prefix |
| Inverted text | `BYTE_ARRAY` | token match |

Reported at 26–40× over full scan on selective predicates; the crossover with
full scan is ~60% selectivity (`bench_indexed_lookup`).

This feature adds the ematix-flow-side capabilities to **produce** and
**consume** sidecars, plus site documentation and a with/without-index
benchmark.

## Does it require Iceberg? — No.

The core sidecar is **per-file** (`<source>.parquet.idx` next to `<source>.parquet`)
and works on any list of Parquet files (local FS, S3, GCS). Iceberg is **not**
required.

`ematix-iceberg` is a separate, **feature-gated** (`--features iceberg`, off by
default) crate that *lifts* the per-file sidecar to multi-file tables: it stamps
a per-index `(min_key, max_key)` summary + relative sidecar path into each
Iceberg `data_file` manifest entry, so a query prunes at the manifest level
(`O(1)` per file, no I/O) *before* opening any sidecar. That only matters at
million-file-table scale. **Decision: ship the per-file path first (no Iceberg);
treat Iceberg as an optional later phase.**

## Dependency status — buildable today

flow-core already depends on `ematix-parquet-codec = "0.17"`. The index API
(`ematix_parquet_codec::index::{IndexBuilder, ParquetIndex, Tokenizer}`) is in
the published 0.17 (`pub mod index` on `release/v0.17.0`). No ematix-parquet
release is needed for phases 1–3.

**Iceberg is also buildable today** (scouted 2026-07-07, verdict revised — it is
*not* "planned"): `ematix-iceberg` v0.17.0 is a published, feature-gated
(`--features iceberg`, off by default) workspace crate wrapping iceberg-rust 0.6.
It ships and end-to-end tests the full read path — `collect_data_files(table)`
(async), `prune_data_files_eq` / `prune_data_files_range` (conservative,
never a false-negative skip), `pair_with_extensions`, `resolve_sidecar_uri` —
and the write path (`attach_extension`, `encode_key_metadata` stamp the
per-index `(min_key, max_key)` summary + relative sidecar path into
`DataFile.key_metadata`). The **only** missing piece is an *async*
`ParquetIndex` opener for S3 sidecars; MVP workaround is sync
`ParquetIndex::open` under `spawn_blocking` on `file://`-stripped paths.

## Integration points (from `ematix-parquet/docs/ematix-flow-integration.md`)

- **Read (Pattern 1):** the parquet scan path opens a file, checks for a sidecar
  at `<source>.parquet.idx`; if present and fresh, `ParquetIndex::open` +
  `read_column_*_where_eq/_range` masked-decodes only matching rows; else full
  scan. `SourceFingerprintMismatch` is **recoverable** — treat as "no sidecar,"
  emit a metric, fall through. Hook point: `EmatixFastParquetTableProvider`
  (`crates/ematix-flow-core/src/ematix_fast_parquet.rs`) — the shipped scan
  provider — pushing an eq/range predicate into an index lookup.
- **Write (Pattern 3):** when flow owns ingest (object-store Parquet target),
  emit a sidecar after each successful Parquet write via `IndexBuilder`, in the
  same atomic-rename pattern as the data file.
- **Planner (selectivity-aware):** use the per-file `IndexSummary` (or column
  stats) to choose index vs scan at the ~60–80% selectivity crossover — a
  cost-based signal the codec itself stays blind to.

## Work breakdown (phased)

### Phase 1 — read-side pushdown (no Iceberg)
- Detect `<source>.parquet.idx` in `EmatixFastParquetTableProvider`; open lazily
  only when the scan carries a supported eq/range predicate on an indexed column.
- Map a DataFusion filter expr (`col = lit` / `col BETWEEN lo AND hi`) to the
  index's `read_column_*_where_*` call; fall back to the existing scan for
  unsupported predicates or missing/stale sidecars.
- Recoverable-staleness handling + a `sidecar_{hit,miss,stale}` metric.
- Tests: oracle test (indexed result == full-scan result) on a fixture with a
  known sidecar; staleness fallback test; unsupported-predicate fallthrough.

### Phase 2 — write-side sidecar emission (opt-in)
- Config surface for "which columns to index" (see Open questions).
- Emit `.parquet.idx` after each object-store Parquet write; atomic-rename;
  drop-sidecar-on-drop-data lifecycle note.
- Tests: written sidecar opens + returns correct rows; index absent when not
  configured.

### Phase 3 — planner selectivity gate
- Thread column stats / `IndexSummary` into a scan-vs-index decision.
- Tests: high-selectivity predicate picks scan; low-selectivity picks index.

### Phase 4 — site documentation
- `ematix.dev` page: "Indexing Parquet with sidecars" — what a sidecar is, the
  four index types, **how to create indexes** (write-side config + the
  `IndexBuilder` API), staleness/lifecycle, and when to use it. Sourced from
  `ematix-parquet/docs/sidecar-indexes.md` + this integration.

### Phase 5 — benchmark with/without index
- Extend the TPC-H (or a point-lookup) harness to run the same selective query
  against a source with and without a sidecar; report the speedup and the
  selectivity crossover. Single-node first; distributed/Iceberg later.

### Phase 6 — Iceberg table-level pruning (IN SCOPE, owner request 2026-07-07)

Promoted from "optional/deferred" to a first-class track: combine manifest-level
file pruning with the per-file sidecar for the SF1000 goal. Buildable today
against `ematix-iceberg` v0.17.0 (see Dependency status). The real gap on the
flow side is that **no multi-file TableProvider exists** — this track builds it,
and makes its file-enumeration step a manifest prune.

- **I.1 — feature scaffold.** Add optional `ematix-iceberg` dep + `iceberg`
  feature to `ematix-flow-core` (off by default; keeps the PyPI wheel lean and
  matches the parquet side). Compile-gate a new `iceberg` module. No behavior yet.
- **I.2 — single-node `IcebergTableProvider` (MVP).** A DataFusion
  `TableProvider` that opens an Iceberg table, `collect_data_files`, prunes by
  the pushed predicate (`prune_data_files_eq/range`), pairs survivors with
  sidecar URIs, and builds an `ExecutionPlan` over the surviving files — reusing
  the existing single-file `EmatixFastParquetExec` + `BridgeFilter` +
  `compute_scan_rg_assignments` machinery (files become transparent RG
  containers). Sidecar row-level lookup layers in the Phase 1 primitive. Sync
  opener under `spawn_blocking` for now.
  - Tests: oracle (Iceberg-pruned result == full-scan result) on a fixture table
    with 3 non-overlapping files; a predicate that prunes to 1 file skips the
    other two (assert via a scan-count metric); no-sidecar file still full-scans.
- **I.3 — distributed manifest prune → fan-out.** Move the prune to the mesh
  **coordinator**: run `collect_data_files` + prune once, emit WorkUnits carrying
  only surviving files (extend `Input::ParquetPartition` with an explicit file
  list, or add `Input::IcebergScan`). This is the compounding win — the mesh
  starts with a strictly smaller task set.
- **I.4 — write-side manifest stamping.** When flow writes an Iceberg table,
  emit a sidecar per data file (Phase 2) and `attach_extension` the summary +
  sidecar path into each `DataFile.key_metadata`.
- **I.5 — async sidecar opener (ematix-parquet ask).** Only true blocker for
  fully-async S3 SF1000: an async `ParquetIndex` opener over `object_store`.
  File as a cross-repo request; the `spawn_blocking` path unblocks everything up
  to this.

Milestone target: prove manifest-prune + sidecar + fan-out on the parted TPC-H
layout (SF100 first, SF1000 once data-gen is affordable).

## Decisions (owner, 2026-07-07)

- **Index creation = BOTH write-time config AND a `flow index build` CLI
  backfill.** The CLI backfilling sidecars onto already-written Parquet is the
  headline "index without rewriting the file" story and the primary thing the
  site docs will show; write-time emission covers flow-owned ingest.
- **Start with Phase 1 (read-side pushdown) now** — it needs no API decision and
  is the half that makes indexes pay off.

## Still-open (revisit at each phase)

1. **Which index type per column** — infer from the column type + a marker arg
   (`index(kind="bloom")`), or always sorted with opt-in bloom/inverted?
2. **Predicate coverage in Phase 1** — eq first, then range. (Start eq.)
3. **Bench target** — reuse the TPC-H harness (which columns?) or a dedicated
   point-lookup dataset that better shows the index's selective-predicate win?
