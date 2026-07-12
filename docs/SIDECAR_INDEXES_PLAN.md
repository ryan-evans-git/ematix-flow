# Sidecar indexes in ematix-flow — design & plan

Status: **in progress — P1/P2/P3/P5 + I.1/I.3/I.4 landed** (see "Status by
phase"). Branch: `feat/sidecar-indexes`.

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

### Phase 3W — provider wiring (SQL path) [designed 2026-07-12]

The lookup primitives (P1) and gate (P3) have no provider callers — SQL
never reaches a sidecar, and run-shard's `Indexed` targets carry
`sidecar_uri` unopened. This phase makes the fast-parquet providers
answer selective predicates via the sidecar:

- **Insertion point**: `EmatixFastParquetTableProvider::
  scan_with_partition_budget` — the multi provider and run-shard's
  IcebergScan targets both delegate per part, so one hook covers local
  SQL, parted tables, and distributed WorkUnits.
- **Eligibility (plan time, per part)**: sidecar file exists AND
  filters contain `col = <int literal>` where `col` names a sorted-i64
  index on the sidecar AND every projected column's type is
  materializable by the codec (`Int64`, `Int32`/`Date32`,
  `Utf8`/`Binary` via `read_column_{i64,i32,byte_array}_where_eq`) AND
  the P3 selectivity gate (footer-stats uniform estimate ≤
  `EMAT_SIDECAR_MAX_SELECTIVITY`, default 0.05) approves. Any miss →
  that part scans normally (mixed unions are fine); `eq` first, range
  variants later (codec API already exists).
- **`SidecarLookupExec`**: 1-partition leaf exec; execute = shared
  open (P3 pattern) → typed per-projected-column
  `read_column_*_where_eq` → one RecordBatch. Lazy v3 lookups make the
  per-column re-probe cheap; a one-hit-set multi-column materializer
  is a codec follow-up ask.
- **Pushdown honesty**: the provider keeps reporting `Inexact` — the
  eq (and any other filters) re-apply above the exec; correctness
  never depends on the index being complete.
- **Off-switch**: `EMAT_SIDECAR_SQL` tri-state, default ON — inert
  wherever no sidecar file exists (all current benches), so shipped
  defaults stay benched defaults.
- **Observability**: the existing `sidecar_{hit,miss,stale,
  skipped_selectivity}` counters fire from the exec/gate; plan
  display shows `SidecarLookupExec(index=<col>, key=<k>)`.

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

## Status by phase (updated 2026-07-08)

Per-phase: what landed, and any deviation from the section below that
specified it.

- **Phase 1 (read-side eq primitive)** — LANDED `014cd21f`
  (`sidecar_index::indexed_i64_eq`). Deviations: eq-only (range deferred, as
  the open questions anticipated); the `sidecar_{hit,miss,stale}` metric
  slipped to P3, where it landed.
- **Phase 2 (write-side + `flow index build` CLI)** — LANDED `5b69bad0`
  (`sidecar_build::build_sorted_sidecar` + CLI). No deviations.
- **Phase 3 (planner selectivity gate)** — LANDED `fec3419d`.
  `sidecar_i64_eq(_opt)` gates index-vs-scan on a uniform-model estimate from
  the indexed column's footer `[min,max]`; `EMAT_SIDECAR_MAX_SELECTIVITY`
  (default 0.05, `docs/EMAT_FLAGS.md`). Also lands the
  `sidecar_{hit,miss,stale,skipped_selectivity}` counters
  (`sidecar_metrics()`). Deviations: threshold default 0.05, far below the
  codec's advertised ~60% crossover — deliberate (crude estimator, asymmetric
  miscall costs), and vindicated by the P5 numbers below. Follow-up inside
  P3 (same branch): the gate shares one source+sidecar open with the lookup
  after the P5 bench showed the open costs ~140 ms at 10M rows.
- **Phase 4 (site documentation)** — NOT STARTED.
- **Phase 5 (with/without-index bench)** — LANDED (this commit):
  `examples/sidecar_bench.rs`, local fixture, no AWS. Deviations: fixture is
  a clustered-duplicate id column (16 rows/id) written at 8 192-row row
  groups — a fully-unique single-page id column (first attempt) is the
  builder's worst case and overflows snappy's 4 GiB buffer cap below 300K
  rows (see Results). Results below; the headline is a **negative** local
  result that re-scopes where the index pays.
- **I.1 (feature scaffold)** — LANDED `c6c92b7a` (`iceberg` feature on
  ematix-flow-core, off by default).
- **I.2 (single-node IcebergTableProvider MVP)** — NOT STARTED. The
  single-node multi-file provider it plans to build on landed independently
  (`076ea8e8`, `ematix_fast_parquet_multi`).
- **I.3 (distributed manifest prune → fan-out)** — LANDED across
  `c6c92b7a` (prune planner), `319c598c` (WorkUnit wire schema), and
  `f2c33ed7` (coordinator lowering `ematix-flow-distributed::iceberg_lower`,
  new off-by-default `iceberg` feature on that crate + worker decode in
  `flow run-shard`). Deviations: worker decode lives in
  `ematix-flow-cli/src/run_shard.rs` (that is where WorkUnits are executed),
  not in ematix-flow-distributed; the wire schema gained additive v1 fields
  (`predicate` now optional, `Query::Sql`) because Iceberg scans are not
  bound to the TPC-H catalog; worker-side per-file sidecar lookup on
  `Indexed` targets is carried but not yet exercised (Phase 3 provider
  wiring is the hook point); execution knobs (dict-preservation/late-mat)
  are not yet threaded through the multi-file provider.
- **I.4 (write-side manifest stamping)** — LANDED `f9dad871`
  (`iceberg_stamp`: `summary_from_footer` / `extension_from_footer` /
  `stamped_data_file`). Deviations: bounds come from the parquet footer
  statistics (self-contained, no sidecar open) rather than from the sidecar;
  round-trip proven via manual `ManifestWriterBuilder` construction (the
  buildable-today path) rather than a full catalog transaction.
- **I.5 (async sidecar opener)** — superseded in spirit by ematix-parquet
  0.17.2's `LazyParquetIndex` (footer-only open + group-pruned lookups);
  the 0.17.3 lazy `i32`/`byte_array` materializers shipped 2026-07-12
  and `sidecar_materializable` widened in lockstep (below).
- **P3W (SQL provider wiring)** — LANDED (this commit, 2026-07-12).
  `sidecar_exec::try_sidecar_lookup` hooks
  `EmatixFastParquetTableProvider::scan_with_partition_budget`, so the
  multi provider and run-shard's IcebergScan targets inherit it per
  part. Covered `col = <int>` predicates plan a `SidecarLookupExec`
  (or an empty relation via footer bounds — the parted range-prune);
  int-eq shapes became sidecar-conditionally pushable
  (`supports_filters_pushdown` claims Inexact only when a sidecar file
  exists, keeping sidecar-less plans byte-identical). Projections
  widened to `Int64`/`Int32`/`Date32`/`Utf8` on ematix-parquet 0.17.3
  (2026-07-12; the index KEY stays i64) — pinned by
  `widened_projections_answer_from_sidecar` against a pure-scan
  oracle. Range predicates remain deferred.

## P5 rerun on the lazy path (2026-07-12, local)

Same fixture/machine as the 2026-07-08 table, ematix-parquet 0.17.2
(`LazyParquetIndex`): scan 5.8–6.2 ms vs index 575–637 ms — **the
scattered-match sweep still loses ~100×**. The lazy open removed the
body-decode cost, so what remains is MATERIALIZATION: uniform-stride
matches touch ~every source page, and masked per-row decode of every
page loses to the vectorized full scan by construction. The index's
honest wins are (a) **clustered matches** — sorted sources where a
key's rows sit in one page/row-group (the SF1000 lineitem shape;
`sidecar_lookup_parted` measures it), and (b) **parted range-prune** —
point lookups on part-contiguous tables answer from ONE part while
every other part proves empty from footer bounds (the
`sql_point_lookup_answers_from_sidecar_across_parts` test pins this
end-to-end through SQL). The P3 gate's conservative default remains
correct for scattered shapes.

## Phase 5 results (2026-07-08, local)

Machine: Apple Silicon dev box (darwin 25.5), local NVMe, warm page cache.
`sidecar_bench` defaults: N=10M rows `(id, val)` i64 pairs, 8 192-row row
groups (one PLAIN page each), clustered-duplicate ids (16 rows/id ≈ 626K
distinct), planted keys per match fraction scattered at uniform stride,
median of 5. Scan = `SELECT val FROM t WHERE id = K` through
`EmatixFastParquetTableProvider`; index = `sidecar_i64_eq_opt` with the P3
gate held open. Both paths oracle-checked identical (count + checksum)
before any number is reported.

uncompressed:

| fraction | matches | scan ms (median) | index ms (median) | speedup | P3 gate @ 0.05 |
| --- | --- | --- | --- | --- | --- |
| 0.0001 | 1000 | 7.22 | 573.79 | 0.0x | index (est 1.60e-6) |
| 0.001 | 10000 | 7.10 | 681.15 | 0.0x | index (est 1.60e-6) |
| 0.01 | 100000 | 6.76 | 687.52 | 0.0x | index (est 1.60e-6) |
| 0.1 | 1000000 | 7.63 | 694.58 | 0.0x | index (est 1.60e-6) |

snappy (`--codec snappy`):

| fraction | matches | scan ms (median) | index ms (median) | speedup | P3 gate @ 0.05 |
| --- | --- | --- | --- | --- | --- |
| 0.0001 | 1000 | 6.39 | 570.34 | 0.0x | index (est 1.60e-6) |
| 0.001 | 10000 | 6.39 | 676.04 | 0.0x | index (est 1.60e-6) |
| 0.01 | 100000 | 6.66 | 681.73 | 0.0x | index (est 1.60e-6) |
| 0.1 | 1000000 | 6.74 | 686.17 | 0.0x | index (est 1.60e-6) |

**Findings (honest negative result):**

1. **No crossover exists in this configuration** — the index path loses at
   every fraction, by ~80-100×. A separate probe split the cost: sidecar
   open ≈ 140 ms + `read_column_i64_where_eq` ≈ 450 ms even for 1 000
   matches on a pre-opened index. The codec's eq lookup appears to decode
   the entire index body (~630K sorted entries + rowset bitmaps) per call,
   so its cost is O(index size), not O(log n + matches).
2. The flow scan baseline is simply brutal locally: 10M rows, 2 columns,
   pushdown, ~6-7 ms — and snappy vs uncompressed barely moves it. The
   codec's advertised 26-40× (`bench_indexed_lookup`) is measured against a
   different baseline/shape; on flow's local read path the advantage
   evaporates.
3. **Consequences:** (a) the P3 gate's conservative default is the right
   call — an over-eager index path is a large regression here; (b) the
   index's realistic wins move to where the scan is expensive (S3-resident
   data, wide tables, cold cache — the SF1000/I.5 territory) or after the
   codec gains partial index-body reads (binary search into the sorted
   body) + an open cache. Both are codec-side asks to file alongside I.5.
4. Builder scaling note: sidecar build wants **clustered duplicates and
   small pages**. A unique-id single-page source allocates one page-sized
   rowset bitmap per row and overflows snappy's 4 GiB cap below 300K rows;
   with the clustered fixture it builds 10M rows in ~1.2 s.

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
