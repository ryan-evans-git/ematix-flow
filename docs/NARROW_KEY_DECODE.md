# NARROW.DEC — stats-driven narrow decode for downcast join/agg keys

_2026-07-01. Wires ematix-parquet v0.17's `read_column_i64_downcast`
(REV.12 foundation + REV.14 decode-speed-neutral narrowing) into the
flow-side scan. Flag: `EMAT_NARROW_KEY_DECODE` (opt-in, default OFF —
see `docs/EMAT_FLAGS.md`)._

## Problem

KEYS.2 (`EMAT_DOWNCAST_KEYS`, 2026-05-31) already narrows the
*advertised* Arrow type of a stats-proven INT64 join/group key to
`Int32`: `EmatixFastParquetTableProvider` rewrites the schema when the
column name ends in `key` and the file-level min/max prove every value
fits `i32`. That is where the big footprint win lives — DataFusion's
hash-join builds and aggregation hash keys carry 4-byte instead of
8-byte key columns end-to-end.

But the *decode* side never narrowed. `decode_schema()` widens every
narrowed leaf back to `Int64`, every reader family decodes a full
`Vec<i64>` per row group, and `narrow_stream_to_advertised()` pays an
Arrow `cast(Int64 → Int32)` per batch at the stream boundary. Per
SF=100 lineitem RG that is an ~8 MB transient `Vec<i64>` + a full
copy-narrow pass per key column — exactly the "flow's bridge still
decodes full Vec<i64> for key columns" gap. ematix-parquet v0.17 ships
the missing kernel: `read_column_i64_downcast(file, rg, col) ->
NarrowedI64` decodes *directly* into the narrowest stats-proven width
(PLAIN and dict pages alike, no transient `Vec<i64>`), and REV.14 made
that decode speed-neutral vs the wide path.

## Options considered

**(a) Narrow only inside specific consumers (join build / agg hash
keys); bridge keeps emitting Int64.** Rejected as the *primary*
mechanism: the L13 `EmatHashJoiner` gathers keys via `key_as_i64` into
`Vec<i64>` and the RobinHood kernel is i64-monomorphised, so realizing
the win there means genericizing the kernel crate (explicitly deferred
in `ematix-flow-hash-join/src/lib.rs` §"Out of scope") — and it would
cover only the opt-in `EMAT_HASH_JOIN` path, not the stock DataFusion
`HashJoinExec` that actually executes Q09's partsupp 2-key build.
KEYS.2 already realizes the consumer-side win through the schema, for
*every* downstream operator at once.

**(b) Emit narrowed Arrow arrays + planner-inserted casts.** This is
what KEYS.2 does (without explicit planner casts — the schema is
narrowed at the provider, so plans type-check natively; FK↔PK domains
narrow consistently across tables so join key types match). Already
landed; not this change.

**(c) Emit narrowed arrays where the plan provably tolerates them.**
Subsumed by (b): the provider narrows only stats-proven columns, and
the catalog type itself changes, so every plan tolerates it by
construction. The residual risk (SQL that depends on the erased Int64
type) is why `EMAT_DOWNCAST_KEYS` stays opt-in.

**Chosen: complete (b) at the decode layer.** Keep KEYS.2's
choke-point architecture (readers decode to `decode_schema`, one
reconciling cast at the stream boundary) and, behind
`EMAT_NARROW_KEY_DECODE`, let the **eager streaming reader**
(`EmatArrowBatchReader` — the reader that owns multi-RG fact-table
partitions, i.e. the Q09/Q10 key tonnage) decode narrowed leaves
straight to `Int32` via `read_column_i64_downcast`. The boundary cast
stays in place and becomes a per-column no-op for already-narrow
columns, while still reconciling every reader family we did *not*
teach (page-streaming, inline-streaming, whole-RG bridge) — so no
decode path can silently emit the wrong type.

## Where the footprint win is realized

Two layers, two flags:

1. `EMAT_DOWNCAST_KEYS` (KEYS.2, pre-existing): the steady-state win.
   Hash-join build key columns and agg hash keys are 4 B/row instead
   of 8 B/row through the whole plan. Q09's partsupp 2-key build
   (~128 MB DRAM-spill-bound per `docs/PERF_REVIEW_2026_05.md` §
   rejection-re-look: "if we could compress the i64 keys to u32 at
   decoder level the build would fit in 64 MB → near L3") keeps
   16 B/row of pure key payload today; narrowed it keeps 8 B/row.
   Q10's 7-column group-by narrows `c_custkey` the same way.
2. `EMAT_NARROW_KEY_DECODE` (this change): the decode-side win that
   makes (1) transient-free. Removes the per-RG `Vec<i64>`
   materialisation (~8 MB per SF=100 lineitem RG per key column, ×
   concurrent partitions) and the per-batch cast pass, decoding
   directly at 4 B/row on the eager streaming path.

## Mechanism (this slice)

- `ematix_parquet_bridge::decode_column_chunk_i64_downcast_to_i32` —
  façade over `read_column_i64_downcast`, re-widening sub-i32 targets
  (I8/U8/I16/U16 → i32) and check-narrowing the defensive I64/U32
  fallbacks (error, never wrap, if a value can't hold i32 — unreachable
  when the provider's stats gate agreed, but decode must not trust the
  planner).
- `EmatArrowBatchReaderBuilder::with_narrow_i64_leaves(leaves)` — the
  exec passes the file-leaf indices of narrowed keys; `build()`
  rewrites those projected fields Int64→Int32 so the reader emits
  `Int32Array` directly.
- `decode_one_column` / `masked_decode_one_column` self-route: target
  `Int32` + physical `INT64` → downcast decode (dense) /
  `masked_decode_i64` + checked narrow (filtered). Everything else is
  byte-identical to before.
- The RG decode cache (`RgCacheKey` = path/rg/leaf, type-blind) is
  bypassed for narrowed leaves so a narrow-decoded `Int32` column can
  never be served to a wide consumer or vice versa.
- `EmatixFastParquetExec::execute()` derives the narrowed-leaf set from
  the (advertised, decode) schema pair — no constructor churn — and
  only when `EMAT_NARROW_KEY_DECODE` is on.

Observability: `emat_arrow_reader::NARROW_KEY_DECODES` counts narrow
column-chunk decodes (mirrors `TAG_BUILDS`/`RH_BUILDS`).

## What remains (documented, not wired)

- **Page-streaming / inline-streaming readers** still decode wide +
  boundary-cast. They own single-RG dim-table partitions (≤900 k rows)
  — small transients, low value. Teaching them means an
  `Int64→i32` narrowing `ColumnPageStream`; do it only if the A/B says
  the eager-path win is real.
- **Whole-RG bridge path** (`streaming_arrow_reader=false`, non-default)
  likewise stays on decode-wide + cast.
- **L13 `EmatHashJoiner` (`EMAT_HASH_JOIN=1`)** re-widens Int32 keys to
  `Vec<i64>` in `key_as_i64`; a narrow kernel table is the
  "generic-over-K monomorphisation" already filed in the kernel crate.
- **2-key pack**: Q09's partsupp join keys, both u32-narrowable, could
  pack into a single i64 and route through the single-key kernel —
  planner work, separate arc.

## A/B measurement plan

Strict A/B at SF=100 on the shared bench box, via
`scripts/bench/strict_ab.sh`, once per arm:

```
# arm A (baseline)          # arm B (narrowed keys + narrow decode)
<no EMAT_ overrides>        EMAT_DOWNCAST_KEYS=1 EMAT_NARROW_KEY_DECODE=1
```

- Queries: Q09 (partsupp 2-key build, DRAM-spill bound — expect the
  build to move toward cache residency; Q09 is currently noise-class
  ±10-30% vs DuckDB per `docs/BENCHMARKS.md` 2026-06-21, so gate on
  the *variance collapsing*, not just the mean) and Q10 (worst SF=100
  loss ~0.73×; 7-col group-by, `c_custkey`/`o_custkey` narrow).
- Record `flags::dump_active()` per run (both flags must appear in arm
  B's dump); capture `NARROW_KEY_DECODES` > 0 as the fired-proof.
- Secondary guard: 22q geomean must not regress > 1 pp with both flags
  on (the flags stay opt-in either way).
- A separate single-flag A/B (`EMAT_DOWNCAST_KEYS=1` alone vs both)
  attributes the decode-side delta specifically.

## Verification landed with this change

- Bridge oracle tests: downcast decode == wide decode on lineitem
  fixture + synthetic columns with negatives and `i32::MIN`/`i32::MAX`
  edges; checked-narrow error on out-of-range.
- Reader round-trip: narrowed leaf emits `Int32Array` byte-equal to
  the wide path's cast output (dense + masked), non-narrowed columns
  untouched; flag-off control stays `Int64`.
- Provider-level toggle test: `EMAT_NARROW_KEY_DECODE` flips the path
  (counter) with identical query results.
- `ematix_fast_parquet::tests::narrow_key_decode_q09_identity_on_off`:
  TPC-H Q09 (join-heavy) executed once with the narrowed path off and
  once with it on, full result-set equality (FP-tolerant on the profit
  sum). Runs on the synthetic mini fixture in CI (lives in the lib
  tests because the fixture is crate-internal); point `TPCH_DATA_DIR`
  at the SF=1 dataset for the real-data run — verified 2026-07-01 at
  SF=1 (`%green%`, identical results, narrow counter fired). NOT a
  benchmark.

Nulls: TPC-H keys are REQUIRED and the emat scan path is
REQUIRED-only today (`ematix_parquet_bridge` module doc), so a
narrowed key can never carry nulls through this path; null-key join
semantics are covered by the existing `EmatHashJoiner` null tests and
are unaffected by this change.
