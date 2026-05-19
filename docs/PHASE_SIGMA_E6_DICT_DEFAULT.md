# Σ.E6 — dict-preserved arrival by default + broaden dict-aware ops

**Status:** scoped 2026-05-19. Follows Σ.E3 (substrate + first operator)
and Σ.E5 (Emat provider as the default scan path).

**One-line goal:** make `Dictionary(UInt32, Utf8View)` the default
arrival type for low-cardinality string columns, and extend the
dict-aware operator surface from `COUNT(*)` to the four aggregates
that actually appear in TPC-H plus a single-key probe-side join.

**Why now:** the substrate landed in Σ.E3 (#78 / #79 / #87, ematix-parquet
#34) but is opt-in. The Emat provider is the default scan path as of
v0.3.0, but it still materialises to `Utf8View` — so
`EnableDictGroupCountRule` is dormant on real TPC-H queries. The
2.17× kernel-bench win for `DictGroupCountExec` doesn't reproduce
end-to-end until arrival is fixed. This is the gating fix for the
SF=10 geomean ceiling (currently ~0.92).

## Where we are today

- ematix-parquet 0.12.0 ships `read_column_byte_array_dict_preserved`
  and the masked variant.
- `EmatixFastParquetTableProvider::with_dict_preservation(true)`
  threads the call through and rewrites the schema to
  `Dictionary(UInt32, Utf8View)`.
- `DictFilterExec` + `EnableDictFilterRule` (Σ.E3a) fire when Dict
  arrives — Eq / IN / OR-of-Eq / `LIKE 'prefix%'`.
- `DictGroupCountExec` + `EnableDictGroupCountRule` (Σ.E3b.1) fire on
  `COUNT(*) GROUP BY <single dict col>` over the
  `AggregateExec(Partial+Final)` shape.
- **None of the above fires by default** because the default Emat
  provider still surfaces `Utf8View`.

Verified via `probe_dict_arrival` example, 2026-05-18.

## Scope

**In scope (Σ.E6):**
- Σ.E6a — make dict-preserved arrival the default in
  `EmatixFastParquetTableProvider`, with auto-eligibility per column
  (heuristic on parquet RG stats: distinct-count ≤ N or dict-encoded
  in every RG) and an explicit `with_dict_preservation(false)` escape.
- Σ.E6b — extend `DictGroupCountExec` → `DictHashAggregateExec`
  covering `SUM` (i64/f64), `MIN`/`MAX` (i64/f64), `AVG` (= SUM+COUNT).
  Single dict group key only.
- Σ.E6c — `DictHashJoinExec` probe side. Build side is hash-keyed on
  the join column's `Utf8View`; probe side translates each dict code
  to its build-side hash on first encounter, caches the translation
  vector.

**Out of scope (defer to Σ.E7 / Σ.E4):**
- Multi-key dict group-by (Σ.E3b.3 in the original spec).
- Common-dict join optimisation (when both sides share dict).
- Sort / SortMergeJoin / Window on dict columns.
- NUMA-pinned dict hash tables (Σ.E4c).

**Won't do:**
- Make dict arrival the default on the DataFusion-native scan path.
  Users who choose the default DataFusion reader keep its Utf8View
  behavior; if they want dict-aware, they register the Emat provider
  (which is already what `register_dict_aware_parquet` does).

## Concrete shape

### Σ.E6a — default-on dict arrival in EmatixFastParquetProvider

**Eligibility heuristic (per column at provider construction):**
1. Column type is `BYTE_ARRAY` with Arrow inferred `Utf8` / `Utf8View`.
2. **All** row groups report a dictionary page in their column-chunk
   metadata (`encodings` contains `PLAIN_DICTIONARY` /
   `RLE_DICTIONARY`).
3. Distinct-count estimate (from RG stats `distinct_count` if
   populated, else `min(rg.num_values, dict_page_size_bytes / 8)`) is
   ≤ **65535** across the file. Above that, the U16 dict-key win
   evaporates and Utf8View wins; we surface Utf8View.

If all three hold, the schema field becomes
`Dictionary(UInt32, Utf8View)` and the reader emits dict-preserved
arrays. Otherwise it falls back to Utf8View (today's behavior).

**Escape:** `EmatixFastParquetTableProvider::with_dict_preservation(false)`
forces Utf8View regardless. (The existing
`with_dict_preservation(true)` becomes a no-op for compat, since
eligibility now drives the default.)

**Acceptance:**
- `probe_dict_arrival` shows `Dict(U32,Utf8View)` on
  `l_returnflag` / `l_shipmode` / `l_linestatus` / `o_orderpriority` /
  `p_brand` / `n_name` (low-cardinality string cols across the TPC-H
  schema). High-cardinality cols (`o_comment`, `l_comment`,
  `p_name`) stay Utf8View.
- 22-query parity bench shows no individual query regresses > +5 %
  on SF=1, geomean does not regress.
- `EnableDictFilterRule` + `EnableDictGroupCountRule` fire on
  expected queries: Q01 (group-by returnflag,linestatus), Q12 (filter
  shipmode IN ...), Q13 (the dict cols don't help — sanity check),
  Q16 (group-by p_brand,p_type,p_size — partial: p_brand should
  flip), Q17 (filter p_brand), Q19 (filter shipmode IN +
  shipinstruct =), Q22 (group-by substring of phone — won't help).

### Σ.E6b — `DictHashAggregateExec` SUM / MIN / MAX / AVG

Rename `DictGroupCountExec` → `DictHashAggregateExec`. State extends
from `Vec<u64>` (counts) to per-key accumulator slots typed by the
aggregate:

```rust
enum DictAcc {
    Count(Vec<u64>),
    SumI64(Vec<i128>),       // i128 to avoid overflow on big SF
    SumF64(Vec<f64>),
    MinI64(Vec<i64>),  MaxI64(Vec<i64>),
    MinF64(Vec<f64>),  MaxF64(Vec<f64>),
    // AVG = SumI64+Count or SumF64+Count; emit both, divide at finalize.
}
```

`EnableDictGroupCountRule` widens to
`EnableDictHashAggregateRule` — same matcher (Partial+Final agg over
single dict group key) plus aggregate-function validation
(`sum`, `min`, `max`, `avg`, `count`). Mixed aggregates in one
plan (e.g. Q01 has 8 aggregates) require the matcher to verify
*every* aggregate is supported; if any isn't, pass through.

**Acceptance:**
- Q01 SQL (`SELECT returnflag, linestatus, sum(qty), sum(extprice), avg(qty), avg(extprice), avg(disc), count(*) ...`) runs through `DictHashAggregateExec` end-to-end with bit-equal output vs DataFusion default.
- Q12, Q16 (the dict-col group-by part), Q17, Q19 cover SUM and MIN/MAX paths.
- Synthetic bench shows ≥ 1.8× over the corresponding
  `FilterMultiAggSpec` template at 4-key cardinality (the size of the
  Q01 group set).

### Σ.E6c — `DictHashJoinExec` (probe-side dict)

Standard hash-join layout (build hash table on dim table) with one
twist: when the probe side is dict-encoded, translate each
unique dict code on first encounter to its build-side hash bucket,
and cache (`Vec<Option<u32>>` indexed by dict code). Subsequent
probe rows look up by code, not by value.

**Acceptance:**
- Q05 (`n_name` from nation → many fact tables joined down) shows a
  measurable win on SF=1; the join is on `n_name` which is low-card.
- Build-side cardinality < probe-side dict cardinality is the gate;
  if the dict has more distinct codes than the build hash table has
  keys, fall back to standard hash join.
- Bit-equal output vs DataFusion default on Q05.

## Implementation surface

```
crates/ematix-flow-core/src/
  ematix_fast_parquet.rs        # Σ.E6a — eligibility heuristic + default flip
  dict_aggregate_exec.rs        # Σ.E6b — widen DictGroupCountExec accumulators
  dict_aggregate_rule.rs        # Σ.E6b — broaden matcher to {sum,min,max,avg,count}
  dict_hash_join_exec.rs        # Σ.E6c — new
  dict_hash_join_rule.rs        # Σ.E6c — new (or hang off existing physical_optimizer)
```

ematix-parquet stays at 0.12.0 — no decoder-side changes needed.

## Acceptance criteria for the phase as a whole

1. **No-regression baseline:** 22-query SF=1 parity bench's
   per-query medians stay within run-to-run noise (±5 %) for every
   query. Geomean improves by ≥ 3 pp.
2. **Real-data win:** at least three SF=1 queries that the v0.3.0
   release already wins on speed up by ≥ 15 % wall-clock (Q01, Q12,
   one of Q05/Q19).
3. **Correctness:** every query's output matches the DataFusion
   default plan bit-for-bit on i64/u64/string columns, and within
   `1e-10` relative error on f64.
4. **Default-on:** no opt-in flags. The release notes can say "no
   change in user code; rebuild and re-run."

## Risks + open questions

- **Schema-rewriting visibility.** Today the provider rewrites the
  Arrow schema field type to `Dictionary(U32, Utf8View)`. Downstream
  DataFusion operators that don't have a dict path (Sort, SortMergeJoin,
  Window) will silently fall back to materialising the dict, costing
  a small fixed overhead on each batch. Mitigation: the eligibility
  heuristic skips columns where the query plan obviously sorts on the
  string column. (We don't know the query at provider-build time, so
  this is best-effort — TBD whether to add a per-query opt-out hook.)
- **Mixed-encoding row groups.** A column that's dict-encoded in RG1
  but PLAIN in RG2 (size threshold spillover) violates the
  "Dict in every RG" precondition. Today we'd correctly fall back. At
  SF=10+ this is a real risk; an audit on real data is part of Σ.E6a's
  bench-gate.
- **i128 SumI64 overhead.** SF=10 `SUM(l_extendedprice)` doesn't
  overflow i64 but we use i128 defensively. Microbench needed to
  confirm the regression vs i64 is < 5 % at the kernel level.
- **AVG numerical stability.** Two-pass SumF64+Count + finalize divide
  matches DataFusion's strategy; relative error should be `1e-12` or
  better. Pinned by the bit-equality test.

## Effort + sequencing

| Slice | Estimate | Gating |
|---|---|---|
| Σ.E6a — default arrival + heuristic + bench-gate | ~1 wk | Σ.E5 landed (done) |
| Σ.E6b — extend aggregate kernel + rule | ~1.5 wk | Σ.E6a (arrival) |
| Σ.E6c — probe-side dict join | ~1.5 wk | Σ.E6a + Σ.E6b |
| Total | ~4 wk | |

Each slice is a separate PR. Σ.E6a can ship without Σ.E6b or Σ.E6c
landing — the wins from `EnableDictFilterRule` + `EnableDictGroupCountRule`
firing on real data are already a meaningful release on their own.

## Predecessors

- Σ.E3a (dict filter) — `feat/sigma-e3a-dict-filter` merged
- Σ.E3b.1 (dict count) — merged via #87
- Σ.E5 — Emat provider default flip merged via v0.3.0

## What this is not

- Not a rewrite of DataFusion's HashAggregate / HashJoin. We register
  parallel `ExecutionPlan` variants behind physical-optimiser rules,
  the way Σ.E3a / Σ.E3b already do.
- Not a dictionary-on-disk format change. Parquet's existing
  dict encoding stays as-is.
- Not the only way to fix the SF=10 ceiling. The streaming-reader
  rewrite (Σ.E5.6, deferred) is the alternative path. We pick this
  one because it generalises (string-heavy ops are common; SF=10
  is not).
