# Σ.E6 — dict-preserved arrival by default + broaden dict-aware ops

**Status:** **REJECTED 2026-05-20.** Reverted after the full
Σ.E6a-pre + Σ.E6a + Σ.E6b + Σ.E6c-lite stack failed to net-positive
the bench gate. Geomean ratios across the iteration:

| Stack | TPC-H SF=1 geomean | Wins | Losses |
|---|---:|---:|---:|
| v0.3.0 baseline | 1.0000 | — | — |
| Σ.E6a alone (catastrophic) | ~1.17 | — | — |
| Σ.E6a + Σ.E6a-pre | 1.0020 | 4 | 4 |
| Σ.E6a + Σ.E6a-pre + Σ.E6b | 1.0020 (Q01 -15%) | 4 | 4 |
| Σ.E6 full + Σ.E6c-lite (blanket cast) | 1.0186 | 4 | 5 |
| Σ.E6 full + Σ.E6c-lite (key-only cast) | 1.0137 | 4 | 6 |

The cheap-option spike conclusively ruled out the two hypotheses we'd
have built `DictHashJoinExec` to fix:

- **Per-row hash in HashJoin is NOT the cost** — casting Dict→Utf8View
  before the join (which would have helped if HashJoin was per-row
  hashing inefficiently) didn't move Q10/Q15/Q20 at all.
- **Once-per-batch materialization inside the join is NOT the cost
  either** — same evidence.

Q10/Q15/Q20's regression lives somewhere we didn't anticipate — most
likely DataFusion's output `ProjectionExec` or how `DictionaryArray`
propagates through Sort / Aggregate / output. Without a profiling
spike to locate the actual cost, building `DictHashJoinExec` would
have been speculative.

Plan kept as historical reference. The architectural lessons fold
into Σ.F (shape catalog) and inform any future dict-preservation
attempt. See "What we'd do differently" at the end of this doc.

Follows Σ.E3 (substrate + first operator) and Σ.E5 (Emat provider as
the default scan path).

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

## Bench-gate finding — 2026-05-20

The original plan was to ship Σ.E6a (default-on dict arrival) by itself
as a meaningful release. The 22-query SF=1 triangulation bench against
the implementation invalidates that:

| Q | v0.3.0 | Σ.E6a | Δ |
|---:|---:|---:|---:|
| Q01 | 28.11 | 54.41 | **+94%** |
| Q05 | 20.93 | 33.28 | +59% |
| Q14 | 11.28 | 15.12 | +34% |
| Q19 | 18.81 | 23.69 | +26% |
| Q08 | 20.76 | 25.72 | +24% |
| Q12 | 14.72 | 17.79 | +21% |
| Q07 | 28.96 | 32.57 | +12% |
| Q17 | 35.71 | 40.13 | +12% |
| ...most others | | | +5-15% |

Wins fall **18 → 14 / 22**. Geomean regresses ~17%.

**Root cause.** Σ.E6a forces `streaming_arrow_reader = false` whenever
any column is dict-rewritten, because the streaming reader can't
emit `DictionaryArray` — it materialises to `Utf8View`. Almost every
TPC-H query touches at least one low-card string column
(`l_returnflag`, `l_shipmode`, `n_name`, `p_brand`, ...), so dict
rewriting kicks in everywhere, dragging every query onto the bridge.
Per memory `project_sigma_e2_fastparquet_remaining_losses`: streaming
geomean 1.04 vs bridge 1.51 — the bridge is materially slower on the
non-dict-aware path.

We pay the bridge tax to get dict arrival, but **none of the
dict-aware operators consume the rewritten columns** because the
only Dict-keyed rule today is `EnableDictGroupCountRule` (count-only).
Q01's 8-aggregate shape goes through `InjectFilterMultiAggRule` which
targets `Utf8View` group keys, not Dict — so it picks up the cost of
the rewrite and none of the benefit.

## Effort + sequencing (revised)

| Slice | Estimate | Gating |
|---|---|---|
| **Σ.E6a-pre** — streaming reader emits `DictionaryArray` for dict-rewritten cols | ~1 wk | Σ.E5 landed (done) |
| Σ.E6a — default-on dict arrival (current substrate) | ~3 days | Σ.E6a-pre |
| Σ.E6b — extend aggregate kernel + rule to consume Dict group keys | ~1.5 wk | Σ.E6a |
| Σ.E6c — probe-side dict join | ~1.5 wk | Σ.E6a + Σ.E6b |
| Total | ~5 wk | |

**Slices land together.** Σ.E6a alone fails the bench gate above;
Σ.E6a-pre alone is a no-op (eliminates the bridge tax but doesn't
do anything new). The shippable boundary is Σ.E6a-pre + Σ.E6a together,
with Σ.E6b ideally bundled so Q01 actually wins from the dict path.

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

---

## Appendix A — Future considerations: Vortex (research note)

Researched 2026-05-19 by the architect agent. Not part of Σ.E6
scope; captured here so the work isn't repeated and so the
post-Σ.E6 follow-up is queued.

### Summary

Vortex (https://github.com/vortex-data/vortex, LF AI&Data, v0.71.0
2026-05-18) is three things: an on-disk file format competing with
parquet, an in-memory array library competing with Arrow, and a
Scan API across DataFusion / DuckDB / Spark / Polars. Its
distinguishing architecture is **cascading lightweight encodings**
(FastLanes bit-packing, FoR, RLE, ALP/ALPrd for floats, FSST for
strings, dict, run-end) with **compute kernels over the compressed
form** instead of canonicalising to flat Arrow.

### Verdict

**Evaluate further, but only as a kernel cherry-pick post-Σ.E6.**
Reject the file-format-replacement, in-memory-replacement, and
parallel-backend options.

### Options considered

| Option | Decision | Why |
|---|---|---|
| (a) Adopt as on-disk format (.vortex replaces parquet) | **Reject** | Breaks parquet interop — Iceberg / Delta / DuckDB / Snowflake `COPY INTO` / `pandas.read_parquet` are all parquet-anchored. Two-format world for years. |
| (b) Adopt as in-memory scan layer (parquet → Vortex arrays → DataFusion) | **Reject** | Forces a rewrite of `InjectFilterMultiAggRule` / `InjectFilterSumRule` / `EnableDictGroupCountRule` on Vortex's `ScalarFnArray` primitives instead of Arrow. Multi-quarter rewrite competing directly with Σ.E6. The "stay compressed end-to-end" pitch only pays off if we also write Vortex on disk — see (a). |
| (c) Cherry-pick encoding kernels (ALP, FSST, FastLanes) | **Evaluate further** | ALP for f64 pages and FSST for string pages are real research wins we don't have. FastLanes is partially covered by our Σ.E5 small-bw NEON+AVX2 unpackers. Clean blast radius if the crates are independent of `vortex-array`. |
| (d) Ship as separate backend (`VortexTableProvider` alongside `EmatixFastParquetTableProvider`) | **Reject** | Carries a dep + test matrix for a path that competes with our default and that we wouldn't tune. Maintenance hazard. |

### Where Vortex would actually beat ematix-flow today

- **Wide tables (1k+ columns) with cold metadata.** FlatBuffer footer is
  O(1); thrift parquet footer is O(n). We don't bench this shape; if a
  feature-store table with 5k columns lands, parquet-rs metadata parse
  dominates and ematix-parquet inherits that. Vortex would win —
  possibly by a lot.
- **Random-access point lookups over object storage.** Vortex's "100×
  random access" claim is real. Our scan path is sequential RG-decode
  optimised; we don't compete here.
- **F64-heavy analytics with low entropy** (sensor data, financial tick).
  ALP beats parquet PLAIN/DELTA_BINARY_PACKED for doubles by 2-4× on
  size, matches it on decode speed.
- **String-heavy tables, high but not unbounded cardinality** where
  FSST wins over PLAIN+Snappy (URLs, paths, log lines). Q19 lineitem
  strings are plausible; Q13's `c_comment` is the opposite shape
  (short LIKE-able, dict-friendly).

Where Vortex does **not** beat us, with evidence:

- **TPC-H SF=1, the workload we tune for.** Our geomean vs DF+parquet-rs
  is 0.90. Vortex's published TPC-H gain is vs **default** DataFusion +
  parquet, not vs a tuned scan. The "10-20× scan" headline collapses
  against ematix-parquet.
- **Dict-aware aggregation.** Σ.E6 (this doc) closes the same lever —
  group by dict codes, not strings — at the operator layer.

### Spike (post-Σ.E6 only)

Goal: measure whether ALP + FSST close gaps on workloads we don't
already win.

1. Add `vortex-alp = "0.71"` and `vortex-fsst = "0.71"` as **build-only**
   deps in `ematix-parquet/Cargo.toml`. **Do not pull in `vortex-array`** —
   we don't want the Array abstraction.
2. New codec adapter (`ematix-parquet/src/codec/alp.rs`, `fsst.rs`)
   registered as non-standard codec IDs. Side-band, not parquet-compat.
3. Build derived datasets:
   - `lineitem_alp.parquet`: `l_quantity`, `l_extendedprice`, `l_discount`,
     `l_tax` re-encoded with ALP pages.
   - `orders_fsst.parquet`: `o_comment`, `o_clerk` re-encoded with FSST.
4. Run Q01/Q06/Q14 (f64 hot) and Q13/Q21 (string hot) against
   baseline vs ALP/FSST builds.
5. Gate: **ALP must show ≥10% wall-time improvement on Q01 or Q06 at
   SF=1 with no regression elsewhere; FSST must show ≥10% on Q13.**
   If neither hits, reject.

Effort: ~1 week, one engineer. No production exposure, no API
surface change, no user-visible artifact.

### Open questions before spiking

- Are `encodings/fastlanes`, `encodings/alp`, `encodings/fsst`
  publishable independently of `vortex-array`? If the dependency
  graph is tangled, the cherry-pick is materially harder and may
  require re-vendoring. Confirm by reading
  `encodings/{name}/Cargo.toml` in the Vortex repo before starting.
- Does ematix-parquet's PLAIN-stream + dict-preserved path interact
  cleanly with an ALP-decoded page (different control-flow shape —
  patch list at end)?
- **Σ.E6 lands first.** Do not run this spike until dict-default is
  merged; otherwise FSST will appear to win against an un-tuned
  baseline and we'll false-positive ourselves.

### Stop criteria

If the spike shows wins, write a separate ADR for adopting ALP/FSST
as codec plugins in ematix-parquet. **Do not** escalate to in-memory
or file-format adoption without a separate ADR and a much higher
evidence bar.

### Risks if we ever revisit (a) or (b)

- API churn: 0.x releases every 1–3 weeks (0.62 → 0.71 in 10 weeks).
  File format stable since 0.36; Rust crate surface explicitly not.
- Doubled test matrix across parquet+CSV+JSON+ORC+Delta × local+S3.
- Compute-surface mismatch: our hot-path rules pattern-match
  DataFusion physical plans over Arrow. Vortex bypasses Arrow.
- Loss of ematix-parquet leverage (v0.12.0: dict-preserved Utf8View,
  parallel page-Snappy, NEON+AVX2 unpackers, per-filter pushdown,
  late-mat). Replacing it deletes that investment.
- Vortex carries `vortex-cuda` / `vortex-cxx` / `vortex-jni` /
  `vortex-duckdb` in-workspace — the project's priorities (GPU, Spark)
  pull away from where we live (single-node Rust on Arrow).

---

## Appendix B — Shape-extension follow-ups for Inject* rules

Once Σ.E6 lands and Σ.E6b widens the dict-aware aggregate surface,
the next axis of generalisation is the plan-shape matchers on the
existing rules (`InjectFilterMultiAggRule`, `InjectFilterSumRule`,
`EnableDictGroupCountRule`, `EnableDictFilterRule`). All four match
narrow shapes today; widening them unlocks more TPC-H queries
without new operator types.

Ordered by wins-per-effort:

### B1 — Nested ANDs + Filter-over-Filter chain

**Cheap, broad.** DataFusion sometimes splits `a AND b AND c` into
`Filter(a) → Filter(b AND c)` or even three `Filter` nodes. Today
the matcher walks one `Filter` node. Walking the entire Filter
chain and concatenating predicates would unlock several TPC-H
shapes without touching any exec. ~150 LOC matcher change, 0
exec changes.

### B2 — OR-of-AND on `InjectFilterSumRule`

**Medium, narrow.** Q19 is exactly
`SUM(extprice * (1 - disc)) WHERE (a AND b AND c) OR (d AND e AND f) OR (g AND h AND i)`.
The matcher rejects `OR` today; the exec already handles
disjunctive predicates internally. Recognising the shape unlocks
Q19's full SQL path. ~100 LOC matcher.

### B3 — 3-key group-by in `InjectFilterMultiAggRule`

**Medium, narrow.** Q16 (`GROUP BY p_brand, p_type, p_size`) and
Q18 (`GROUP BY o_orderkey, o_custkey, o_orderdate, o_totalprice`).
Needs a 3-key (and 4-key) `FilterMultiAggSpec` template
instantiation. Σ.E6 dict-arrival composes naturally: pack 3 dict
codes into u64 (1+2 bytes for `p_brand`+`p_type` + 4 bytes for
`p_size`). ~300 LOC template + matcher.

### B4 — `InjectFilterJoinAggRule`

**Harder, broad.** New shape: `Aggregate → HashJoin → Filter → scan`.
Covers Q3 / Q5 / Q10 / Q21 — currently the join sits between
filter and aggregate and the rule passes through unchanged. Two
sub-options:

- Pre-filter the build side, then standard hash join, then
  aggregate (cheap, modest win).
- Substitute the hash join with the dict-aware probe-side join
  from Σ.E6c (composable with the existing FilterMultiAggSpec
  aggregate). ~800 LOC; ships as its own phase Σ.E7-ish.

### B5 — Group-by on simple expressions

**Harder, narrow.** Q22 has `GROUP BY substring(c_phone, 1, 2)`.
Two sub-options:

- Push the expression into the `FilterMultiAggSpec`'s group-key
  extractor (Photon-style "expression as kernel"). Cleaner; one
  template instantiation per expression class.
- Require an upstream `ProjectionExec` to materialise the
  expression before the aggregate. Simpler but loses inlining.

Deferred until a query in our workload demands it (TPC-H Q22 alone
isn't enough motivation).

### Sequencing

| Slice | Effort | Ship with |
|---|---|---|
| B1 (nested ANDs + filter chain) | ~1 day | Σ.E6 follow-up PR (touches matcher only, low risk) |
| B2 (OR-of-AND on FilterSum)     | ~2 days | Σ.E6 follow-up PR |
| B3 (3-key group-by)             | ~1 wk   | Σ.E6b (composes naturally with dict-keys) |
| B4 (Join + Filter + Agg rule)   | ~2-3 wk | Σ.E7 (new phase) |
| B5 (expression group-by)        | --      | Deferred |

### What we should not do

- Rebuild a generic "auto-router" rule (we retired
  `EnableFusedJitRule` in #534 — see [[delete-dead-substrate]]).
  Each new shape gets an explicit template instantiation;
  predictability + bit-equality testing wins over "one rule
  fires everywhere".

---

## Appendix C — Kernel sharing across the Inject* family

Question: should the Inject* rules share a common execution
kernel, beyond just sharing the plan-matcher walker?

Four shapes of "kernel" considered, ordered by ambition:

### C1 — Shared plan-matcher kernel (already partly done)

Single library that provides `BinaryExpr` extraction, AND-chain
flattening, scan-column resolution, ScalarValue normalisation, CSE
resolution. Today's `AggregateShapeConfig` walker landed
([[sigma-d-phase-d-checkpoint]]) cut per-rule code from ~150 to
~30 lines. Worth completing — finish the migration of
`InjectFilterSumRule` + `EnableDictGroupCountRule` onto the same
walker.

**Verdict: ship.** Pure refactor, mechanical, no perf risk.

### C2 — Unified vectorised execution kernel

One generic `FilterAggKernel` parameterised on group-key
type/arity + aggregate function + predicate shape. Replaces
`FilterMultiAggSpec` / `FusedFilterSumExec` / `DictGroupCountExec`
with one template family.

**Verdict: don't.** Photon's template-specialisation pattern works
*because* each instantiation is monomorphic. A unified kernel
either does runtime dispatch (kills the specialisation) or
produces the same number of monomorphic instantiations under a
different name. Code-organisation only; no perf delta.

### C3 — Macro kernel ("Photon-style" filter+agg interpreter)

A kernel that takes an Arrow batch + a pre-compiled "filter
program" (DSL or bytecode) + a pre-compiled "aggregate program",
runs a tight inner loop, returns updated accumulator state.
Inspired by Photon (Behm et al. VLDB '22 §3.2) and Velox's
`Expr` interpreter.

What it would buy us that we don't have:

- **Cross-predicate sub-expression sharing.** Q19's three OR
  branches all reference `l_shipmode`; today we evaluate it three
  times. CSE inside the kernel would evaluate once.
- **Per-column SIMD primitive composition.** Our NEON+AVX2
  unpackers + dict-lookup kernels could be wired together by the
  macro kernel rather than hard-coded per template.
- **Coverage of shapes that aren't worth a dedicated template.**
  Rare-shape queries would have a "good enough" path instead of
  falling back to DataFusion's generic agg.

What it would cost:

- ~3-5 weeks for a viable v1.
- The macro kernel runs in an interpretation loop — slower
  per-row than the monomorphic templates. Wins come from CSE +
  primitive composition, not raw loop speed.
- We just deleted the JIT substrate (#534). The macro kernel is
  "JIT-shaped" in spirit — a bytecode interpreter instead of
  generated machine code — but the maintenance surface is large.

**Verdict: separate phase doc, post-Σ.E7.** Possibly Σ.E8.
Pre-requisite: nail down Σ.E6/E7 wins first so we know what
*isn't* already addressed by template specialisation; the macro
kernel only earns its keep if there's a meaningful tail of
shape-coverage gap.

### C4 — Page-stream-integrated kernel

Skip Arrow-batch materialisation entirely; the kernel walks
parquet decode pages directly. Each new decoded page is filtered
+ aggregated in place; the accumulator updates without ever
materialising a `RecordBatch`. ematix-parquet's `PageWalker` would
plug in.

**Verdict: don't.** Couples decoding and execution in a way that
breaks DataFusion's pluggable operator model. The Σ.E5.6
streaming reader experiments ([[sigma-e5-6-streaming-doesnt-win]])
already showed that at SF=1 the sync-overhead penalty matches the
decode-time win. The bigger architectural cost (no plan tree,
no operator extensibility) isn't worth the unproven perf.

### Recommendation

| Option | Action | Effort |
|---|---|---|
| C1 — shared plan-matcher kernel | Ship as Σ.E6 follow-up | ~3 days |
| C2 — unified vectorised exec kernel | Reject | — |
| C3 — macro kernel (Photon-style) | Defer to Σ.E8 (post-Σ.E7), with separate phase doc | ~3-5 wk |
| C4 — page-stream kernel | Reject | — |

---

## Appendix D — What we'd do differently (2026-05-20 post-mortem)

Three things made Σ.E6 fail despite the bench-gate methodology:

### D1 — We assumed the downstream-cost taxonomy without measuring

The plan doc speculated that the bridge tax + dict-cast cost were
the regression sources. The bench-gated diagnostic spike
(`InjectCastDictBeforeJoinRule` — both blanket and key-only)
*conclusively* ruled out the join as the cost center for
Q10/Q15/Q20. But by the time we ran that spike, we'd already
landed Σ.E6a + Σ.E6a-pre + Σ.E6b.

**Lesson:** before flipping any default in the scan layer, write a
microbench that isolates Dict-vs-Utf8View through each downstream
operator class (HashJoin / HashAggregate / Sort / output). Σ.E6's
"land then measure" approach burned a day. The right order would
have been "measure each operator's Dict-vs-Utf8View cost in
isolation, find which operator regresses, target that".

### D2 — Default-on for a substrate without consumers loses by definition

Σ.E6a flipped Dict default-on, but the only Dict-aware consumer at
the time was `EnableDictGroupCountRule` (count-only). Every query
touching a low-card string column paid the bridge tax / cast tax
without any consumer benefit. This is the architectural reason
Σ.E6a alone scored geomean ~1.17 (catastrophic).

**Lesson:** scan-layer defaults must change *after* the consumers
land, not before. The right ordering would have been:
`Σ.E6b (widen rules to consume Dict) → Σ.E6c (probe-side join Dict)
→ Σ.E6a (flip default)`. We had it backwards.

### D3 — Per-query bench variance is wide; trust the multi-run geomean

Single-run Q15 looked like +35%; multi-run was +7% (within noise).
Single-run Q01 was -16% (huge win); multi-run was -12% (still a
win but smaller). Q06 σ was sometimes 100% of the mean.

**Lesson:** any go/no-go decision on a Σ-phase needs ≥3 bench runs.
Triage by mean-of-medians + per-query σ before committing
to any conclusion.

### Implications for next attempts

- A future scan-layer dict-arrival push needs the operator
  benchmark suite first (D1), the consumer rules already in place
  (D2), and the multi-run gate as a hard rule (D3).
- The Σ.E6b two-key Dict aggregate path (a real win on Q01 in
  isolation) is salvageable code if the consumer-first ordering is
  followed.
- Σ.F (shape catalog) inherits the multi-run gate practice.
