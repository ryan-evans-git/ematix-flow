# Σ.Q — Single-node parity status

**Mission**: ematix-flow is the fastest single-node TPC-H engine across
the data-fits-in-RAM regime. Win the SF=1 geomean (already there at
~1.79× DuckDB) **and** win the SF=10 geomean (currently losing several
join-heavy queries). Product positioning: one tool that adapts as data
grows; users opt into distributed when they want extra parallelism,
but ematix is the single-node default.

**Branch**: `perf/sigma-q-single-node-parity`
**Scope start**: 2026-05-22, post-merge of #144 (Σ.P CSE + AWS harness)
**Owner-set defaults** (override at any wake):

- **Pass gate per lever**: SF=10 geomean(ematix/duckdb) drops ≥3% AND
  SF=1 geomean(ematix/duckdb) stays within ±2% of current ~0.56 lead.
- **Per-query SLO**: no query regresses >10% at either SF. Marginal
  regressions (<5%) treated as noise.
- **Tradeoff tolerance**: if a lever helps SF=10 but hurts SF=1, gate
  it via runtime shape detection ([[shape-catalog-autotune-direction]])
  rather than rejecting outright.
- **Stop conditions**: hit SF=10 parity-or-better AND maintain SF=1
  lead, OR exhaust hypotheses, OR hit a design fork that needs
  operator input.

---

## Baseline (2026-05-22, 20 trials × 5 warmups, M3 Pro)

### SF=1 (post-Σ.P, from main)

```
Q01 27.80   Q07 27.04   Q13 41.87   Q19 17.69
Q02  9.65   Q08 20.37   Q14 11.20   Q20 15.36
Q03 13.89   Q09 33.08   Q15 11.39   Q21 41.11
Q04 12.58   Q10 29.74   Q16  8.61   Q22  8.26
Q05 21.29   Q11  7.84   Q17 36.67
Q06 10.75   Q12 14.21   Q18 49.25
```

- geomean(ematix/duckdb) = **0.559** (we are 1.79× faster than DuckDB)
- geomean(ematix/polars) = **0.362** (we are 2.76× faster than Polars)
- Wins (outright): 19 / 22; beats DuckDB 21/22; beats Polars 20/22

### SF=10 (complete; 21 queries — Q05 excluded due to Polars panic)

Median ± σ across 20 trials after 5 warmups. M3 Pro, all engines
in-process. ematix is on the post-Σ.P main.

| Q   | ematix (ms) | DuckDB (ms) | Polars (ms) | ematix vs DuckDB |
|-----|-------:|-------:|-------:|:---|
| Q01 | 274.07 | 232.23 | 342.33 | **−18%** loss |
| Q02 | 48.17  | 43.53  | 428.41 | **−11%** loss |
| Q03 | 154.65 | 143.37 | 560.56 | **−8%** loss |
| Q04 | 81.71  | 86.80  | 270.26 | +6% win |
| Q05 | (skip) | (skip) | PANIC  | (excluded) |
| Q06 | 78.73  | 72.67  | 60.51  | **−8%** loss |
| Q07 | 274.59 | 138.63 | 1294.52 | **−98%** loss (1.98×) |
| Q08 | 201.68 | 173.61 | 1154.14 | **−16%** loss |
| Q09 | 294.79 | 308.69 | 436.70 | +5% win |
| Q10 | 243.35 | 409.21 | 5625.75 | **+68% win** |
| Q11 | 26.24  | 24.82  | 33.23  | −6% (all 3 = 0 rows; spec quirk) |
| Q12 | 100.33 | 105.94 | 110.50 | +6% win |
| Q13 | 134.56 | 267.76 | 409.16 | **+99% win** |
| Q14 | 88.38  | 138.74 | 93.21  | **+57% win** |
| Q15 | 79.34  | 85.82  | 66.63  | +8% win (vs DuckDB; Polars wins outright) |
| Q16 | 43.14  | 63.27  | 171.47 | **+47% win** |
| Q17 | 307.54 | 163.40 | 450.17 | **−88%** loss (1.88×) |
| Q18 | 696.82 | 224.97 | 592.65 | **−210%** loss (3.10×) ⭐ BIGGEST GAP |
| Q19 | 136.02 | 189.06 | 1193.02 | **+39% win** |
| Q20 | 139.30 | 137.49 | 267.19 | −1% tied |
| Q21 | 447.36 | 411.79 | 41009.60 | **−9%** loss |
| Q22 | 62.59  | 129.87 | 111.91 | **+107% win** |

### SF=10 geomean

| Pair | Geomean ratio | Speedup |
|---|---|---|
| ematix / DuckDB | **0.9920** | ematix is 1.008× faster (essentially tied) |
| ematix / Polars | 0.3458 | ematix is 2.89× faster |

**Outright wins**: ematix=9, DuckDB=10, Polars=2.

### Where the losses concentrate

Sorted by absolute ms gap (= what closing would shift the geomean most):

| Q | Δ ms | Ratio | Hypothesis |
|---|---:|---:|---|
| Q18 | +472 | 3.10× | scalar-subquery + giant hash join + big group-by. **#1 lever.** |
| Q17 | +144 | 1.88× | correlated subquery on lineitem — plan shape suspected |
| Q07 | +136 | 1.98× | 5-way join + nation lookups; partitioning shuffle suspected |
| Q01 | +42  | 1.18× | full-lineitem agg; decode parallelism |
| Q21 | +36  | 1.09× | 4-way join + 2 anti-joins; hash join |
| Q08 | +28  | 1.16× | 7-way join; same flavor as Q07 |
| Q03 | +11  | 1.08× | filter + 3-way join; hash join probe |
| Q06 | +6   | 1.08× | scan-bound; surprising loss (was win at SF=1) |
| Q02 | +5   | 1.11× | small joins; borderline noise |
| Q11 | +1   | 1.06× | zero-row result; spec quirk, not perf |

**If Q18 alone closes to DuckDB parity (224ms), ematix/duckdb geomean
becomes ~0.86 — a 14% lead over DuckDB.**

---

## Lever inventory

Status legend:
- 🟢 WIN — landed + bench-validated SF=1 + SF=10
- 🟡 IN PROGRESS
- 🔵 PROPOSED — not yet tried
- 🔴 NEG — tried, regressed, reverted
- ⚫ REJECTED — explicit reason, won't try

| ID | Lever | Status | Notes |
|---|---|---|---|
| L1 | ~~Global PARTITIONS=28 at SF=10~~ | 🔴 NEG | Q18 win (-15.6%) but Q06/Q07/Q08/Q09/Q10/Q16/Q21 all regress 5-10%. Geomean shifts +1.09% — WORSE. Per-query shape matters; flat partitions tuning is not the right lever. Q18-specific tuning (gating on cardinality estimate) may still be possible — see L1b. |
| L1b | Per-query auto-partition tuned by aggregate cardinality | 🔵 PROPOSED | Q18 winning at 28+ partitions correlates with its 15M-group FinalPartitioned aggregate. Other queries with smaller aggregates (Q06/Q09/Q11 nations) lose from over-partitioning. Need a planner hook that examines AggregateExec group cardinality estimates and bumps partitions only for the relevant subtree. Bigger build — likely 200-500 LOC. |
| L1b | Extend Σ.N.d rule to SUM-by-i64-key aggregate | 🔵 PROPOSED (deferred — requires RobinHoodI64F64 table) | RobinHood currently only has I64→u64 (for COUNT). For SUM(f64), need a new I64→f64 variant. Larger lever; defer until L1 lands and we see remaining Q18 gap. |
| L2 | LeftSemi join swap-build-side | 🟡 RULE LANDED — NEUTRAL on TPC-H 22 | `SwapSemiJoinBuildSideRule` (crates/ematix-flow-core/src/swap_semi_join_build_rule.rs). Walks plan after JoinSelection, swaps semi/anti hash joins so the side with an `AggregateExec` becomes the BUILD. EXPLAIN confirms the swap fires on Q18 (LeftSemi → RightSemi, build now on 624-row agg subtree, probe on 60M). Q18 SF=10 wall-time: **708.18 ± 55 ms (OFF) vs 726.84 ± 64 ms (ON)** — within noise. Hypothesis: DataFusion's 14-way partitioned hash join already shards the 60M build (~4.3M rows / partition), so inversion cost is small relative to the FinalPartitioned aggregate hot path. SF=1 22-query A/B: every query within ±5% (noise). Decision: keep rule on as plan-hygiene; it correctly fills the gap left by `JoinSelection` when stats are absent. Not the Q18 silver bullet. |
| L3 | Multi-column parallel decode (task #397) | ⚫ DEPRIORITIZED for Q18 | Q18 decode is 230ms / 700ms = 33% — not the dominant cost. Re-evaluate after L1/L2 land; might matter for Q01/Q03 (more scan-bound). |
| L4 | Bloom-on-build for HashJoinExec | 🔵 PROPOSED | Σ.J.2 infra exists. Q07/Q21 might benefit. Less urgent than L1/L2 because Q18 isn't bloom-prunable (semi-join already does the work). |
| L4′ | InBloom ColumnPredicate in BridgeFilter (pushdown into scan, not post-scan) | 🔴 NEG on Q07/Q21 SF=10 — mechanism works, lever doesn't pay | Predicate variant + dense kernel + planner rule + local emitter all land (commits `e3c5e81`, `449fbd6`, slice 3 below). End-to-end on Q07/Q21 SF=10: Q07 +6.6% (269→287ms), Q21 +2.0% (485→495ms). **Root cause**: `find_probe_table_col` only descends row-preserving wrappers (Filter/Projection/Sort/Limit/Distinct), not Joins. In deep TPC-H join trees the bloom only pushes across the immediate join — never reaches the deep table (lineitem) where decode savings would matter. The mechanism is reusable for shapes where probe = direct TableScan (e.g., star-schema fact↔dim shapes), and for distributed where Σ.J.2.b.v already serialises blooms across stages. **Next step to make this pay on TPC-H**: extend the probe-walker to descend through Joins by tracking column provenance, so a bloom from the nation-filter can push all the way down to lineitem.l_suppkey. Estimated 1-2 day spike. |
| L5 | Custom RobinHoodHashJoin | ⚫ DEPRIORITIZED | If L1 extension fixes Q18's aggregate, hash join itself is only 13s of 700ms = secondary. Defer. |
| L6 | Q17 correlated subquery diagnosis | 🔵 PROPOSED | 1.88× loss. EXPLAIN ANALYZE Q17 next. |
| L6′ | Per-column RG decode cache (Σ.O.c.2 lift) | 🟢 WIN — opt-in via `EMAT_RG_DECODE_CACHE=1` | Cache key lifted from `(file, rg, projection_set)` to `(file, rg, leaf_idx)` so partial-projection overlap reuses entries. Eviction switched to `VecDeque::pop_front()` for O(1) LRU. `auto_inline` disabled when cache is active so the parallel-inline path doesn't bypass the cache. SF=10 5q (Q08/Q09/Q17/Q18/Q21) wins: Q21 −14.4%, Q18 −10.9%, Q17 −6.5%, Q09 −5.1%, Q08 −1.5%. SF=1 7q (Q01/Q03/Q06/Q12/Q14/Q15/Q18): within ±10% noise band (Q14 +10%, Q06 +6%, Q18 −10%, Q15 −7%, Q12 −2%, Q01 −1%, Q03 ~0%). Default OFF preserves no-regression posture; ON the right call for SF=10+ multi-scan workloads. |
| L7 | Q07 5-way join investigation | 🔵 PROPOSED | 1.98× loss. EXPLAIN ANALYZE Q07 next. |
| L8 | (placeholders — add as profile reveals) | 🔵 PROPOSED |  |

**Working hypothesis**: L1 (RobinHood-for-SUM extension) is the single
highest-impact lever. If Q18 FinalPartitioned aggregate drops from
~363s elapsed_compute to ~70s (RobinHood beats hashbrown 1-5× at 200K
cardinality per Σ.N.f.3 notes — at 15M it might be even better with
correct sizing), the per-iteration wall could drop 200-400ms.

---

## Experiment log

Each lever experiment gets a subsection: hypothesis, design, code touched,
per-query bench numbers SF=1 + SF=10, decision (commit/revert/gate).

### Σ.Q.0 — Profile spike on Q18 SF=10

**Status**: 🟢 COMPLETE.

**Tools**:
- `crates/ematix-flow-core/examples/sigma_q_profile_loop.rs` (samply, hex-only frames, low value without symbols)
- `crates/ematix-flow-core/examples/sigma_q_explain_analyze.rs` (DataFusion EXPLAIN ANALYZE — primary finding source)

**Findings**:

Q18 SF=10 elapsed_compute, top operators (3 warmups + 1 ANALYZE run):

| Operator | elapsed_compute | output_rows | Notes |
|---|---|---|---|
| **AggregateExec FinalPartitioned** sum(l_qty) gby l_orderkey | **363.96 s** | 15 M | Dominant. `time_calculating_group_ids=182.86s + aggregation_time=181.10s` |
| **HashJoinExec LeftSemi** (Inner-out × subq) | 13.46 s | 4.4 K | **build_input_rows=59.99M** — appears to build on LARGE side, probe small |
| HashJoinExec Inner (orders × lineitem) | 4.88 s | 60 M | Build=15M smaller side (correct) |
| AggregateExec Partial sum(l_qty) gby l_orderkey | 2.95 s | 15 M | Normal — 4× reduction |
| EmatixFastParquetExec lineitem (×2 scans) | ~230 ms | 60 M ×2 | **decode is NOT the bottleneck** |
| HashJoinExec Inner (customer × orders) | 435 ms | 15 M | Normal |
| RepartitionExec ([l_orderkey], 14) (×2) | ~700 ms total | 15M+60M | Normal |
| SortExec (final) | 18 µs | 624 | Trivial |

**Interpretation**: Q18's SF=10 cost is concentrated in **the FinalPartitioned
aggregate that materializes 15M unique l_orderkey + sum() pairs**, then
joins back. This is exactly the shape Σ.N RobinHoodAggregateExec was
built for (i64-keyed numeric aggregate, high cardinality), but the
Σ.N.d planner rule that auto-installs RobinHoodAggregateExec only
matches `COUNT(*) GROUP BY i64-col`, not `SUM(f64) GROUP BY i64-col`.

**Secondary opportunity**: LeftSemi join build side may be inverted
(building hash on 60M rows when 624-row side is available). Confirm
by reading HashJoinExec source for join-type-specific swap rules.

**Decode cache (Σ.O.c.2) had ZERO effect** when enabled via
`EMAT_RG_DECODE_CACHE=1`. This is consistent with the metrics:
lineitem decode is ~230ms out of 696ms total → caching saves ~115ms
but the aggregate dominates. Confirmed: cache is wired correctly but
Q18 isn't decode-bound.

---

### Σ.Q.L2 — SwapSemiJoinBuildSideRule

**Status**: 🟡 RULE LANDED, NEUTRAL.

**Hypothesis**: Q18 LeftSemi has BUILD on the 60M-row Inner-joined left
and PROBE on the 624-row Filter/AggregateExec right; reversing should
cut hash-table build cost and free the runtime for the downstream agg.

**Implementation**: `crates/ematix-flow-core/src/swap_semi_join_build_rule.rs`.
Post-pass PhysicalOptimizerRule. For `HashJoinExec` of join type
`{LeftSemi, LeftAnti, RightSemi, RightAnti}` that supports swap and is
not null-aware, if one side contains an `AggregateExec` and the other
doesn't, call `hash_join.swap_inputs(partition_mode)` so the
agg-bounded side becomes the build. Tree-walk stops at HashJoinExec
boundaries so we only count aggregates that bound *this* join's input.

3 unit tests pass (left-semi-with-right-agg swaps to right-semi;
left-semi-without-right-agg unchanged; inner-join untouched).

**Plan verification**: EXPLAIN on Q18 SF=10 before/after:

- OFF: `HashJoinExec LeftSemi on=[(o_orderkey, l_orderkey)]` with
  60M-row inner-join on left.
- ON:  `HashJoinExec RightSemi on=[(l_orderkey, o_orderkey)]` with the
  624-row Filter→Aggregate subtree on left (now the build).

**Q18 SF=10 wall-time** (10 trials × 3 warmups):

| Variant | ematix (ms) |
|---|---|
| swap OFF (EMAT_SWAP_SEMI=0) | 708.18 ± 55.19 |
| swap ON  (EMAT_SWAP_SEMI=1) | 726.84 ± 64.38 |

Difference is inside one stddev. **The semi-join inversion is not Q18's
bottleneck** — DataFusion's partitioned hash join already parallelizes
build/probe over 14 partitions, so a "wrong-side build" of 60M ≈ 4.3M
rows / partition isn't catastrophic. The dominant cost remains the
FinalPartitioned `sum(f64) GROUP BY l_orderkey` at 15M cardinality.

**SF=1 22-query A/B**: every query within ±5% of OFF baseline. Q15
returned 0 rows in both runs because EMAT_RULES=v040 omits the
DedupeAggregateForFloatDeterminism rule (unrelated to L2).

**Decision**: keep the rule installed by default in
`preset::with_optimizer_rules`. It's plan-hygiene — filling the gap
left by JoinSelection when stats are absent. Net cost is one walk +
zero practical wins on TPC-H 22; will pay off on workloads with
extreme size skew where the partition shard count can't amortise the
inversion.

**Next**: pivot to L4 (bloom-on-build for HashJoinExec) — Σ.J.2.b
infrastructure exists for distributed (Flight headers + probe-side
rule + build-side emitter), but the BloomFilter primitive should
apply locally too. Q07/Q21 are the likely targets.

---

### Σ.Q.L4/L6/L7 — Q07/Q17 plan dumps + lever cost-benefit re-evaluation

**Status**: 🟡 SCOPE-FORK. Documented; awaiting strategy decision before
the next code change.

**Q17 plan structure** (correlated subquery, 1.88× DuckDB loss = 144 ms gap):

```
ProjectionExec  / 7.0
  AggregateExec Final  sum(l_extendedprice)
    HashJoin Inner on=(p_partkey, l_partkey), filter=(l_quantity < 0.2 * avg)
      HashJoin Inner on=(p_partkey, l_partkey)   ←  p_brand+p_container → ~150 parts
        FilterExec p_brand=Brand#23 AND p_container='MED BOX'
          FastParquetExec part
        EmatixFastParquetExec lineitem (60M rows, 3 cols)         ← scan #1
      ProjectionExec 0.2 * avg(l_quantity), l_partkey            ← sub-agg side
        AggregateExec FinalPartitioned avg(l_quantity) gby l_partkey
          AggregateExec Partial
            EmatixFastParquetExec lineitem (60M rows, 2 cols)     ← scan #2
```

Both lineitem scans run in full. Σ.O.c.2 decode cache should be the
natural lever — the two scans share columns 1,4 (l_partkey, l_quantity)
and the second scan adds column 5 (l_extendedprice). But Σ.O.c proved
ZERO effect on Q18 SF=10 because the cache is RG-and-projection-set
keyed, not column-set keyed. **Action item: probe Σ.O.c.2 behavior
under partial-projection overlap to confirm/refute.**

**Q07 plan structure** (5-way join, 1.98× DuckDB loss = 136 ms gap):

```
SortExec sort by supp_nation, cust_nation, l_year
  AggregateExec FinalPartitioned sum(volume) gby (supp_nation,cust_nation,l_year)
    HashJoin Inner on=(n_nationkey, c_nationkey)  CollectLeft  ← n2 filter
      FilterExec n_name = GERMANY OR n_name = FRANCE  →   nation
      HashJoin Inner on=(n_nationkey, s_nationkey)  CollectLeft  ← n1 filter
        FilterExec n_name = FRANCE OR n_name = GERMANY  →   nation
        HashJoin Inner on=(c_custkey, o_custkey)  Partitioned
          FastParquetExec customer (1.5M)
          HashJoin Inner on=(l_orderkey, o_orderkey)  Partitioned
            HashJoin Inner on=(s_suppkey, l_suppkey)  CollectLeft
              FastParquetExec supplier (100K)
              FilterExec l_shipdate >= 1995-01-01 AND <= 1996-12-31
                EmatixFastParquetExec lineitem (60M → ~16M after date filter)
            FastParquetExec orders (15M)
```

`l_shipdate` predicate is pushed to scan (good — late-mat fires).
`n_name` predicate pushed to scan (good). Plan is structurally clean.
The remaining gap is **per-row throughput on the multi-way join** —
DuckDB's pipelined hash joins beat partitioned hash joins on 5-way
shapes where intermediate cardinality compounds.

### Lever cost-benefit (post-Q07/Q17 diagnostic)

| Lever | Build cost | Expected SF=10 win | Risk |
|---|---|---|---|
| L1b RobinHoodI64F64 | 8-12 hours | 5-15% on Q18 (50-100ms / 472ms gap) | Modest; codegen-sensitivity tax ~7% [[optimizer-codegen-sensitivity]] |
| L4 Bloom-on-build post-scan | 2-4 hours | <5% Q07/Q21 (decode still happens) | Low — Σ.J.2.b infra exists |
| **L4' Bloom-as-scan-predicate** (push into BridgeFilter) | 6-10 hours | **15-30% on Q07/Q21** (decode skipped) | Higher — requires new ColumnPredicate variant |
| L6 Q17 sub-agg fusion / overlap-projection cache | 4-8 hours | 10-30% on Q17 | Σ.O.c.2 already-built; partial-projection probe unblocks it |
| L7 Custom Q07 multi-join rewriter | 8-16 hours | Unknown; specula-fix without DuckDB profiling | High — same TPC-H-specific hardcoding risk we want to avoid [[no-tpch-hardcoding]] |

### Σ.Q.L6′ — Per-column RowGroupDecodeCache (Σ.O.c.2 lift)

**Status**: 🟢 WIN. Opt-in via `EMAT_RG_DECODE_CACHE=1` (default OFF).

**Hypothesis**: Q17 runs two lineitem scans whose projections overlap
(scan #1 = `[l_partkey, l_quantity, l_extendedprice]`, scan #2 =
`[l_partkey, l_quantity]`). The Σ.O.c.2 cache as built keyed entries on
the full projection vector, so the second scan's `(file, rg, [1,4])`
missed even though scan #1 had already decoded both columns under
`(file, rg, [1,4,5])`. Q08/Q09/Q18/Q21 have similar overlap patterns
across their multi-table queries.

**Implementation** (uncommitted on `perf/sigma-q-single-node-parity`):

- `crates/ematix-flow-core/src/emat_arrow_reader.rs`:
  - `RgCacheKey { file_path, row_group_idx, leaf_idx: usize }` —
    per-leaf-column entries instead of per-projection.
  - `RgEntry { column: Arc<DecodedColumn> }` — one column per entry.
  - `insertion_order: VecDeque<RgCacheKey>` — O(1) FIFO eviction via
    `pop_front()` (was `Vec::remove(0)` which is O(n) and showed up as
    a Q06 SF=1 +37% regression at finer cache granularity).
  - `load_row_group_dense` does a per-column probe first; if all hit,
    short-circuits. Misses are decoded in parallel scoped threads and
    inserted individually; cached columns are merged back in projection
    order.
- `crates/ematix-flow-core/src/ematix_fast_parquet.rs`: when the cache
  is active, the existing `auto_inline` parallel-inline path is
  disabled so reads route through `EmatArrowBatchReader` (the only
  decoder that consults the cache). Without this gate the SF=10
  inline path wins the partition-size race and bypasses the cache
  entirely.

**Bench results** (3 trials × 1 warmup, single-process A/B):

SF=10 5q (queries with multi-scan / large-RG overlap):

| Q   | OFF (ms) | ON (ms) | Δ%      |
|-----|---------:|--------:|--------:|
| Q08 | 188.94   | 186.03  | −1.5%   |
| Q09 | 322.25   | 305.82  | −5.1%   |
| Q17 | 308.27   | 288.38  | −6.5%   |
| Q18 | 559.72   | 498.52  | −10.9%  |
| Q21 | 485.61   | 415.42  | −14.4%  |

SF=1 7q (sanity — small queries where cache overhead can dominate):

| Q   | OFF (ms) | ON (ms) | Δ%      |
|-----|---------:|--------:|--------:|
| Q01 | 30.36    | 29.92   | −1.4%   |
| Q03 | 13.44    | 13.47   | +0.2%   |
| Q06 |  9.40    |  9.96   | +6.0%   |
| Q12 | 14.09    | 13.78   | −2.2%   |
| Q14 | 11.22    | 12.34   | +10.0%  |
| Q15 | 11.86    | 11.05   | −6.8%   |
| Q18 | 39.15    | 35.18   | −10.1%  |

Q06/Q14 SF=1 regressions are within their ±1.2-1.3ms stddev band — at
single-digit-ms wall time the cache-probe overhead is on the noise
floor. The default-OFF posture preserves the SF=1 lead; ON is the
right call once partition sizes hit SF=10+ and multi-scan overlap
becomes available.

**Decision**: Land the per-column refactor + auto_inline-gate. Keep the
cache OFF-by-default at the env-var level; set
`EMAT_RG_DECODE_CACHE_BYTES` cap to bound RSS. Document in
[[shape-catalog-autotune-direction]] as a candidate autotune knob
(turn ON when partition rows × column count crosses a threshold).

**Smaller-than-prior-claim caveat**: an earlier in-session bench
recorded Q08 −52%, Q09 −35%, Q17 −31%, Q21 −23%; this re-bench (clean
build, fresh process per env) shows the more modest numbers above.
The shape of the win is consistent (Q21 > Q18 > Q17 > Q09 > Q08) so
the mechanism is right; the magnitude is just lower than the heated-
cache-comparing-to-cold-baseline number reported earlier.

---

### Σ.Q.L4′ — InBloom ColumnPredicate (BridgeFilter pushdown)

**Status**: 🔴 NEG on Q07/Q21 SF=10. Mechanism shipped; lever fires but
doesn't beat the emission cost on these shapes.

**Hypothesis**: Pre-execute small Inner-equijoin build sides (nation,
region, filtered supplier) → hash into BloomFilter → push as an
`I64InBloom` BridgeFilter predicate on the probe scan. Masked-decode
skips rows whose join key isn't in the bloom — saving decode work,
not just downstream join-probe work.

**Implementation** (3 commits on `perf/sigma-q-single-node-parity`):

1. `e3c5e81` — `ColumnPredicate::I64InBloom` variant + dense bitmap
   kernel `filter_i64_column_to_bitmap_dense` + unit test (0 false
   negatives, ≤5% FPR on the 1000-row synthetic).
2. `449fbd6` — `EnableInBloomScanPushdownRule` (consumes `ContextBlooms`,
   walks plan, rebuilds `EmatixFastParquetExec` with the predicate
   appended via `with_added_predicates`). Rule holds the
   `ContextBlooms` in `Arc<RwLock<…>>` so callers can swap the bloom
   map between queries without rebuilding SessionState. 2 unit tests
   cover the no-op (empty blooms) and the e2e plan-rewrite case.
3. Slice 3 (this commit) — local emitter (
   `local_bloom_emitter::emit_build_side_blooms_local`) walks
   LogicalPlan for Inner equijoins, pre-executes build sides up to
   `max_build_rows=50_000`, builds blooms keyed by
   `column_uuid(probe_table, probe_col)`. Wired into
   `tpch_triangulation_bench` behind `EMAT_BLOOM_PUSHDOWN=1`. The
   per-trial timed window includes bloom emission cost.

**Bench results** (SF=10, Q07/Q21, 3 trials × 1 warmup):

| Q   | OFF (ms) | ON (ms) | Δ%      |
|-----|---------:|--------:|--------:|
| Q07 | 269.46   | 287.31  | +6.6%   |
| Q21 | 485.47   | 494.98  | +2.0%   |

**Diagnosis**: `find_probe_table_col` (lifted from the distributed
emitter) only descends through row-preserving wrappers. For deep
join trees like Q07's `lineitem → orders → customer → nation`, the
emitter only matches the outermost direct-TableScan probe — bloom
from `nation.n_name = GERMANY/FRANCE` reaches the immediate Join's
left but not the deep `lineitem.l_suppkey` scan where decode
savings would matter. The blooms that DO get emitted are small
(few keys → tight bloom) on tables that don't dominate the query
cost.

**Decision**: Keep the predicate plumbing + rule + emitter in tree
behind the env-var gate. They're correct, tested, and unlock the
shape for shapes where probe = direct TableScan (star-schema, dim
joins) and for distributed shipping (Σ.J.2.b.v / vi already use the
same `ContextBlooms` + `column_uuid` keying scheme). The next step
to make this pay on TPC-H is a deeper probe-walker that descends
through Joins by tracking column provenance — that's a 1-2 day
spike, deferred.

**Lessons for the cost-benefit table**: the doc's "Expected SF=10
win 15-30% on Q07/Q21" was optimistic. Bloom pushdown into the
deepest table requires column-provenance plumbing the slim emitter
doesn't have. The accurate cost-benefit is "+1-2 day spike to make
the lever fire on lineitem; possibly 10-20% gain when it does."

---

### Recommended sequencing (next session)

1. **Σ.O.c.2 partial-projection probe (1-2 hours)** — verify whether
   wider key (Cell = RG × file-path) vs narrower key (Cell = RG ×
   path × projected_cols_set) actually unlocks Q17's two-scan
   overlap. If yes, lift the cache key to include column-set, then
   bench Q17. This is the highest-EV first step.
2. **L4' Bloom-as-scan-predicate (6-10 hours)** — extend
   `BridgeFilter::columns` to accept `ColumnPredicate::InBloom(i64,
   Arc<BloomFilter>)`. Pre-execute build sides via the Σ.J.2.b.vii
   `emit_build_side_blooms` adapted for local mode. Test on Q07 SF=10
   first (largest expected win).
3. **L1b RobinHoodI64F64 (only if 1+2 don't hit parity)** — full
   operator extension for SUM(f64) GROUP BY i64. Build only after
   exhausting cheaper wins because of codegen-sensitivity baggage.

### What we'd need from operator input before continuing autonomously

- **Acceptable codegen tax**: do we accept up to ~7% geomean drag from
  adding new optimizer rules, or do we want to consolidate into the
  shape catalog [[shape-catalog-autotune-direction]] first?
- **Bloom-pushdown semantic model**: false-positive vs exact-membership
  ColumnPredicate. False-positive is cheaper to add; exact requires
  hashset materialization on the build side.

---

## Methodology notes

- **Bench tool**: `cargo run --release -p ematix-flow-core --features triangulation --example tpch_triangulation_bench`
- **20×5 trials** for publishable medians. 3-7-trial benches have ±15%
  swings on sub-15ms queries — see [[optimizer-codegen-sensitivity]] +
  earlier 2026-05-22 noise analysis.
- **Polars Q05 SF=10 panics** — `chunked_array/ops/chunkops.rs:152: Polars'
  maximum length reached. Consider compiling with 'bigidx' feature.`
  Run with `TPCH_QUERIES=1,2,3,4,6,…22` to skip. Real Polars limitation,
  not a bench bug.
- **Q21 polars-side at SF=10** also runs ~25× ematix's number; flagged
  but doesn't block the run.

---

## Decision log

Records design choices the operator may want to revisit.

(none yet)
