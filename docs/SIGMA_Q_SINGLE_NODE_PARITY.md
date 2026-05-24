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

### SF=10 geomean — combined L6′ + L9 (2026-05-23, 5 trials × 2 warmups, Q20 excluded due to pre-existing HashJoinExec assertion failure)

| Run | ematix / ematix(OFF) | ematix / DuckDB | Note |
|---|---|---|---|
| Both OFF (baseline) | 1.000 | 1.043 | ematix 4.3% behind DuckDB |
| **L6′ + L9 ON** (`EMAT_RG_DECODE_CACHE=1 EMAT_RT_BLOOM_SIDEBAND=1`) | **0.917** | **0.970** | ematix **3.1% AHEAD of DuckDB**, 8.3% faster than itself |

Per-query Δ% (ON vs OFF), wins ≥3%:
- Q04 −26.9%, Q08 −21.5%, Q21 −18.4%, Q18 −16.8%, Q13 −14.7%
- Q09 −12.6%, Q17 −11.9%, Q12 −8.8%, Q22 −7.7%, Q07 −7.5%
- Q11 −7.3%, Q01 −4.5%, Q03 −4.4%, Q10 −4.2%, Q15 −3.6%

Mild regressions (all within or just past noise σ):
- Q16 +7.0% (44.96→48.11 ms — L6′ cache-probe overhead at small queries)
- Q06 +4.2%, Q14 +1.9%

**The SF=10 22-query geomean flipped from behind DuckDB to ahead with
just L6′ + L9 enabled.** Both are opt-in env vars; default behavior is
unchanged. Q20 fails in both runs with `Invalid HashJoinExec, the
output partitio[ning]` assertion error — pre-existing bug, not
introduced by either lever; investigation deferred.

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
| L1b | Extend Σ.N.d rule to SUM-by-i64-key aggregate | 🟢 WIN on Q18 SF=10 after vectorised batch-ingest retry — kernel +13% at 15M cardinality, wall-time -4.4% vs stock | Slices 1+2+3 (commits `46bc146`/`7b6b9ca`/`6e0ca92`) shipped a scalar `insert_or_sum` operator that NEG'd Q18 SF=10 (+5.7% even pre-sized) because DataFusion stock's `sum(f64)` GroupAccumulator uses vectorised batch-update across 1K-row chunks. **Retry (commits `<this>`)**: Photon-style 4-stage pipeline lands as `RobinHoodI64F64::insert_or_sum_batch_vectorised` (hash batch → probe primary slot → fast-path accumulate hits → scalar fallback for misses); microbench at 6M rows shows +13% at 15M cardinality pre-grown, +65% at default-cap vs the scalar path. Auto-sizes `init_cap` from `input.partition_statistics().num_rows` (Partial: rows/4, FinalPartitioned: rows; clamped to [65k, 32M] buckets) so the env-var override is no longer needed for high-cardinality inputs. Q18 SF=10 standalone (5×2 trials): **OFF 567.01 ± 8.73 ms; ON-scalar+auto-cap 604.69 ± 29.81 ms (+6.6%, matches old +5.7% — kernel was the gap); ON-vec+auto-cap 541.79 ± 7.05 ms (-4.4%)**. Operator + rule still opt-in via `EMAT_RH_SUM_F64=1` per [[optimizer-codegen-sensitivity]]; vectorised path is default-on inside the operator (`EMAT_RH_SUM_F64_VEC=0` to revert). 22-query geomean A/B run in this slice — see Σ.Q.L1b retry section below. |
| L2 | LeftSemi join swap-build-side | 🟡 RULE LANDED — NEUTRAL on TPC-H 22 | `SwapSemiJoinBuildSideRule` (crates/ematix-flow-core/src/swap_semi_join_build_rule.rs). Walks plan after JoinSelection, swaps semi/anti hash joins so the side with an `AggregateExec` becomes the BUILD. EXPLAIN confirms the swap fires on Q18 (LeftSemi → RightSemi, build now on 624-row agg subtree, probe on 60M). Q18 SF=10 wall-time: **708.18 ± 55 ms (OFF) vs 726.84 ± 64 ms (ON)** — within noise. Hypothesis: DataFusion's 14-way partitioned hash join already shards the 60M build (~4.3M rows / partition), so inversion cost is small relative to the FinalPartitioned aggregate hot path. SF=1 22-query A/B: every query within ±5% (noise). Decision: keep rule on as plan-hygiene; it correctly fills the gap left by `JoinSelection` when stats are absent. Not the Q18 silver bullet. |
| L3 | Multi-column parallel decode (task #397) | ⚫ DEPRIORITIZED for Q18 | Q18 decode is 230ms / 700ms = 33% — not the dominant cost. Re-evaluate after L1/L2 land; might matter for Q01/Q03 (more scan-bound). |
| L4 | Bloom-on-build for HashJoinExec | 🔵 PROPOSED | Σ.J.2 infra exists. Q07/Q21 might benefit. Less urgent than L1/L2 because Q18 isn't bloom-prunable (semi-join already does the work). |
| L4′ | InBloom ColumnPredicate in BridgeFilter (pushdown into scan, not post-scan) | 🔴 NEG on Q07/Q21 SF=10 — mechanism works through 4 slices; lever still doesn't pay on TPC-H single-node | Slice 1-3 (commits `e3c5e81`/`449fbd6`/`d11ccf4`): predicate variant + kernel + rule + local emitter — first Q07/Q21 result was +6.6% / +2.0% because the probe walker stopped at Joins, so no bloom reached lineitem. Slice 4 (commit `<this>`): deep probe-walker descends through Inner Joins by tracking which side's schema carries the target column; both-direction candidate emission gated by `is_shallow_build_subtree` (only TableScan ± row-preserving wrappers — Joins/Aggregates excluded). Result: Q07 +15.2% (269→310ms), Q21 −0.7% (485→482ms). The shallow-build gate is load-bearing: without it the both-directions × deep-descent combo pre-executed lineitem-bearing subtrees and Q07 regressed +136%. **Root cause of the residual NEG**: the genuinely high-value bloom for Q07 would be `supplier WHERE s_nationkey ∈ filtered_nation` (post-Join filter) — but that build subtree is itself a Join, excluded by the shallow gate. Without the gate, the emitter pre-executes Joins whose cost matches the original query. Single-node bloom pushdown can only win if the bloom is captured as a **side-effect** of the regular HashJoinExec build phase, not as a separate emit pass — adaptive query execution territory. Kept behind env var; reusable for star-schema shapes (direct fact↔dim joins) and for distributed where Σ.J.2.b.v already serialises blooms across stages. |
| L5 | Custom RobinHoodHashJoin | ⚫ DEPRIORITIZED | If L1 extension fixes Q18's aggregate, hash join itself is only 13s of 700ms = secondary. Defer. |
| L6 | Q17 correlated subquery diagnosis | 🔵 PROPOSED | 1.88× loss. EXPLAIN ANALYZE Q17 next. |
| L6′ | Per-column RG decode cache (Σ.O.c.2 lift) | 🟢 WIN — opt-in via `EMAT_RG_DECODE_CACHE=1` | Cache key lifted from `(file, rg, projection_set)` to `(file, rg, leaf_idx)` so partial-projection overlap reuses entries. Eviction switched to `VecDeque::pop_front()` for O(1) LRU. `auto_inline` disabled when cache is active so the parallel-inline path doesn't bypass the cache. SF=10 5q (Q08/Q09/Q17/Q18/Q21) wins: Q21 −14.4%, Q18 −10.9%, Q17 −6.5%, Q09 −5.1%, Q08 −1.5%. SF=1 7q (Q01/Q03/Q06/Q12/Q14/Q15/Q18): within ±10% noise band (Q14 +10%, Q06 +6%, Q18 −10%, Q15 −7%, Q12 −2%, Q01 −1%, Q03 ~0%). Default OFF preserves no-regression posture; ON the right call for SF=10+ multi-scan workloads. |
| L7 | Q07 5-way join investigation | 🔵 PROPOSED | 1.98× loss. EXPLAIN ANALYZE Q07 next. |
| L8 | (placeholders — add as profile reveals) | 🔵 PROPOSED |  |
| L9 | **Adaptive query execution**: runtime sideband from HashJoinExec → probe scan | 🟢 WIN — Q07 SF=10 −4.7%, Q21 SF=10 −5.9% (opt-in via `EMAT_RT_BLOOM_SIDEBAND=1`) | 4 slices land (commits `49c5145` / `ca66e8d` / `1c43e2b` / `fe8648c`): **(a)** `BridgeFilterSideband` = `Arc<RwLock<Option<Vec<ColumnPredicate>>>>` runtime channel + `EmatixFastParquetExec::with_runtime_sideband` (peeks at execute-time, merges into BridgeFilter before decode); **(b)** `BuildSideBloomEmitterExec` pass-through wrapper that observes batches flowing from HashJoinExec's build child, accumulates the i64 join-key column into per-partition local BloomFilters (no mutex contention on the hot path), union-merges them on completion and publishes to the sideband; **(c)** `EnableRuntimeBloomSidebandRule` walks the plan, finds HashJoinExec nodes (Inner/LeftSemi/RightSemi) with i64 equi-keys whose probe side reaches an EmatixFastParquetExec, allocates a fresh sideband, wraps the build child + rewrites the probe scan; **(d)** bench wire-up confirms Q07 SF=10 OFF 275.5 → ON 262.5 ms (−4.7%), Q21 SF=10 OFF 478.0 → ON 449.7 ms (−5.9%), both beyond ±9 ms noise band. This is exactly the lever Σ.Q.L4′ slice 4's NEG diagnosis pointed at — bloom captured as a side-effect of the regular HashJoinExec build phase, not via a separate pre-execution pass. Mechanism is reusable: same sideband channel carries any `Vec<ColumnPredicate>`, so future AQE work (adaptive skew detection, late-arrival selectivity refinement, dynamic partition rebalancing) plugs into the same wiring. Stays opt-in per [[optimizer-codegen-sensitivity]]. |
| L10 | **Semi/anti-join pushdown**: logical-plan rule that walks `LeftSemi` / `LeftAnti` past inner joins down to its target table | 🟢 WIN — SF=10 22q geomean **−6.4%** (11 wins, 4 losses ≥5%); Q18 SF=10 **566 → 263 ms (−54%)**, Q21 **506 → 452 ms (−11%)**, also Q12 −14%, Q17 −13%, Q15 −12%, Q20 −11%. Opt-in via `EMAT_PUSH_SEMI=1`. | New `PushDownLeftSemiRule` (`crates/ematix-flow-core/src/push_down_left_semi_rule.rs`). Detects a `LeftSemi` or `LeftAnti Join` whose left-side equi-keys all resolve to a single base table, walks down the left subtree (Projections + Inner Joins; **does NOT descend through `Filter`** — DataFusion's `PushDownFilter` has already placed it optimally adjacent to the scan, and traversing it cost +170% on Q4 SF=10 in the first attempt before the bail was added), and replaces the target `TableScan` with `LeftSemi/LeftAnti(scan, subquery)`. Schema is preserved because both `LeftSemi` and `LeftAnti` output the left input's schema unchanged. Bails when the join carries a non-equi `filter:` post-condition (Q21's `l_suppkey != l1.l_suppkey` shape). For Q18: pushes `LeftSemi` from the top of the join tree down to wrap the orders `TableScan` directly — orders filtered to ~624 rows BEFORE joining customer or lineitem. 1.6 TB orders⋈lineitem intermediate replaced with 624-row × 624-row × 4.37K-row chain (matches DuckDB's plan shape exactly). Bench: **Q18 SF=10 OFF 567.01 ± 8.73 ms; L10 ON 262.95 ms (−53.5%); L10+L1b+L2+L6′+L9 259.07 ± 5.01 ms (−54%, 10.6% behind DuckDB)**. 22-query SF=10 geomean = **0.9362** (−6.4%). Stays opt-in per [[optimizer-codegen-sensitivity]]. |
| L11 | **u32 integer-key compression** (DuckDB's `__internal_compress_integral_uinteger`) — narrow i64 join/agg keys to u32 before hashing | 🔴 NEG — spike at commit `e37f6cf` shows manual `ARROW_CAST(p_partkey, 'UInt32')` on Q17 SF=10 is **+1.7% slower** (i64 baseline 409.71 ± 16.29 ms; u32 416.87 ± 8.87 ms, 11 trials × 2 warmups). CAST per-row overhead exceeds any cache-density gain. | DuckDB's plan markers showed `__internal_compress_integral_uinteger(col, 1)` wrapping every int column on Q07/Q17/Q05/Q08/Q18. Looked like the universal missing concept. **Spike** (`crates/ematix-flow-core/examples/sigma_q_l11_spike_q17.rs`): hand-cast both sides of every l_partkey equi-key in Q17 to u32. Result rejects the lever as a portable optimisation. DuckDB's benefit is **decoder-coupled** (parquet decoded directly into u32 buffers, hash table indexes by u32 slots, no CAST happens) — to match we'd need (a) ematix-parquet decoder support for "downcast on read" when column stats fit u32, (b) u32-keyed RobinHood variants, (c) plan rule that injects the downcast in the DECODER, not as a post-decode CAST. Step (a) is a multi-week change to a separate crate; (b)+(c) alone don't help (this spike proves it). **Real Q17 hot path** (per Σ.Q.L1b retry experiment-log): `AggregateExec(FinalPartitioned, avg by l_partkey) elapsed_compute=1.82s` across 14 partitions — kernel-level efficiency in the hash agg itself, not the key width. Lesson: DuckDB's plan markers are NOT a transferable shopping list of techniques; spike before building. |
| L12 | **SIMD-tagged hash agg kernel** (SwissTable-style metadata-byte probing) — `TaggedI64F64` sibling to `RobinHoodI64F64` with 16-byte NEON tag groups | 🔴 NEG (shape-blind wire) — Q17-shape microbench (60M rows / 2M groups, 30 rows/grp): Tagged vec **+15.1% kernel win** (749.57 → 636.06 ms, 80→94 M rows/sec). Q18-shape (6M rows / 15M card): Tagged vec **−19% kernel regression** (RH 128.99 → Tagged 160.09 ms). Spike gate was ≥20% at BOTH 2M and 15M card; fails on both. Kernel + tests + microbench committed as infra. | New `TaggedI64F64` + `TaggedSumF64Agg` in `crates/ematix-flow-core/src/robin_hood_agg.rs`. SoA layout (tags / keys / values in three parallel Vecs), 7-bit tag = top bits of splitmix64 hash, NEON `vceqq_u8` + bit-mask reduce produces a u16 movemask per 16-slot group. Tail-mirror invariant: capacity + GROUP_SIZE pad with slots [0..GROUP_SIZE) mirrored to [cap..cap+GROUP_SIZE) so SIMD load at any real slot is safe without a wrap branch. 8 TDD tests pass (insert/lookup, accumulation, grow, batch equivalence vs scalar, Q18-shape parity with RobinHoodI64F64). **Finding**: tagged probing wins decisively on low-cardinality / high-duplication aggregates (200K card / 30 rows/grp: **+38%**), borderline at Q17 shape (+15%, under 20% gate, ~−3.4% extrapolated Q17 wall-time), regresses at high-cardinality / low-duplication (15M card / 0.4 rows/grp: −19%). Stage-2 SIMD overhead per row dominates when most rows are cold inserts. Net Q17/Q18 wall-time wash with shape-blind wiring. Future: shape-aware routing (gate on estimated rows-per-group at plan time) could harvest Q17/Q05/Q08-class wins without Q18 regression — kept as infra for that work. Lever inventory entry but not wired. |
| L13 | **Filter-apply dispatch fix** — flip `load_row_group_parallel_bitmap_dense` from default-on (opt-out via `EMAT_NO_PARALLEL_BITMAP`) to default-off (opt-in via `EMAT_FORCE_PARALLEL_BITMAP`) | 🟢 WIN — **22q SF=10 ematix-flow/DuckDB geomean ≈ 0.94 (11 ematix wins, 9 DuckDB, 2 Polars)**. T2 scan-only (lineitem + date filter, SF=10) **7318 → 170 ms (43× speedup)**. Q07 SF=10 287 ms vs DuckDB 151 ms. Q18 SF=10 essentially parity (267 vs 257 ms). Q21/Q19/Q22 SF=10 all flip to ematix wins. | Σ.Q.L13 scan-only A/B (`crates/ematix-flow-core/examples/sigma_q_l13_scan_only_ab.rs`) compared ematix-parquet vs DataFusion's native parquet reader (`FastParquetTableProvider`) vs DuckDB on three lineitem SF=10 shapes (T1 decode-only, T2 decode+date-filter, T3 decode+date+IN-list). T1 showed ematix-parquet's decoder is competitive (1.45× DuckDB, matches FastP). T2/T3 showed ematix-parquet **63× DuckDB** while FastP stayed at 1.42× DuckDB — the 44× gap between ematix-parquet and FastP isolated the slow path to ematix-flow's `emat_arrow_reader::load_row_group_masked` dispatch. With `EMAT_NO_PARALLEL_BITMAP=1`, T2 ran at 168 ms — identified `load_row_group_parallel_bitmap_dense` (the work-stealing parallel bitmap+dense path) as the catastrophic offender at SF=10. Flipped default at `emat_arrow_reader.rs:1071` from `!disable_parallel` (opt-out) to `force_parallel` (opt-in). The earlier "Σ.E5 Phase 1.8 SF=1 22q geomean 0.89 → 0.856 win" justification was almost certainly inside [[optimizer-codegen-sensitivity]] noise — SF=10 reveals the actual cost. 14 emat_arrow_reader unit tests pass. Default ON in this commit (no env required); opt-in to old behaviour via `EMAT_FORCE_PARALLEL_BITMAP=1` for benchmarking. |
| L9.q21 | **L9 Inner-join correctness fix + default-off** (commit 162df4f) | 🟢 WIN — Q21 SF=10 row count: 1798 → **4009** (matches DuckDB). 22q SF=10 ematix-flow/DuckDB geomean **0.94 → 0.90** (12 ematix wins, 9 DuckDB, 1 Polars). All 22 row counts match DuckDB. | Q21 was returning 1798 vs DuckDB's 4009. Two bugs: (1) `rewrite_probe_subtree` matched scans by `path()` equality, so on Q21 (three lineitem references l1/l2/l3 with same parquet path) the sideband was multi-attached. Fix: capture original Arc in `find_probe_scan_for_column` and match by `Arc::ptr_eq` in `rewrite_probe_subtree`. (2) Even with ptr_eq, the supplier-built bloom filtered l1 from 60M → 3.70M (~4K Saudi-equivalent) — but the supplier scan is unfiltered at the bloom's point of construction (Saudi filter joins later via nation). Root cause not narrowed; disabling Inner-join firings restores correctness AND improves performance on other queries too (Q05 266→192ms, Q08 266→197ms — Inner-join L9 was net-negative). LeftSemi / RightSemi firings still default-on (where Q18/Q21 wins live). Inner accessible via `EMAT_RT_BLOOM_INNER_JOIN=1` for benchmarking. |
| L14 | **L9 col_idx file-schema fix** (commit c2c0520) + **`tpch_validate`** (commit e60c6af) | 🟢 WIN — Q07 SF=10 sums were 94% wrong with Inner-L9 ON (silent corruption masked by row-count-only bench). Fix: `find_probe_scan_for_column` returns the file-schema index, not the projected-schema index. New `tpch_validate` example collects DuckDB results as ground truth and compares ematix-flow values cell-by-cell with 1e-6 FP tolerance. 22/22 PASS at SF=1 and SF=10 after the fix. | `ColumnPredicate::I64InBloom { col_idx }` is consumed by `filter_i64_column_to_bitmap_dense` which reads `md.row_groups[rg].columns[col]` — requires the leaf parquet column index. L9 rule was threading the projected schema index. For Q07 lineitem projection=[0,2,5,6,10] and target_col_idx=1, the bloom applied to l_partkey (file col 1) instead of l_suppkey (file col 2). l_partkey ranges {1..2M} while supplier bloom contains {1..100K}, so ~95% of legit FK matches were dropped. The "Q07 -4.7% with L9 ON" claim from Σ.Q.L9 was a wrong-answer-faster claim. Inner-L9 still default-OFF post-fix because the L4'-style bloom-on-FK is still perf-negative when applied correctly (Q07 281→379 ms when bloom is built from unfiltered supplier). New regression-prevention infra (`tpch_validate`) catches this class of bug — runs the 22 TPC-H queries through DuckDB + ematix-flow, compares sorted cell-by-cell with FP rtol for f64s, exact match for everything else. |
| L15 | **Inner-L9 + ratio=1024 + all-tables-Emat** (commit 0de4ca7) | 🟢 WIN — **22q SF=10 ematix-flow/DuckDB geomean ≈ 0.83 (14 ematix wins, 6 DuckDB, 2 Polars)**. Q07 SF=10: 281 → 159 ms (**−43%**, 1.95× → 1.17× DuckDB). Q05 244→190, Q08 269→196, Q09 347→276 (flips to ematix win), Q03 196→145 (flips to ematix win), Q11 46→14, Q22 56→32. All 22 sum values match DuckDB. | Post-L14, the L9 sideband is now safe to fire on Inner joins. Three env-gated knobs combine: `EMAT_RT_BLOOM_INNER_JOIN=1` enables Inner firings, `EMAT_RT_BLOOM_RATIO=1024` gates out the L4'-style bloom-on-FK shape (s⋈l where supplier is unfiltered — 100K × 64 < 18M passes default gate but bloom passes ~100% via FK, net-negative), and `EMAT_ALL_TABLES_EMAT=1` registers supplier/customer/etc. via EmatixFastParquetTableProvider so L9's `find_probe_scan_for_column` can target them (lineitem+orders alone aren't enough — Q07's helpful firings are nation_filtered ⋈ supplier-chain pruning supplier 100K→8K at the scan). Three spike examples kept as documentation (`sigma_q_l15_collect_stats_spike`, `sigma_q_l15_tight_ratio_spike`, `sigma_q_l15_all_emat_spike`). All env-gated — production opts in. |
| L16 | **L9 sideband peek-wait timeout** (commit bf43dc0) | 🟢 WIN — Q17 SF=10: 281 → 215 ms (**−26%**, 1.69× → 1.31× DuckDB). 22q SF=10 geomean → **0.80** (14 wins, 6 DuckDB, 2 Polars). All 22 sum values still match DuckDB. | The Σ.Q.L9 deferred-peek fix from `e1f2d7d` deferred the probe peek to first poll but did not block. For small-build Inner-L9 firings (e.g. Q17 SF=10's filtered_part ⋈ lineitem where the build is 2K rows finishing in ~6 ms) `EMAT_L9_TRACE` showed 12 of 14 lineitem partitions peeking `None` and running unfiltered for 60 M rows. Fix: `BridgeFilterSideband` now wraps an `Arc<tokio::sync::Notify>` alongside the predicate slot. `publish()` calls `notify_waiters()`. New `wait_for_publish(timeout)` async method blocks until publish or timeout. The probe scan in `EmatixFastParquetExec::execute()`'s first poll calls this with a default 200 ms timeout (env override via `EMAT_L9_PEEK_TIMEOUT_MS`). Long enough for fast builds (Q17's is 6 ms), short enough to bound wall-time penalty on builds that exceed it. Post-L16, Q17 lineitem scan output is 60M → **61K** (3700× reduction in agg cost). |
| L17 | **Remaining gaps need structural work** (no commit) | 🟦 PROPOSED — three remaining SF=10 gaps above 1.10× (Q05 1.42×, Q06 1.19×, Q17 1.31×) all require deeper work than env knobs. | **Q05 (1.42×)**: 6-way star join with ASIA filter. Dominant op is `HashJoin(o⋈l_orderkey)` at 852 ms — build=date-filtered orders (2.28M), probe=lineitem (60M). DuckDB filters orders much more aggressively via dynamic-filter propagation from customer's nation filter. Closing this requires **join reordering / dim filter propagation** — a substantial logical-optimizer rule. **Q06 (1.19×)**: pure single-table scan+filter+sum (no joins). 14 ms gap is **ematix-parquet decoder-bound** — needs decoder-side optimization. **Q17 (1.31×)**: even with L16's bloom now correctly applied, the lineitem scan still has to decode l_partkey for all 60 M rows to evaluate the bloom — closing further needs **page-index-level dynamic pruning** (DuckDB's mechanism prunes whole pages via order-key range stats before decode). All three are bigger initiatives; none are quick-knob fixes. |

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

### Slice 4 — Deep probe-walker + shallow-build gate

**Hypothesis**: extend `find_probe_table_col` to descend through
Inner Joins (track which side's schema carries the target column),
and emit candidates in BOTH directions per `join.on` tuple so the
small-build side wins regardless of left/right placement in the
LogicalPlan. Gate build-side execution on `is_shallow_build_subtree`
(TableScan ± Filter / Projection / Limit / Sort / Distinct / Alias —
no Joins or Aggregates) to keep emission cheap.

**Implementation**:

- `local_bloom_emitter.rs`:
  - `collect_join_candidates` now emits two candidates per
    `(left_expr, right_expr)` — one per direction. Each is gated by
    `is_shallow_build_subtree(build_side)`.
  - `find_probe_table_col` adds a `LogicalPlan::Join(Inner)` arm
    that resolves which side carries `target_col` (checking both
    schemas) and recurses into that side.
  - `build_candidate` takes explicit `probe_plan` + `build_plan`
    parameters so the caller can run it once per direction.
- New unit test `descends_through_inner_join_to_deep_table` confirms
  a 3-table chain (`lineitem ↔ supplier ↔ nation`) produces a
  `lineitem.l_suppkey` bloom under the optimized plan.
- Renamed the cap test to `skips_when_both_sides_exceed_cap` and
  picked both-large data so the cap still verifies under
  both-direction emission.

**Sequence of bench results** on Q07/Q21 SF=10 (3 trials × 1 warmup,
single-process A/B, baseline = bloom OFF):

| Variant | Q07 (ms) | Q21 (ms) | Notes |
|---|---:|---:|---|
| Baseline (OFF) | 269.46 | 485.47 | reference |
| Slice 3 (walker stops at Join) | 287.31 (+6.6%) | 494.98 (+2.0%) | bloom never reaches lineitem |
| Slice 4 attempt A (deep walker, no shallow gate) | 677.76 (+136%) | 539.82 (+9%) | both-directions × deep-descent ⇒ pre-executing lineitem-bearing build subtrees |
| Slice 4 attempt B (deep walker + shallow-build gate) | 310.34 (+15.2%) | 481.94 (−0.7%) | mechanism correct; lever still not a TPC-H net win |

**Diagnosis of the residual Q07 NEG**: the genuinely high-value
bloom is `supplier WHERE s_nationkey ∈ filtered_nation` — the
post-join filtered supplier set, which has ~200 of 10K rows and
would prune lineitem strongly. But that build subtree is itself
a Join, excluded by `is_shallow_build_subtree`. Without the gate,
the emitter pre-executes Joins whose cost is comparable to the
original query (slice 4 attempt A). With the gate, only raw dim
tables become candidates, and their bloom isn't selective enough
to pay back the emission + bloom-probe-during-decode overhead.

**Decision**: Keep the predicate plumbing + rule + emitter + deep
walker + shallow gate behind the env-var. They're correct, unit-
tested, and the lever pays where probe IS direct TableScan
(star-schema fact↔dim shapes, distributed shipping via Σ.J.2.b.v).
For TPC-H single-node, the lever can only become a net win once
the build-side bloom is captured as a **side-effect of the
HashJoinExec build phase** rather than a separate pre-execution
pass — that's adaptive-query-execution work (Σ.L-class adaptive
runtime). Deferred.

### Σ.Q.L1b retry — Vectorised batch-ingest

**Status**: 🟢 WIN on Q18 SF=10 (−4.4%).

**Hypothesis** (from slice 3 NEG diagnosis): the scalar
`insert_or_sum` loop loses to DataFusion's stock `sum(f64)` because
stock GroupAccumulator does vectorised batch-update across 1K-row
chunks, while we call into a per-row function for every input row.
A Photon-style 4-stage batch-ingest pipeline should close the gap.

**Design**: `RobinHoodI64F64::insert_or_sum_batch_vectorised`
processes the input in 1024-row chunks:

1. Hash all keys → ideal-slot array.
2. Probe primary slot for direct hit (no displacement) → boolean
   hit array.
3. Fast-path accumulate hits — tight loop the compiler can
   autovectorise; ~70-80% of rows take this path at 70% load.
4. Scalar fallback for misses (insertions + collisions) via
   `insert_or_sum`. Stage 3's in-place writes survive any
   `grow()` triggered here because `grow()` re-inserts every
   existing bucket.

Bundled changes:

- `RobinHoodSumF64Agg::ingest_batch_vectorised(keys, values)` —
  null-aware wrapper that calls the kernel on the raw `.values()`
  slice when both columns are null-free (the Q18 case) and falls
  back to scalar per-row on null inputs.
- `RobinHoodSumF64Exec::try_new` derives `init_cap` from
  `input.partition_statistics().num_rows`: Partial mode uses
  rows/4 (assuming ~4 rows/group); FinalPartitioned uses
  rows×1 (one row = one group post-shuffle). Clamped to
  `[65_536, 32M]`. Eliminates the `EMAT_RH_SUM_F64_INIT_CAP`
  env var as a precondition for not paying the default-cap
  grow chain (which was the +15.9% in slice 3).
- Operator switches to the vectorised path by default
  (`EMAT_RH_SUM_F64_VEC=0` to revert to scalar for A/B).

**Code touched**:

- `crates/ematix-flow-core/src/robin_hood_agg.rs` —
  kernel + agg-level wrapper + 10 TDD tests.
- `crates/ematix-flow-core/src/robin_hood_sum_f64_exec.rs` —
  auto-sized `init_cap` field + vec/scalar toggle in `execute()`.
- `crates/ematix-flow-core/examples/robin_hood_sum_f64_batch_microbench.rs` —
  new SUM(f64) microbench mirroring the COUNT one.

**Kernel microbench (6M rows × 5-rep median)**:

| Cardinality | scalar pre-grown | vec pre-grown | Δ | vec default-cap vs scalar default-cap |
|---:|---:|---:|---:|---:|
| 200K | 137 M rows/sec | 143 M rows/sec | +4% | +3% |
| 2M | 58 M rows/sec | 60 M rows/sec | +3% | +27% |
| 15M | 41 M rows/sec | 47 M rows/sec | **+13%** | **+65%** |

Win scales with cardinality; the default-cap win is largest because
stage 3 bypasses the per-row `needs_grow()` branch that dominates the
scalar path under grow churn.

**Q18 SF=10 standalone (5 trials × 2 warmups)**:

| Config | Q18 wall-time | Δ vs OFF |
|---|---:|---:|
| OFF (stock DF) | 567.01 ± 8.73 ms | baseline |
| ON-scalar + auto-cap (no vec) | 604.69 ± 29.81 ms | +6.6% |
| **ON-vec + auto-cap** | **541.79 ± 7.05 ms** | **−4.4%** |

The ON-scalar+auto-cap row reproduces slice 3's `EMAT_RH_SUM_F64_INIT_CAP=2M`
result (+5.7%), confirming the auto-sizer correctly matches the
hand-tuned cap and that the kernel was the residual gap. The
vectorised path is what flips L1b from NEG to WIN.

**Why standalone Q18 isn't enough**: the rule install costs
~5-8% codegen perturbation across the rest of the binary per
[[optimizer-codegen-sensitivity]]. Decision: **rule stays opt-in**
(`EMAT_RH_SUM_F64=1`) per the codegen-tax pattern; the vectorised
kernel itself is default-on within the operator (revert via
`EMAT_RH_SUM_F64_VEC=0`) since it costs nothing when the rule
doesn't fire.

**22-query SF=10 geomean (5×2 trials, post-commit `f865779`)**:

  ON-vec / OFF = **1.0206** (+2.06% slower across all 22 queries).

The codegen tax shows up clearly: Q06/Q07/Q08/Q09/Q10/Q13/Q19 all
regress +5-13% even though the L1b rule never fires on those queries
(none of them are single-`SUM(Float64-col) GROUP BY Int64-col`).
Q18 itself in the 22q sequence runs +7.8% slower (566→610 ms) — the
standalone Q18 -4.4% win doesn't survive session-state context
(allocator/page-cache/codegen interaction with preceding queries).

This sharpens the Σ.R sequencing: **do not add a new optimizer rule**.
Σ.R.1 (radix) and any future variants must live inside the existing
`RobinHoodSumF64Exec` operator, gated by the same `EMAT_RH_SUM_F64=1`
flag. Adding a separate rule would compound the codegen tax.

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

---

## 2026-05-23/24 session — Σ.Q.M closeout + profile-guided pivot identified

### Final 22q SF=10 state

| Metric | Value | Source |
|--------|-------|--------|
| 22q ematix-flow/DuckDB geomean | **0.80** (ematix 20% faster than DuckDB) | post-L16 baseline |
| ematix wins | **14/22** | Q04, Q10, Q12, Q13, Q14, Q15 (Polars), Q16, Q17 (no), Q19, Q20, Q22, etc. |
| Correctness (`tpch_validate`) | **22/22** match DuckDB | cell-level, FP tol 1e-6 |
| Q05 SF=10 | 188 ms (1.32× DuckDB) | remaining structural gap |
| Q07 SF=10 | 159 ms (1.17× DuckDB) | acceptable |
| Q08 SF=10 | 202 ms (1.13× DuckDB) | acceptable |
| Q17 SF=10 | 215 ms (1.31× DuckDB) | hot kernel identified |
| Q21 SF=10 | 524 ms (1.30× DuckDB) | similar to Q17 shape |

### Σ.Q.M arc — three negative results

Attempted static-rewrite "redundant LeftSemi injection" — a static
analogue of DuckDB's dynamic-filter propagation. **All three slices
proved unviable in DataFusion's plan model.**

| Slice | Result | Commit | Why it failed |
|-------|--------|--------|---------------|
| Slice 1: shallow Filter→TableScan detection | Opt-in infra | [301e644](commit) | +3pp regression alone, no Q-level wins |
| Slice 2: depth-1 Inner Join descent in dim walker | REJECTED | [5b6b4a0](commit) | 22q geomean 0.80 → 1.02 (-27pp); double-eval of joined dim subtree |
| Slice 4 SPIKE: hardcoded orders→lineitem with minimal extracted dim | REJECTED | uncommitted | Q05 SF=10 188 → 218 ms (+16%); double hash-build (CSE doesn't share Join builds) |

**Conclusion**: every variant of redundant-semi injection pays either
double-evaluation of a joined dim subtree, double hash-build for the
same equi-keys, or has to remove a join key (would change Inner
semantics). DataFusion's `common_subexpression_elimination` does NOT
share Join outputs or HashJoin build sides — so any "let me re-use
this dim filter" rewrite ends up paying for the work twice.

### Join-reorder investigation — DataFusion is already correct

Hypothesis going in: Q05's 1.32× gap is because DataFusion picks the
wrong build/probe sides for the critical `orders ⋈ lineitem` join.
Mechanism: dump the physical plan with the new `EMAT_DUMP_PLAN=1`
helper and inspect HashJoinExec mode/side choices.

**Reality**: DataFusion already picks the correct order. The critical
join is `HashJoinExec(mode=Partitioned, build=customer⋈orders_filtered
[~2.3M rows], probe=lineitem [60M rows])`. Build is the small
filtered side, probe is the big fact table. This is what DuckDB does.

**So the gap is elsewhere**: lineitem decodes all 60M rows in the
probe phase because there's no mechanism to push the build's row
set into the parquet scan's page-skip. Closing this requires
adaptive-query-execution: the HashJoinExec build phase must emit a
bloom (as a side-effect, not a pre-execution pass) that the probe
scan consumes for actual page-skip. **Σ.L-class work, deferred.**

### Parquet decoder survey — already at LLVM cycle floor

Surveyed `ematix-parquet/crates/ematix-parquet-codec/` for hot-path
optimization opportunities that haven't been profiled at SF=10.
Inspected: varint decode, RLE inner loop, BYTE_STREAM_SPLIT, dict
gather, Snappy literal-run, page header parsing.

Selected Opportunity 1 (varint prefetch). Built `bench_varint`
microbench. Baseline: 0.70 ns/value for 1-byte (≈2.8 cycles on
M-series). Attempted two alternative implementations:

| Variant | 1byte ns | mixed ns | 10byte ns |
|---------|---------:|---------:|----------:|
| baseline (per-byte read_u8) | **0.70** | **0.75** | **2.25** |
| fast path `for i in 0..10` + `get_unchecked` | 2.26 | 2.00 | 2.64 |
| manual 10-step unroll (b0..b9 const shifts) | 2.36 | 1.98 | 2.37 |

**Both regressed**. Reason: LLVM is already eliding the per-byte
bounds check via inlining + CFG analysis. The baseline's tight
5-instruction inner loop is at the cycle floor.

**Conclusion**: code-inspection surveys can identify pattern
candidates but cannot judge LLVM's current optimization state.
Future opportunistic-wins searches in the decoder should be
**profile-guided**, not inspection-guided. The bench stays as a
regression guard.

Sister-repo commits: `bc8c4c3` on `feat/compression-hygiene`
(ematix-parquet) — bench infra + negative-result documentation.

### Profile-guided pivot — Q17 SF=10 hot kernels

Built `examples/profile_query.rs` (samply-compatible, full DWARF
debuginfo via `CARGO_PROFILE_RELEASE_DEBUG=true`) at commit
[35b433c](commit). 30-trial Q17 SF=10 profile, symbol resolution via
samply's `--unstable-presymbolicate` sidecar.

Top hot kernels by self time (excluding idle):

| Function | Self % | Class | Actionable? |
|----------|-------:|-------|-------------|
| `__psynch_cvwait` | 31.8% | OS thread idle | No (pool overhead between trials) |
| **`GroupValuesPrimitive::intern`** | **21.6%** | DF GROUP BY i64 hash | **YES — top lever** |
| `snap::decompress::Decoder::decompress` | 9.8% | parquet Snappy | Rejected (hand-roll -12% on Q14) |
| `AvgGroupsAccumulator::merge` | 5.0% | DF AVG kernel | Overlaps #2 |
| `arrow_select::take::take_native` | 5.0% | Arrow take | Maybe |
| `BatchPartitioner::partition_iter` | 2.0% | Hash repartition | Unclear |
| `hash_single_array` | 1.8% | DF hash function | Unclear |
| `ematix_parquet_codec::bitpack_neon::unpack_lookup_into_neon_bw6` | 1.5% | parquet bit-unpack | Already SIMD-optimal |

ematix-parquet kernels collectively account for **~3% of self time**
on Q17 — the decoder is doing its job. The remaining structural gap
lives in DataFusion's GROUP BY i64 hashing.

### Recommended next session: Σ.R.2 — RobinHoodAvgF64Exec

Profile says the highest-EV remaining lever is replacing DataFusion's
`GroupValuesPrimitive` (the i64 GROUP BY hash) with a Robin Hood
variant that handles `AVG(f64) GROUP BY i64` — extending the existing
[`RobinHoodSumF64Exec`](../crates/ematix-flow-core/src/robin_hood_sum_f64_exec.rs)
pattern that already wins 1-5% on `SUM(f64)`.

Estimate: ~1700 lines of TDD work across kernel + exec + rule + bench.
6-10 hours of focused work. Multi-session project.

### Q05 / Σ.L adaptive-runtime — deferred

Q05's 1.32× gap is structurally bounded: closing it requires
adaptive-query-execution where the HashJoinExec build phase emits a
bloom (as a side-effect, not a pre-execution pass) that the probe
scan consumes for page-skip. This is the path DuckDB and Photon take
internally. Multi-week phase, deferred.

### Infrastructure landed this session

| Item | Location | Purpose |
|------|----------|---------|
| `EMAT_DUMP_PLAN=1` env switch | [tpch_validate.rs](../crates/ematix-flow-core/examples/tpch_validate.rs) | Optimized logical + physical plan dump |
| `profile_query` example | [profile_query.rs](../crates/ematix-flow-core/examples/profile_query.rs) | Samply-compatible per-query profiling |
| `synthetic_left_semi_rule` (Σ.Q.M Slice 1) | [synthetic_left_semi_rule.rs](../crates/ematix-flow-core/src/synthetic_left_semi_rule.rs) | Opt-in dim→fact LeftSemi producer (inert, kept as infra) |
| `bench_varint` regression guard | ematix-parquet `feat/compression-hygiene` | Documents LLVM-elided bounds-check baseline (0.70 ns/1-byte) |

### Commit chronology

```
35b433c profile_query: minimal binary for samply-based per-query profiling
072b92d tpch_validate: extend EMAT_DUMP_PLAN to include physical plan
0a4baee tpch_validate: EMAT_DUMP_PLAN env switch for plan-shape investigations
5b6b4a0 Σ.Q.M Slice 2 REJECTED: depth-1 Inner Join descent regresses 22q SF=10
301e644 Σ.Q.M Slice 1: SyntheticLeftSemiRule (opt-in) + TableProvider stats
```

Sister repo (ematix-parquet, `feat/compression-hygiene`):

```
bc8c4c3 perf: bench_varint regression guard + negative-result documentation
```

---

## 2026-05-24 session — Σ.R.2 RobinHoodAvgF64Exec REJECTED

### Build

TDD bundle landed across three slices in one session (4 hours, not 6-10
as the closeout estimated — most of the savings came from copying the
`RobinHoodSumF64Exec` pattern verbatim with bucket-shape changes):

| Slice | Module | LOC | Tests |
|-------|--------|-----|-------|
| Σ.R.2.a kernel | [robin_hood_agg.rs](../crates/ematix-flow-core/src/robin_hood_agg.rs) (+715) | `RobinHoodI64AvgF64` (bucket = `(key i64, sum f64, count u64, psl u32)`) + `RobinHoodAvgF64Agg` streaming agg, vectorised + scalar paths | 18 unit |
| Σ.R.2.b exec + Σ.R.2.c rule | [robin_hood_avg_f64_exec.rs](../crates/ematix-flow-core/src/robin_hood_avg_f64_exec.rs) (new, 624) | `RobinHoodAvgF64Exec(Partial → (k, sum, count); FinalPartitioned → (k, avg))` + `EnableRobinHoodAvgF64Rule` opt-in via `install_robin_hood_avg_f64_rule` | 5 integration |
| Σ.R.2.d bench | [sigma_r2_q17_ab.rs](../crates/ematix-flow-core/examples/sigma_r2_q17_ab.rs) | Q17 SF=10 5×2 A/B harness with plan-check sanity, OFF #1 → ON → OFF #2 ordering to spot drift | n/a |

All 952 crate-wide tests pass. Operator + rule are opt-in only (per
[[optimizer-codegen-sensitivity]]); env toggles `EMAT_RH_AVG_F64_VEC`
and `EMAT_RH_AVG_F64_INIT_CAP`.

### Q17 SF=10 gate result — REJECTED

5 trials × 2 warmup, M3 Pro, release+LTO. Plan check confirmed
`RobinHoodAvgF64Exec` is present under ON, absent under OFF.

| Config | OFF #1 (ms) | ON (ms) | OFF #2 (ms) | Δ vs OFF #1 |
|---|---:|---:|---:|---:|
| Default (vec=on, init_cap=auto) | 410.5 | 603.2 | 412.2 | **+46.9%** |
| `EMAT_RH_AVG_F64_VEC=0` (scalar) | 422.0 | 638.9 | 411.8 | **+51.4%** |
| `EMAT_RH_AVG_F64_INIT_CAP=4194304` (pre-sized for 2M groups) | 407.7 | 570.8 | 399.1 | **+40.0%** |

Three independent dials confirm a +40-55% regression. The pre-sized
table modestly lessens the gap (no grow chain), but the operator is
still 163 ms slower than DataFusion's stock AVG kernel on Q17. Pre-gate
threshold was ≥10% improvement; we are firmly on the wrong side. **22q
SF=10 publishable run skipped.**

### Why it fails — split-vs-fused pipeline shape

The Σ.Q profile correctly identified `GroupValuesPrimitive::intern` at
21.6% self time. The mistake was assuming the SUM-operator pattern
("hash + probe + accumulate inside the bucket, one row at a time")
would transfer at Q17's cardinality (~2M distinct `l_partkey`).

DataFusion's stock AVG pipeline splits the work into two SIMD-amenable
batch passes:

1. `GroupValuesPrimitive::intern(keys)` — hash-and-lookup over the
   whole batch, returns a `Vec<group_idx>`.
2. `AvgGroupsAccumulator::update_batch(values, group_indices)` —
   indexed scatter: `sums[g[i]] += values[i]; counts[g[i]] += 1`.

Each pass is tight and L1-resident. The accumulator arrays live
contiguously in DRAM; the group_idx pass touches them sequentially.

Our pipeline fuses both inside `insert_or_update`:

1. Hash key, probe primary slot, possibly chain.
2. Accumulate into the matched bucket (sum += value, count += 1)
   **in the same loop**.

At 2M groups × 32 B/bucket = 64 MB the table blows L2 (24 MB on M3
Pro), so every per-row probe pulls a cold cache line. The vectorised
4-stage variant doesn't help because Stage 4 (chained probes for non-
primary-slot keys) still pays the cold-cache cost row-by-row, and at a
non-empty table the Stage 2 primary-hit rate is just the 70%-load
bound — Stage 4 still fires for ~30% of rows.

**Lesson**: 21.6% self time in `GroupValuesPrimitive` does NOT mean
"replace it and reclaim 21.6%." Most of that share is the unavoidable
hash-and-lookup that any replacement also pays. The remaining ~5% is
accumulator merge work that's already SIMD-optimal in DataFusion. The
profile pointed at a high-share kernel; it did not predict whether
replacing it with the SUM pattern would win.

This is the AVG-specific analogue of the Σ.Q.L11 / Σ.Q.L12 lesson —
plan-marker / profile-driven hypotheses still need a cheap kernel-
level A/B before committing to the operator build.

### What stays

The operator + rule + tests + bench are all kept, opt-in via
`install_robin_hood_avg_f64_rule`. They are:

- **Reusable infra** for a future SoA-layout redesign that splits the
  hash table from the accumulator arrays (matches DataFusion's
  pipeline shape, may win when the accumulator vectors are i64/u64
  rather than f64). Worth retrying if the underlying GroupValuesPrim
  changes shape upstream.
- **Regression guard** — the 23 unit + integration tests verify the
  kernel's correctness against DataFusion stock at multiple
  cardinality / partition shapes, useful if anyone tries this lever
  again.

### Decision: where next

| Option | Why considered | Why not now |
|---|---|---|
| Σ.R.2 fused operator | profile-identified | rejected this session, +40% Q17 |
| Σ.R.2′ SoA-layout redesign (split intern from accumulate) | matches DF pipeline, may close the gap | ~3-5 more days of work for an uncertain 60ms win; not the next-best EV |
| **Q06 SF=10 decoder lever** | scan-bound, 14ms gap, single-table | clearest, smallest-scope win; ematix-parquet sibling repo |
| Σ.L adaptive-runtime (Q05) | structural | multi-week phase, deferred per closeout |

Next-session pick: **Q06 SF=10 decoder lever** in the
[[ematix-parquet-repo]] sibling. Smaller scope, clearer profile-to-fix
mapping than another GroupValuesPrimitive attempt.

### Commit chronology

```
(this session, pending) Σ.R.2.a kernel + Σ.R.2.b/c exec+rule + Σ.R.2.d bench
                        — REJECTED Q17 SF=10 +40-55%; kept opt-in
```

---

## 2026-05-24 follow-on session — Q06 LZ4_RAW + L9 evolution

### Q06 LZ4_RAW investigation

Pursued [[q06-sf10-polars-gap-wall]] / [[ematix-parquet-lz4-decode-bug]]
levers. Result: codec migration is net-positive for DuckDB but
net-negative for ematix on the natural DuckDB-COPY output (Optional
columns force a V1 level-skip path costing ~30 ms wall at SF=10).

The investigation surfaced TWO latent bugs:

| Side | Bug | Fix |
|------|-----|-----|
| ematix-flow | `emat_page_stream::decompress_into` missing LZ4_RAW arm — non-masked column reads cleanly rejected the codec | LZ4_RAW arm + `uncompressed_size` threaded through 4 call sites |
| ematix-parquet (sibling) | V1 data pages of Optional columns wire `[rep_lev RLE][def_lev RLE][values]`; `data_page_view` passed the whole decompressed body as values | Added `compute_max_levels` + `skip_v1_level_prefixes` (no Vec<u16> materialisation); threaded `(max_rep, max_def)` through 15+ call sites |

Sibling fix landed on `feat/compression-hygiene` as commit `16c912b`,
shipped in v0.15.0 release commit `de9b073` + tag `v0.15.0` (local-
only, awaiting push to trigger crates.io publish). Until then,
ematix-flow uses a `[patch.crates-io]` block to point at the local
clone.

Net: NO production perf change. The fix unbreaks a class of valid
parquet files (V1 + Optional + any codec, exposed via LZ4_RAW DuckDB
COPY) that ematix-flow could previously corrupt silently.

### Σ.Q.L9.HashSet — exact i64 set for small builds

Q17 fresh profile (2026-05-24) showed `BloomFilter::might_contain_hash`
at 13.4% self-time as the new top hot kernel (the closeout's #1,
`GroupValuesPrimitive::intern`, dropped out of the top 30 — the
L9/L10/L15/L16 stack eliminated most of its work).

Microbench at Q17 shape (2K-key build, 1M probes, ~0.1% hit):

| Path | ns/probe | FP rate |
|---|---:|---:|
| BloomFilter::might_contain_i64 (legacy) | 17.2 | 1.13% |
| std HashSet (SipHash) | 5.1 | 0% |
| Manual i64 open-addr table | **1.3** | **0%** |

Built `crates/ematix-flow-core/src/i64_set.rs`: `I64Set` open-
addressing exact-membership table, multiply-shift hash, 50% load
cap, `i64::MIN` sentinel. 11 unit tests including a parity test vs
`std::collections::HashSet`.

Wired into `BuildSideBloomEmitterExec`: each partition maintains
BOTH a bloom AND an `Option<I64Set>` — the set is dropped once it
overflows `EMAT_L9_SET_THRESHOLD` (default 32K = 256 KB I64Set).
At finalize, if every partition kept its set AND the union stays
under threshold, publishes the new `ColumnPredicate::I64InSet`
(exact membership, single hash + lookup); otherwise falls back to
`ColumnPredicate::I64InBloom` (existing path).

22q SF=10 result (5×2 trials):

| Metric | Pre-HashSet | Post-HashSet | Δ |
|---|---|---|---|
| ematix-flow/DuckDB geomean | 0.77 | **0.75** | −2pp |
| ematix wins | 16/22 | **17/22** | +1 (Q17 flipped) |
| **Q17** | 211 ms (1.26× DuckDB) | **159 ms (0.98×)** | **flipped to ematix win, −25%** |

### Lever 1 — `#[inline(always)]` on bloom hot path

Added `#[inline(always)]` to `BloomFilter::might_contain_hash`,
`might_contain_i64`, `block_idx`, and `hash_i64`. Microbench Q17-
shape: 17.2 → 15.6 ns/probe (−9%). Helps Q07/Q08/Q21-shape builds
(>32K keys → bloom path). Zero risk; ships default-on.

### Lever 3 — `I64Range` predicate (INFRA-ONLY, default-off)

Designed `ColumnPredicate::I64Range { col_idx, lo, hi }` to mirror
DuckDB's dynamic range pushdown alongside the bloom (per the Q05
DuckDB EXPLAIN ANALYZE showing `l_partkey IN BF AND l_partkey >= N
AND l_partkey <= M`).

Wired through:
- `BuildSideBloomEmitterExec` accumulates per-partition `(min, max)`
  in a tight pass alongside the existing bloom insert loop.
- `BridgeFilter::build_bitmap` dispatch: reads `rg_i64_min_max`
  from parquet column-chunk stats and short-circuits to all-zeros
  bitmap (no decode) when the RG range doesn't overlap `[lo, hi]`.
  Also handles "fully inside" (all-ones, skip per-row check) and
  "partial overlap" (per-row check) cases.

Bench result: NET-NEGATIVE at the always-on default. Q17 SF=10 went
211 → 244 ms (+15%); Q05/Q07/Q08 also slower. Root cause: the extra
predicate triggers an extra `ParquetFile::open` + metadata read per
RG (~800 extra opens at 14 partitions × 58 RGs across the typical
query), and on TPC-H the build's value range typically overlaps
~100% of the column distribution (filtered_part keys span ~100% of
l_partkey), so no RG can actually be skipped.

Decision: kept the infra (predicate variant + RG-skip dispatch + RG-
stats helpers + accumulator) gated behind `EMAT_L9_EMIT_RANGE=1`.
Future fix: cache parquet metadata across `build_bitmap` calls so
the extra predicate doesn't double-open the file. Until then, the
emit path is a no-op default-off.

### L9.SelectiveBuild — "fire only when build subtree has FilterExec"

Q05 fresh profile + DuckDB plan diff: DuckDB closes Q05 via a
**cascading dynamic-filter chain** (region → nation → customer →
orders → lineitem), pushing both a bloom AND a min/max range down
to every scan. ematix's L9 ratio gate at 1024 blocks the firings on
the central c⋈o and o⋈l joins (build_rows × 1024 ≥ probe_rows).
Lowering the ratio fires more joins but regresses Q05/Q08/Q17
because firing on FK-shape joins (build unfiltered → bloom passes
~100% by referential integrity) is pure overhead.

Added `EnableRuntimeBloomSidebandRule.require_filtered_build` flag
+ `build_subtree_has_filter(plan)` walker. When set, Inner-join L9
firings are gated on whether the build subtree contains at least
one `FilterExec`. LeftSemi/RightSemi unaffected (intrinsically
selective).

Bench result at default ratio=1024: filter gate is largely inert
(the ratio gate already does its job; small Q05/Q08 gains within
noise). At lower ratios the gate alone doesn't fix the
net-negative — see Σ.S design doc for the deeper analysis.

Decision: shipped default-on (zero cost at default ratio; safety
net for future ratio tuning). Disable via `EMAT_L9_REQUIRE_FILTERED_
BUILD=0`. Also added `EMAT_L9_TRACE=1` diagnostic mode (one-line
stderr per HashJoinExec visit explaining fire/skip).

### Σ.S.A — Apache Impala "splash" bloom layout (LANDED)

Bloom layout swap: sequential multiply-shift + early-out (legacy) →
Apache Impala "splash" (256-bit block = 8 × u32 lanes; 8
independent salted hashes per probe; no early-out, fully data-
parallel).

`SPLASH_SALT` constants per Daniel Lemire's "Cache-, Hash- and
Space-Efficient Bloom Filters" (Daniel Lemire / Apache Impala).
Each hash i maps deterministically to its OWN u32 lane via
`(h32 * SALT[i]) >> 27` → 5-bit position in that lane. Probe:
`(block AND mask) == mask` per lane → OR-reduce of mismatches.

Microbench (M3 Pro, 2026-05-24):

| Workload | Legacy | Splash | Speedup | FP Δ |
|---|---:|---:|---:|---:|
| Q17-shape (2K keys, 0.1% hit) | 15.66 ns | **1.54 ns** | 10.15× | 8.8× lower |
| Q05-o⋈l-shape (2.3M keys, 15% hit) | 12.60 ns | **1.69 ns** | 7.45× | 4.3× lower |
| FK-shape (1.5M keys, 100% hit) | 5.40 ns | **1.61 ns** | 3.36× | both 0 |
| Q21-shape (500 keys, 0.05% hit) | 12.94 ns | **1.38 ns** | 9.36× | 11× lower |

Wire format magic bumped `EBLM0001` → `EBLM0002`. Cross-stage
distributed deployments must roll forward both sender and receiver
together; a v0001 sender → v0002 receiver gets `BadMagic` and the
receiver proceeds without that bloom (no wrong answers — selectivity
just doesn't propagate that hop).

22q SF=10 wall time: unchanged at **0.75 geomean**. The 3-10×
microbench gain doesn't translate to wall time at the default
ratio=1024 because only supplier-side firings happen there, and
those were already a small fraction of total time. Splash is
infrastructure for Σ.S.B + a future-proof layout match for what
every modern engine uses (Impala, Photon, Velox).

### Σ.S.B — Cascading L9 (PLANNED, NOT YET BUILT)

See `docs/PHASE_SIGMA_S_PIPELINED_SCAN_FILTER_JOIN.md`.

The real Q05 lever. Walks past the immediate probe scan to attach
sidebands to **downstream** scans in the same FK chain, matching
DuckDB's region → nation → customer → orders → lineitem cascade.
Multi-week effort with careful tests for correctness (Σ.Q.L14
col_idx bug is the canonical "silent correctness" trap).

### Final 22q SF=10 state at session close

| Metric | Value | Notes |
|---|---|---|
| 22q geomean (ematix/DuckDB) | **0.75** | 25% faster than DuckDB |
| ematix wins | 17/22 | Q01/Q03/Q06/Q17 are noise-band races vs DuckDB |
| Correctness | 22/22 cell-by-cell | tpch_validate with full env stack |
| Test suite | 977 lib tests + 22 oracle | all green |
| Remaining gap queries | Q05 1.27×, Q07 1.10×, Q08 1.09× | Σ.S.B is the lever for all three |

### Commit chronology (this session, perf/sigma-q-single-node-parity)

```
0ae9b22  Σ.R.2 RobinHoodAvgF64Exec — REJECTED Q17 SF=10 +40-55%
(pending) Q06 LZ4_RAW investigation — bridge LZ4 arm + V1+Optional fix
(pending) Σ.Q.L9 evolution: splash bloom + HashSet + selective build + range infra
```

Sister repo (`ematix-parquet`, `feat/compression-hygiene`):

```
de9b073  release: v0.15.0 — V1 page-body level-prefix fix + varint regression guard
16c912b  fix: strip rep+def level prefixes in V1 data pages
bc8c4c3  perf: bench_varint regression guard + negative-result documentation
```

Tag `v0.15.0` exists locally, NOT pushed. Pushing it triggers the
release workflow that auto-publishes 5 crates to crates.io.
