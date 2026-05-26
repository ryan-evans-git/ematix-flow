# Σ.AH.3 — Build-vs-probe side-swap optimizer rule

**Status:** drafted, not active
**Parent:** [`docs/PERF_REVIEW_2026_05.md`](../PERF_REVIEW_2026_05.md) (Phase C ranking #3 by query count)
**Hypothesis:** Five queries currently exhibit Inner-join build-vs-probe side mis-ordering — DataFusion's planner picks the larger table as build, causing build-cost domination and L2/L3 cache spills. A pre-plan walker that swaps sides when post-filter build cardinality exceeds probe cardinality (by a margin) will recover ~60-80 ms wall across the 5 queries.
**Queries impacted:** Q07 (depth 13: build 1.46M > probe 117k), Q08 (depth 13: build 1.5M > probe 122k), Q09 (depth 9: build 15M = 120 MB > probe 3.26M = 78 MB, biggest absolute), Q10 (cust ⋈ orders: build 1.5M > probe 573k), Q14 (part ⋈ lineitem-filt: build 2M > probe 749k).
**Predicted impact range:** **~60-80 ms wall across 5 queries** = **2-3 pp SF=10 geomean**.
**Effort estimate:** 2-3 person-weeks (rule + post-filter cardinality estimator + correctness suite — Inner-only initially).
**Risk level:** **L-M** for Inner-only scope; jumps to **H** if we extend to Left/Right/Full joins.

## Bench gate (ship-if / reject-if)

### Microbench
- No new kernel. The arc is purely planner-level rewrite.
- **Correctness:** all 22 TPC-H queries return byte-identical results to the pre-swap baseline. Hard rule — any deviation is a reject.

### Wall-time
- **Required:** Q09 SF=10 wall drop ≥ **20 ms** (the biggest single target) AND Q07+Q08+Q10+Q14 collectively drop ≥ **30 ms** AND 22q SF=10 geomean improves by **≥ 1 pp** AND no single query regresses by **> 3%** (tighter than other arcs because side-swap touches more queries).
- **Tighter bar on regression** because the swap rule may inadvertently fire on queries we already optimised — e.g., Q03's cust+orders ⋈ lineitem currently picks the smaller side as build correctly.

### Reject-if
- Any query regresses > 3%, OR
- Correctness fails on any query (especially Q05/Q21 — composite-key Inner joins where side-swap semantics are subtler), OR
- Codegen tax from rule installation costs > 2 pp geomean (per `[[optimizer-codegen-sensitivity]]`).

## Hard constraints (inherited)

- **No new PhysicalOptimizerRule** — implement as a **pre-plan walker** running between `df.into_optimized_plan()` and physicalization. The walker inspects LogicalPlan Inner joins, looks at post-filter cardinality stats from `EmatixFastParquetTableProvider::partition_statistics`, swaps sides if build > probe by ≥ 2× margin.
- **TDD** — write correctness tests against all 22 queries BEFORE the swap logic lands. Each test verifies that the swap-applied plan returns identical results to the no-swap plan on a representative slice of SF=1 data.
- **No TPC-H-specific hardcoding** — the swap heuristic is "build > probe × 2.0" with no per-query carve-outs.

## Story skeleton (no tasks)

- **Story 1 — post-filter cardinality estimator audit.** Σ.AE.1 already added `partition_statistics` with `estimate_dropped_filter_pass_rate` (memory `[[sigma-ae-complete]]`). Verify this gives accurate post-filter row counts for each TPC-H table after typical filters. If the estimator under-counts (e.g., on string-LIKE), the swap rule fires incorrectly. Audit + fix before writing the swap rule.
- **Story 2 — pre-plan walker scaffold.** Walk LogicalPlan; for each `LogicalPlan::Join(Inner)`, compute estimated post-filter row counts for both children. If `right_rows × 2 < left_rows` (right is probe by convention; left is build), emit a swapped Join with sides reversed. Correctness tests: all 22 queries return identical results.
- **Story 3 — wall-time bench + gate.** Land Story 2 behind `EMAT_SIDE_SWAP=1`. 22q SF=10 bench; verify per-query no-regression bar (3% — tighter than other arcs).
- **Story 4 — composite-key safety.** Q05/Q21 have multi-key Inner joins where swap semantics need care. Add a guard: only swap single-equijoin-key Inner joins for the first ship. Story 4 lifts the guard later if testing covers composite keys.
- **Story 5 — soak + default-on flip.** Same pattern as Σ.AG.7 / Σ.AH.2.

## Risks + watch-items

- **Cardinality estimator under-count → false-swap → regression.** Q03's cust+orders ⋈ lineitem currently picks the cust+orders side (1.46M) as build correctly because lineitem post-filter is 32M; if the estimator under-counts lineitem post-filter, the rule might wrongly swap.
- **Σ.T deferred for a reason** (memory `[[archived sigma-t]]`). This arc is the narrowest, lowest-confidence slice of Σ.T. If the broader Σ.T turns out to be needed anyway, this work either composes (good) or gets superseded (acceptable).
- **Codegen tax from yet-another pre-plan walker.** Per `[[optimizer-codegen-sensitivity]]`, the walker count matters. Watch baseline geomean during Story 2 install — even a no-op walker that just traverses the tree adds branches. Mitigation: walker is gated on `EMAT_SIDE_SWAP=1` until soak finishes.
- **Composite-key joins** (Q05 supplier 2-key, Q09 partsupp 2-key) are explicitly out of scope for the initial swap. The Story 4 guard makes this safe.
- **Build-side projection requirements.** DataFusion projects the build side's columns differently in some cases (NULL semantics on Left/Right joins). Inner-only scope avoids this — if Story 4 extends to other join types, the projection-rewrite is the hard part.
- **Q14 has an existing inject rule (`InjectFusedQ14`?) that may interact with the swap.** Audit during Story 2 — does Q14 already get pre-swapped via another rule? If yes, the new arc must not double-swap.

## References

- Phase C ranking entry: [`docs/PERF_REVIEW_2026_05.md`](../PERF_REVIEW_2026_05.md) Σ.AH.3 section
- Post-filter cardinality work (precondition): memory `[[sigma-ae-complete]]`
- Σ.T cost-based reorder (parent arc — superset): archived plan `docs/plans/archive/2026-05-25-sigma-t-v5-tier-1.md`
- Codegen-tax precedent: memory `[[optimizer-codegen-sensitivity]]`
- Per-query evidence: [`docs/PERF_Q07.md`](../PERF_Q07.md), [`docs/PERF_Q08.md`](../PERF_Q08.md), [`docs/PERF_Q09.md`](../PERF_Q09.md), [`docs/PERF_Q10.md`](../PERF_Q10.md), [`docs/PERF_Q14.md`](../PERF_Q14.md)
- Related rejection (re-look flag): `[[sigma-q-l2-rejected]]` semi-join swap — different shape but conceptually adjacent
