# Σ.AJ.1 — Q17 operator-level scoping

**Status:** **CLOSED POSITIVE 2026-05-27** — Lever C (Σ.U Phase 1 + bench-harness fix) ships default-ON. Q17 SF=10 -103.6 ms (-37%), 22q SF=10 net -225.9 ms (-6.64%), 3 wins (Q08/Q17/Q18), zero >2σ regressions. See `[[sigma-aj-1-lever-c-spike]]` and `docs/PHASE_SIGMA_AJ_1_LEVER_C_SPIKE.md` for the 1-day spike outcome that revised the estimate from 1-2 wks (because the rule was already implemented as Σ.U Phase 1, dormant behind `EMAT_AGG_SEMI=1`). Lever A (neutral), B (+60% rejected), D (EXPLAIN ANALYZE no kernel lever) all closed.
**Active plan:** [docs/plans/CURRENT.md](plans/CURRENT.md).
**Companion methodology:** `scripts/bench/strict_ab.sh` (interleaved A/B strict bench).

## Baseline (post-Lever A default-on)

Q17 at SF=10, ematix-flow vs DuckDB:

| Engine | Q17 median | Gap |
|---|---:|---:|
| ematix-flow | ~275-310 ms | reference |
| DuckDB | ~160-165 ms | -110 to -150 ms (-40% to -50%) |

Wall is ~275 ms; total elapsed_compute is ~6970 ms (25× parallel overlap on 14 cores).

## Q17 plan structure (current default + Lever A)

The query decorrelates to an Inner join between:
- LEFT: `part_filtered ⋈ lineitem` projected to (p_partkey, l_quantity, l_extendedprice)
- RIGHT: `__scalar_sq_1 = SELECT 0.2 * avg(l_quantity), l_partkey FROM lineitem GROUP BY l_partkey`
- With filter `l_quantity < __scalar_sq_1.0.2 * avg(l_quantity)`

Stage profile from the Σ.AH.1 spike (Mode B = Lever A on, the new default):

| Operator | Depth | elapsed_compute | Output rows | Notes |
|---|---:|---:|---:|---|
| HashJoinExec | 4 | **4367 ms** | 5526 | outer Inner join with non-equi filter |
| AggregateExec | 6 | **1994 ms** | 2,000,000 | FinalPartitioned AVG GROUP BY l_partkey |
| EmatixFastParquetExec | 8 | 155 ms | 59.99M | outer lineitem scan |
| Total compute | | **6970 ms** | | |
| Wall median | | **276 ms** | | (25× parallel overlap) |

**91% of compute** is in two operators: the outer HashJoin (4367 ms, 63%) and the AVG aggregation (1994 ms, 29%). The lineitem scans together are ~300 ms (4%).

## The structural waste

The AVG subquery computes `AVG(l_quantity) GROUP BY l_partkey` for **all ~200k distinct partkeys**. But only ~200 of those partkeys survive the `p_brand='Brand#23' AND p_container='MED BOX'` filter. **~99.9% of the AVG aggregation work is thrown away** before the outer join's first row is produced.

DuckDB's Q17 is 1.7× faster largely because it pushes the part-filter into the subquery — its decorrelation rewrites the subquery to only compute AVG for partkeys matching the filter.

## Live measurement: L9 doesn't fire on Q17 by default

Tracing the L9 bloom-sideband rule on Q17 (fresh measurement 2026-05-27):

```
[L9.trace] matched key l_partkey → probe scan @ col_idx=1
[L9.trace] Inner join — build_rows=Some(399994) probe_rows=Some(59986052) ratio_gate=1024
[L9.trace] skip — gate rejects: b(399994) × ratio(1024) >= p(59986052)
[L9.trace] skip Inner — build subtree has no FilterExec (require_filtered_build=true)
```

Two L9 attempts, both rejected:
1. **Outer Inner join** — rejected by ratio gate because DataFusion's selectivity estimator gives build_rows=400k (intermediate of `part⋈lineitem`). Actual post-filter cardinality is ~2000 lineitem-rows-with-matching-partkey.
2. **Subquery Inner join** — rejected by `require_filtered_build` (the subquery's AVG isn't a FilterExec).

With `EMAT_L9_TIGHT_CARDINALITY=1` (Σ.AH.2 Story 1'.3, currently opt-in):

```
[L9.trace] Inner join — build_rows=Some(2000) probe_rows=Some(59986052) ratio_gate=1024
[L9.trace] WRAP Inner join — expected_keys=2000
```

The estimator correctly drops to 2000. L9 fires on the outer join (bloom of ~142 keys per partition). **The subquery still doesn't get the bloom** — `require_filtered_build` rejects it because the AVG subquery doesn't have a FilterExec on its build path.

## Ranked levers

### A. Re-validate `EMAT_L9_TIGHT_CARDINALITY=1` under interleaved A/B

**Mechanism**: existing opt-in lever from AH.2 Story 1'.3. With Lever A's fused-probe now default-on, the prior Q08 +55ms regression that blocked tight cardinality may have changed character. Worth a fresh interleaved A/B before any new code.

**Expected impact**: unknown. The outer L9 (200-row bloom over 60M lineitem probe = 0.003% selective) is exactly the shape where fused-probe wins on Q21 (-50ms). May produce a similar effect on Q17.

**Effort**: zero new code. ~25 min strict_ab.sh bench.

**Risk**: low. Already-banked machinery; we just measure under the new methodology.

### B. Cascade the part-filter bloom into the subquery's lineitem scan

**Mechanism**: Extend `EnableCascadingBloomRule` to walk the LogicalPlan and identify "same parquet path, different join lineage" sibling scans. The bloom built from `part_filtered ⋈ lineitem` (~2000 lineitem rows) carries the partkeys of interest; cascade it to the subquery's lineitem scan so the AVG aggregation only operates on rows whose l_partkey is in the bloom.

**Expected impact**: AggregateExec input drops from 60M → ~2000 rows. Even if the bloom check costs ~5 ms / partition (per Σ.AH.2 fused-probe measurements), the AVG compute drops ~99%. Net savings: most of the 1994 ms AggregateExec compute. After parallel overlap, **~80-120 ms wall**.

**Effort**: 3-5 days. Need to:
1. Modify `fk_chain` walker to detect sibling-same-parquet shapes (Q17's subquery vs outer)
2. Extend cascade emitter to propagate to subquery scans
3. Verify no double-counting / correctness regressions

**Risk**: M. The existing self-join sibling-skip guard (`[[sigma-s-b-fix]]`) currently rejects this exact case (same parquet path). Lifting the guard for non-self-join shapes is delicate.

### C. Predicate pushdown across decorrelated subquery (logical rewrite)

**Mechanism**: A LogicalPlan walker that pushes the part-filter (`p_brand AND p_container`) into the subquery as a `WHERE l_partkey IN (SELECT p_partkey FROM part WHERE ...)`. After pushdown, the subquery's AVG only aggregates over the ~200 matching partkeys. Equivalent semantically to Lever B but at the logical level — DataFusion would re-optimize from there.

**Expected impact**: same as Lever B — drops AVG input from 60M to ~2000 rows. **~80-120 ms wall**.

**Effort**: 1-2 weeks. This is a non-trivial logical rewrite — needs to:
1. Detect the "scalar subquery + downstream filter" shape
2. Construct a semi-join on the filtered keys
3. Rewrite the subquery's input to include the semi-join
4. Preserve original semantics

**Risk**: H. Logical-plan rewrites are correctness-sensitive; DataFusion's existing decorrelation pipeline is complex and we'd be inserting ahead of or alongside it.

### D. AVG kernel optimization (REJECTED precedent)

Σ.R.2 already attempted a custom `RobinHoodAvgF64Exec`. Per `[[sigma-r2-rejected]]`: "Q17 SF=10 +40-55% across 3 dials (vec/scalar/pre-sized). Fused hash+probe+accumulate loses to DataFusion's split intern→batch-accumulate at 2M cardinality (table blows L2). 21.6%-self-time hot kernels don't mean 'replace and reclaim 21.6%'."

Not retrying without a different design.

### E. Outer HashJoin non-equi filter optimization

The outer HashJoin evaluates `l_quantity < 0.2 * avg(l_quantity)` per probe row (4367 ms). DataFusion uses Arrow's vectorized compute_filter, so this is already pretty optimal. Without a deep profile of WHERE this 4367 ms is spent (hash table probe vs filter eval vs output construction), no clear lever.

**Effort**: investigation-first; ~3-5 days to identify a real bottleneck.

**Risk**: M-H. May not find an attackable kernel.

## Recommended sequence

1. **Lever A first** (zero cost) — re-bench `EMAT_L9_TIGHT_CARDINALITY=1` under interleaved A/B. ~25 min. If positive, flip default-on. Even if not net-positive overall, the Q17-specific effect tells us how much value remains for B/C to attack.

2. **Lever B if A doesn't unlock Q17** — cascade the bloom into the subquery scan. 3-5 days. Has the highest impact ceiling that's still tractable (Q17 specifically should gain 80-120 ms wall). Reuses existing cascade rule infrastructure.

3. **Lever C is the strategic option** — predicate pushdown across decorrelated subqueries. 1-2 weeks but a much more general optimization (helps Q17 + any other scalar-subquery-with-downstream-filter pattern).

Pair each lever with strict interleaved A/B bench-gate per `[[bench-methodology-3-invocations]]` and `[[sigma-ai-1-strict-bench-landed]]`.

## §4 Outcomes (2026-05-27)

### Lever A — re-validated, no flip
Interleaved A/B (2 runs × 8 invocations each) showed `EMAT_L9_TIGHT_CARDINALITY=1` is neutral net (within 2σ noise floor across 22q). Q17-specifically: -3 to -7 ms (insufficient to motivate global flip given Q08 +55ms risk from AH.2 history). Stays opt-in.

### Lever B — POC rejected (+60% Q17 wall)
Implementation banked at `crates/ematix-flow-core/src/broadcast_sibling_blooms_rule.rs` (opt-in via `EMAT_L9_BROADCAST_SIBLINGS=1`). Plan dump confirmed correct broadcast firing (emitter + sibling scan both wired). **But Q17 wall regressed +180 ms (+60.4%)** in strict interleaved A/B.

**Why the hypothesis was wrong:**
- Bloom doesn't skip the 60M lineitem scan — it filters output rows
- Per-row bloom probe (~84 ms / partition) + per-batch SIMD filter on sparse output (1 row in 30000 passes) exceeds the AVG savings
- Fused-probe path (default-on since 2026-05-27) is tuned for denser pass-rates (Q21 5-10%), suboptimal at Q17's 0.003%

POC saved 3-5 days of clean implementation. Banked as opt-in infrastructure. See `[[sigma-aj-1-lever-b-rejected]]`.

### Lever D — EXPLAIN ANALYZE investigation (Option 3, replaced Lever E)
Single-invocation `EXPLAIN ANALYZE` decomposition of Q17 SF=10 outer HashJoin's compute pile (full traces in `[[sigma-aj-1-q17-explain-analyze]]`):

| Operator | elapsed_compute (14p sum) | Hot internal metric |
|---|---:|---|
| **AVG FinalPartitioned** | **2.26 s** | `time_calculating_group_ids=1.82s` (80%) |
| Outer HashJoin | 1.47 s | `build_time=735ms` (upstream wait), **`join_time=8ms`** |
| Repartition Hash | 1.12 s | l_partkey hash both paths |
| Inner HJ (part⋈lineitem) | 416 ms | healthy 30 ms/partition |
| Lineitem scans (2 paths) | 291 ms | — |
| AVG Partial | 75 ms | 97% skipped (streaming pre-agg) |

**Key finding: the non-equi filter (`l_quantity < 0.2 * avg`) is only 8 ms of join_time.** The previous "4367 ms outer HashJoin compute" working number conflated cooperative pipeline-wait + repartition + actual join work. The actual hot kernel is the AVG aggregation at 2M cardinality (peak 220 MB busts L3).

This is exactly the kernel Σ.R.2 attacked with `RobinHoodAvgF64Exec` and lost (+40-55%). Fused hash+probe+accumulate loses to DataFusion's split intern→batch-accumulate when the table blows L2.

**No tractable kernel-level lever exists inside Q17's outer HashJoin compute.**

### Final decision

**Σ.AJ.1 closes.** Two viable paths forward, both honest:

1. **(C) Commit to Lever C** — 1-2 weeks of logical predicate pushdown work; targets the structural waste exactly. Converts 60M→2K scan via parquet RG pruning at decode time, drops AVG output 2M→2K, drops peak hash table 220MB→22KB.
2. **(Accept)** — Q17 remains a stretch target until distributed/SF≥100 scaling changes the AVG kernel's parallelism economics; pivot to other levers.

**Cost-benefit analysis: Lever C is justified only if its 1-2 weeks unlocks ~80-120 ms Q17 wall AND ports to other shapes (Q15, Q22 scalar-subquery patterns).** Recommend a 2-day Phase 0 spike: probe DataFusion's existing `decorrelate_predicate_subquery` to see if the rewrite can hook in there rather than from scratch. If the spike shows tractable integration, commit. If not, defer.

## Risk register

- **Tight cardinality's Q08 regression history** (AH.2): we documented Q08 +55ms with tight cardinality alone. The Lever A default-on may have changed that — fresh interleaved A/B settles it.
- **Cascade rule complexity** (Σ.S.B): the self-join sibling skip exists for good reasons (Q21 cascade safety). Lifting it for Q17's shape requires careful targeting.
- **Logical rewrite correctness** (Lever C): scalar-subquery semantics are tricky; need a strong test harness covering NULL handling and edge cases.

## References

- AH.1 stage profile origin: [`docs/PHASE_SIGMA_AH_1_DESIGN.md`](PHASE_SIGMA_AH_1_DESIGN.md) § 4
- Existing cascade infra: `crates/ematix-flow-core/src/runtime_bloom_cascading_rule.rs`, `[[sigma-s-b-fix]]`
- Tight cardinality: AH.2 Story 1'.3 (`f30034f`), `[[sigma-ah-2-arc-closed]]`
- AVG kernel attempt: `[[sigma-r2-rejected]]`
- Q18 DuckDB plan diff (similar structural lever): `[[q18-sf10-duckdb-plan-diff]]`
- Methodology: `[[sigma-ai-1-strict-bench-landed]]`, `scripts/bench/strict_ab.sh`
