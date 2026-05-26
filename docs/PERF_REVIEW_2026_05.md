# Σ.AH — Q01-Q22 SF=10 performance review (Phase C synthesis)

**Created:** 2026-05-26
**Scope:** Cross-query synthesis of `PERF_Q01.md` through `PERF_Q22.md`, plus the Phase A.1 floor-constants audit appendix in `STAGE_PROFILING_METHODOLOGY.md`.
**Baseline:** `BENCHMARKS.md` 2026-05-26 refresh (post-Σ.AG.7, plan cache default ON). 22q SF=10 ematix-flow vs DuckDB: **15 ematix wins, 7 DuckDB wins**.

This document does NOT design implementations. It ranks candidate arcs and is the input to Phase D (arc shells).

---

## TL;DR

After re-profiling all 22 queries with a projection-cost-aware floor model and the Phase A.1 audit-revised constants:

- **15 of 22 queries are at-or-near their realistic-parallelism floor** on the current plan shape. Their gaps to DuckDB (where present) are *structural plan-shape*, not per-stage kernel inefficiency.
- **7 queries have identifiable Q-specific waste** that's not yet captured by an existing rule: Q01, Q02, Q07, Q08, Q09, Q10, Q13, Q17.
- **Three cross-query patterns dominate the future wins**:
  1. **L9 scan-level integration** — bloom should filter at decode time, not at probe time. Affects Q17, Q18 dominantly; cascades to Q05, Q07, Q08, Q09 via Partitioned-mode extension.
  2. **Build-vs-probe side-swap** — five queries pick the larger side as build.
  3. **L9 Partitioned-mode extension** — current emitter only targets CollectLeft joins; four queries with textbook small-build/large-probe Partitioned shapes don't get the bloom.

Estimated combined wall-time impact of the three arcs: **~200-300 ms across 8-10 queries** = roughly **5-8 pp SF=10 geomean** improvement, holding everything else constant.

---

## Methodology corrections discovered

The Phase B sweep surfaced three methodology errors. Captured for future sweeps:

### 1. Per-column Snappy floor (B.1 redo)

Using a uniform Snappy throughput (1.61 GB/s/thread) on a multi-column projection undercounts the floor for queries with mixed-compressibility columns. From the `snappy_decompress_probe` data (Phase A.1):

| Column class | Throughput | Notes |
|--------------|-----------:|-------|
| Dict-encoded (l_returnflag, l_linestatus, l_quantity) | ~10 GB/s | Snappy ratio ≈ 1.0; near memcpy |
| Numeric f64 (l_extendedprice, l_discount, l_tax) | ~1.66 GB/s | Snappy ratio ~0.73 |
| Date i32 (l_shipdate, l_receiptdate, l_commitdate) | ~1.66 GB/s | (estimated, similar to f64 cols) |

For floor calculations, weight per-column. Example: Q01 reads 7 cols of lineitem; using uniform 1.61 GB/s gave floor 39 ms; using per-column weighted gave 74 ms.

### 2. Projection-cost-aware FilterExec floor (B.4)

`FilterExec` cost is not just the predicate kernel. It's:
```
floor_ms = (in_rows × kernel_ns) + (out_rows × out_cols × 4 B / 70 GB/s aggregate)
```

The memcpy projection (filtered batch construction) dominates when the output is wide. Without this term, Q03's FilterExec looked "3× over floor" — a false flag (B.4 retracted the candidate).

### 3. Q05 dominant-operator misidentification (B.5)

The 2026-05-25 sweep claimed the supplier 2-key join was the bottleneck (9.1M output). Re-reading the depth tree: the **9.1M is from (cust+orders) ⋈ lineitem at depth 11**; the supplier 2-key join (depth 10) is a *reducer* (9.1M → 364k via c_nationkey = s_nationkey). Lesson: always trace output-row counts down the depth tree before naming the bottleneck.

---

## Cross-query effective-parallelism map

| Tier | Effective parallelism | Queries | Pattern |
|------|----------------------:|---------|---------|
| **A** | 80-106% | Q06 (80%), Q13 (90%), Q21 (106% *via cache*), Q22 (106% *via cache*) | Single-table fused agg, or multi-scan with RG-cache replay |
| **B** | 70-79% | Q12 (73%), Q14 (71%), Q16 (57% — borderline), Q19 (73%), Q11 (55% — small) | Few joins, mostly hash-partitioned |
| **C** | 50-65% | Q01 (54%), Q03 (47%), Q04 (52%), Q07 (60%), Q09 (50%), Q10 (61%), Q15 (95% *via cache*), Q17 (83%), Q18 (50%), Q20 (54%) | 3-5 table chains |
| **D** | 35-45% | Q02 (41%), Q05 (38%), Q08 (35%) | 6-8 table chains with CollectLeft small-dim + Partial→Final agg |

**Pattern:** parallelism degrades with plan depth + CollectLeft count + Partial-Final agg presence. Tier D queries lose ~30 pp parallelism vs Tier A. This is the **single biggest cross-query "waste" by aggregate ms**, but the levers to fix it are structural (rewrite pipelining, sub-RG splitting, side-swap), not kernel-level.

---

## Per-query waste summary

(Wall ms canonical 2026-05-26. "At floor" = within 20% of realistic-parallelism floor.)

| Q | Wall | vs DuckDB | Floor status | Top remaining lever |
|---|----:|----:|--------------|---------------------|
| Q01 | 235 | tie (+0.8%) | **gap (3.3×)** | Parallelism imbalance 54% — sub-RG splitting |
| Q02 | 29 | −35% | **gap (2.4×)** | AGG Partial+Final on partition key (146× over floor on Final) |
| Q03 | 146 | tie (+0.1%) | at floor (1.16×) | L9 bloom firing verification; customer 2-RG |
| Q04 | 54 | −39% | **at floor** | none |
| Q05 | 186 | +25% | at floor (Σ/parallelism) | **L9 cascade region→supplier→lineitem** (~50 ms) |
| Q06 | 76 | tie (+2%) | **at floor** (80% parallel) | LZ4_RAW codec swap (deferred) |
| Q07 | 157 | +11% | at floor | **L9 cascade nation→supplier→lineitem** + build-side swap (~40 ms) |
| Q08 | 189 | +7% | at floor | **L9 Partitioned-mode for part_filt→lineitem** (~50-80 ms) |
| Q09 | 273 | −13% | **gap (~80 ms wall waste)** | **L9 Partitioned + cascade to BOTH lineitem AND partsupp** (~80 ms); build-side swap (~28 ms) |
| Q10 | 232 | −43% | **gap (~130 ms wall waste)** | **Functional-dep group-by simplifier** (~50 ms); HashJoin probe-rate investigation |
| Q11 | 12 | −62% | at floor | none (too small) |
| Q12 | 88 | −24% | at floor | none |
| Q13 | 96 | −65% | mild gap (~55 ms) | LIKE matcher overhead, 1.5M-group count — structural |
| Q14 | 85 | −38% | at floor | build-side swap (part 2M build > 749k filt probe; ~10 ms) |
| Q15 | 77 | −19% | at floor | none (Σ.P doing its job) |
| Q16 | 50 | −27% | at floor | NOT LIKE pushdown (~3 ms) |
| Q17 | 175 | +6% | mild gap | **L9 bloom push into BridgeFilter** (~80 ms wall) |
| Q18 | 244 | +6% | at floor | L9 push into BridgeFilter on outer lineitem (~40 ms) |
| Q19 | 139 | −34% | at floor | none |
| Q20 | 131 | −13% | at floor | none |
| Q21 | 312 | −30% | at floor | none (Σ.O.c.2 closes 2nd/3rd lineitem scans) |
| Q22 | 23 | −84% | **at floor** | none |

**Sum of identified Q-specific wall waste:** ~400 ms across 7 queries (Q01, Q02, Q07-Q10, Q13, Q17). About 80% of that maps to two cross-query arcs (L9 work + side-swap).

---

## Ranked Σ.AH arc candidates

Ranked by `wall_savings × confidence × query_count`. Each arc clusters Q-specific candidates that share a common mechanism.

### Σ.AH.1 — L9 scan-level integration (push bloom into BridgeFilter)

**Mechanism:** Today's L9 bloom is consumed at HashJoinExec probe time — rows are decoded then dropped. Push the bloom into `EmatixFastParquetExec`'s `BridgeFilter` so rows whose join key isn't in the bloom skip decoding entirely.

**Queries affected:** Q17 (~80 ms wall), Q18 (~40 ms wall), cascade benefits to Q05/Q07/Q08 if combined with Σ.AH.2.

**Expected impact:** **~120 ms wall across 2-3 queries** = ~3-4 pp SF=10 geomean.

**Confidence:** medium. Mechanism is clear (bloom filter on key column during decode). Risks: bloom timing (build side must complete before probe-side scan starts, requires plan synchronisation), additional decode-time CPU cost on bloom probe.

**Memory precedents:**
- `[[sigma-j2b-v-landed]]`, `[[sigma-j2b-vi-landed]]`: distributed bloom transport + probe-side rule. The mechanism exists in the codebase but for cross-stage / distributed cases.
- `[[sigma-q-l9-landed]]`: current single-stage L9 implementation — consumed at operator level.

**Bench gate:** Q17 wall drop ≥ 60 ms AND 22q SF=10 geomean −2 pp or better. Reject if any single query regresses > 5%.

---

### Σ.AH.2 — L9 emitter Partitioned-mode extension

**Mechanism:** Current `BuildSideBloomEmitterExec` only wraps CollectLeft joins. Extend to Partitioned-mode joins where the build side is small enough that a bloom would be useful (probe/build ratio > some threshold, e.g., 1024).

**Queries affected:** Q05 (part⋈lineitem and supplier⋈lineitem edges currently not getting Partitioned-mode blooms), Q07 (nation→supplier edge), Q08 (part_filt 13k→lineitem 60M edge), Q09 (part_filt 108k→lineitem 60M AND part_filt→partsupp 8M cascade).

**Expected impact:** **~150-200 ms wall across 4 queries** = ~4-6 pp SF=10 geomean.

**Cascade with Σ.AH.1:** if both arcs land, Q05/Q07/Q08/Q09 get bloom-at-scan-time on the Partitioned join. Q09 specifically gets compound benefit: the bloom pre-filters BOTH lineitem (60M → ~3M) AND partsupp (8M → ~108k), shrinking the 128 MB DRAM-spill 2-key build to 1.7 MB L1-resident.

**Confidence:** medium. Mechanism is the same as CollectLeft — just plug it into the Partitioned planner branch. Risk: the build-side hash already partitions, may need a sub-partition merge step for the bloom.

**Memory precedents:**
- `[[sigma-q-l9-landed]]`, `[[sigma-q-l13-to-l16-session]]`: prior L9 work, all CollectLeft scope.

**Bench gate:** Q08 wall drop ≥ 30 ms AND Q09 wall drop ≥ 50 ms AND 22q SF=10 geomean −2 pp or better.

---

### Σ.AH.3 — Build-vs-probe side-swap optimizer rule

**Mechanism:** When DataFusion's planner picks Inner-join sides such that build cardinality > probe cardinality, swap them. Five queries currently exhibit this:
- Q07 depth 13 (build 1.46M > probe 117k)
- Q08 depth 13 (build 1.5M > probe 122k)
- Q09 depth 9 (build 15M = 120 MB > probe 3.26M = 78 MB)
- Q10 (cust ⋈ orders, build 1.5M > probe 573k)
- Q14 (part build 2M > lineitem-filt probe 749k)

**Expected impact:** **~60-80 ms wall across 5 queries** = ~2-3 pp SF=10 geomean.

**Confidence:** low-medium. Mechanism is clear; risk is that DataFusion picked the larger side as build for a reason (column statistics estimation, projection downstream, NULL semantics). Naive swap can break correctness.

**Approach:** pre-plan walker that inspects post-filter cardinality stats and swaps when the size delta is large (e.g., > 2× difference) AND the join type permits (Inner-only safe; Left/Right require care).

**Memory precedents:**
- `[[sigma-q-l2-rejected]]` for the semi-join swap rule that was rejected (a different shape but same conceptual lever). Re-look flag below.
- Σ.T (cost-based join reorder) — deferred multi-month arc; this side-swap rule is a narrow piece of it.

**Bench gate:** Q07+Q08+Q09 each drop ≥ 5 ms wall AND 22q geomean stable.

---

### Σ.AH.4 (chore) — Customer.parquet re-emit with more row groups

**Mechanism:** customer.parquet at SF=10 has only 2 row groups → only 2-way parallel scan → bottleneck for early-pipeline parallelism in any query that starts from customer.

**Queries affected:** Q02, Q03, Q05, Q07, Q08, Q10, Q22 (7 queries).

**Expected impact:** ~3-5 ms wall each × 5 substantive cases = ~20-25 ms wall total = **~1 pp SF=10 geomean**.

**Confidence:** **high** — it's a data-prep change, not a code change. Re-emit the file with `pyarrow.parquet.write_table(row_group_size=100_000)` or similar.

**Risk:** none on perf. Tiny risk on disk size (more metadata) but customer.parquet is small.

**Bench gate:** trivial; any improvement on Q03/Q05/Q07 confirms.

---

### Σ.AH.5 — Functional-dependency group-by simplifier

**Mechanism:** When a GROUP BY clause includes a UNIQUE-key column plus other columns from the same table, the other columns are functionally determined and don't need to be in the hash key. Group by the unique key only, project the rest after aggregation.

**Queries affected:** Q10 (7-col gby with c_custkey unique on customer; the 6 other cols are functional passthrough). Possibly Q13 (gby c_count after agg — 1.5M groups; structurally different but worth analysing).

**Expected impact:** **~50 ms wall on Q10** = ~1 pp SF=10 geomean.

**Confidence:** medium. Requires FK + UNIQUE-key awareness in the planner; we have these on the EmatixFastParquetTableProvider but not yet wired to a logical-plan rule.

**Bench gate:** Q10 wall drop ≥ 30 ms AND 22q geomean stable.

---

### Σ.AH.6 — Σ.AE.2 selectivity-gate tune for 30% band

**Mechanism:** The Σ.AE.2 BridgeFilter selectivity-gate falls back to dense decode when in-RG selectivity > 1/3. For 30% selectivity (the boundary), dense decode pays full-RG bandwidth; masked decode might be cheaper.

**Queries affected:** Q06 (~15% file selectivity, ~30% in-RG), Q07 (~30% lineitem selectivity overall), possibly Q19.

**Expected impact:** ~10-15 ms wall on Q07; possibly ~5 ms on Q06 = **~20 ms wall** = ~0.5 pp geomean.

**Confidence:** low-medium. Need a kernel-level A/B at 30% selectivity before committing.

**Bench gate:** A/B kernel bench shows masked < dense at 30% selectivity AND no 22q regression on dense-favourable queries (Q01, Q14).

---

## Rejection re-look flags

The Phase B sweep surfaced new evidence relevant to prior rejected arcs. **These remain rejected by default**; Phase D should consider re-running their bench gates only if Σ.AH.1-3 land first.

### Σ.S.B cascading-L9 (`project_sigma_sb_cascade_neg.md`)

**Original verdict:** neutral on 22q geomean; opt-in via `EMAT_L9_CASCADE=1`.
**New evidence:** Q05 and Q07 specifically need region→nation→supplier→lineitem cascade. The original A/B may have been net-neutral because the queries that benefit (Q05/Q07) were balanced against queries that regressed.
**Re-look only if:** Σ.AH.1 + Σ.AH.2 land and the cascade is tested specifically on Q05/Q07 in isolation.

### Σ.Q.L11 u32 hash-key compression (`project_sigma_q_l11_rejected.md`)

**Original verdict:** rejected because SQL-level CAST is slower than i64.
**New evidence:** Q09 partsupp 2-key join build is 128 MB DRAM-bound. If we could compress the i64 keys to u32 at decoder level (different mechanism than the rejected SQL CAST), the build would fit in 64 MB → near L3.
**Re-look only if:** Σ.AH.2 lands and Q09's partsupp 2-key join is still the dominant cost.

### Σ.R.2 RobinHoodAvgF64Exec (`project_sigma_r2_rejected.md`)

**Original verdict:** Q17 SF=10 +40-55% across 3 dials. Kept opt-in as infra.
**New evidence:** Q17 closed half its gap to DuckDB since 2026-05-25 (was −22% → now −6%). The Robin Hood path for AVG may not be needed at all if Σ.AH.1 lands (push L9 bloom into scan).
**Re-look:** not needed unless Σ.AH.1 doesn't deliver on Q17.

### Σ.T cost-based join reorder (archived plan)

**Original verdict:** deferred for multi-month effort.
**New evidence:** Σ.AH.3 (build-vs-probe swap) is a narrow piece of Σ.T. If Σ.AH.3 lands well, the rest of Σ.T may be less urgent. If it doesn't, Σ.T resumes from the archived plan.

---

## What's NOT a candidate arc

For completeness, the Phase B sweep ruled out (or confirmed mitigated):

- **FilterExec batch-boundary overhead** — false flag (B.3 retracted in B.4); kernel + projection floor is correctly tight.
- **partsupp double-scan** in Q02/Q11 — closed by Σ.O.c.2 RG decode cache. Same for lineitem ×3 in Q21.
- **σ run-to-run variance** — collapsed (often 5-10× tighter) on Q08/Q12/Q15/Q19/Q20 thanks to Σ.O.c.2 default-on stabilising RG cache warmth.
- **Customer scan output filtered to 120k via L9 nation bloom (Q07)** — Σ.Q.L9 working as designed; was flagged as dead-weight in 2026-05-25, now confirmed productive.
- **Q15 SharedSubtreeExec replay** — Σ.P doing its job; not a waste.

---

## Recommended Phase D sequencing

1. **Σ.AH.4 (customer re-emit)** — cheap, fast, no codegen risk. Land first to clear floor noise on Q03/Q05/Q07.
2. **Σ.AH.2 (L9 Partitioned-mode)** — biggest absolute impact (~150-200 ms across 4 queries). Mechanism is well-understood (parallel of existing CollectLeft L9). Should ship before Σ.AH.1.
3. **Σ.AH.1 (L9 scan-level)** — the bigger win on Q17/Q18 (~120 ms) and the cascade enabler for Σ.AH.2. Higher complexity (decode-time bloom probe + plan synchronisation).
4. **Σ.AH.3 (build-vs-probe swap)** — opportunistic gain (~60 ms across 5 queries). Lower confidence; defer until Σ.AH.1/2 land.
5. **Σ.AH.5 (functional-dep group-by)** — single-query payoff on Q10 (~50 ms). Self-contained; can interleave with Σ.AH.3.
6. **Σ.AH.6 (selectivity-gate tune)** — small (~20 ms). Best done as part of a broader Σ.AE.2 tuning pass.

**Total expected wall-time impact (all arcs combined):** ~400-500 ms across 8-10 queries = **roughly 8-12 pp SF=10 geomean** improvement, modulo bench-gate validation.

---

## Open methodology questions for the next sweep

1. **Why is `elapsed_compute_ms` sometimes sub-floor or > 14× wall?** Async pipelining (RepartitionExec, scan-side decode) credits work to consumer operators, distorting the per-stage numbers. The Σ/parallelism = wall identity is reliable but the per-stage attribution is fuzzy.
2. **DuckDB plan diff would be informative.** All "structural gap" findings (Q05, Q07, Q08, Q17, Q18) would benefit from a side-by-side optimised-plan dump. Phase B didn't include this; consider for the next sweep.
3. **Floor accounting for non-Snappy files.** customer is dict-heavy, partsupp is mixed. Per-column probe data only exists for lineitem so far. If we re-emit customer (Σ.AH.4) the floor model should be updated.

---

## Related plans

- Plan doc: [`docs/plans/CURRENT.md`](plans/CURRENT.md) — Σ.AH structure
- Methodology: [`docs/STAGE_PROFILING_METHODOLOGY.md`](STAGE_PROFILING_METHODOLOGY.md) (incl. 2026-05-26 audit appendix)
- Per-query writeups: `docs/PERF_Q01.md` through `docs/PERF_Q22.md`
- Bench baseline: [`BENCHMARKS.md`](../BENCHMARKS.md)
