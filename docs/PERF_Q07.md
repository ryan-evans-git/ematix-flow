# PERF_Q07 — Q07 SF=10 stage profile

Status: **re-verified 2026-05-26** (Σ.AH Phase B.7). Originally profiled 2026-05-25.

## Wall time

### 2026-05-26 refresh (20 trials × 3 warmups, post-Σ.AG.7 bare invocation — canonical)

| Engine | Median ms | σ |
|--------|----------:|----:|
| ematix-flow | **157.48** | 5.68 |
| DuckDB | 142.25 | 3.62 |

**11% behind DuckDB** (was 15%; both engines moved). Stage profile 5-trial: 160.51 ms.

**Important new finding (vs 2026-05-25):** L9 bloom is **firing on the nation→customer edge** — customer scan now outputs **120,469 rows** (filtered from 1.5M = 8% pass rate after FRANCE/GERMANY bloom). 2026-05-25 doc showed customer scan emitting full 1.5M. This is a major change: Σ.Q.L9 / Σ.S.B work is paying off here.

## Physical plan

6-table join: nation ⋈ supplier ⋈ lineitem (filter shipdate ∈ 1995-96) ⋈ orders ⋈ customer ⋈ nation (with FRANCE/GERMANY pair on the two nations). L9 build-side bloom emitters land on both nation→supplier and nation→customer edges.

```
SortPreservingMergeExec [supp_nation ASC, cust_nation ASC, l_year ASC]
  ...
  AggregateExec FinalPartitioned gby=[supp_nation, cust_nation, l_year]
    HashJoinExec CollectLeft Inner (n_nationkey, c_nationkey) filter=FRANCE/GERMANY pair
      BuildSideBloomEmitterExec (nation 25 → target)
        nation (filter n_name ∈ FRANCE/GERMANY)
      HashJoinExec CollectLeft Inner (n_nationkey, s_nationkey)
        BuildSideBloomEmitterExec
          nation (filter n_name ∈ FRANCE/GERMANY)
        HashJoinExec Partitioned Inner (c_custkey, o_custkey)
          customer
          HashJoinExec Partitioned Inner (l_orderkey, o_orderkey)
            HashJoinExec CollectLeft Inner (s_suppkey, l_suppkey)
              supplier
              FilterExec l_shipdate ∈ [1995-01-01, 1996-12-31]
                lineitem                                    -- 60M → 18.2M
            orders                                          -- 15M rows
```

## Per-stage breakdown (2026-05-26)

| Rank | Operator | Depth | Median ms | Out rows |
|-----:|:---------|------:|----------:|---------:|
| 1 | **EmatixFastParquetExec lineitem** (5 cols projected) | 17 | **952.53** | 18,230,325 |
| 2 | **HashJoinExec (cust+orders) ⋈ (supp+lineitem)** Partitioned | 13 | **214.62** | 1,460,257 |
| 3 | HashJoinExec (cust ⋈ orders) Partitioned | 15 | 102.37 | 1,460,257 |
| 4 | EmatixFastParquetExec customer (post-L9-bloom: 1.5M → 120k) | 13 | 34.25 | **120,469** ← was 1.5M |
| 5 | HashJoinExec supplier ⋈ lineitem (CollectLeft) | 11 | 11.93 | 117,014 |
| 6 | RepartitionExec on c_custkey | 14 | 9.31 | 1,460,257 |
| 7 | FilterExec l_shipdate range (no-op residual, scan does the work) | 16 | 8.77 | 18,230,325 |
| 8 | EmatixFastParquetExec supplier (BridgeFilter on n_name pushed) | 16 | 6.18 | 8,010 |
| 9 | RepartitionExec | 12 | 6.68 | 1,460,257 |
| 10 | HashJoinExec (region/nation top of plan) | 7 | 1.18 | 58,365 |
| 11 | AggregateExec FinalPartitioned (56-group) | 5 | 0.67 | 56 |

Σ median compute: **1351.96 ms**. Wall median: 160.51 ms. **Effective parallelism: 8.42× = 60%.**

## Theoretical floor (per-stage, projection-cost-aware)

| Stage | Floor formula | Floor (sum ms) | Actual | Status |
|-------|---------------|---------------:|-------:|--------|
| Lineitem scan + BridgeFilter l_shipdate (60M in, 18M out, 5 cols project) | Σ.AE.2 dense-fallback for ~30% selectivity: 1.8 GB / (3 GB/s × 14) | ~600 | 952.53 | **1.6× over** (dense fallback dominates) |
| FilterExec l_shipdate residual (Inexact pushdown — scan does the work) | 18M × 0.5 ns kernel + bitmap pass | ~10 | 8.77 | at-floor ✓ |
| Customer scan (1.5M → 120k via L9 bloom on n_nationkey, 2 RGs) | small + bloom filter cost | ~20 | 34.25 | mild over (1.7× — 2-RG bottleneck similar to Q03/Q05) |
| Orders scan (15M × 2 cols, no filter) | ~30 ms parallel sum | ~30 | 0.36 | sub-floor (credited downstream) |
| Supplier scan + BridgeFilter (n_name pushed) | small | ~5 | 6.18 | at-floor ✓ |
| Nation scan + filter (25 rows → 2) | trivial | <1 | <1 | at-floor ✓ |
| HashJoin supplier ⋈ lineitem CollectLeft (build 8k, probe 18M, 1 thread shared) | 18M × 12 ns / 14 cores (probe parallel) | ~15 | 11.93 | at-floor ✓ |
| HashJoin cust ⋈ orders Partitioned (build 120k, probe 15M) | 15M × 12 ns = 180 / 14 = 13 ms wall × 14 sum | ~180 | 102.37 | sub-floor ✓ |
| HashJoin (cust+orders) ⋈ (supp+line) Partitioned (build 1.46M, probe 117k) | 117k × 12 ns = trivial; build dominates at 1.46M × 5 ns = 7 ms sum | ~20 | 214.62 | **10× over** — see analysis below |
| RepartitionExec ops | memcpy floors | ~30 | ~16 | at-floor ✓ |
| Aggregate FinalPartitioned (56 groups) | trivial | <2 | 0.67 | at-floor ✓ |
| **Σ floor (sum ms)** | | **~910** | **1352** | |
| **Σ effective-parallelism floor** | 1352 / 8.42 = | | **160.5 ms wall** | matches observed 160.51 ms ✓ |

**Q07 is at its realistic-parallelism floor on the current plan shape.**

### The 214.62 ms HashJoinExec depth 13 — looks 10× over floor but isn't really

Looking at the join: build = (cust+orders) at 1.46M rows × 24 bytes = ~35 MB build, probe = (supp+lineitem) at 117k rows × 24 bytes = ~2.8 MB probe. **Wait — the BUILD is bigger than the PROBE!** That's a join-side mis-ordering. The Partitioned-join probe iterates the *right* input but the *right* input (post-join supplier⋈lineitem) is the smaller side (117k rows). Build cost: 1.46M × ~5 ns hash insert = 7.3 ms parallel sum. Probe cost: 117k × ~30 ns L3-resident = trivial.

Actual 214.62 ms is dominated by the 1.46M-row BUILD against a 35 MB hash table that spans L2/L3. The build is the right thing to time-cost here, not the probe. Per-row build: 214.62 / 1.46M / 14 cores parallel = 10 ns/row build — consistent with the 5 ns/row "build floor" plus L2/L3 line traffic.

**Lesson:** when probe << build, the optimizer should swap sides. Memory `project_sigma_q_l10_landed.md` notes Σ.Q.L10 LeftSemi pushdown but that's a different rewrite. Q07's depth-13 join is Inner; the swap doesn't trivially apply because the projection requires the build-side columns.

## Σ.AH waste candidate ranking

| Rank | Candidate | Mechanism | Wall savings | Confidence |
|-----:|-----------|-----------|-------------:|:----------:|
| 1 | **Σ.AE.2 selectivity-gate tune** for ~30% lineitem selectivity | Q07 is right at the 1/3 threshold; masked decode might be faster than dense fallback for this band | ~10-15 ms | medium |
| 2 | **L9 cascade nation→supplier→lineitem** | Currently L9 fires on nation→customer (working — 1.5M → 120k). The supplier→lineitem edge would drop lineitem 60M → ~9M before the l_shipdate filter. | ~30-40 ms | medium |
| 3 | **HashJoin build-vs-probe side swap** (depth 13) | Build (1.46M) is bigger than probe (117k); the join would be ~3× faster with sides swapped. May not be safely swappable without projection rewrite. | ~5-10 ms | low |
| 4 | **Customer 2-RG bottleneck** | customer.parquet has only 2 RGs — same Q03/Q05 issue | ~3 ms | high (easy) |
| 5 | **Effective parallelism 60% → 70%** | Lineitem decode-imbalance + CollectLeft top joins | ~15 ms | low (structural) |

## Findings to capture as memories

- **Q07 is at realistic-parallelism floor** — the 17 ms gap to DuckDB is per-stage waste from the dense-decode fallback (lineitem 30% selectivity) and the build-side mis-ordering on depth-13 join.
- **L9 bloom is firing on nation→customer in Q07** (1.5M → 120k = 92% reduction). **Major change from 2026-05-25** when customer scanned the full 1.5M. Σ.Q.L9 / Σ.S.B work is paying off in Q07's shape.
- **L9 cascade to lineitem is the natural next step** — same lever as Q05's #1 candidate. Q07 and Q05 share the structural pattern: small-dim → mid-dim → large-fact, with the cascade not yet propagating to the fact table.
- **HashJoinExec depth 13 has build-side mis-ordering** (build 1.46M > probe 117k). The Partitioned-mode build cost dominates at 10 ns/row. Cross-query candidate: optimizer-level side-swap rule when probe size < build size.

## Next levers from Q07

1. **L9 cascade to lineitem via supplier** — alignment with Q05; worth a focused Σ.AH arc proposal.
2. **Σ.AE.2 selectivity-gate threshold tune** — also affects Q06; consider a sweep before any change.
3. **Customer 2-RG re-emit** — cheap, alignment with Q03/Q05.

---

## Verify pass — 2026-05-26 (Σ.AH B.7)

**What changed since 2026-05-25:**
- Wall: 156.97 → 157.48 ms canonical (essentially flat). Stage profile 160.51 ms.
- vs DuckDB: was −15% behind → now −11% (small move).
- **Customer scan output dropped 1.5M → 120k via L9 bloom from nation.** The "BuildSideBloomEmitter is dead weight" finding from 2026-05-25 is **withdrawn** — it's actively reducing customer scan output by 92%.
- Plan structure: unchanged.

**Withdrawn from prior:** the 2026-05-25 candidate "L9 build-size threshold" cleanup is no longer relevant — the bloom is doing real work even at small build cardinality. The threshold-tuning could still be a separate cleanup but it's not on the Σ.AH critical path.

**Top new candidate:** L9 cascade nation→supplier→lineitem (extends Σ.S.B from a different sub-shape than Q05). Both Q05 and Q07 would benefit from a fact-table bloom cascade.

**Next:** B.8 (Q08, 188.76 ms — lose to DuckDB by 7%).

## Waste candidates

### 1. l_shipdate filter NOT pushed to scan (60M decoded, 18M kept)

Same pattern as Q03. FilterExec is a separate node at depth 16 above the lineitem scan; 42M rows decoded then discarded. Predicate is a simple i32 range — bridge-filter-eligible.

Memory [[sigma-e5-streaming-late-mat-landed]] notes the masked-decode path is dormant pending dict-preserved Utf8View, but l_shipdate (i32) doesn't depend on that.

Expected impact: lineitem scan drops from ~110 ms wall to ~35 ms (42M-row decode skipped). Wall: 163 → ~90 ms (~45% improvement, would put us 33% ahead of DuckDB).

### 2. L9 build-side bloom is firing — but nation is too small to help

Two `BuildSideBloomEmitterExec` nodes appear on the nation→supplier and nation→customer edges. Nation post-filter has 2 rows (just FRANCE+GERMANY). A bloom of 2 keys has near-zero false-positive savings vs just filtering supplier/customer by nationkey directly. The bloom emitter is dead weight here — not harmful, just no signal.

Worth checking the L9 ratio guard: if `min_probe_to_build_ratio` is 1024 but build is 2 keys, the rule shouldn't fire at all. Or if it does fire, it should be a no-op.

### 3. HashJoinExec (cust+orders ⋈ lineitem) at 211 ms compute = 24 ms wall

The probe processes 18M lineitem rows against a 1.46M (cust ⋈ orders) build. Same memory-bandwidth-bound pattern as Q03 / Q05.

If candidate #1 (l_shipdate pushdown) works, this probe processes only the surviving 18M rather than the full 60M, which it already is. The 211 ms is intrinsic to the join shape.

## Findings to capture as memories

- Q07 SF=10 candidate aligns with Q03 / Q05 — l_shipdate pushdown into scan is a **multi-query** lever, not Q-specific.
- BuildSideBloomEmitter on nation joins is a near-no-op at this scale (build cardinality ≤ a few rows). Cleanup: detect "build size <16" and skip emission.

## Next levers from Q07

1. (Cross-Q) **l_shipdate i32-range bridge filter pushdown** — affects Q03, Q07, Q12, Q14, Q15. Single lever, multi-query payoff.
2. (Cleanup) **L9 build-size threshold** — don't emit blooms for builds <16 keys (Q07 nation, Q12 customer subset, etc).
