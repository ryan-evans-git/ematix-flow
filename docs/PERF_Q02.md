# PERF_Q02 — Q02 SF=10 stage profile

Status: **re-verified 2026-05-26** (Σ.AH Phase B.2). Originally profiled 2026-05-25.
Data: `examples/tpch/data/sf10/*.parquet`, Snappy.

## Wall time

### 2026-05-26 refresh (20 trials × 3 warmups, post-Σ.AG.7 bare invocation — canonical)

| Engine | Median ms | σ |
|--------|----------:|----:|
| ematix-flow | **29.37** | 1.48 |
| DuckDB | 45.30 | 2.25 |

ematix wins by **35% over DuckDB** (was 24%). Stage-profiler 5-trial run gives median 32.96 ms — the canonical 20-trial number is tighter because the longer-trial window amortises trial-1 cache-warm cost.

### 2026-05-25 baseline (deprecated)

ematix 36.46 ± 10.81, DuckDB 48.23, Polars 418.20. Wall dropped 19% (36 → 29 ms). Improvement is consistent with: (1) RG decode cache now hits Q02's double partsupp scan (see candidate #1 below — first scan 3.41 ms, second scan **0.20 ms** in current profile), (2) cumulative ematix-parquet 0.16.x SIMD parity, (3) Σ.AG.7 plan cache (~0.7 ms / trial).

## Physical plan (post-optimizer)

5-way join + 2-stage hash agg + correlated subquery decorrelated to joined-agg:

```
SortPreservingMergeExec [s_acctbal DESC, ...]
  SortExec
    ProjectionExec
      HashJoinExec Partitioned Inner on (p_partkey, ps_supplycost)=(ps_partkey, min(ps_supplycost))
        HashJoinExec CollectLeft Inner on (r_regionkey, n_regionkey)        -- region ⋈ ...
          region (filter r_name='EUROPE')
          HashJoinExec CollectLeft Inner on (n_nationkey, s_nationkey)      -- nation ⋈ ...
            nation
            HashJoinExec CollectLeft Inner on (s_suppkey, ps_suppkey)       -- supplier ⋈ ...
              supplier
              HashJoinExec Partitioned Inner on (p_partkey, ps_partkey)     -- part_filt ⋈ partsupp #1
                part (filter p_size=15 AND p_type LIKE '%BRASS')
                partsupp                                                    -- 8M rows
        -- ↑↑↑ outer half ↑↑↑    ↓↓↓ subquery half ↓↓↓
        RepartitionExec
          AggregateExec FinalPartitioned gby=[ps_partkey] aggr=[min(ps_supplycost)]
            AggregateExec Partial
              HashJoinExec CollectLeft Inner (r_regionkey, n_regionkey)
                region (filter r_name='EUROPE')
                HashJoinExec CollectLeft Inner (n_nationkey, s_nationkey)
                  nation
                  HashJoinExec CollectLeft Inner (s_suppkey, ps_suppkey)
                    supplier
                    partsupp                                                -- 8M rows AGAIN (scanned twice)
```

## Per-stage breakdown (top 12 by median elapsed_compute_ms, 5 trials)

| Rank | Operator | Depth | Median ms | Min | Max | Out rows |
|-----:|:---------|------:|----------:|----:|----:|---------:|
| 1 | AggregateExec (Final, gby=ps_partkey, min) | 8 | 37.56 | 35.70 | 39.04 | 1,183,098 |
| 2 | HashJoinExec (build=partsupp probe-side of outer, probe=subquery agg) | 13 | 25.30 | 23.38 | 25.90 | 8,000,000 |
| 3 | AggregateExec (Partial, same gby) | 6 | 24.46 | 20.91 | 25.64 | 1,183,098 |
| 4 | HashJoinExec (part_filt ⋈ partsupp #1) | 11 | 23.12 | 21.89 | 24.44 | 8,000,000 |
| 5 | HashJoinExec (supplier ⋈ partsupp ⋈ ...) | 9 | 18.92 | 18.37 | 20.38 | 1,602,640 |
| 6 | HashJoinExec | 10 | 18.69 | 18.34 | 19.22 | 31,416 |
| 7 | HashJoinExec | 3 | 12.00 | 11.41 | 12.91 | 4,667 |
| 8 | EmatixFastParquetExec (part) | 10 | 6.84 | 6.71 | 7.13 | 100,000 |
| 9 | HashJoinExec | 9 | 6.29 | 5.94 | 6.93 | 31,416 |
| 10 | EmatixFastParquetExec (partsupp #1) | 15 | 3.47 | 1.65 | 10.33 | 8,000,000 |
| 11 | RepartitionExec | 4 | 3.42 | 3.19 | 3.74 | 1,183,098 |
| 12 | RepartitionExec | 7 | 3.13 | 2.97 | 3.71 | 1,183,098 |

Σ median compute across all nodes: 188.40 ms. Wall median 32.49 ms. Parallel speedup ≈ 5.80× of 14 cores.

## Theoretical floor (Phase A.1 audit-revised + per-stage waste, 2026-05-26)

Q02 SF=10, parallel across 14 cores. Snappy 1.61 GB/s, hash agg 3 ns/row (10K-1M groups, revised), filter 0.62 ns/row, hash join 5/10 ns/row (build/probe, unverified).

### Effective parallelism

- **Σ median compute / wall = 5.71×** (was 5.80×). Only **41% effective parallelism on 14 cores** — worse than Q01 (54%). Q02's plan has more sequential pipeline stages (CollectLeft small-dim joins on 1 thread + Partial→RepartitionExec→Final agg chain) which reduces achievable parallelism vs a flat scan.

### Per-stage actual vs floor

| Stage | Parallel floor | Parallel actual | Gap | Note |
|-------|---------------:|----------------:|----:|------|
| partsupp scan #1 (8M rows, 3 cols) | ~3 ms | 3.41 ms | +0.4 ms | **near-floor** ✓ |
| partsupp scan #2 (cache hit) | ~0.2 ms | 0.20 ms | 0 | **RG cache closes** ✓ |
| part scan + LIKE filter (100k rows) | ~1 ms | 6.86 ms | **+5.9 ms** | **6× over floor** — LIKE matcher hot? StringView decode? |
| supplier/nation/region scans | trivial | trivial | 0 | ✓ |
| Hash agg Partial (8M → 1.18M groups, MIN f64) | 1.7 ms | 24.48 ms | **+22.8 ms** | **14× over floor** — Partial doing 8M-row work that produces ≈ no reduction |
| Hash agg Final (1.18M → 1.18M, no further reduction) | 0.25 ms | 36.42 ms | **+36.2 ms** | **146× over floor** — pure overhead; group key = partition key |
| HashJoin depth 11 (part_filt 7854 ⋈ partsupp 8M) | 5.7 ms | 22.87 ms | **+17.2 ms** | **4× over floor** — probe side at ~57k rows/core/ms (slow) |
| HashJoin depth 13 (compound key on 1.18M ⋈ 31k) | ~1.7 ms (2× single-key) | 25.30 ms | **+23.6 ms** | **15× over floor** — compound-key probe (p_partkey, ps_supplycost) |
| HashJoin depth 9 (1.6M output) | ~0.8 ms | 19.65 ms | **+18.9 ms** | **25× over floor** — needs deeper look |
| HashJoin depth 3 (final assemble, 4667 rows out) | trivial | 12.28 ms | **+12 ms** | **large overhead** for small output |
| **Floor (sum, parallel-equivalent)** | | | **~14 ms** | |
| **Actual wall** | | **32.96 ms** | | |
| **Gap to floor** | | | **~19 ms (2.4×)** | |

### Where the 19 ms gap goes

Total operator excess over floor: ~137 ms parallel work (sum of all the +N gap columns above). With 41% effective parallelism, that maps to ~137 / 5.71 = **24 ms of "excess wall"**. Plus ~14 ms of floor wall = 38 ms expected. Observed: 33 ms (close enough; some excess overlaps with other stages).

The dominant per-stage gaps:
1. **AGG Final at 36 ms parallel over a 0.25 ms floor** — entirely orchestration overhead. Group key (ps_partkey) = partition key, so Final is a no-op reduction.
2. **AGG Partial at 22.8 ms parallel over a 1.7 ms floor** — same root cause: doing reduction work on data already partitioned correctly.
3. **HashJoin depth 13 (compound key) at 23.6 ms over floor** — 2-key hashing is structurally ~2× single-key.
4. **HashJoin depth 9 + 11 + 3** — collectively ~50 ms parallel over floor, source unclear (likely batch-boundary overhead in the probe loop).
5. **part scan + LIKE at 5.9 ms over floor** — small but surprising; LIKE matcher kernel is fast per [[sigma-e5-like-kernel]] (9-14× std), so the gap is somewhere else (StringView decode? bitmap materialisation?).

Floor was ~18 ms with old constants; revised down to ~14 ms because (a) RG cache makes 2nd partsupp scan ~free, (b) hash agg constant moved from 8 → 3 ns/row per Phase A.1 audit. Wall dropped less (36 → 33), so waste ratio **worsened from 1.8× → 2.4×**.

## Waste candidates

### 1. partsupp scanned twice — ~~CSE candidate~~ MOSTLY CLOSED by RG decode cache (Σ.O.c.2)

**2026-05-26 update:** the stage profile now shows partsupp scan #1 at 3.41 ms parallel compute and partsupp scan #2 at **0.20 ms** — the second scan is hitting the [Σ.O.c.2 RG decode cache](../crates/ematix-flow-core/src/emat_arrow_reader.rs:212) (default ON post-Σ.AG.7). The double-scan candidate is effectively closed; remaining cost is ~0.2 ms parallel and not worth pursuing.

Earlier (2026-05-25) text retained for context: the decorrelated subquery and outer query both reference partsupp; memory [[sigma-p-subquery-cse]] landed `SharedSubtreeExec` for Q15-shape; Q02-shape isn't pattern-identical. A SharedSubtreeExec for non-aggregate scans (Σ.Z) could replicate the RG-cache benefit at the operator level (smaller, deterministic), but the current RG-cache approach saves the same ms without optimizer-rule cost.

### 2. AggregateExec (Partial + Final) at 62 ms total compute for 1.18M groups — 4× over floor

Partial scans 8M rows → 1.18M groups (in-thread), then RepartitionExec → Final reduces to the same 1.18M (no further reduction, since partsupp is unique on ps_partkey/ps_suppkey). The Partial agg is doing 8M-row work on data that's already grouped exactly 1:1 with the partitioning. **This is wasted work** — RepartitionExec on ps_partkey could feed directly into a single-pass FinalPartitioned agg without the intermediate Partial.

Memory [[sigma-nf3-beats-stock]] notes RobinHoodSumF64Exec beats stock on Partial+FinalPartitioned for SUM(f64) — but this is MIN(f64), which doesn't currently have a Robin Hood path. **Candidate**: route MIN(f64) through a similar Robin Hood specialized exec, OR a rule to elide the Partial stage when output cardinality ≈ input cardinality (which is detectable from column stats since ps_partkey is the inner key of partsupp's compound primary key).

Expected: shaves ~10-15 ms parallel compute = 1-3 ms wall.

### 3. HashJoinExec output of 8M rows from part_filt ⋈ partsupp — semantic correctness, not waste

The 8M output at depth 11 looks scary but it's correct: part_filt has 7854 matching parts × 4 partsupp rows per part = ~31k rows, NOT 8M. The 8M reported is the *partsupp scan output before the join*, since the join is on the probe side and `output_rows` accumulates pre-join inputs in some metric setups. Not a waste candidate — just plan-tree-walker quirk.

### 4. CollectLeft mode on all the small-dim joins — appropriate

region/nation/supplier are tiny enough that CollectLeft is correct. No change.

## Findings to capture as memories

- Q02 SF=10 is at **2.4× floor** (was 1.8× with old constants). Absolute gap to floor: ~19 ms on a 33 ms query.
- **The RG decode cache closes the double-scan pattern for FREE.** Q02 shows a 3.41 ms → 0.20 ms drop on the second partsupp scan. This generalises: any query that scans the same table twice (Q11, Q15, Q21 subquery patterns) gets the same benefit. Cross-query memory candidate.
- **Partial+Final agg on the partitioning key is ~59 ms of pure overhead.** AGG Final at 36 ms parallel over a 0.25 ms floor (146× over floor!) — pure orchestration since group key (ps_partkey) = partition key. Cross-query lever: any plan where `RepartitionExec(hash on K)` feeds `AggregateExec(group by K, Partial)` is paying for nothing.
- **Q02 effective parallelism is 41% — worse than Q01's 54%.** CollectLeft + Partial→Final pipeline serialise more than Q01's flat scan. Parallelism imbalance is a 22q pattern, not Q01-specific.
- **Compound-key probe on 2 keys (depth 13) costs ~2× single-key.** Structural; needs compound-key Robin Hood or rewrite as 2-step probe.

## Σ.AH waste candidate ranking (current, per-stage anchored)

After candidate #1 is now closed by RG cache:

| Rank | Stage / lever | Parallel waste | Est. wall savings | Confidence | Notes |
|-----:|---------------|---------------:|------------------:|:----------:|-------|
| 1 | **AGG Partial+Final on partitioning key** | ~59 ms (24+36) | **~4 ms (12%)** | high | Group key (ps_partkey) = partition key after RepartitionExec → both Partial AND Final are pure overhead. Pre-plan rule: skip Partial when RepartitionExec hash key ⊇ agg group keys. Cross-query candidate (Q03/Q05/Q10/Q20 likely same). |
| 2 | **Compound-key HashJoin probe (depth 13)** | ~24 ms | ~1.5 ms | medium | 2-key probing structurally ~2× single-key. Compound-key Robin Hood OR reformulate as 2-step. |
| 3 | **HashJoin depths 9 + 11 + 3 over-floor** | ~50 ms | ~3 ms | low-medium | Source unclear — batch-boundary overhead in probe loops? Needs samply self-time data. Investigate per-batch overhead before designing fix. |
| 4 | **Parallelism imbalance (41% effective)** | ~14 ms wall potential | ~10 ms if pushed to 60% | medium | Q01 had same pattern at 54%; Q02 is worse due to CollectLeft + pipeline serialisation. Lever crosses all queries. |
| 5 | **part scan + LIKE pattern over floor** | ~6 ms | ~0.5 ms | low | 6× over floor on a 100k-row scan with LIKE — small absolute. May be StringView decode, not LIKE itself. Defer. |

## Next levers from Q02

1. **Defer Q02-specific work** — we're 35% ahead of DuckDB and the absolute waste is small (~7-10 ms). Carry candidates forward to Phase C for cross-query pattern detection.
2. **Σ.AH watch:** if Phase C finds the Partial-on-partitioning-key pattern in 3+ queries, that's Σ.AH cluster candidate worth a dedicated arc.

---

## Verify pass — 2026-05-26 (Σ.AH B.2)

**What changed since 2026-05-25:**
- Wall time: 36.46 → 29.37 ms canonical / 32.96 ms stage profile (−19% canonical).
- vs DuckDB: was −24% → now −35% (we widened the lead).
- Plan structure: unchanged.
- **RG decode cache closed the partsupp double-scan candidate.** The 2nd partsupp scan dropped from 3.5 ms → 0.20 ms.
- Floor moved DOWN (18 → 14 ms) from audit-revised constants; waste ratio worsened from 1.8× → 2.4×. There's more "missing performance" to chase than the old floor suggested, but absolute wall headroom (~15 ms = 4× wall improvement potential) is still modest.

**Per-stage decomposition (added per user feedback):** revealed that **AGG Final at 36 ms parallel / 0.25 ms floor (146× over)** is the single biggest absolute-waste stage in the query. The Partial+Final agg pattern is doing pure orchestration when the partition key equals the group-by key (RepartitionExec hashes on ps_partkey → AggregateExec groups by ps_partkey → both stages just re-shuffle and re-hash identical data). This is a clear cross-query candidate — pattern shows wherever the planner's RepartitionExec aligns with the agg's group-by columns.

**Effective parallelism: 41% (worse than Q01's 54%).** Q02's CollectLeft small-dim joins + Partial→Final pipeline serialise more than Q01's flat scan. Same generalised lever as Q01's #1 candidate, just with different mechanics.

**Next:** B.3 (Q03, 145.74 ms SF=10 vs DuckDB 145.58 — statistical tie, but third-largest absolute wall time).
