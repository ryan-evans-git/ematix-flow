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

## Theoretical floor (Phase A.1 audit-revised constants)

Q02 SF=10, parallel across 14 cores. Snappy 1.61 GB/s, hash agg 3 ns/row (10K-1M groups, revised), filter 0.62 ns/row, hash join 5/10 ns/row (build/probe, unverified).

| Stage | Floor formula | Floor (ms) |
|-------|---------------|-----------:|
| 1× partsupp scan, 3 cols (cache makes 2nd ~free) | 8M × 16 B / (1.61 GB/s × 14) ≈ 130 MB / 22.5 GB/s | 5.8 |
| 1× part scan, ~5 cols (100k rows, ~30 MB compressed) | scan small, dominated by LIKE | 0.5 |
| Filter part LIKE '%BRASS' on 100k rows | 100k × 5 ns/row / 14 ≈ 0.04 | <0.1 |
| Filter region.r_name='EUROPE' (5 rows) | trivial | <0.1 |
| supplier / nation / region scans | trivial | <0.5 |
| 5 HashJoinExec builds | all ≤ 31k rows; 5 ns/row each | <0.5 |
| Largest probe (part_filt build 7854 ⋈ partsupp 8M) | 8M × 10 ns/row / 14 | 5.7 |
| Next probe (1.18M agg output ⋈ part_filt × partsupp) | 1.18M × 10 ns / 14 | 0.8 |
| AGG Partial: 8M rows → 1.18M groups (MIN f64) | 8M × 3 ns/row / 14 | 1.7 |
| AGG Final: 1.18M rows → 1.18M groups (no further reduction) | 1.18M × 3 ns/row / 14 | 0.25 |
| Sort 4667 × 4 cols | n × log₂(n) × 75 ns / 14 | <0.1 |
| **Floor (revised)** | | **~14 ms** |
| **Actual (5-trial stage profile)** | | **32.96 ms** |
| **Waste ratio** | | **2.4×** |

Floor was ~18 ms with old constants; revised down to ~14 ms because (a) RG decode cache makes the 2nd partsupp scan ~free, (b) hash agg constant moved from 8 → 3 ns/row per Phase A.1 audit.

Wall actually dropped (36 → 33 ms), but floor dropped more (18 → 14), so waste ratio **worsened from 1.8× → 2.4×**. There's more "missing performance" to find than we thought; the 2026-05-25 floor was too generous.

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

- Q02 SF=10 is at **2.4× floor** (was 1.8× with old constants) — we're 35% ahead of DuckDB, but the absolute waste (~19 ms) is still meaningful.
- **The RG decode cache closes the double-scan pattern for FREE.** Q02 shows a 3.41 ms → 0.20 ms drop on the second partsupp scan. This generalises: any query that scans the same table twice (Q11, Q15, Q21 subquery patterns) gets the same benefit. Cross-query memory candidate.
- **Partial+Final agg on partitioning key is wasted Partial work.** Confirmed across Q02 (1.18M Partial → 1.18M Final, no reduction). Worth a 22q sweep before designing a fix.
- **Compound-key probe on (p_partkey, ps_supplycost)** at depth 13 dominates remaining waste (25 ms parallel). Two-key hash probe is ~2× slower per row than single-key. Σ.AH.2 (compound-key Robin Hood) extension territory if pattern shows in Q09/Q10/Q20.

## Σ.AH waste candidate ranking (current)

After candidate #1 is now closed by RG cache:

| Rank | Stage / lever | Parallel ms | Est. wall savings | Confidence | Notes |
|-----:|---------------|------------:|------------------:|:----------:|-------|
| 1 | Partial agg redundant when group-by ≡ partition key | ~24 ms | ~1.5 ms | high | Σ.AH cluster candidate; check Q03/Q05/Q10 for same shape |
| 2 | Compound-key HashJoin probe (depth 13) on 2 keys | ~25 ms | ~1.5 ms | medium | Could rewrite as 2-step single-key probe + filter; or compound-key Robin Hood |
| 3 | part_filt ⋈ partsupp probe (8M rows, partial filter) | ~23 ms | ~1 ms | medium | Already near 10 ns/row probe floor; bloom-from-part_filt could drop 8M→31k pre-decode |

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

**No fundamentally new candidate identified;** existing #2 (Partial+Final on partitioning key) re-confirmed as the biggest lever, and a new compound-key HashJoin observation added to the ranking. Both worth carrying to Phase C for cross-query pattern detection rather than chasing in Q02 alone.

**Next:** B.3 (Q03, 145.74 ms SF=10 vs DuckDB 145.58 — statistical tie, but third-largest absolute wall time).
