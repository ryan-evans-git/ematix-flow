# PERF_Q11 — Q11 SF=10 stage profile

Status: **re-verified 2026-05-26** (Σ.AH B.11).

## Wall time (20×3 canonical 2026-05-26)

| Engine | Median ms | σ |
|--------|----------:|----:|
| ematix-flow | **11.59** | 0.64 |
| DuckDB | 30.70 | 2.94 |

**62% ahead of DuckDB** (was 54%). Stage profile 5-trial: 9.70 ms.

## Per-stage decomposition

Σ compute 74.56 ms / wall 9.70 ms = **7.69× parallelism = 55%**.

| Stage | Floor | Actual | Status |
|-------|-------|--------|--------|
| partsupp scans ×2 (8M rows × 3 cols each) — RG decode cache closes 2nd | 1st ~10; 2nd ~free | 2.46 + 2.23 | **sub-floor** (cache hit ✓) |
| AggregateExec Partial+Final on ps_partkey (8M → 304k) | 8M × 3 ns = 24 ms parallel | 6.43 + 5.19 + 6.86 = 18.5 | at-floor ✓ |
| HashJoin supplier ⋈ partsupp (CollectLeft) | 8M × 15 ns / 14 = 9 ms | 1.18 + 0.90 = 2.08 | sub-floor ✓ |
| NestedLoopJoinExec final filter | trivial | 6.86 | mild over (NL sequential) |
| Top ops | trivial | <1 | ✓ |

Σ floor ~55 ms; observed 74.56 ms. Σ/parallelism = 9.7 ms wall ≈ observed.

## Findings

**Q11 is at realistic-parallelism floor.** RG decode cache closes the partsupp double-scan (2nd scan 2.23 ms ≈ 1st scan 2.46 ms — cache hit confirmed). No new candidate. Too small for meaningful levers.

**Next:** B.12 (Q12 — 87.64 ms; +24% vs DuckDB).

(All 3 engines return 0 rows — Q11's "important parts" threshold of 0.0001 × total nation-stock-value happens to be > the max partkey-stock-value for this SF=10 GERMANY-filtered dataset.)

## Physical plan

Decorrelated subquery: scan partsupp twice (once for global SUM filter threshold, once for per-partkey grouping), then NestedLoopJoinExec for the `> 0.0001 × total` comparison.

```
SortPreservingMergeExec [value DESC]
  NestedLoopJoinExec Inner filter (sum > threshold)
    -- Left side: scalar (the 0.0001× threshold)
    AggregateExec Final gby=[] sum
      AggregateExec Partial gby=[] sum
        HashJoinExec CollectLeft (n_nationkey, s_nationkey)
          nation (GERMANY)
          HashJoinExec CollectLeft (s_suppkey, ps_suppkey)
            supplier
            partsupp                                      -- scan #1
    -- Right side: per-partkey
    AggregateExec FinalPartitioned gby=[ps_partkey] sum
      AggregateExec Partial
        HashJoinExec CollectLeft (n_nationkey, s_nationkey)
          nation (GERMANY)
          HashJoinExec CollectLeft (s_suppkey, ps_suppkey)
            supplier
            partsupp                                      -- scan #2
```

## Per-stage breakdown (top 8)

| Rank | Operator | Depth | Median ms | Out rows |
|-----:|:---------|------:|----------:|---------:|
| 1 | HashJoinExec (supplier ⋈ partsupp #2) | 9 | 24.57 | 323,920 |
| 2 | HashJoinExec (supplier ⋈ partsupp #1) | 10 | 21.68 | 323,920 |
| 3 | NestedLoopJoinExec | 3 | 7.02 | 0 |
| 4 | AggregateExec FinalPartitioned (ps_partkey, sum) | 6 | 6.37 | 304,774 |
| 5 | AggregateExec Partial (ps_partkey, sum) | 4 | 5.16 | 304,774 |
| 6 | EmatixFastParquetExec (partsupp) | 11 | 2.15 | 8,000,000 |
| 7 | HashJoinExec (nation ⋈ supplier+partsupp) | 7 | 1.13 | 323,920 |

Σ median compute: 71 ms. Wall median 9.58 ms. Parallel speedup ≈ 7.4× of 14 cores.

## Theoretical floor

| Stage | Floor (ms) |
|-------|-----------:|
| 2× partsupp scan (8M × 3 cols, Snappy) | 5 |
| supplier scan (100k × 2 cols) | <1 |
| nation | <1 |
| 2× HashJoin supplier ⋈ partsupp (each 100k build × 8M probe / 14) | 6 |
| 2× HashJoin nation ⋈ (supplier+partsupp) (1 build × 323k probe) | <1 |
| 2× Hash agg (304k groups, sum f64 × f64) | 5 |
| NestedLoopJoinExec (1 × 304k filter) | 1 |
| Sort | <1 |
| **Floor** | **~17 ms** |
| **Actual** | **9.6 ms** |
| **Waste ratio** | **0.6×** |

We are **below** the floor — the floor model is conservative. Q11 is genuinely optimal in our setup.

## Waste candidates

### Marginal: 2× partsupp scan (same as Q02)

Same CSE opportunity as Q02 — the decorrelated subquery and the outer agg both scan partsupp. Memory [[sigma-p-subquery-cse]] is the relevant infra (SharedSubtreeExec). At 9.6 ms wall, eliminating one of the partsupp scans saves ~2 ms — well below noise.

## Findings

Q11 is at floor. No actionable waste. Move on.

## Next levers

(none — Q11 already optimal)
