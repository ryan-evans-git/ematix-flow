# PERF_Q21 — Q21 SF=10 stage profile

## Wall time

| Engine | Median ms | σ | Rows |
|--------|----------:|----:|----:|
| ematix-flow | 299.17 | 3.19 | 4,009 |
| DuckDB | 406.56 | 2.63 | 4,009 |
| Polars | 34,213.00 | 1,762.89 | 4,009 |

**26% ahead of DuckDB**, **114× ahead of Polars** (Polars takes 34 seconds on Q21 SF=10).

## Physical plan — 3 lineitem scans

Q21 has 3 separate lineitem reads, 2 of them with identical projection:

```
SortPreservingMergeExec [numwait DESC, s_name ASC]
  AggregateExec FinalPartitioned gby=[s_name] count
    AggregateExec Partial
      HashJoinExec Partitioned LeftAnti (l_orderkey, l_orderkey) filter=l_suppkey != l_suppkey
        HashJoinExec Partitioned LeftSemi (l_orderkey, l_orderkey) filter=l_suppkey != l_suppkey
          BuildSideBloomEmitterExec
            HashJoinExec CollectLeft Inner (n_nationkey, s_nationkey)            -- nation SAUDI
              nation
              HashJoinExec Partitioned Inner (o_orderkey, l_orderkey)             -- orders.F
                orders (filter o_orderstatus='F')
                HashJoinExec CollectLeft Inner (s_suppkey, l_suppkey)
                  supplier
                  FilterExec l_receiptdate > l_commitdate
                    lineitem #1  proj=[0,2,11,12]                                 -- 60M, scan A
          lineitem #2  proj=[0,2]                                                 -- 60M, scan B
        FilterExec l_receiptdate > l_commitdate
          lineitem #3  proj=[0,2,11,12]                                           -- 60M, scan C (SAME as A!)
```

## Per-stage breakdown (top 10)

| Rank | Operator | Median ms | Out rows |
|-----:|:---------|----------:|---------:|
| 1 | HashJoinExec (LeftAnti, top) | 1953.34 | 39,448 |
| 2 | HashJoinExec (LeftSemi) | 929.30 | 707,593 |
| 3 | EmatixFastParquetExec (lineitem, scan B proj=[0,2]) | 433.80 | 59,986,052 |
| 4 | EmatixFastParquetExec (lineitem, scan C) | 147.95 | 59,986,052 |
| 5 | HashJoinExec (cust+orders+supplier ⋈ lineitem #1) | 145.20 | 734,523 |
| 6 | HashJoinExec (supplier ⋈ lineitem #1) | 140.31 | 1,522,366 |
| 7 | FilterExec (scan C residual) | 85.10 | 37,929,348 |
| 8 | EmatixFastParquetExec (orders) | 82.91 | 15,000,000 |
| 9 | FilterExec (scan A residual) | 76.48 | 37,929,348 |
| 10 | FilterExec (scan B) | 70.00 | 7,309,184 |

Σ median compute: 4194 ms. Wall median 301 ms. **Parallel speedup ≈ 13.93×** — very strong.

## Theoretical floor

| Stage | Floor (ms) |
|-------|-----------:|
| If only 1 lineitem scan with widest projection (60M × 4 cols) | 8 |
| If 2 scans (current behaviour): | 16 |
| If 3 scans (worst): | 24 |
| orders scan + filter | 5 |
| supplier + nation | <1 |
| 2× FilterExec `l_receiptdate > l_commitdate` (60M × 2-col cmp × 14) | 4 |
| HashJoin chain (Inner → CollectLeft Inner → LeftSemi → LeftAnti) | ~30 |
| Hash agg s_name (40 groups, count) | <1 |
| Sort 4009 rows | <1 |
| **Floor (with single lineitem scan)** | **~50 ms** |
| **Floor (with 3 scans as today)** | **~70 ms** |
| **Actual** | **301 ms** |
| **Waste ratio** | **6×** vs single-scan ideal, **4.3×** vs 3-scan today |

## Waste candidates

### 1. Three lineitem scans, two are projection-identical — CSE candidate

Scans A and C have the **exact same projection** `[0,2,11,12]` (l_orderkey, l_suppkey, l_commitdate, l_receiptdate) and the **same FilterExec** on l_receiptdate > l_commitdate above them. Functionally identical subtrees.

`SharedSubtreeExec` ([[sigma-p-subquery-cse]]) is the existing infra; today it's scoped to Q15's correlated-subquery pattern. Extending the CSE detector to match identical filtered-scan subtrees would deduplicate A+C.

Expected impact: lineitem decode count: 3 → 2. Wall: 301 → ~250 ms (~17% improvement; takes us 38% ahead of DuckDB).

### 2. `l_receiptdate > l_commitdate` two-column filter is not pushed to scan

Same pattern as Q04 and Q12 — 2-column comparison stays as FilterExec, not BridgeFilter. Three FilterExecs in the plan each evaluate this on 60M (or 38M post-other-filter) rows.

If BridgeFilter is extended to two-column compare ([[Q04 candidate]]): scans A, B, C all decode fewer rows. Wall: 301 → ~180 ms.

### 3. LeftAnti+LeftSemi sequence at 2.9 sec combined compute

The two anti-/semi-joins on the same key `l_orderkey` materialise then negate each other. There's likely a single-pass formulation. But this is a structural rewrite (LogicalPlan-level), not a simple lever.

## Findings

- **Q21 is the strongest example of CSE-on-scan opportunity.** Two identical filtered lineitem subtrees should share data.
- **2-column-compare BridgeFilter extension affects Q04, Q12, and Q21** — the biggest single multi-query lever.

## Next levers

1. **2-column-compare BridgeFilter extension** (cross-Q for Q04 / Q12 / Q21): single change unlocks lineitem scan pushdown in 3+ queries.
2. **SharedSubtreeExec for identical scan+filter subtrees** (cross-Q for Q02 / Q11 / Q21): generalisation of [[sigma-p-subquery-cse]].
