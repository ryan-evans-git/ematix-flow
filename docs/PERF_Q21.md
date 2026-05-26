# PERF_Q21 — Q21 SF=10 stage profile

Status: **re-verified 2026-05-26** (Σ.AH B.21).

## Wall time (20×3 canonical 2026-05-26)

| Engine | Median ms | σ |
|--------|----------:|----:|
| ematix-flow | **311.87** | 13.64 |
| DuckDB | 443.53 | 7.84 |

**30% ahead of DuckDB** (was 26%). Polars skipped at SF=10. Stage profile 5-trial: 296.92 ms.

## Per-stage decomposition

Σ compute 4416.98 ms / wall 296.92 ms = **14.88× parallelism = 106% — exceeds 14 cores** (async pipelining + RG cache replay must be giving sub-floor measurement).

| Stage | Floor | Actual | Status |
|-------|-------|--------|--------|
| Multiple lineitem scans (3× 60M, 2 with l_receiptdate > l_commitdate filter) | ~600 each = ~1800 ms total | 145.78 + 27.19 + others = ~200 | **sub-floor (RG cache)** ✓ |
| FilterExec l_receiptdate > l_commitdate ×3 (each 60M → ~38M) | 60M × 0.5 ns × 3 = ~90 ms | 96.91 + 78.82 + 77.66 = 253 ms | mild over (3 sequential filters) |
| HashJoinExec depth 12 (1.52M output) | small | 136.05 | mild |
| RepartitionExec 38M ×2 | ~50 each | 44.67 + 35.55 = 80 | at-floor ✓ |
| Top HashJoinExec ⋈ chain (LeftAnti/LeftSemi after Σ.Q.L10) | small | 3.24 | sub-floor ✓ |
| AggregateExec | tiny | 1.20 + 0.37 = 1.6 | at-floor ✓ |

Σ floor ~2200 ms (most is the 3 lineitem scans + filters); observed 4417 ms — but 14.88× parallelism implies wall = 4417 / 14.88 = 297 ms = matches observed.

**Critical: RG decode cache makes the 2nd and 3rd lineitem scans effectively free.** First scan 145 ms parallel, second 27 ms, third tiny. Σ.O.c.2 doing its job on Q21's multi-scan pattern.

## Findings

- **Q21 is the biggest absolute wall-time query (312 ms canonical)** but is essentially at-floor — most of the work is the unavoidable 3× lineitem touches + LeftSemi/LeftAnti pipeline.
- **RG decode cache cuts 2 of 3 lineitem scans** to near-zero, same pattern as Q02's partsupp + Q11's partsupp. Confirmed cross-query benefit.
- Σ.Q.L10 PushDownLeftSemiRule + L9 bloom firing visibly. Memory `[[sigma-q-l13-to-l16-session]]` Q21 fix landed; this is the steady-state performance.
- Effective parallelism >14× (106%) — async pipelining + RG cache replay create the appearance of more-than-perfect parallelism in the elapsed_compute metric. Q21 is a useful "ceiling" example showing how multi-scan + caching can push past the naive parallelism floor.

**Next:** B.22 (Q22 — 23.35 ms, +84% vs DuckDB; biggest relative win).

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
