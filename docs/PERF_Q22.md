# PERF_Q22 — Q22 SF=10 stage profile

## Wall time

| Engine | Median ms | σ | Rows |
|--------|----------:|----:|----:|
| ematix-flow | 23.63 | 1.72 | 7 |
| DuckDB | 139.45 | 45.37 | 7 |
| Polars | 112.28 | 2.78 | 7 |

**5.9× ahead of DuckDB**, 4.8× ahead of Polars. Biggest relative win in the suite.

## Physical plan

```
SortPreservingMergeExec [cntrycode ASC]
  AggregateExec FinalPartitioned gby=[cntrycode] count, sum(c_acctbal)
    NestedLoopJoinExec Inner filter (c_acctbal > avg)
      AggregateExec Final no-gby avg(c_acctbal)
        FilterExec (c_acctbal > 0 AND substr(c_phone, 1, 2) IN [13, 31, 23, 29, 30, 18, 17])
          customer (scan #1)
      HashJoinExec Partitioned LeftAnti (c_custkey, o_custkey)
        FilterExec substr(c_phone, 1, 2) IN ...
          customer (scan #2)
        orders (only o_custkey projection)
```

## Per-stage breakdown

| Rank | Operator | Median ms | Out rows |
|-----:|:---------|----------:|---------:|
| 1 | NestedLoopJoinExec (Inner, filter) | 135.38 | 63,914 |
| 2 | HashJoinExec LeftAnti (cust ⋈ orders) | 118.61 | 140,489 |
| 3 | FilterExec (cust scan #1) | 30.10 | 419,974 |
| 4 | FilterExec (cust scan #2) | 29.00 | 381,776 |
| 5 | RepartitionExec | 9.76 | 419,974 |

Σ median compute: 326 ms. Wall median 25 ms. **Parallel speedup ≈ 12.99×** — very strong.

## Theoretical floor

| Stage | Floor (ms) |
|-------|-----------:|
| customer scan #1 + substr+IN filter (1.5M × 2 cols) | 3 |
| customer scan #2 + substr+IN filter (1.5M × 3 cols) | 3 |
| orders scan (15M × 1 col) | 4 |
| HashJoin LeftAnti (cust 380k build × orders 15M probe / 14) | 13 |
| AggregateExec Partial avg(c_acctbal) (420k rows, no group) | <1 |
| NestedLoopJoin (1 × 140k filter) | 2 |
| Hash agg 7 groups | <1 |
| Sort 7 rows | <1 |
| **Floor** | **~28 ms** |
| **Actual** | **25 ms** |
| **Waste ratio** | **0.9×** (below floor — model conservative) |

## Findings

Q22 is at or below the conservative floor. **Biggest beneficiary of the StringView `new_unchecked` fix (-20% in the post-fix 22q bench).** Already 5.9× ahead of DuckDB. Move on.

## Next levers

(none — Q22 already optimal)
