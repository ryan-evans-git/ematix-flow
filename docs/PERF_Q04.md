# PERF_Q04 — Q04 SF=10 stage profile

Status: profiled 2026-05-25.

## Wall time

| Engine | Median ms | σ | Rows |
|--------|----------:|----:|----:|
| ematix-flow | 55.36 | 15.15 | 5 |
| DuckDB | 91.26 | 13.35 | 5 |
| Polars | 270.06 | 11.38 | 5 |

**39% ahead of DuckDB**, 5× ahead of Polars. Strong existing position.

## Physical plan

LeftSemi (orders ⋈ lineitem) → group-by o_orderpriority. Classic semi-join shape.

```
SortPreservingMergeExec [o_orderpriority ASC]
  ...
  AggregateExec FinalPartitioned gby=[o_orderpriority] count
    AggregateExec Partial
      HashJoinExec Partitioned LeftSemi on (o_orderkey, l_orderkey)
        orders (filter o_orderdate ∈ [1993-07-01, 1993-10-01))   -- 573k rows
        lineitem (filter l_receiptdate > l_commitdate)           -- 38M rows from 60M
```

## Per-stage breakdown (top 8)

| Rank | Operator | Median ms | Out rows |
|-----:|:---------|----------:|---------:|
| 1 | HashJoinExec LeftSemi (build=orders 573k, probe=lineitem 38M) | 172.00 | 526,040 |
| 2 | EmatixFastParquetExec (orders, filter o_orderdate pushed) | 159.28 | 573,671 |
| 3 | FilterExec (l_commitdate < l_receiptdate) | 40.09 | 37,929,348 |
| 4 | RepartitionExec (Hash(l_orderkey)) | 21.04 | 37,929,348 |
| 5 | AggregateExec Partial | 4.25 | 70 |
| 6 | RepartitionExec | 2.67 | 573,671 |
| 7 | EmatixFastParquetExec (lineitem) | 2.13 | 59,986,052 |

(Note: the 60M-row lineitem scan shows 2 ms compute because `elapsed_compute` measures time inside `rx.recv().await` — the producer thread streams batches faster than the consumer drains them, so the timer effectively measures consumer-bound time. The actual decode work is real but credited to the downstream FilterExec/HashJoin.)

Σ median compute: 402 ms. Wall median 55 ms. Parallel speedup ≈ 7.28× of 14 cores.

## Theoretical floor

| Stage | Floor (ms) |
|-------|-----------:|
| orders scan (3 cols, 15M → 573k) + o_orderdate filter pushed | 4 |
| lineitem scan (3 cols, 60M) | 8 |
| Filter l_receiptdate > l_commitdate (60M × 2 ns / 14) | 8 |
| HashJoin LeftSemi (build 573k, probe 38M × 12 ns / 14) | 32 |
| Hash agg (5 groups, count) | <1 |
| **Floor** | **~52 ms** |
| **Actual** | **55 ms** |
| **Waste ratio** | **1.06×** |

Q04 is essentially at the floor. Already 39% ahead of DuckDB — no obvious win to chase.

## Waste candidates

### Marginal: 60M → 38M filter not pushed into scan

`FilterExec(l_receiptdate > l_commitdate)` is a separate node. 22M rows decoded just to be discarded by the filter. Same pattern as Q03's l_shipdate filter. But this is a **two-column** comparison, not a column-vs-literal predicate, so the existing single-predicate BridgeFilter path doesn't cover it without extension. Lower-priority than the Q03 case.

### Marginal: build side 573k doesn't fit in L2

Same as observed in [[sigma-r2-rejected]]. HashJoin probe pays L3 access per row × 38M rows. The Σ.J.2.b bloom pushdown could pre-filter lineitem rows whose orderkey isn't in orders → ~8% pass rate on the bloom would cut the probe to ~3M rows, but Q04 is already 55 ms — wall savings would be ~2-3 ms.

## Findings

Q04 is near-optimal. Listed candidates are not high-priority. Move on.

## Next levers

- (Cross-Q lever) Two-column filter pushdown to scan: l_receiptdate > l_commitdate is one of TPC-H's common patterns (Q04, Q12). If we get cross-query data showing both queries pay the same way, the lever opens.
