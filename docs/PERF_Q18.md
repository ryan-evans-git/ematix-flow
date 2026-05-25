# PERF_Q18 — Q18 SF=10 stage profile

## Wall time

| Engine | Median ms | σ | Rows |
|--------|----------:|----:|----:|
| ematix-flow | 247.92 | 10.46 | 624 |
| DuckDB | 239.57 | 15.53 | 624 |
| Polars | 626.40 | 19.62 | 624 |

**3% behind DuckDB** — essentially at parity. (Memory [[sigma-q-l10-landed]] notes the gap was +153% before PushDownLeftSemiRule landed; we closed most of it.)

## Physical plan

LeftSemi pushdown is firing: lineitem sums-by-orderkey filter `sum>300` decorrelated into RightSemi, with BuildSideBloomEmitter wrapping it to narrow the outer lineitem read.

```
SortPreservingMergeExec [o_totalprice DESC, o_orderdate ASC]
  AggregateExec gby=[c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice] sum(l_quantity)
    HashJoinExec Partitioned Inner (o_orderkey, l_orderkey)         -- ← dominant
      HashJoinExec Partitioned Inner (c_custkey, o_custkey)
        customer
        HashJoinExec Partitioned RightSemi (l_orderkey, o_orderkey)  -- pushed-down semi
          BuildSideBloomEmitterExec target=orders.o_orderkey
            FilterExec sum > 300
              RobinHoodSumF64Exec FinalPartitioned gby=l_orderkey sum(l_quantity)  -- ← RH path
                RobinHoodSumF64Exec Partial
                  lineitem (full scan)
          orders                                                      -- 15M
      lineitem                                                       -- 60M, full scan again
```

## Per-stage breakdown (top 6)

| Rank | Operator | Median ms | Out rows |
|-----:|:---------|----------:|---------:|
| 1 | HashJoinExec (orders+cust ⋈ lineitem main, on o_orderkey) | 1128.12 | 624 |
| 2 | EmatixFastParquetExec (lineitem main, no bloom on this side) | 338.76 | 624 |
| 3 | HashJoinExec (cust ⋈ orders+filter) | 140.64 | 4,368 |
| 4 | HashJoinExec RightSemi (sum-filter ⋈ orders) | 33.61 | 624 |
| 5 | EmatixFastParquetExec (lineitem #2, for the sum agg) | 26.16 | 59,986,052 |
| 6 | RepartitionExec | 4.75 | 59,986,052 |

Σ median compute: ~1680 ms. Wall median 248 ms. Parallel speedup ≈ 6.8×.

## Theoretical floor

| Stage | Floor (ms) |
|-------|-----------:|
| lineitem scan #1 for sum agg (60M × 2 cols) | 6 |
| Hash agg (Partial+Final, l_orderkey 15M distinct, sum f64) [RobinHood] | 8 |
| Filter sum > 300 (most rows pass through) | <1 |
| orders scan (15M × 4 cols) | 5 |
| HashJoin RightSemi (filtered ⋈ orders) | 3 |
| customer scan (1.5M × 2 cols) | 1 |
| HashJoin cust ⋈ orders+filter | 2 |
| lineitem scan #2 for the final join (60M × 2 cols) | 6 |
| HashJoin (cust+orders+filter) ⋈ lineitem main (build small, probe 60M × 8 ns / 14) | 34 |
| Final agg + sort 624 rows | <1 |
| **Floor** | **~66 ms** |
| **Actual** | **248 ms** |
| **Waste ratio** | **3.8×** |

## Waste candidates

### 1. Lineitem main scan is unfiltered — no L9 from the (cust+orders+filter) build to lineitem

The plan does L9 on lineitem-#2 → orders (narrowing orders via the sum-filter bloom), but the **lineitem-#1 (the final-join probe)** is full-table 60M. The build side at this point is (cust ⋈ orders ⋈ sum-filtered) which has 4368 rows — a tiny build with a 60M probe. Textbook L9 case.

Memory [[sigma-q-l10-landed]] already closed the bigger gap (153% → 6%). The remaining 3% could be a **secondary** L9 pushing from the customer-orders-orderkey set into the lineitem main scan.

Expected impact: bloom narrows lineitem main from 60M decoded to ~30k rows. Wall: 248 → ~180 ms (~25% improvement, takes us 20% ahead of DuckDB).

### 2. The final HashJoin at 1128 ms compute = 100 ms wall

60M probe rows × 4368 build size → 60M × 8 ns / 14 = ~34 ms floor for the probe alone. We're at 100 ms wall — 3× over floor. Could be:
- Build hashing cost (4368 keys × ~10 ns each = 44 µs — trivial)
- Build dwell waiting for upstream to complete
- Memory pressure with concurrent lineitem scan #2

Same as candidate #1 — if lineitem-main probes fewer rows, this drops.

### 3. RobinHoodSumF64Exec firing correctly on the sum agg

Plan confirms `RobinHoodSumF64Exec` is being used for the `sum(l_quantity) gby l_orderkey` step. Memory [[sigma-nf3-beats-stock]] reports RH beats stock by 1-5% — this is doing its job.

## Findings

- **Q18 has a secondary L9 opportunity** on the final-join lineitem read. Build is 4368 rows (post-filter); probe is 60M lineitem. The existing L9 rule doesn't propagate the bloom through the LeftSemi-pushed structure to the OUTER lineitem read.
- This is structurally similar to Q17's gap — both queries scan lineitem twice and L9 fires on only one of the two scans.

## Next levers

1. (Cross-Q for Q17 + Q18) **Double-scan L9** — detect when the same large fact table is scanned twice in a plan and propagate the most-selective bloom to both. Single rule extension could close residual gaps on Q17 and Q18.
