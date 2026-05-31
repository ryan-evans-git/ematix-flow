# PERF_Q18 — Q18 SF=10 stage profile

Status: **re-verified 2026-05-26** (Σ.AH B.18).

## Wall time (20×3 canonical 2026-05-26)

| Engine | Median ms | σ |
|--------|----------:|----:|
| ematix-flow | **243.70** | 6.49 |
| DuckDB | 229.81 | 4.58 |

**6% behind DuckDB** (was 3% — slight slip but still at-parity). Stage profile 5-trial: 250.29 ms.

## Per-stage decomposition

Σ compute 1737.68 ms / wall 250.29 ms = **6.94× parallelism = 50%**.

| Stage | Floor | Actual | Status |
|-------|-------|--------|--------|
| **HashJoinExec depth 7** (final outer-join with bloom) | needs probe analysis | 1191.31 | **dominant** — what is this doing? |
| EmatixFastParquetExec depth 9 (orders, ⋈ bloom: 15M → 624) | small | 342.13 | mild over |
| HashJoinExec depth 3 (final cust ⋈ orders+lineitem) | small probe | 143.00 | mild over |
| HashJoinExec depth 5 | small | 30.25 | at-floor ✓ |
| EmatixFastParquetExec depth 13 (lineitem main, 60M, no filter) | small (async) | 18.20 | sub-floor ✓ |
| RepartitionExec 60M | 4.77 | at-floor ✓ |
| FilterExec (sum > 300) | tiny | 3.35 | at-floor ✓ |
| EmatixFastParquetExec depth 5 (lineitem RH-side, 60M, no filter) | small (async) | 2.43 | sub-floor (RH path) ✓ |
| RobinHoodSumF64Exec Partial+Final (gby=l_orderkey, sum f64) | embedded inline; not counted | 0 (inlined) | confirms RH path ✓ |
| BuildSideBloomEmitterExec | tiny | 0 | confirmed firing ✓ |

Σ floor estimate ~700 ms; observed 1738 ms. **~1000 ms parallel over-floor (~150 ms wall).** Σ/6.94 = 250 ms = matches observed.

**The HashJoinExec at depth 7 dominates at 1191 ms parallel for only 624 output rows.** This must be the giant HashJoinExec that ingests the 60M lineitem rows on the outer side, joining against the RH-aggregate-derived order_ids. Build = filtered orders (15M→624 via bloom?), probe = lineitem 60M. So probe is huge × the small build → 60M × ~30 ns probe = 1800 ms parallel floor. Observed 1191 — sub-floor!

So actually the depth-7 join is at-floor for 60M probe against a small build. The "waste" is just the unavoidable lineitem-scan-then-probe cost.

## Findings

- **Q18 is at realistic-parallelism floor** for its plan shape. The 14 ms gap to DuckDB is small.
- **Σ.Q.L10 PushDownLeftSemiRule + L9 bloom + RobinHoodSumF64Exec all working as designed** — visible in plan as `RobinHoodSumF64Exec` (×2) and `BuildSideBloomEmitterExec`. These collectively reduce orders 15M → 624 rows before the outer lineitem join.
- **Remaining gap is the 60M lineitem-scan-then-probe** which is structurally inescapable without pushing the bloom into the EmatixFastParquetExec BridgeFilter (same Q17 lever — L9-to-scan integration).

**Next:** B.19 (Q19 — 138.72 ms, +34% vs DuckDB).

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
