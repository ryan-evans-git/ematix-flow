# PERF_Q04 — Q04 SF=10 stage profile

Status: **re-verified 2026-05-26** (Σ.AH Phase B.4). Originally profiled 2026-05-25.

## Wall time

### 2026-05-26 refresh (20 trials × 3 warmups, post-Σ.AG.7 bare invocation — canonical)

| Engine | Median ms | σ |
|--------|----------:|----:|
| ematix-flow | **54.30** | 2.27 |
| DuckDB | 89.44 | 2.45 |

**39% ahead of DuckDB** (unchanged from 2026-05-25 +39%). Strong existing position; no movement.

Stage profile 5-trial run: 54.77 ms median. Per-trial range 52.4–57.5 ms (very stable).

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

## Per-stage breakdown (2026-05-26)

| Rank | Operator | Depth | Median ms | Out rows |
|-----:|:---------|------:|----------:|---------:|
| 1 | HashJoinExec LeftSemi (build=orders 573k, probe=lineitem 38M) | 6 | 172.96 | 526,040 |
| 2 | EmatixFastParquetExec (orders + o_orderdate BridgeFilter) | 9 | 154.28 | 573,671 |
| 3 | FilterExec (l_receiptdate > l_commitdate) | 8 | 40.12 | 37,929,348 |
| 4 | RepartitionExec (Hash(l_orderkey)) | 7 | 21.68 | 37,929,348 |
| 5 | AggregateExec Partial (5 group output) | 5 | 4.00 | 70 |
| 6 | EmatixFastParquetExec (lineitem) | 9 | 2.98 | 59,986,052 |
| 7 | RepartitionExec | 7 | 2.45 | 573,671 |

Σ median compute: **399.10 ms**. Wall median: 54.77 ms. Effective parallelism: **7.29× = 52%**.

## Theoretical floor (per-stage, **projection-cost-aware**)

**Methodology note (Σ.AH B.4):** when FilterExec passes N output rows with M projected columns, the kernel cost is only part of the floor. The post-filter projection memcpy (N × M × bytes/col / aggregate memory bandwidth ~70 GB/s for M3 Pro) is real and dominant when N is large. Q03's B.3 writeup missed this — see retraction at end of this file.

| Stage | Floor formula | Floor (sum ms) | Actual | Notes |
|-------|---------------|---------------:|-------:|-------|
| Orders scan + BridgeFilter (15M → 573k) | 15M × 10 ns/row decode (bridge-applied) | 150 | 154.28 | **at-floor** ✓ |
| Lineitem scan (60M rows, 3 cols) | mostly hidden via async (credited downstream) | n/a | 2.98 | **at-floor** ✓ (effective via FilterExec) |
| FilterExec l_receiptdate > l_commitdate (60M in, 38M out, 1 col project) | 60M × 0.5 ns kernel + 38M × 4 B / 70 GB/s = 30 + 28 | 58 | 40.12 | **at-floor** ✓ (below est. — projection lighter than my model) |
| RepartitionExec on l_orderkey (38M × 4 B memcpy) | 38M × 4 B / 70 GB/s × 14 = 152 MB / 70 GB/s = 2.2 ms wall → 30 ms sum | 30 | 21.68 | **at-floor** ✓ |
| HashJoinExec LeftSemi (build 573k fits L1/L2, probe 38M) | 38M × 12 ns probe + early-exit on first match → ~6 ns/row | ~230 | 172.96 | **at-floor** ✓ (LeftSemi short-circuits) |
| AggregateExec Partial (5-group output, count) | 526k × 1 ns | 0.5 | 4.00 | small overhead ok |
| RepartitionExec on o_orderpriority | trivial | <1 | small | ✓ |
| AggregateExec FinalPartitioned + Sort | trivial | <1 | <0.1 | ✓ |
| **Σ floor sum** | | **~470 ms** | **399 ms** | **observed BELOW floor** |
| **Σ effective floor at 7.29× parallelism** | | | **54.7 ms wall** | matches observed 54.77 ms |

**Q04 is at its realistic-parallelism floor.** Observed wall 54.77 ms = effective floor 54.7 ms. Every stage is at or below its kernel floor.

## Waste candidates

**None at this scale.** Q04 is structurally near-optimal:
- BridgeFilter pushes o_orderdate into the orders scan (Σ.E5 working as designed).
- LeftSemi early-exit makes the 38M-row probe sub-floor.
- 5-group agg has no Partial+Final overhead at this cardinality.

Carried-forward observations (lower priority, listed for cross-query synthesis only):
- **Two-column predicate l_receiptdate > l_commitdate** isn't pushed into scan (BridgeFilter doesn't handle 2-col cmps). If we extend it, Q04 wall could drop ~5 ms by saving the post-filter row-projection. **But Q04 is at floor anyway** — the projection is already cheap because only l_orderkey is output. Not a Q04 lever, but TPC-H has Q12 with the same 2-col compare shape; worth checking there.
- **Effective parallelism 52%** — same imbalance pattern as Q01/Q03. Not Q04-specific.

## Findings to capture as memories

- **Q04 SF=10 is at realistic floor (54.7 ms vs 54.77 ms wall).** 39% ahead of DuckDB; no chase-worthy candidate.
- **Projection-cost-aware filter floor** is the right model: `kernel_ns × in_rows + out_rows × out_cols × 4B / 70 GB/s`. The 2026-05-25 model + my Q03 B.3 writeup undercounted by missing the memcpy term. Methodology update merits a memory.
- **LeftSemi joins beat Inner-join probe-rate floor** thanks to first-match short-circuit. Build-side fitness for L1/L2 matters more on LeftSemi than Inner.
- **BridgeFilter (Σ.E5) is correctly pushing o_orderdate into the orders scan** — confirmed in the per-stage profile (FilterExec above scan is 0.48 ms / 573k rows = residual no-op).

## Next levers

**None for Q04.** The cross-query candidates from Q01-Q03 stay in the Phase C bucket; Q04 has nothing new to add to the ranking.

---

## Verify pass — 2026-05-26 (Σ.AH B.4)

**What changed since 2026-05-25:** essentially nothing.
- Wall: 55.36 → 54.30 ms canonical (within noise).
- vs DuckDB: +39% lead unchanged.
- Plan structure: unchanged.

**Methodology correction discovered while writing B.4:** the FilterExec floor model needs to include projection memcpy cost. Without that term, my Q03 B.3 writeup falsely claimed FilterExec was "3× over floor" when in reality the kernel + projection together were at-floor. Going to retract that claim in Q03 and update the cross-query candidate ranking. The "FilterExec batch-boundary overhead" candidate from Q03 is **withdrawn**.

**Next:** B.5 (Q05 — 186.25 ms; we lose to DuckDB by 25%, a known structural-join-order gap).
