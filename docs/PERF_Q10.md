# PERF_Q10 — Q10 SF=10 stage profile

Status: **re-verified 2026-05-26** (Σ.AH Phase B.10).

## Wall time (20×3 canonical 2026-05-26)

| Engine | Median ms | σ |
|--------|----------:|----:|
| ematix-flow | **231.97** | 9.02 |
| DuckDB | 408.76 | 7.78 |

**43% ahead of DuckDB** — biggest win margin of any query. Stage profile 5-trial: 237.52 ms.

## Per-stage (current; Σ compute 2026.34 ms / wall 237.52 ms → **8.53× parallelism = 61%**)

| Stage | Floor (proj-aware) | Actual sum | Status |
|-------|-------------------:|-----------:|--------|
| HashJoin (cust+orders) ⋈ lineitem (build 573k=L2, probe 14.8M) | 14.8M × 12 ns = 178 ms | 824.47 | **4.6× over** (build > probe? No — probe 14.8M > build 573k. So why slow?) |
| EmatixFastParquetExec lineitem + l_returnflag='R' BridgeFilter (60M → 14.8M) | ~600 ms | 478.76 | **sub-floor** (scan pushdown working ✓) |
| RepartitionExec (573k Hash(o_orderkey)) | ~30 ms | 268.62 | 9× over — surprising; the 573k is small |
| AggregateExec Partial (1.15M → 482k groups, 7-col key) | ~28 ms (10 ns × 1.15M ×~2.5 for 7-col) | 159.13 | **5.7× over** — 7-col gby very expensive |
| HashJoin cust ⋈ orders (build 1.5M, probe 573k) | 573k × 15 ns = 9 ms; build 1.5M × 5 ns = 7.5 ms | ~17 ms | 131.77 | **7.8× over** — build >> probe (1.5M build, 573k probe) |
| AggregateExec FinalPartitioned | ~30 ms | 68.78 | 2.3× over |
| RepartitionExec | ~20 | 39.40 | mild over |
| Sort + SortPreservingMerge | ~20 | 27 | at-floor ✓ |

**Σ floor sum: ~930 ms; observed 2026 ms. ~1100 ms over-floor parallel waste (~130 ms wall).**

## Σ.AH waste candidates

| Rank | Candidate | Wall savings | Confidence | Notes |
|-----:|-----------|-------------:|:----------:|-------|
| 1 | **Group-by functional-dependency simplifier** (c_custkey unique → group by 1 col, project 6 others) | ~50 ms | medium | 7-col gby compute 159+69 = 228 ms parallel. Drop to ~30 ms (i64-only key, RH-eligible). 7-col-gby is the dominant inefficiency. |
| 2 | **HashJoin (cust+orders) ⋈ lineitem at 824 ms** vs 178 ms floor — needs samply | ~30 ms | low | Build is small (573k → L2) but probe is 14.8M; 824/14.8M = 56 ns/row probe is 5× the L2 floor of 12 ns. Possible cache thrashing from 14 partitions × 573k = 47 MB total build (each partition's build ~3.3 MB but L2 cluster shared). |
| 3 | **HashJoin cust ⋈ orders build-vs-probe mis-order** (build 1.5M > probe 573k) | ~7 ms | low | Same Q07/Q08/Q09 pattern. |
| 4 | **Customer 2-RG bottleneck** | ~3 ms | high | Same Q03/Q05/Q07/Q08. |

## Findings

- **Q10 is +43% ahead of DuckDB but has the most "still-could-improve" waste** — 7-col gby is the dominant lever. Memory `feedback_no_tpch_hardcoding.md` requires generalised, not Q-specific; functional-dependency simplifier is a generalised pattern (any UNIQUE-key passthrough).
- **Q10 confirms l_returnflag = string filter pushdown WORKS** (60M → 14.8M during scan), in contrast to l_shipdate > date which doesn't push (Q03/Q07).
- 4th query showing **build-vs-probe mis-order** (Q07, Q08, Q09, Q10).

**Next:** B.11 (Q11 — 11.59 ms; smallest by far, 62% ahead of DuckDB).

## Physical plan

3-way join + wide group-by. Group key is 7 columns including Utf8 strings.

```
SortPreservingMergeExec [revenue DESC]
  AggregateExec FinalPartitioned gby=[c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment] sum(revenue)
    HashJoinExec CollectLeft Inner (n_nationkey, c_nationkey)   -- nation
      nation
      HashJoinExec Partitioned Inner (o_orderkey, l_orderkey)
        HashJoinExec Partitioned Inner (c_custkey, o_custkey)
          customer                                              -- 1.5M, 7 cols read
          orders (filter o_orderdate ∈ [1993-10-01, 1994-01-01))-- 573k rows
        lineitem (filter l_returnflag='R')                      -- 14.8M (filter pushed to scan)
```

## Per-stage breakdown (top 8)

| Rank | Operator | Depth | Median ms | Out rows |
|-----:|:---------|------:|----------:|---------:|
| 1 | HashJoinExec (orders+cust ⋈ lineitem) | 8 | 798.27 | 1,147,084 |
| 2 | EmatixFastParquetExec (lineitem, BridgeFilter l_returnflag='R') | 11 | 663.49 | 14,808,183 |
| 3 | RepartitionExec | 9 | 252.40 | 573,157 |
| 4 | AggregateExec Partial (7-col key) | 5 | 150.25 | 482,528 |
| 5 | HashJoinExec (cust ⋈ orders) | 10 | 148.68 | 573,157 |
| 6 | AggregateExec FinalPartitioned | 3 | 69.88 | 381,105 |
| 7 | RepartitionExec | 4 | 36.67 | 482,528 |
| 8 | FilterExec | 10 | 15.81 | 14,808,183 |

Σ median compute: 2184 ms. Wall median 240 ms. **Parallel speedup ≈ 9.09×** — strong.

## Theoretical floor

| Stage | Floor (ms) |
|-------|-----------:|
| lineitem scan + filter pushed (60M → 15M decode skip) | 6 |
| orders scan + filter (15M → 573k) | 5 |
| customer 1.5M × 7 cols (incl Utf8) | 5 |
| nation | <1 |
| HashJoin cust ⋈ orders (build 1.5M, probe 573k × 12 ns) | 6 |
| HashJoin (cust+orders) ⋈ lineitem (build 573k, probe 14.8M × 12 ns / 14) | 13 |
| Hash agg 7-col key (Utf8 cols) → 381k groups, 14.8M rows × ~25 ns / 14 | 26 |
| Sort 381k × revenue desc | 5 |
| **Floor** | **~66 ms** |
| **Actual** | **240 ms** |
| **Waste ratio** | **3.6×** |

## Waste candidates

### 1. AggregateExec Partial 150 ms compute on 7-col group key

7 columns in the group key including 3-5 variable-length strings (c_name, c_phone, c_address, c_comment). Per-row hash and probe through SwissTable involves hashing/comparing all 7 cols. The "Robin Hood" specialized path only handles i64 keys ([[sigma-nf3-beats-stock]]) — doesn't apply here.

The 7-col key is also functionally redundant: c_custkey alone uniquely identifies the customer; the other 6 columns are passthrough. A rule could detect this (custkey is unique in customer → group_by_functionally_dependent_columns) and group by c_custkey only, then re-attach the rest after aggregation.

Expected impact: cuts 7-col-key compute by ~5-6× — agg compute 150 → ~25 ms, wall 240 → ~210 ms.

Memory [[sigma-p-subquery-cse]]-related: this is a different kind of CSE — functional-dependency-based group-by simplification.

### 2. HashJoinExec (orders+cust ⋈ lineitem) at 798 ms compute = 88 ms wall

15M lineitem rows probe against 573k (cust ⋈ orders) build. 88 ms wall is 6 ns/row probe — already close to floor. Hard to improve without join order.

### 3. l_returnflag='R' filter IS being pushed (already)

The scan output rows (14.8M) matches FilterExec output — confirms filter pushdown. Scan only emits passing rows. **Good — Q10 doesn't have the Q03/Q05/Q07/Q08/Q09 lineitem-pushdown miss.**

This is a useful data point: when does the filter pushdown work and when doesn't it? Comparing:
- Q01 (Snappy l_shipdate range): pushed ✓
- Q06 (Snappy l_shipdate AND l_discount AND l_quantity): pushed ✓
- Q10 (l_returnflag string eq): pushed ✓
- Q03 (l_shipdate > date): NOT pushed ✗
- Q07 (l_shipdate BETWEEN): NOT pushed ✗

The negative cases all use the **post-stage FilterExec**, suggesting the rule that converts FilterExec → BridgeFilter isn't matching the BETWEEN / > shapes — maybe an `Expr` pattern mismatch (`a > x AND a < y` vs `a BETWEEN x AND y`). This refines the L9 audit task.

## Findings

- **Q10 lineitem filter pushdown DOES work** for `l_returnflag = string`. Compare to Q03 (`l_shipdate > date`) which doesn't push. The InjectFusedFilter rule pattern likely accepts equality but not range. Worth a closer look — a 5-character extension to the pattern matcher could close several queries.
- **7-col group-by with c_custkey-unique-passthrough is a generalisable lever**: detect functional dependency, group by smaller key, project the rest.

## Next levers

1. (Cross-Q) **InjectFusedFilter pattern matcher**: extend to range and BETWEEN predicates on date/i32. Single edit could unlock Q03, Q05, Q07, Q12 scan pushdown.
2. **Group-by functional-dependency simplifier** — applicable to Q10. Detect via FK+unique-key constraints (we already have these in the table provider).
