# PERF_Q10 — Q10 SF=10 stage profile

## Wall time

| Engine | Median ms | σ | Rows |
|--------|----------:|----:|----:|
| ematix-flow | 258.29 | 5.36 | 381,105 |
| DuckDB | 427.45 | 20.24 | 381,105 |
| Polars | 4,122.30 | 124.30 | 381,105 |

**39% ahead of DuckDB**, 16× ahead of Polars. Big ematix win.

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
