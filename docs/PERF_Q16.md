# PERF_Q16 — Q16 SF=10 stage profile

## Wall time

| Engine | Median ms | σ | Rows |
|--------|----------:|----:|----:|
| ematix-flow | 50.22 | 0.63 | 27,840 |
| DuckDB | 66.20 | 0.61 | 27,840 |
| Polars | 176.11 | 17.67 | 27,840 |

**24% ahead of DuckDB**, 3.5× ahead of Polars.

## Physical plan

Count-distinct query: count(distinct ps_suppkey) per (p_brand, p_type, p_size). The distinct is implemented as 2-stage groupby (groupby-then-groupby pattern).

```
SortPreservingMergeExec [supplier_cnt DESC, ...]
  AggregateExec FinalPartitioned gby=[brand, type, size] count
    AggregateExec Partial
      AggregateExec FinalPartitioned gby=[brand, type, size, ps_suppkey]    -- distinct step
        AggregateExec Partial
          HashJoinExec Partitioned Inner (p_partkey, ps_partkey)
            part (filter p_brand != Brand#45 AND p_type NOT LIKE 'MEDIUM POLISHED %' AND p_size IN (8 vals))
            HashJoinExec CollectLeft RightAnti (s_suppkey, ps_suppkey)
              supplier (filter s_comment LIKE '%Customer%Complaints%')
              partsupp                                               -- 8M
```

## Per-stage breakdown (top 8)

| Rank | Operator | Median ms | Out rows |
|-----:|:---------|----------:|---------:|
| 1 | HashJoinExec (part ⋈ partsupp_filt) | 103.99 | 7,995,520 |
| 2 | AggregateExec Partial (distinct gby + brand/type/size) | 56.15 | 1,186,339 |
| 3 | HashJoinExec RightAnti (supplier_filt ⋈ partsupp) | 52.18 | 1,186,602 |
| 4 | AggregateExec FinalPartitioned (distinct) | 50.27 | 1,186,580 |
| 5 | AggregateExec Partial (final count) | 37.51 | 364,665 |
| 6 | RepartitionExec | 24.67 | 1,186,580 |
| 7 | RepartitionExec | 20.37 | 7,995,520 |
| 8 | EmatixFastParquetExec (part) | 14.32 | 307,006 |

Σ median compute: ~390 ms. Wall median 50 ms. Parallel speedup ≈ 7.8×.

## Theoretical floor

| Stage | Floor (ms) |
|-------|-----------:|
| part scan + filter (1.5M → 307k) | 3 |
| partsupp scan (8M × 2 cols) | 5 |
| supplier scan + LIKE filter (100k × 2 cols) | 1 |
| RightAnti join supplier_filt × partsupp | 5 |
| HashJoin part_filt × partsupp_filt | 5 |
| 4-stage count-distinct (groupby-then-groupby) | 10 |
| Sort 27k rows | <1 |
| **Floor** | **~30 ms** |
| **Actual** | **50 ms** |
| **Waste ratio** | **1.7×** |

## Waste candidates

### 1. SIMD LIKE on supplier.s_comment — small gain, same lever as Q13

LIKE filter on 100k supplier rows is trivial — 1-3 ms max. SIMD kernel wouldn't move the needle here.

### 2. Part filter (3 predicates) — most pushed, multi-predicate may benefit from BridgeFilter extension

Plan shows `FilterExec` as a separate node above the part scan with all 3 predicates. The scan output rows (307k) doesn't equal the FilterExec output rows directly visible here, but the rule may not be folding the multi-predicate filter into BridgeFilter.

Lower-priority — part is only 1.5M rows; reading more or less of it is ~1 ms.

### 3. Count-distinct stage at ~145 ms total compute (rows 2, 4, 5) = 19 ms wall

The groupby-then-groupby distinct pattern is the standard DataFusion lowering. 4-column compound group key (brand, type, size, ps_suppkey) is wide. Same lever as Q10's wide-key agg (functional dependency / smaller hash key).

For Q16 specifically, count-distinct could be implemented as a specialized SIMD-tagged hash agg (memory [[sigma-q-l12-rejected]] explored this but found it shape-dependent: +38% low-cardinality / -19% Q18-shape; not currently wired).

## Findings

Q16 is well within its floor band. The candidates listed wouldn't move the wall by more than 5-10%. Move on.

## Next levers

(none with payoff >5% — Q16 is already strong)
