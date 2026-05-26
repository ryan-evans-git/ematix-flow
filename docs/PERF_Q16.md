# PERF_Q16 — Q16 SF=10 stage profile

Status: **re-verified 2026-05-26** (Σ.AH B.16).

## Wall time (20×3 canonical 2026-05-26)

| Engine | Median ms | σ |
|--------|----------:|----:|
| ematix-flow | **50.01** | 1.53 |
| DuckDB | 68.51 | 2.49 |

**27% ahead of DuckDB** (was 24%). Stage profile 5-trial: 49.57 ms.

## Per-stage decomposition

Σ compute 393.35 ms / wall 49.57 ms = **7.94× parallelism = 57%**.

| Stage | Floor | Actual | Status |
|-------|-------|--------|--------|
| AggregateExec FinalPartitioned (distinct step, 1.19M → 364k groups, 4-col key) | ~30 ms | 50.94 | 1.7× over (4-col key) |
| AggregateExec Partial (distinct step) | ~30 ms | 39.43 | at-floor ✓ |
| RepartitionExec on 4-col key (1.19M rows) | ~25 ms | 23.35 | at-floor ✓ |
| RepartitionExec (partsupp 8M) | ~80 ms | 20.38 | sub-floor (async) |
| EmatixFastParquetExec part (2M → 307k via filter+LIKE) | ~20 ms | 14.03 | at-floor ✓ |
| AggregateExec FinalPartitioned (count → 27k groups, 3-col key) | ~10 ms | 6.86 | mild over |
| RepartitionExec on 3-col key (365k rows) | small | 6.17 | at-floor ✓ |
| EmatixFastParquetExec supplier (100k) | trivial | 5.23 | mild |
| FilterExec residuals, top ops | small | <3 each | at-floor ✓ |

Σ floor ~270 ms; observed 393 ms — ~120 ms over-floor (~15 ms wall). Σ/7.94 = 49.5 ms wall = matches observed.

## Findings

- **Q16 at realistic floor with mild over-floor on the 4-col distinct group-by (50 ms Partial+Final for 364k groups).**
- The distinct-as-groupby pattern (group-by 4-col key then count distinct ps_suppkey) is structurally expensive. DuckDB and ematix both pay it; we win via faster ingest. No new lever.
- Q16's 3-clause part filter (`p_brand != x AND p_type NOT LIKE y AND p_size IN [8 vals]`) is residual FilterExec (cost 2.56 ms) — small but non-zero. The NOT-LIKE pushdown into scan would save the residual at ~3 ms wall.

**Next:** B.17 (Q17 — 175.26 ms, −6% behind DuckDB; correlated-subquery shape).

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
