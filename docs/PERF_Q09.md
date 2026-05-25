# PERF_Q09 — Q09 SF=10 stage profile

## Wall time

| Engine | Median ms | σ | Rows |
|--------|----------:|----:|----:|
| ematix-flow | 278.33 | 18.53 | 175 |
| DuckDB | 318.26 | 12.18 | 175 |
| Polars | 441.81 | 19.05 | 175 |

**13% ahead of DuckDB**, 1.6× ahead of Polars.

## Physical plan

6-table join: part (filter p_name LIKE '%green%') ⋈ lineitem ⋈ supplier ⋈ partsupp ⋈ orders ⋈ nation. 2-key join on (ps_partkey + ps_suppkey).

```
SortPreservingMergeExec [nation ASC, o_year DESC]
  AggregateExec gby=[nation, o_year] sum(amount)
    HashJoinExec CollectLeft (n_nationkey, s_nationkey) -- nation
      HashJoinExec Partitioned (o_orderkey, l_orderkey)
        orders
        HashJoinExec Partitioned (2-key) (ps_suppkey, l_suppkey) ∧ (ps_partkey, l_partkey)
          partsupp -- 8M
          HashJoinExec CollectLeft (s_suppkey, l_suppkey)
            supplier
            HashJoinExec Partitioned (p_partkey, l_partkey)
              part (filter p_name LIKE '%green%')   -- ~10k matching parts
              lineitem                              -- 60M, no filter
```

## Per-stage breakdown (top 8)

| Rank | Operator | Depth | Median ms | Out rows |
|-----:|:---------|------:|----------:|---------:|
| 1 | HashJoinExec (part_filt ⋈ lineitem) | 16 | 506.25 | 3,261,613 |
| 2 | HashJoinExec (above ⋈ supplier) | 9 | 444.70 | 3,261,613 |
| 3 | HashJoinExec (above ⋈ partsupp 2-key) | 12 | 303.65 | 3,261,613 |
| 4 | EmatixFastParquetExec (lineitem) | 18 | 180.56 | 59,986,052 |
| 5 | AggregateExec (Partial gby=2 cols, sum f64) | 5 | 64.17 | 2,450 |
| 6 | EmatixFastParquetExec (orders) | 11 | 55.24 | 15,000,000 |
| 7 | HashJoinExec (above ⋈ orders) | 15 | 42.17 | 3,261,613 |
| 8 | ProjectionExec | 6 | 31.80 | 3,261,613 |

Σ median compute: 1737 ms. Wall median 276 ms. Parallel speedup ≈ 6.28×.

## Theoretical floor

| Stage | Floor (ms) |
|-------|-----------:|
| lineitem scan + decode 60M × 6 cols | 18 |
| partsupp scan 8M × 3 cols | 5 |
| orders scan 15M × 2 cols | 4 |
| part scan + LIKE filter (1.5M × 2) | 2 |
| supplier/nation | <1 |
| HashJoin part_filt 10k build × lineitem 60M probe × 15 ns / 14 | 64 |
| HashJoin chain remaining | ~10 |
| Hash agg (2450 groups, partial+final, f64 sum) | 3 |
| **Floor** | **~110 ms** |
| **Actual** | **276 ms** |
| **Waste ratio** | **2.5×** |

## Waste candidates

### 1. Same L9 missing-bloom-edge as Q05/Q07/Q08

Q09 has the same shape: small-build (part_filt 10k) → large-probe (lineitem 60M). No `BuildSideBloomEmitterExec` visible in the plan between part_filt and lineitem. If L9 fired, lineitem decode could be skipped on the 99.8% of rows whose l_partkey doesn't match the filter.

This is now the **4th consecutive query** showing this miss (Q05, Q07, Q08, Q09). It's clearly a pattern.

### 2. AggregateExec Partial at 64 ms compute for 2450 groups — high

Partial agg processing 3.26M rows into 2450 groups: 64 ms compute = ~22 ns/row. Memory [[sigma-nf3-beats-stock]] says RobinHoodSumF64Exec beats stock here. But Q09's group-by is `(nation, o_year)` — 2-column key (Utf8 + i32). RobinHoodSumF64Exec is keyed on i64 only.

Worth a separate Robin-Hood-like specialization for 2-column-key SUM(f64) aggregations.

### 3. partsupp 8M scan visible as separate node (no L9)

partsupp acts as the build of the 2-key join (ps_suppkey, ps_partkey). Build side is 8M rows, probe is the post-supplier output. The 2-key build is large enough to spill out of L2.

If a runtime bloom were emitted from (part_filt ⋈ lineitem) on l_partkey, partsupp could be pre-filtered to only the ~10k matching ps_partkey rows. Same lever as candidate #1.

## Findings

- **Strong consistent pattern across Q05/Q07/Q08/Q09: small-build → lineitem L9 bloom not firing.** Worth investigating as a single audit + fix rather than 4 separate query-level investigations.
- Q09's `(nation, o_year)` group-by aggregate is 22 ns/row — multi-column-key specialization is a possible lever.

## Next levers

1. **L9 audit** — single deliverable across multiple queries.
2. Multi-column Robin-Hood agg variant (deferred — first prove L9 is the bigger lever).
