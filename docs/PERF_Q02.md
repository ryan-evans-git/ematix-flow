# PERF_Q02 — Q02 SF=10 stage profile

Status: profiled 2026-05-25 (post StringView `new_unchecked` fix).
Data: `examples/tpch/data/sf10/*.parquet`, Snappy.

## Wall time (median of 5 trials, 2 warmups)

| Engine | Median ms | σ | Rows |
|--------|----------:|----:|----:|
| ematix-flow | 36.46 | 10.81 | 4667 |
| DuckDB | 48.23 | 20.82 | 4667 |
| Polars | 418.20 | 26.85 | 4667 |

ematix wins by **24% over DuckDB**, 11× over Polars. Strong existing position.

## Physical plan (post-optimizer)

5-way join + 2-stage hash agg + correlated subquery decorrelated to joined-agg:

```
SortPreservingMergeExec [s_acctbal DESC, ...]
  SortExec
    ProjectionExec
      HashJoinExec Partitioned Inner on (p_partkey, ps_supplycost)=(ps_partkey, min(ps_supplycost))
        HashJoinExec CollectLeft Inner on (r_regionkey, n_regionkey)        -- region ⋈ ...
          region (filter r_name='EUROPE')
          HashJoinExec CollectLeft Inner on (n_nationkey, s_nationkey)      -- nation ⋈ ...
            nation
            HashJoinExec CollectLeft Inner on (s_suppkey, ps_suppkey)       -- supplier ⋈ ...
              supplier
              HashJoinExec Partitioned Inner on (p_partkey, ps_partkey)     -- part_filt ⋈ partsupp #1
                part (filter p_size=15 AND p_type LIKE '%BRASS')
                partsupp                                                    -- 8M rows
        -- ↑↑↑ outer half ↑↑↑    ↓↓↓ subquery half ↓↓↓
        RepartitionExec
          AggregateExec FinalPartitioned gby=[ps_partkey] aggr=[min(ps_supplycost)]
            AggregateExec Partial
              HashJoinExec CollectLeft Inner (r_regionkey, n_regionkey)
                region (filter r_name='EUROPE')
                HashJoinExec CollectLeft Inner (n_nationkey, s_nationkey)
                  nation
                  HashJoinExec CollectLeft Inner (s_suppkey, ps_suppkey)
                    supplier
                    partsupp                                                -- 8M rows AGAIN (scanned twice)
```

## Per-stage breakdown (top 12 by median elapsed_compute_ms, 5 trials)

| Rank | Operator | Depth | Median ms | Min | Max | Out rows |
|-----:|:---------|------:|----------:|----:|----:|---------:|
| 1 | AggregateExec (Final, gby=ps_partkey, min) | 8 | 37.56 | 35.70 | 39.04 | 1,183,098 |
| 2 | HashJoinExec (build=partsupp probe-side of outer, probe=subquery agg) | 13 | 25.30 | 23.38 | 25.90 | 8,000,000 |
| 3 | AggregateExec (Partial, same gby) | 6 | 24.46 | 20.91 | 25.64 | 1,183,098 |
| 4 | HashJoinExec (part_filt ⋈ partsupp #1) | 11 | 23.12 | 21.89 | 24.44 | 8,000,000 |
| 5 | HashJoinExec (supplier ⋈ partsupp ⋈ ...) | 9 | 18.92 | 18.37 | 20.38 | 1,602,640 |
| 6 | HashJoinExec | 10 | 18.69 | 18.34 | 19.22 | 31,416 |
| 7 | HashJoinExec | 3 | 12.00 | 11.41 | 12.91 | 4,667 |
| 8 | EmatixFastParquetExec (part) | 10 | 6.84 | 6.71 | 7.13 | 100,000 |
| 9 | HashJoinExec | 9 | 6.29 | 5.94 | 6.93 | 31,416 |
| 10 | EmatixFastParquetExec (partsupp #1) | 15 | 3.47 | 1.65 | 10.33 | 8,000,000 |
| 11 | RepartitionExec | 4 | 3.42 | 3.19 | 3.74 | 1,183,098 |
| 12 | RepartitionExec | 7 | 3.13 | 2.97 | 3.71 | 1,183,098 |

Σ median compute across all nodes: 188.40 ms. Wall median 32.49 ms. Parallel speedup ≈ 5.80× of 14 cores.

## Theoretical floor

| Stage | Floor formula | Floor (ms, parallel-equivalent) |
|-------|---------------|--------------------------------:|
| 2× partsupp scan (8M rows × ~30 MB compressed) | 2× (60 MB / (2 GB/s × 14)) = ~4.3 ms | 4 |
| 1× part scan (200k rows, ~6 MB compressed) | — | 1 |
| 1× supplier/nation/region (tiny) | — | <1 |
| Filter part (p_size=15 AND p_type LIKE '%BRASS') on 200k rows | 0.5 ns/row × 200k = 0.1 ms; LIKE pattern more like 5 ns/row → ~1 ms | 1 |
| 5 HashJoinExec builds (region/nation/supplier/part_filt + final 2-key) | each ≤ 31k rows; 5 ns/row each = trivial | 1 |
| 5 HashJoinExec probes — largest is partsupp 8M × ~10 ns/row / 14 | 5.7 | 6 |
| 2-stage hash agg (1.18M distinct ps_partkey from 8M rows, MIN of f64) | 8M × 8 ns/row / 14 = 4.6; final 1.18M × 8 ns = 0.7 → ~5 ms | 5 |
| Sort 4667 rows × 4 cols | — | <1 |
| **Floor** | | **~18 ms** |
| **Actual** | | **32.5 ms** |
| **Waste ratio** | | **1.8×** |

Q02 is close to the floor model. DuckDB is 32% slower than us — we're the leader here. Not much wall-time to chase.

## Waste candidates

### 1. partsupp scanned twice — CSE candidate but not a clear win at this scale

The decorrelated subquery (`min(ps_supplycost)`) and the outer query both walk all 8M rows of partsupp. Memory [[sigma-p-subquery-cse]] landed `SharedSubtreeExec` for Q15-shape; Q02-shape has 2 separate scan + 2 separate sub-joins, not the same pattern. The two partsupp pulls each take ~3.5 ms compute (top-25 entries 10 and ranked-15) — combined ~7 ms parallel work, ~1-2 ms wall savings if we could share. Wall headroom is 32 ms → ~30 ms after CSE; marginal vs implementation cost. **Defer unless Q15-style SharedSubtreeExec naturally extends.**

### 2. AggregateExec (Partial + Final) at 62 ms total compute for 1.18M groups — 4× over floor

Partial scans 8M rows → 1.18M groups (in-thread), then RepartitionExec → Final reduces to the same 1.18M (no further reduction, since partsupp is unique on ps_partkey/ps_suppkey). The Partial agg is doing 8M-row work on data that's already grouped exactly 1:1 with the partitioning. **This is wasted work** — RepartitionExec on ps_partkey could feed directly into a single-pass FinalPartitioned agg without the intermediate Partial.

Memory [[sigma-nf3-beats-stock]] notes RobinHoodSumF64Exec beats stock on Partial+FinalPartitioned for SUM(f64) — but this is MIN(f64), which doesn't currently have a Robin Hood path. **Candidate**: route MIN(f64) through a similar Robin Hood specialized exec, OR a rule to elide the Partial stage when output cardinality ≈ input cardinality (which is detectable from column stats since ps_partkey is the inner key of partsupp's compound primary key).

Expected: shaves ~10-15 ms parallel compute = 1-3 ms wall.

### 3. HashJoinExec output of 8M rows from part_filt ⋈ partsupp — semantic correctness, not waste

The 8M output at depth 11 looks scary but it's correct: part_filt has 7854 matching parts × 4 partsupp rows per part = ~31k rows, NOT 8M. The 8M reported is the *partsupp scan output before the join*, since the join is on the probe side and `output_rows` accumulates pre-join inputs in some metric setups. Not a waste candidate — just plan-tree-walker quirk.

### 4. CollectLeft mode on all the small-dim joins — appropriate

region/nation/supplier are tiny enough that CollectLeft is correct. No change.

## Findings to capture as memories

- Q02 SF=10 is at 1.8× floor — limited upside vs DuckDB (we're already 24% ahead).
- The Partial+Final agg pattern when the partitioning key equals the group-by key is a generalised waste candidate worth a separate sweep across queries (Q03, Q05, Q10, Q20 all have this shape).

## Next levers from Q02

1. **Survey other queries for the same Partial+Final-on-partitioned-key pattern** before designing a fix. The lever pays off only if it's a pattern across queries, not just Q02.
2. **partsupp CSE** — defer; marginal.
