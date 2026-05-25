# PERF_Q07 — Q07 SF=10 stage profile

Status: profiled 2026-05-25.

## Wall time

| Engine | Median ms | σ | Rows |
|--------|----------:|----:|----:|
| ematix-flow | 156.97 | 11.55 | 4 |
| DuckDB | 135.89 | 4.26 | 4 |
| Polars | 1307.83 | 46.51 | 4 |

**15% behind DuckDB**, 8× ahead of Polars.

## Physical plan

6-table join: nation ⋈ supplier ⋈ lineitem (filter shipdate ∈ 1995-96) ⋈ orders ⋈ customer ⋈ nation (with FRANCE/GERMANY pair on the two nations). L9 build-side bloom emitters land on both nation→supplier and nation→customer edges.

```
SortPreservingMergeExec [supp_nation ASC, cust_nation ASC, l_year ASC]
  ...
  AggregateExec FinalPartitioned gby=[supp_nation, cust_nation, l_year]
    HashJoinExec CollectLeft Inner (n_nationkey, c_nationkey) filter=FRANCE/GERMANY pair
      BuildSideBloomEmitterExec (nation 25 → target)
        nation (filter n_name ∈ FRANCE/GERMANY)
      HashJoinExec CollectLeft Inner (n_nationkey, s_nationkey)
        BuildSideBloomEmitterExec
          nation (filter n_name ∈ FRANCE/GERMANY)
        HashJoinExec Partitioned Inner (c_custkey, o_custkey)
          customer
          HashJoinExec Partitioned Inner (l_orderkey, o_orderkey)
            HashJoinExec CollectLeft Inner (s_suppkey, l_suppkey)
              supplier
              FilterExec l_shipdate ∈ [1995-01-01, 1996-12-31]
                lineitem                                    -- 60M → 18.2M
            orders                                          -- 15M rows
```

## Per-stage breakdown (top 8)

| Rank | Operator | Depth | Median ms | Out rows |
|-----:|:---------|------:|----------:|---------:|
| 1 | EmatixFastParquetExec (lineitem) | 17 | 1049.48 | 18,230,325 |
| 2 | HashJoinExec (supp+l ⋈ orders+cust) | 13 | 210.97 | 1,460,257 |
| 3 | HashJoinExec (cust ⋈ orders) | 15 | 91.02 | 1,460,257 |
| 4 | EmatixFastParquetExec (customer) | 13 | 34.56 | 120,469 |
| 5 | HashJoinExec (supplier ⋈ lineitem) | 11 | 11.15 | 117,014 |
| 6 | RepartitionExec | 14 | 9.42 | 1,460,257 |
| 7 | EmatixFastParquetExec (supplier) | 16 | 7.70 | 8,010 |
| 8 | FilterExec (l_shipdate range) | 16 | 7.52 | 18,230,325 |

Σ median compute: 1434 ms. Wall median 163 ms. Parallel speedup ≈ 8.78× of 14 cores.

## Theoretical floor

| Stage | Floor (ms) |
|-------|-----------:|
| lineitem scan + decode 60M × 5 cols (Snappy) | 15 |
| Filter l_shipdate range (60M → 18M) | 2 |
| orders scan (15M × 2) | 4 |
| customer scan (1.5M × 2) | 1 |
| supplier/nation | <1 |
| Probe lineitem (18M) through cust+orders build (1.46M × 12 ns / 14) | 16 |
| Other joins | ≤2 |
| Hash agg (56 groups) | <1 |
| **Floor** | **~40 ms** |
| **Actual** | **163 ms** |
| **Waste ratio** | **4.1×** |

## Waste candidates

### 1. l_shipdate filter NOT pushed to scan (60M decoded, 18M kept)

Same pattern as Q03. FilterExec is a separate node at depth 16 above the lineitem scan; 42M rows decoded then discarded. Predicate is a simple i32 range — bridge-filter-eligible.

Memory [[sigma-e5-streaming-late-mat-landed]] notes the masked-decode path is dormant pending dict-preserved Utf8View, but l_shipdate (i32) doesn't depend on that.

Expected impact: lineitem scan drops from ~110 ms wall to ~35 ms (42M-row decode skipped). Wall: 163 → ~90 ms (~45% improvement, would put us 33% ahead of DuckDB).

### 2. L9 build-side bloom is firing — but nation is too small to help

Two `BuildSideBloomEmitterExec` nodes appear on the nation→supplier and nation→customer edges. Nation post-filter has 2 rows (just FRANCE+GERMANY). A bloom of 2 keys has near-zero false-positive savings vs just filtering supplier/customer by nationkey directly. The bloom emitter is dead weight here — not harmful, just no signal.

Worth checking the L9 ratio guard: if `min_probe_to_build_ratio` is 1024 but build is 2 keys, the rule shouldn't fire at all. Or if it does fire, it should be a no-op.

### 3. HashJoinExec (cust+orders ⋈ lineitem) at 211 ms compute = 24 ms wall

The probe processes 18M lineitem rows against a 1.46M (cust ⋈ orders) build. Same memory-bandwidth-bound pattern as Q03 / Q05.

If candidate #1 (l_shipdate pushdown) works, this probe processes only the surviving 18M rather than the full 60M, which it already is. The 211 ms is intrinsic to the join shape.

## Findings to capture as memories

- Q07 SF=10 candidate aligns with Q03 / Q05 — l_shipdate pushdown into scan is a **multi-query** lever, not Q-specific.
- BuildSideBloomEmitter on nation joins is a near-no-op at this scale (build cardinality ≤ a few rows). Cleanup: detect "build size <16" and skip emission.

## Next levers from Q07

1. (Cross-Q) **l_shipdate i32-range bridge filter pushdown** — affects Q03, Q07, Q12, Q14, Q15. Single lever, multi-query payoff.
2. (Cleanup) **L9 build-size threshold** — don't emit blooms for builds <16 keys (Q07 nation, Q12 customer subset, etc).
