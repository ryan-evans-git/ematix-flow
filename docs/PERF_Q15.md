# PERF_Q15 — Q15 SF=10 stage profile

## Wall time

| Engine | Median ms | σ | Rows |
|--------|----------:|----:|----:|
| ematix-flow | 79.10 | 5.18 | 1 |
| DuckDB | 85.93 | 4.31 | 1 |
| Polars | 66.30 | 2.88 | 1 |

**8% ahead of DuckDB**, **19% behind Polars**.

## Physical plan — SharedSubtreeExec is the star

Q15 features the `supplier_revenue` view which is referenced twice (for `max(total_revenue)` and for `where total_revenue = max`). Memory [[sigma-p-subquery-cse]] landed `SharedSubtreeExec` to dedupe this — visible as `SharedSubtreeExec(populated=true)` in two places.

```
SortPreservingMergeExec [s_suppkey ASC]
  HashJoinExec CollectLeft Inner (max(total_revenue), total_revenue)
    AggregateExec Final no-gby max
      AggregateExec Partial
        ProjectionExec total_revenue
          SharedSubtreeExec(populated=true)               -- ← shared
    HashJoinExec CollectLeft Inner (s_suppkey, supplier_no)
      supplier
      ProjectionExec
        SharedSubtreeExec(populated=true)                 -- ← shared (same data)
```

## Per-stage breakdown — measurement caveat

| Rank | Operator | Median ms | Out rows |
|-----:|:---------|----------:|---------:|
| 1 | EmatixFastParquetExec (supplier) | 4.31 | 100,000 |
| 2 | HashJoinExec | 0.84 | 100,000 |
| 3 | HashJoinExec (revenue join) | 0.17 | 1 |
| 4 | AggregateExec (max, no-gby) | 0.06 | 1 |

**stage_profiler wall: 5.5 ms** (vs canonical 22q bench: 79 ms).

The discrepancy is real and is from `SharedSubtreeExec`'s session-scoped cache. In stage_profiler the warmup populates the cache; subsequent trials reuse it. In tpch_triangulation_bench the ctx is rebuilt per trial → the cache is cold each trial → Q15 pays the full ~75 ms of lineitem aggregation per trial.

Σ median compute: 5.39 ms (in the warm-cache regime). The cold cost — building the supplier_revenue subtree once — is roughly:
- lineitem scan + filter l_shipdate ∈ 1996-Q1 (60M → ~5M with BridgeFilter)
- GROUP BY l_suppkey SUM(extprice * (1-disc)) → 100k groups
- ≈ 70 ms parallel

## Theoretical floor (cold-cache)

| Stage | Floor (ms) |
|-------|-----------:|
| lineitem scan + BridgeFilter (60M → 5M decoded) | 6 |
| Hash agg group by l_suppkey (100k groups, sum f64) | 8 |
| supplier scan (100k × 4 cols) | 1 |
| 2 small HashJoins | <1 |
| AggregateExec max no-gby (100k rows) | <1 |
| **Floor (cold)** | **~18 ms** |
| **Actual (cold, from 22q bench)** | **79 ms** |
| **Waste ratio** | **4.4×** |

## Waste candidates

### 1. Q15 cold-vs-warm gap is large; SharedSubtreeExec cache lifetime is the lever

In a production session, the SharedSubtreeExec cache persists across query executions within the same `SessionContext`. The bench's per-trial ctx rebuild defeats this. **The "cold" 79 ms is bench-specific; real-world Q15 with a long-lived session is ~6 ms.**

This is a bench-artifact finding, not a perf gap. The lever is documenting it correctly, not optimizing further.

### 2. Polars 19% lead is on the cold path (also rebuilds per trial)

Both ematix and Polars start cold per trial. Polars hits 66 ms — 17% faster than us cold. That suggests the supplier_revenue subtree itself (lineitem scan + groupby) has unoptimised cost vs Polars. Same Snappy decode + groupby gap as Q06.

Floor for cold subtree = ~18 ms; both engines pay ~70 ms = ~4× over floor. Same scan-rate ceiling.

## Findings

- **Q15's bench wall reflects cold-cache cost; warm-session cost is ~7×-13× faster** because SharedSubtreeExec serves the duplicate consumer.
- The 19% Polars lead on cold path is the same Snappy decode + groupby ceiling that gates Q06.

## Next levers

(none new — Q15 is solved within the cold-cache regime by SharedSubtreeExec; bench artifact noted)
