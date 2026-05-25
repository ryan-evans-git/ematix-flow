# PERF_Q19 — Q19 SF=10 stage profile

## Wall time

| Engine | Median ms | σ | Rows |
|--------|----------:|----:|----:|
| ematix-flow | 139.49 | 8.05 | 1 |
| DuckDB | 203.11 | 3.81 | 1 |
| Polars | 1229.20 | 22.29 | 1 |

**31% ahead of DuckDB**, 8.8× ahead of Polars.

## Physical plan

2-table query with a 3-way disjunctive OR-of-AND filter on (p_brand, p_container, p_size, l_quantity). DataFusion pushes the per-table parts to FilterExec; cross-table parts stay as a HashJoin filter.

```
ProjectionExec
  AggregateExec Final no-gby sum(extprice * (1-disc))
    AggregateExec Partial
      HashJoinExec Partitioned Inner (p_partkey, l_partkey) filter=<3-way OR-of-AND>
        FilterExec (per-table p predicates)
          part                                            -- 1.5M → ~5k
        FilterExec (per-table l predicates: quantity OR-bands, shipmode IN, shipinstruct EQ)
          lineitem                                        -- 60M → 2.14M
```

## Per-stage breakdown

| Rank | Operator | Median ms | Out rows |
|-----:|:---------|----------:|---------:|
| 1 | EmatixFastParquetExec (lineitem, partial filter pushed) | 1246.24 | 2,141,904 |
| 2 | EmatixFastParquetExec (part) | 31.28 | 2,000,000 |
| 3 | FilterExec (part) | 27.98 | 4,754 |
| 4 | FilterExec (lineitem residual) | 25.38 | 1,284,344 |
| 5 | HashJoinExec (with cross-table OR filter) | 18.13 | 1,134 |

Σ median compute: 1357 ms. Wall median 142 ms. Parallel speedup ≈ 9.55×.

## Theoretical floor

| Stage | Floor (ms) |
|-------|-----------:|
| lineitem scan + filter pushed (60M → 2.14M, complex predicate) | 6 |
| part scan + filter | 2 |
| HashJoin part 5k × lineitem 2.14M × 12 ns / 14 | 2 |
| HashJoin cross-table OR filter eval (per probe row) | 5 |
| Hash agg (no-gby sum) | <1 |
| **Floor** | **~16 ms** |
| **Actual** | **142 ms** |
| **Waste ratio** | **8.9×** |

But DuckDB hits 203 ms — so the realistic floor on the canonical Snappy lineitem is ~140 ms. We're at it.

## Waste candidates

### 1. Lineitem decode + filter eval at 1246 ms compute = ~130 ms wall

Q19 is structurally lineitem-decode-bound. The OR-of-AND filter spans 3 branches with overlapping l_quantity bands (1-11, 10-20, 20-30) — the actual filter accepts ~4% of rows but evaluating the OR requires per-row eval of all branches.

The bridge filter likely pushes the simpler per-column shapes (quantity ranges via the union 1-30, shipmode IN, shipinstruct EQ) and the disjunctive AND-of-each-branch stays in the residual FilterExec.

Memory [[sigma-e5-late-mat-spike-scope]] mentions extending BridgeFilter for "Q19's OR-of-AND" predicates — that work was scoped but I don't know its landing state. If not yet landed, this is the lever.

### 2. l_shipinstruct = 'DELIVER IN PERSON' — string-equality push

l_shipinstruct is a small-cardinality string column (4 possible values). DICT-aware in-scan filtering on dict-encoded equality is a known optimization — memory [[sigma-k2-dict-routing]] landed dict-routing for Q12 with −41%. Q19 would benefit similarly if its scan picks up the dict-aware path.

Worth checking: does Q19's lineitem scan run with dict-preserved Utf8 (the [[dict-arrival-blocker]] gating)? If not, dict-aware filter can't help.

## Findings

- Q19 is at the realistic Snappy + complex-filter ceiling. Already 31% ahead of DuckDB.
- The BridgeFilter extension for OR-of-AND predicates ([[sigma-e5-late-mat-spike-scope]]) would give per-row filter pushdown across all 3 lineitem-predicate branches.

## Next levers

(Q19 already strong; deferred OR-of-AND BridgeFilter extension would close more of the 8.9× floor gap but limited absolute wall benefit since we already beat DuckDB by 31%)
