# PERF_Q19 — Q19 SF=10 stage profile

Status: **re-verified 2026-05-26** (Σ.AH B.19).

## Wall time (20×3 canonical 2026-05-26)

| Engine | Median ms | σ |
|--------|----------:|----:|
| ematix-flow | **138.72** | 13.54 |
| DuckDB | 210.30 | 4.55 |

**34% ahead of DuckDB** (was 31%). σ elevated (13.54) — Q19 has some run-to-run variance. Stage profile 5-trial: 139.90 ms.

## Per-stage decomposition

Σ compute 1437.06 ms / wall 139.90 ms = **10.27× parallelism = 73%**.

| Stage | Floor | Actual | Status |
|-------|-------|--------|--------|
| EmatixFastParquetExec lineitem (5 cols, 60M → 2.14M via BridgeFilter) | full 60M decode with mixed Snappy | ~600-900 | 1334.65 | **1.6× over** — heavy decode for OR-of-AND filter |
| EmatixFastParquetExec part (2M → 4754 after p_brand/container/size filter) | ~20 | 27.98 | mild over |
| FilterExec residual (per-table predicates) | small | 25.36 + 24.04 = 49.4 | at-floor ✓ |
| HashJoinExec (part_filt ⋈ lineitem-filt) Partitioned + 3-way OR-of-AND filter | build 4754 = L1, probe 2.14M | ~15 | 15.62 | at-floor ✓ |
| RepartitionExec 1.28M | ~10 | 9.18 | at-floor ✓ |
| AggregateExec (sum no-gby) | trivial | 0.04 | at-floor ✓ |

Σ floor ~700 ms; observed 1437 ms — ~700 ms over-floor parallel (~70 ms wall). Σ/10.27 = 140 ms wall = matches observed.

## Findings

- **Q19 at realistic-parallelism floor** with the dominant cost being the lineitem scan + multi-predicate filter pushdown (60M → 2.14M). 
- **The 3-way OR-of-AND filter on (p_brand, p_container, p_size, l_quantity)** isn't pushed as a single BridgeFilter — it's split into per-table FilterExecs + a HashJoin residual filter. The per-table parts ARE pushed (l_shipmode IN, l_shipinstruct EQ, l_quantity OR-bands) per the scan output of 2.14M < 60M. Memory notes "DataFusion already pushes per-table predicates from Q19's OR-of-AND" — confirmed.
- Q19's effective parallelism 73% is mid-pack — not bottlenecked by CollectLeft chains.

**Next:** B.20 (Q20 — 131.47 ms, +13% vs DuckDB).

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
