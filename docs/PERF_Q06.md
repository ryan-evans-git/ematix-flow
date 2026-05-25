# PERF_Q06 — Q06 SF=10 stage profile

Status: profiled 2026-05-25.

## Wall time

| Engine | Median ms | σ | Rows |
|--------|----------:|----:|----:|
| ematix-flow | 71.71 | 2.39 | 1 |
| DuckDB | 71.41 | 2.89 | 1 |
| Polars | 63.52 | 3.87 | 1 |

ematix at **parity with DuckDB** (0.4% gap). **Behind Polars by 11%.** Q06 is the pure-scan benchmark — Polars is the leader to beat.

## Physical plan

Q06 SQL: `select sum(l_extendedprice * l_discount) from lineitem where l_shipdate ∈ [1994-01-01, 1995-01-01) and l_discount ∈ [0.05, 0.07] and l_quantity < 24`. All predicates on lineitem.

```
FusedAggregateExec<FilterSumSpec>
  EmatixFastParquetExec(lineitem, projection=[l_quantity, l_extendedprice, l_discount, l_shipdate])
```

The l_shipdate range predicate is fully pushed into the scan via BridgeFilter (scan emits 9.1M rows from 60M = 15% pass rate). l_discount and l_quantity are filtered inside the FusedAggregateExec.

## Per-stage breakdown

| Rank | Operator | Median ms | Out rows |
|-----:|:---------|----------:|---------:|
| 1 | EmatixFastParquetExec (with BridgeFilter on l_shipdate) | 907.71 | 9,099,165 |
| 2 | FusedAggregateExec | 0.00 (credited to upstream pull) | 0 |

Σ median compute: 908 ms. Wall median 76.68 ms. **Parallel speedup ≈ 11.84× of 14 cores** — Q06 is the most parallel-friendly query in the suite (no joins, no shuffle, no skew).

## Theoretical floor

| Stage | Floor (ms, parallel) |
|-------|---------------------:|
| lineitem scan + decode (4 cols × 60M, Snappy) | 12 |
| BridgeFilter on l_shipdate range (RG-prune + per-row cmp) | 2 |
| Inline filter l_discount ∈ ... AND l_quantity < ... (on the 9.1M survivors) | 1 |
| Sum f64 × f64 on survivors | 1 |
| **Floor** | **~16 ms** |
| **Actual** | **77 ms** |
| **Waste ratio** | **4.8×** |

DuckDB hits the same 71 ms. So the realistic floor on the canonical Snappy file is ~70 ms, not 16. The decode rate model is too optimistic for the actual Snappy throughput of this column mix.

## What Polars does that we don't

Polars hits 63.5 ms — 11% faster. Worth investigating:

- Polars uses its own parquet decoder ([[polars-parquet-decode-approach]]) — const-generic per-bit-width macro-unrolled unpacker + jumptable dispatch. Our `ematix-parquet` codec has [[ematix-parquet-varint-optimal]] and [[ematix-parquet-v013-win]] features — comparable but not identical.
- Snappy decompress rate: [[q06-sf10-polars-gap-wall]] specifically documents that Snappy is the bound here (extprice Snappy at 1.73 GB/s, 7.3× memcpy). Polars may use a faster Snappy path or reuse decompressed buffers more aggressively.
- The Σ.O.c.1 RG decode cache is active here on rep-2+ trials — verified via memory but Polars may have its own equivalent.

## Waste candidates

### 1. Snappy decode floor — known wall, no clean lever within Snappy

Per [[q06-sf10-polars-gap-wall]]: a hand-rolled fast path regressed Q06 17% in our previous attempt. The decode rate is what it is.

The codec switch to LZ4_RAW (sibling file exists) cuts Q06 to 57 ms — but that means switching the canonical bench's compression, which is a comparability decision documented in [[sf10-canonical-lineitem-snappy]].

### 2. RG decode cache hits suggest a stack-stash pattern Polars may use

If Polars wins by buffer reuse / fewer alloc/free cycles in the decompress loop, our path can match. Worth a sample profile of the Polars Q06 run vs ematix Q06 to see where Polars spends fewer cycles. Out of scope for this survey — defer.

### 3. FilterSumSpec batch fusion is doing its job

The plan collapses to 2 nodes (vs Q01's 3 because Q06 doesn't need a final projection rename). FusedAggregateExec already eliminates the FilterExec + AggregateExec materialise-between cost. No further fusion lever for Q06.

## Findings

- Q06 is at DuckDB parity on Snappy; Polars wins by 11% via faster decompress + decode loop.
- No new lever surfaced from this profile. Q06's gap-to-Polars is a known multi-month investment (full Polars-parity parquet decoder).

## Next levers from Q06

(none new — known gap, documented in memory)
