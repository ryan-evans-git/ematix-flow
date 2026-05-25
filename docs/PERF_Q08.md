# PERF_Q08 — Q08 SF=10 stage profile

## Wall time

| Engine | Median ms | σ | Rows |
|--------|----------:|----:|----:|
| ematix-flow | 196.20 | 58.68 | 2 |
| DuckDB | 171.71 | 13.79 | 2 |
| Polars | 1222.82 | 48.80 | 2 |

**14% behind DuckDB.** High σ on our side (58.68) — Q08 is variable.

## Physical plan

7-table join, market-share-by-year query. Region(AMERICA) → nation → customer → orders(1995-96) → lineitem → supplier → nation, plus part(ECONOMY ANODIZED STEEL) → lineitem.

```
SortPreservingMergeExec [o_year ASC]
  AggregateExec gby=[o_year] aggregates sum(case n=BRAZIL...)/sum(volume)
    HashJoinExec CollectLeft (r_regionkey, n_regionkey) -- region filter AMERICA
      region
      HashJoinExec CollectLeft (n_nationkey, s_nationkey) -- nation
        nation
        HashJoinExec CollectLeft (n_nationkey, c_nationkey) -- nation #2
          nation
          HashJoinExec Partitioned (c_custkey, o_custkey)
            customer
            HashJoinExec Partitioned (o_orderkey, l_orderkey)
              orders (filter o_orderdate ∈ [1995-01-01, 1996-12-31])  -- 4.5M rows
              HashJoinExec CollectLeft (s_suppkey, l_suppkey)
                supplier
                HashJoinExec Partitioned (p_partkey, l_partkey)         -- HOT
                  part (filter p_type='ECONOMY ANODIZED STEEL')        -- ~24k parts
                  lineitem                                              -- 60M, no filter
```

## Per-stage breakdown (top 8)

| Rank | Operator | Depth | Median ms | Out rows |
|-----:|:---------|------:|----------:|---------:|
| 1 | EmatixFastParquetExec (lineitem) | 22 | 636.97 | 59,986,052 |
| 2 | HashJoinExec (part ⋈ lineitem) | 20 | 161.45 | 403,487 |
| 3 | HashJoinExec (supplier ⋈ part+lineitem) | 16 | 78.46 | 122,404 |
| 4 | HashJoinExec (orders ⋈ above) | 13 | 30.23 | 122,404 |
| 5 | EmatixFastParquetExec (orders) | 19 | 29.49 | 4,557,513 |
| 6 | EmatixFastParquetExec (customer) | 15 | 23.40 | 1,500,000 |
| 7 | RepartitionExec (lineitem 60M) | 21 | 18.08 | 59,986,052 |
| 8 | HashJoinExec (cust ⋈ orders+above) | 19 | 4.76 | 403,487 |

Σ median compute: 993 ms. Wall median 192 ms. Parallel speedup ≈ 5.16× of 14 cores.

## Theoretical floor

| Stage | Floor (ms) |
|-------|-----------:|
| lineitem scan + decode 60M × 5 cols | 15 |
| orders scan + filter (15M → 4.5M) | 5 |
| customer scan (1.5M × 2) | 1 |
| part scan + LIKE filter (1.5M × 2) | 2 |
| supplier/nation/region | <1 |
| HashJoin part_filt 24k build × lineitem 60M probe × 12 ns / 14 | 51 |
| Other joins (small) | ≤2 |
| agg (28 groups) | <1 |
| **Floor** | **~75 ms** |
| **Actual** | **192 ms** |
| **Waste ratio** | **2.6×** |

## Waste candidates

### 1. L9 build-side bloom on part_filt → lineitem appears NOT to fire — check guard

Q08's most selective edge is `part (filter p_type='ECONOMY ANODIZED STEEL') ⋈ lineitem on (p_partkey, l_partkey)`. Part filter yields ~24k matching parts. Lineitem has 60M rows with ~12k distinct l_partkey values (well, more — at SF=10 every part has 30+ lineitem rows). The selectivity ratio probe-to-build = 60M / 24k = 2500 ≫ 1024 — should clear the L9 threshold.

But the plan shows NO `BuildSideBloomEmitterExec` between part and lineitem. Either:
- The rule fires only on CollectLeft mode and this join is Partitioned (need to check `EnableRuntimeBloomSidebandRule`)
- The rule fires but the bloom isn't being injected into the scan
- The rule has a guard against this specific shape

Expected impact: if L9 fires, lineitem scan effectively probes only ~12k partkey-matching keys. Bloom selectivity ~99% drops lineitem rows from 60M decode to ~600k. Wall: 196 → ~100 ms. **High-impact lever if it's a simple guard miss.**

### 2. Lineitem scan 637 ms compute with no filter pushed

Same pattern as Q05/Q07/Q08 — full lineitem decode. No predicate, so the only escape is a runtime bloom from upstream join builds. Without L9 firing on Q08's part→lineitem edge, we eat the 60M decode unconditionally.

### 3. High σ (58.68 ms over median 196) — RG decode cache warmth

Cold trial pays for the full RG decode cache miss. Memory [[sigma-oc1-landed]] notes default cache size 1 GB; lineitem at SF=10 is 2 GB. So lineitem decode doesn't fit fully in cache; cold trials decode from page cache, warm trials hit RG cache for the projected cols only. Variance is intrinsic.

## Findings to capture as memories

- **Q08 candidate for the same L9 audit as Q05/Q07**: small-build → lineitem edges may not all be getting bloom emitters. Compile a list of "where L9 should fire vs where it does" across the 22 queries before designing a fix.

## Next levers from Q08

1. (Cross-Q) **Audit L9 firing**: in each of Q03, Q05, Q07, Q08 the small-build → lineitem edge looks like a textbook L9 case. Verify which fire and which don't. May be a single rule-guard fix.
