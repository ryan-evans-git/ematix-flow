# PERF_Q20 — Q20 SF=10 stage profile

Status: **re-verified 2026-05-26** (Σ.AH B.20).

## Wall time (20×3 canonical 2026-05-26)

| Engine | Median ms | σ |
|--------|----------:|----:|
| ematix-flow | **131.47** | 4.34 |
| DuckDB | 150.94 | 4.39 |

**13% ahead of DuckDB** (was 11%). σ stabilised: 57.92 → 4.34 (RG cache default-on). Stage profile 5-trial: 126.51 ms.

## Per-stage decomposition

Σ compute 951.35 ms / wall 126.51 ms = **7.52× parallelism = 54%**.

| Stage | Floor | Actual | Status |
|-------|-------|--------|--------|
| Top HashJoinExec depths 6,4 (multi-join chain) | ~50 ms | 45.72 + 40.30 = 86 ms | mild over |
| FilterExec residual (availqty > 0.5*sum residual) | 21k rows | 15.43 | mild over |
| RepartitionExec 9M | ~30 ms | 13.98 | sub-floor (async) |
| EmatixFastParquetExec part (2M → 4054 via LIKE) | ~10 | 6.91 | at-floor ✓ |
| FilterExec (9.1M residual) | small | 5.21 | at-floor ✓ |
| EmatixFastParquetExec partsupp (8M, RG cache) | small | 0.72 | sub-floor (cache hit?) |
| HashJoin top + nation joins | small | <1 each | at-floor ✓ |

Σ floor ~250 ms; observed 951 ms (most credited to async-pipelined upstream, see top stages above). Σ/7.52 = 126 ms wall = matches.

## Findings

- **Q20 at realistic-parallelism floor.** σ stabilised by Σ.O.c.2 default-on (57.92 → 4.34).
- **Σ.Q.L9 + L10 + RobinHoodSumF64TwoKeyExec (2-key sum agg)** all firing visibly in the plan (BuildSideBloomEmitterExec + RightSemi + 2-key sum). Memory `[[lever4]]` notes 2-key Robin Hood was committed but not enabled by default per the codegen-sensitivity gate. Q20 confirms it's effective when it does fire.
- 5th query confirming σ stabilisation from RG cache default-on (Q08/Q12/Q15/Q19/Q20).

**Next:** B.21 (Q21 — 311.87 ms, +30% vs DuckDB; biggest absolute wall).

## Physical plan

Multi-stage: lineitem aggregate (gby l_partkey, l_suppkey) → HashJoin partsupp on 2-key → LeftSemi supplier → Inner nation.

```
SortPreservingMergeExec [s_name ASC]
  HashJoinExec CollectLeft Inner (n_nationkey, s_nationkey) -- nation CANADA
    BuildSideBloomEmitterExec
      nation (filter n_name=CANADA)
    HashJoinExec CollectLeft LeftSemi (s_suppkey, ps_suppkey)
      supplier
      HashJoinExec Partitioned Inner (2-key) (ps_partkey, l_partkey) ∧ (ps_suppkey, l_suppkey) filter=availqty > 0.5*sum
        HashJoinExec Partitioned RightSemi (p_partkey, ps_partkey)
          part (filter p_name LIKE 'forest%')                   -- ~75 parts
          partsupp                                              -- 8M, narrowed via semi
        AggregateExec FinalPartitioned gby=[l_partkey, l_suppkey] sum(l_quantity)
          AggregateExec Partial
            lineitem (BridgeFilter l_shipdate ∈ [1994-01-01, 1995-01-01))    -- 60M → 9.1M
```

## Per-stage breakdown (top 8)

| Rank | Operator | Median ms | Out rows |
|-----:|:---------|----------:|---------:|
| 1 | AggregateExec FinalPartitioned (2-key, 5.44M groups, sum f64) | 380.34 | 5,441,669 |
| 2 | EmatixFastParquetExec (lineitem) | 316.53 | 9,099,165 |
| 3 | AggregateExec Partial (same) | 66.05 | 9,084,322 |
| 4 | HashJoinExec (partsupp ⋈ aggregated lineitem 2-key) | 56.06 | 86,204 |
| 5 | HashJoinExec (nation ⋈ supplier+) | 39.07 | 58,655 |
| 6 | RepartitionExec | 18.48 | 9,084,322 |
| 7 | EmatixFastParquetExec (part) | 13.01 | 4,054 |
| 8 | FilterExec (residual) | 6.73 | 21,551 |

Σ median compute: ~900 ms. Wall median 130 ms. Parallel speedup ≈ 6.9×.

## Theoretical floor

| Stage | Floor (ms) |
|-------|-----------:|
| lineitem scan + filter pushed (60M → 9.1M decode) | 6 |
| Hash agg 2-key (5.44M groups, sum f64) — large table out-of-L2 | 30 |
| partsupp scan (8M × 3 cols) | 5 |
| part + LIKE filter | 2 |
| HashJoin 2-key partsupp ⋈ agg (5.44M build × 8M probe / 14) | 12 |
| HashJoin RightSemi part ⋈ partsupp | 2 |
| HashJoin LeftSemi supplier ⋈ partsupp_filt | 2 |
| nation ⋈ supplier | <1 |
| Sort 1804 rows | <1 |
| **Floor** | **~60 ms** |
| **Actual** | **130 ms** |
| **Waste ratio** | **2.2×** |

## Waste candidates

### 1. 5.44M-group 2-key SUM aggregation — largest single op (380 ms compute)

Compound i64+i64 group key (l_partkey, l_suppkey). RobinHoodSumF64Exec (single-key i64) doesn't apply. Per-row cost: 380 ms × 14 / 9.1M = 580 ns/row — high (vs RobinHood's ~50 ns/row).

The hash table at 5.44M groups × ~24 bytes per entry = ~130 MB — far past L2 (256-512 KB) and L3 (~8 MB on M-series). Every probe pays DRAM.

**Lever: 2-i64-key Robin Hood variant.** Same kernel pattern as the single-key RH; just packs (k1, k2) into a (key, slot) Robin Hood entry. Memory [[sigma-h1d-rejected]] tried numeric-keyed agg and reverted; a 2-key variant might thread differently.

Expected impact: agg compute 380 → ~80 ms, wall 130 → ~80 ms (~38% improvement). Q20 already wins but would extend lead.

### 2. lineitem scan + filter at 316 ms compute = 46 ms wall

`l_shipdate ∈ 1994` filter pushed to scan. 60M → 9.1M output. 46 ms wall is the realistic decode floor for this shape. Same Snappy decode ceiling as Q06/Q14.

### 3. RobinHood bloom flow already firing

Plan shows `BuildSideBloomEmitterExec` on the nation→supplier edge. The RightSemi on part→partsupp is also doing its job (narrows partsupp via the part filter).

## Findings

- **Q20 has a 5.44M-group 2-key SUM** that would benefit from a Robin-Hood variant on compound i64+i64 keys. Same shape recurs in Q09 (`gby=[nation, o_year]` 2-key) — different types but same need for compound-key kernel.
- Q20's L9 + RightSemi pipeline is well-wired.

## Next levers

1. **Compound-key Robin Hood SUM (2× i64 keys)** — could win Q20, applies broadly to multi-key aggregations.
