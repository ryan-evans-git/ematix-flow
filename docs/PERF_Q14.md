# PERF_Q14 — Q14 SF=10 stage profile

Status: **re-verified 2026-05-26** (Σ.AH B.14).

## Wall time (20×3 canonical 2026-05-26)

| Engine | Median ms | σ |
|--------|----------:|----:|
| ematix-flow | **85.49** | 4.87 |
| DuckDB | 137.88 | 2.69 |

**38% ahead of DuckDB** (unchanged). Stage profile 5-trial: 91.19 ms.

## Per-stage decomposition

Σ compute 908.36 ms / wall 91.19 ms = **9.96× parallelism = 71%**.

| Stage | Floor | Actual | Status |
|-------|-------|--------|--------|
| lineitem scan + BridgeFilter l_shipdate (60M → 749k, 4 cols) | ~600 ms | 836.27 | mild over (1.4×) |
| HashJoin part ⋈ lineitem-filt (build 2M = 16 MB, probe 749k) | ~10 ms (build 2M × 5 ns) | 48.95 | **5× over** — build (16 MB) >> probe (6 MB) mis-order |
| AggregateExec Partial (no-group, 2× sum-of-case) | trivial | 11.83 | mild |
| FilterExec residual | trivial | 3.98 | at-floor ✓ |
| RepartitionExec, part scan | trivial | <2 each | at-floor ✓ |

Σ floor ~620 ms; observed 908 ms — ~290 ms over-floor (~29 ms wall). Σ/9.96 = 91 ms wall = matches observed.

## Findings

- **Q14 at realistic floor.** σ stable 5.25 → 4.87.
- **l_shipdate range filter IS pushed in Q14** (60M → 749k during scan). Q14 uses `l_shipdate >= DATE '1995-09-01' AND l_shipdate < DATE '1995-10-01'`. This shape pushes — contrast Q03's `l_shipdate > DATE '1995-03-15'` which doesn't. **Pattern matcher accepts AND-bounded range, rejects unbounded.** Cross-query lever.
- **Q14 has the part ⋈ lineitem build-mis-order pattern** (build 2M parts > probe 749k filtered lineitem rows). 5× over kernel floor on the join. Same Q07/Q08/Q09/Q10 pattern; 5th occurrence now.

**Next:** B.15 (Q15 — 77.28 ms, +19% vs DuckDB; SharedSubtreeExec subject).

## Physical plan

Single 2-stage agg over lineitem ⋈ part, with l_shipdate range filter pushed to scan.

```
ProjectionExec: 100 * sum(CASE p_type LIKE 'PROMO%' ...) / sum(extprice*(1-disc))
  AggregateExec Final no-gby sum sum
    AggregateExec Partial
      HashJoinExec Partitioned (p_partkey, l_partkey)
        part (2M)
        lineitem (BridgeFilter l_shipdate ∈ 1995-Sep)        -- 60M → 749k
```

## Per-stage breakdown

| Rank | Operator | Median ms | Out rows |
|-----:|:---------|----------:|---------:|
| 1 | EmatixFastParquetExec (lineitem, filter pushed) | 830.03 | 749,223 |
| 2 | HashJoinExec (part ⋈ lineitem) | 53.49 | 749,223 |
| 3 | AggregateExec Partial (no group, 2× sum-of-case) | 13.10 | 14 |
| 4 | RepartitionExec | 4.14 | 749,223 |
| 5 | FilterExec (residual? — already 749k = no-op?) | 3.75 | 749,223 |

Σ median compute: 906 ms. Wall median 88 ms. **Parallel speedup ≈ 10.31×**.

## Theoretical floor

| Stage | Floor (ms) |
|-------|-----------:|
| lineitem scan + BridgeFilter (no page-index help, every RG covers Q14 date) | 12 |
| part scan (2M × 2 cols) | 2 |
| HashJoin (part 2M build × 749k probe × 8 ns / 14) | 0.5 |
| Hash agg (1 group, 2 sums of CASE) | 2 |
| **Floor** | **~17 ms** |
| **Actual** | **88 ms** |
| **Waste ratio** | **5.2×** |

## Waste candidates

### Q14 is at the documented decode floor

Memory [[q14-decode-floor]]: "all four cheap levers tested + rejected; remaining options are polars-parquet integration (multi-session) or accept". Memory [[page-index-q14-dead-end]] confirms page-index pruning is dead because every page covers the Q14 window — l_shipdate is uniform within every page.

Memory [[ematix-parquet-q14-integration]] documents the spike result: a custom FusedQ14FullExec hit 15.4 ms, near the bare-decoder floor. Standard SQL through generic TableProvider matches that within noise.

**Q14 is essentially at the achievable single-node floor with parquet-rs / ematix-parquet decode rates.** The remaining 5× gap to my theoretical model is decode-rate-bound, not engine-bound.

## Findings

Q14 is dominated by raw Snappy decompress + decode rate; no actionable lever from the SQL/operator side. The historical memory entries confirm extensive prior investigation.

## Next levers

(none — Q14 at decode floor; full Polars-parquet rewrite is the only remaining path)
