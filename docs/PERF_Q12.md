# PERF_Q12 — Q12 SF=10 stage profile

Status: **re-verified 2026-05-26** (Σ.AH B.12).

## Wall time (20×3 canonical 2026-05-26)

| Engine | Median ms | σ |
|--------|----------:|----:|
| ematix-flow | **87.64** | 2.95 |
| DuckDB | 115.52 | 2.90 |

**24% ahead of DuckDB** (was 16%). σ stabilised: 43.61 → 2.95 (Σ.O.c.2 RG cache default-on). Stage profile 5-trial: 98.33 ms.

## Per-stage decomposition (Σ.AH B.12)

Σ compute 1002.25 ms / wall 98.33 ms = **10.19× parallelism = 73%** (good).

| Stage | Floor | Actual | Status |
|-------|-------|--------|--------|
| lineitem scan + BridgeFilter l_receiptdate (60M → 2.6M, 5 cols) | ~600 ms | 865.19 | mild over (1.4×) |
| HashJoin orders ⋈ lineitem-filt Partitioned | ~155 ms | 86.56 | sub-floor (async) |
| FilterExec residual (shipmode IN + 2-col compare on 2.6M) | ~40 ms | 40.47 | at-floor ✓ |
| AggregateExec Partial (2-group sum) | <2 ms | 6.36 | mild over |
| Other | ~10 | ~6 | at-floor ✓ |

Σ floor ~800 ms; observed 1002 ms. Σ/10.19 = 98 ms wall = matches observed.

## Findings

- **Q12 at realistic floor.** σ stabilised by RG cache default-on.
- **l_receiptdate range pushdown WORKS** in Q12 (60M → 2.6M during scan). Compare Q03/Q07 where l_shipdate range doesn't push. Confirms a pattern matcher inconsistency in InjectFusedFilter — may be `>= AND <=` vs `>` literal-form sensitivity.
- 2-col cross-column predicates don't push (40 ms residual FilterExec). Same Q04 pattern; 2-col pushdown is a cross-query lever.

**Next:** B.13 (Q13 — 95.81 ms, +65% vs DuckDB).

## Physical plan

```
SortPreservingMergeExec [l_shipmode ASC]
  AggregateExec FinalPartitioned gby=[l_shipmode] sum(CASE...) sum(CASE...)
    HashJoinExec Partitioned Inner (l_orderkey, o_orderkey)
      FilterExec: (l_shipmode IN MAIL/SHIP) AND (l_receiptdate > l_commitdate) AND (l_shipdate < l_commitdate)
        lineitem (BridgeFilter: l_receiptdate ∈ [1994-01-01, 1995-01-01))   -- 60M → 2.6M
      orders                                                               -- 15M rows
```

## Per-stage breakdown

| Rank | Operator | Median ms | Out rows |
|-----:|:---------|----------:|---------:|
| 1 | EmatixFastParquetExec (lineitem, l_receiptdate range pushed) | 875.09 | 2,600,101 |
| 2 | HashJoinExec (lineitem_filt ⋈ orders) | 89.04 | 310,803 |
| 3 | FilterExec (residual: shipmode IN + 2-col date cmp) | 39.47 | 310,803 |
| 4 | AggregateExec Partial (28 groups, 2 sum-of-case) | 6.47 | 28 |

Σ median compute: 1013 ms. Wall median 94 ms. **Parallel speedup ≈ 10.82×** — high.

## Theoretical floor

| Stage | Floor (ms) |
|-------|-----------:|
| lineitem scan + l_receiptdate range pushed (60M → 2.6M decode) | 6 |
| Residual filter (2.6M × multiple predicates × 5 ns / 14) | 1 |
| orders scan (15M × 2 cols) | 4 |
| HashJoin (orders 15M build × 310k probe) | 3 |
| Hash agg (28 groups, 2× sum-of-case Int64) | <1 |
| Sort 2 rows | trivial |
| **Floor** | **~14 ms** |
| **Actual** | **94 ms** |
| **Waste ratio** | **6.7×** |

But DuckDB hits 105 ms — so the realistic floor is ~85-90 ms. The 14 ms floor is too optimistic for the scan path.

## Waste candidates

### 1. Two-column-comparison filters NOT pushed to scan

Of the 4 lineitem predicates, only the `l_receiptdate ∈ range` is pushed. Three are not:
- `l_shipmode IN ('MAIL', 'SHIP')` — string IN, blocked
- `l_receiptdate > l_commitdate` — 2-column compare, blocked
- `l_shipdate < l_commitdate` — 2-column compare, blocked

If all 4 pushed: scan decodes ~310k rows total (not 2.6M then filter to 310k). Could cut lineitem decode by ~88%. Estimated wall: 94 → ~70 ms (~25% improvement).

Pattern is structurally the same as Q04 (`l_receiptdate > l_commitdate`). One implementation could fix Q04 + Q12. Single-column IN on string also needed.

### 2. RG decode cache cold-trial variance

σ 43.61 on a 94 ms median = cold trial probably ~140 ms, warm ~70 ms. Lineitem's 5 cols × 58 RGs × ~10 KB compressed = ~3 GB working set; we're past the default 1 GB cache size (memory [[sigma-oc1-landed]]). For Q12's 5-col projection of lineitem, the cache evicts between trials.

Bump default `EMAT_RG_DECODE_CACHE_BYTES` to ~3 GB (currently 1 GB)? Worth considering as a milestone-config bump.

## Findings

- **Q12 + Q04** share the 2-column-comparison pushdown opportunity. Q03/Q05/Q07 share single-column-range pushdown. These two patterns together would close 5-6 lineitem-pushdown gaps.
- High σ from RG decode cache size constraint — a knob worth re-tuning.

## Next levers

1. (Cross-Q) Extend BridgeFilter pattern matcher for **string-IN** + **two-column-compare** predicates. Covers Q04, Q12.
2. (Config) Re-evaluate `EMAT_RG_DECODE_CACHE_BYTES` default — bump from 1 GB to ~3 GB or scale by available RAM.
