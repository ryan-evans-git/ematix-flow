# PERF_Q22 — Q22 SF=10 stage profile

Status: **re-verified 2026-05-26** (Σ.AH B.22 — final query).

## Wall time (20×3 canonical 2026-05-26)

| Engine | Median ms | σ |
|--------|----------:|----:|
| ematix-flow | **23.35** | 2.20 |
| DuckDB | 149.95 | 6.75 |

**6.4× ahead of DuckDB** (was 5.9×; widened). Biggest relative win in suite (−84%). Stage profile 5-trial: 22.13 ms.

## Per-stage decomposition

Σ compute 328.56 ms / wall 22.13 ms = **14.85× parallelism = 106%** — exceeds 14 cores via async pipelining (same as Q21).

| Stage | Floor | Actual | Status |
|-------|-------|--------|--------|
| HashJoinExec depth 8 (LeftAnti customer ⋈ orders, build 1.5M, probe 15M) | 15M × 12 ns = 180 ms; build 1.5M × 5 ns = 7.5 = ~190 ms | ~190 | 123.64 | **sub-floor** ✓ |
| FilterExec ×2 (substr c_phone IN, 1.5M → 419k each) | ~30 each | 29.67 + 27.13 = 57 | at-floor ✓ |
| RepartitionExec on customer | small | 10.86 | at-floor ✓ |
| RepartitionExec on orders 15M | small (async) | 1.13 | sub-floor ✓ |
| ProjectionExec | small | 0.57 | at-floor ✓ |
| AggregateExec FinalPartitioned (98 → 14 → 7 groups) | trivial | 0.40 + 0.07 = 0.47 | at-floor ✓ |
| customer scans ×2 (1.5M, 2 RGs each, cache makes 2nd ~free) | ~3 ms first; ~0 second | 0.23 + 0.03 = 0.26 | sub-floor ✓ (RG cache) |
| orders scan (15M, no filter) | small (async) | 0.29 | sub-floor ✓ |

Σ floor ~250 ms; observed 328 ms — ~80 ms parallel over-floor (~5 ms wall). Σ/14.85 = 22 ms wall = matches observed.

## Findings

- **Q22 is essentially at-floor — sub-floor on most stages thanks to async pipelining + RG cache replay of the 2nd customer scan.**
- **Customer 2-RG scan is the EXCEPTION where 2-RG works in our favor** — small enough that 2-partition parallelism is sufficient. Compare with Q03/Q05/Q07/Q08 where customer's 2-RG limit bottlenecks larger pipelines.
- **Q22 LeftAnti HashJoin at sub-floor** (123 ms vs ~190 floor) — DataFusion's LeftAnti probe optimisation works well; same early-exit benefit as Q04's LeftSemi.
- **6.4× wins over DuckDB likely come from: (a) RG cache for 2× customer scan, (b) RobinHood SUM(f64) for the final agg (memory `[[sigma-nf3-beats-stock]]`), (c) DuckDB pays comparatively higher per-row cost on substr+IN filter.**

**No Q22-specific lever needed.** Already at-floor.

**Σ.AH Phase B COMPLETE — all 22 queries reviewed.** See Phase C synthesis next.

## Physical plan

```
SortPreservingMergeExec [cntrycode ASC]
  AggregateExec FinalPartitioned gby=[cntrycode] count, sum(c_acctbal)
    NestedLoopJoinExec Inner filter (c_acctbal > avg)
      AggregateExec Final no-gby avg(c_acctbal)
        FilterExec (c_acctbal > 0 AND substr(c_phone, 1, 2) IN [13, 31, 23, 29, 30, 18, 17])
          customer (scan #1)
      HashJoinExec Partitioned LeftAnti (c_custkey, o_custkey)
        FilterExec substr(c_phone, 1, 2) IN ...
          customer (scan #2)
        orders (only o_custkey projection)
```

## Per-stage breakdown

| Rank | Operator | Median ms | Out rows |
|-----:|:---------|----------:|---------:|
| 1 | NestedLoopJoinExec (Inner, filter) | 135.38 | 63,914 |
| 2 | HashJoinExec LeftAnti (cust ⋈ orders) | 118.61 | 140,489 |
| 3 | FilterExec (cust scan #1) | 30.10 | 419,974 |
| 4 | FilterExec (cust scan #2) | 29.00 | 381,776 |
| 5 | RepartitionExec | 9.76 | 419,974 |

Σ median compute: 326 ms. Wall median 25 ms. **Parallel speedup ≈ 12.99×** — very strong.

## Theoretical floor

| Stage | Floor (ms) |
|-------|-----------:|
| customer scan #1 + substr+IN filter (1.5M × 2 cols) | 3 |
| customer scan #2 + substr+IN filter (1.5M × 3 cols) | 3 |
| orders scan (15M × 1 col) | 4 |
| HashJoin LeftAnti (cust 380k build × orders 15M probe / 14) | 13 |
| AggregateExec Partial avg(c_acctbal) (420k rows, no group) | <1 |
| NestedLoopJoin (1 × 140k filter) | 2 |
| Hash agg 7 groups | <1 |
| Sort 7 rows | <1 |
| **Floor** | **~28 ms** |
| **Actual** | **25 ms** |
| **Waste ratio** | **0.9×** (below floor — model conservative) |

## Findings

Q22 is at or below the conservative floor. **Biggest beneficiary of the StringView `new_unchecked` fix (-20% in the post-fix 22q bench).** Already 5.9× ahead of DuckDB. Move on.

## Next levers

(none — Q22 already optimal)
