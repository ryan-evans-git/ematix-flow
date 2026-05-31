# PERF_Q13 — Q13 SF=10 stage profile

Status: **re-verified 2026-05-26** (Σ.AH B.13).

## Wall time (20×3 canonical 2026-05-26)

| Engine | Median ms | σ |
|--------|----------:|----:|
| ematix-flow | **95.81** | 7.10 |
| DuckDB | 273.22 | 6.15 |

**2.85× ahead of DuckDB** (was 2.6×). Stage profile 5-trial: 103.44 ms.

## Per-stage decomposition (Σ.AH B.13)

Σ compute 1301.11 ms / wall 103.44 ms = **12.58× parallelism = 90%** — TIE WITH Q06 FOR BEST IN SWEEP.

| Stage | Floor | Actual | Status |
|-------|-------|--------|--------|
| FilterExec `o_comment NOT LIKE '%special%requests%'` (15M → 14.8M) | LIKE matcher 9-14× std → ~150 ms parallel | 431.59 | **2.9× over** — LIKE on 15M rows is expensive, NOT LIKE matches 99% so projection cost dominates |
| AggregateExec SinglePartitioned (15.3M → 1.5M groups, count) | 15M × 3 ns + 1.5M output finalize = ~50 ms | 350.16 | **7× over** — 1.5M groups (very high cardinality count agg) |
| HashJoinExec Left customer ⋈ orders Partitioned (build 1.5M, probe 14.8M, Left join) | 14.8M × 12 ns = 178 ms | ~180 | 332.58 | **1.9× over** — Left-join semantics need extra null-handling |
| EmatixFastParquetExec orders | ~30 | 180.48 | mild over |
| AggregateExec Partial (count-of-counts → 570 groups) | trivial | 4.26 | at-floor ✓ |
| Repartition | small | <2 | at-floor ✓ |

Σ floor ~600 ms; observed 1301 ms — ~700 ms parallel over-floor (~55 ms wall waste). Σ/12.58 = 103 ms wall = matches.

## Findings

- **Q13 has high parallelism (90%) but stage-level inefficiencies sum to 55 ms wall waste**, anchored to LIKE filter overhead and high-cardinality count agg.
- **AggregateExec at 1.5M groups (one per customer)** is essentially a per-row count via group-by. Photon would specialise this as `count per FK` (a count-grouping on join input where the join key uniquely identifies the group). Probably out of scope.
- The Left-join with 1.5M build × 14.8M probe is doing the right work — just structurally expensive at this cardinality.

**Q13 is the best query for showing the cross-query parallelism trend:** Q06 + Q13 both single-table + agg = 80-90% parallel. Adding joins or shuffles drops parallelism.

**Next:** B.14 (Q14 — 85.49 ms, +38% vs DuckDB).

## Physical plan

LEFT JOIN customer ⋈ orders (with `NOT LIKE %special%requests%` filter), 2-stage agg (per-customer count, then count-of-counts).

```
SortPreservingMergeExec [custdist DESC, c_count DESC]
  AggregateExec FinalPartitioned gby=[c_count] count
    AggregateExec Partial gby=[c_count]
      AggregateExec SinglePartitioned gby=[c_custkey] count(o_orderkey)
        HashJoinExec Partitioned Left (c_custkey, o_custkey)
          customer                                                  -- 1.5M
          FilterExec o_comment NOT LIKE '%special%requests%'
            orders                                                  -- 15M rows
```

## Per-stage breakdown

| Rank | Operator | Median ms | Out rows |
|-----:|:---------|----------:|---------:|
| 1 | FilterExec (NOT LIKE %special%requests%) | 444.08 | 14,837,583 |
| 2 | AggregateExec SinglePartitioned (1.5M groups, count) | 350.41 | 1,500,000 |
| 3 | HashJoinExec Left | 290.87 | 15,337,604 |
| 4 | EmatixFastParquetExec (orders) | 132.19 | 15,000,000 |
| 5 | AggregateExec Partial (570 groups, count) | 4.39 | 570 |

Σ median compute: 1224 ms. Wall median 103 ms. **Parallel speedup ≈ 11.87×** — strong.

## Theoretical floor

| Stage | Floor (ms) |
|-------|-----------:|
| orders scan + decode 15M × 3 cols (incl o_comment Utf8) | 8 |
| LIKE filter via SIMD substring (15M rows × 3 GB/s) | 3 |
| customer scan 1.5M × 1 col | <1 |
| HashJoin Left (build customer 1.5M, probe 15M × 12 ns / 14) | 13 |
| Hash agg group by c_custkey (1.5M groups, count) | 5 |
| Hash agg group by c_count (570 groups, count) | <1 |
| Sort 46 rows | trivial |
| **Floor (with SIMD LIKE)** | **~30 ms** |
| **Floor (without SIMD LIKE)** | **~70 ms** |
| **Actual** | **103 ms** |

## Waste candidates

### 1. SIMD LIKE kernel exists but is dormant — top lever

Memory [[sigma-e5-like-kernel]]: `crates/ematix-flow-core/src/like_matcher.rs` ships a Photon-style SIMD substring kernel at 9-14× over std. Status: "awaiting wire-up to PLAIN LIKE pushdown (blocked on double-decode fix)".

The FilterExec at 444 ms compute = 37 ms wall is doing the LIKE eval via the stdlib path (Arrow's `like_utf8`). If wired to the SIMD kernel, this drops to ~5 ms wall.

Expected wall impact: 103 → ~70 ms (~32% improvement). Q13 would extend its DuckDB lead from 2.6× to ~3.7×.

This is the highest-confidence lever surfaced from any query so far.

### 2. AggregateExec SinglePartitioned on 1.5M groups at 350 ms compute

1.5M-group count aggregation. SinglePartitioned mode means input is already partitioned on the group key, so the agg can run as one pass per partition. Per-row cost: 350 ms × 14 / 15M ≈ 326 ns/row count — that's high. Memory [[sigma-nf3-beats-stock]] says RobinHoodSumF64Exec beats DataFusion stock for SUM(f64), but for COUNT specifically — not sure.

Memory [[sigma-k2-dict-routing]] mentions Q12 −41% confirmed with dict-aware count; Q13 might be eligible too. Not sure if the existing dict-aware count rule fires on c_custkey (i64, not dict).

Worth a closer look — 350 ms is a lot of compute even for 1.5M groups.

### 3. HashJoin Left 290 ms compute — already close to floor

Customer 1.5M build × orders 15M probe with LEFT join. Each row produces 0 or many matches. Output 15.3M = orders + customers with no match. Probe cost 290 ms × 14 / 15M = 270 ns/row probe — high.

The 270 ns/row probe is similar to other large-build joins we've seen. Build doesn't fit in L2 (1.5M rows × ~30 B = 45 MB). L3 access dominates.

## Findings

- **SIMD LIKE wire-up is the #1 lever for Q13** — single-query, high-confidence, kernel already shipped.
- 1.5M-group count agg at 350 ms is suspiciously high — worth a microbench to confirm cost.

## Next levers

1. (Q13-specific) **Wire SIMD LIKE kernel to FilterExec pushdown** — pull from `like_matcher.rs` into either FilterExec evaluation or BridgeFilter on orders scan.
2. (Cross-Q investigation) Why is SinglePartitioned COUNT(c_custkey) so slow vs Robin-Hood SUM?
