# PERF_Q15 — Q15 SF=10 stage profile

Status: **re-verified 2026-05-26** (Σ.AH B.15).

## Wall time (20×3 canonical 2026-05-26)

| Engine | Median ms | σ |
|--------|----------:|----:|
| ematix-flow | **77.28** | 3.80 |
| DuckDB | 95.80 | 3.95 |

**19% ahead of DuckDB** (was 8% — DuckDB now slower, we're flat). Polars skipped at SF=10. Stage profile 5-trial shows **5.46 ms** (median) — the SharedSubtreeExec cache makes trials 2+ very fast; first-trial work is what's reflected in the canonical 77 ms.

## Per-stage decomposition (measurement caveat — see notes)

stage_profiler shows median 5.46 ms across 5 trials with σ tight at ~0.3 ms. This is the cache-hit case: SharedSubtreeExec populates on trial 1 (~80 ms) then replays for trials 2-5 (~5 ms each).

The canonical 22q-bench wall of 77.28 ms is the per-trial figure when the bench fresh-execs each trial without between-trial cache reuse (or with cache being re-populated through different plan-tree identity).

| Stage | Floor | Actual (cache-hit) | Status |
|-------|-------|--------------------|--------|
| supplier scan (100k) | ~4 ms | 4.26 ms | at-floor ✓ |
| HashJoin supplier ⋈ revenue-cached | small | 0.84 ms | at-floor ✓ |
| HashJoin revenue ⋈ max | trivial | 0.16 ms | at-floor ✓ |
| AggregateExec (max, no-gby) | trivial | 0.06 ms | at-floor ✓ |
| SharedSubtreeExec replay | ~0 | 0 ms | **cache hit** ✓ |

**On the cache-miss first trial**, Q15's full work is the revenue subquery (lineitem date-filter + 3 joins + suppkey agg). That subtree is what the canonical 77 ms reflects.

## Findings

- **Q15's structural advantage is SharedSubtreeExec working as designed** — the dedupe rule wraps both consumers of `supplier_revenue` (the `max(total_revenue)` aggregator and the `where total_revenue = max` filter) in `SharedSubtreeExec` pointing at the same `Arc<CachedBatches>`. Without the dedupe, this would be a 2× lineitem scan + 2× join.
- **Q15 at-floor on cache-hit trials.** The first trial pays the full revenue-subquery cost; subsequent trials are cache replays. Canonical 77 ms = per-trial work including misses.
- **No new candidate for Q15.** The structural win is already realized.

**Next:** B.16 (Q16 — 50.01 ms, +27% vs DuckDB).

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

## Closure note — SF=100 partition-mismatch crash resolved (verified 2026-07-01)

The Q15 SF=100 crash recorded in the 2026-05-29 sweep
(`bench-results/sf100-ematix-duckdb-2026-05-29.md`: "execute: Internal
error: Assertion failed: self.mode != PartitionMode::Partitioned ||
left_partitions == ri…") is **resolved as of commit `f144e85`**
(`fix(Σ.BS): repair partitioning after Q15 SharedSubtreeExec wrap —
unblocks SF=100`, 2026-05-29), which is in `main`.

- **Assertion source**: DataFusion `HashJoinExec::execute`
  (`datafusion-physical-plan-53.1.0/src/joins/hash_join/exec.rs:1267`) —
  "Invalid HashJoinExec, partition count mismatch {left}!={right}".
- **Trigger conditions**: the `DedupeAggregateForFloatDeterminism` rule
  (registered by `tpch_triangulation_bench`, i.e. `EMAT_RULES=all|dedupe`)
  wraps Q15's duplicated `revenue0` f64 aggregate in `SharedSubtreeExec`
  (`UnknownPartitioning(1)`). When `JoinSelection` has chosen
  `PartitionMode::Partitioned` for the `supplier ⋈ revenue0` join — build
  side over the single-partition threshold, which happens naturally at
  SF=100 — that wrap collapsed one join input N→1 with no
  `RepartitionExec` above it. At SF=1/10 the join is `CollectLeft` (build
  side must be 1 partition — the wrapper satisfies it), so only SF=100
  crashed. Harness-specific: the preset/rebench path without the dedupe
  rule never triggers it.
- **Fix**: `f144e85` re-runs `EnforceDistribution` after the wrap
  (gated on the rule actually transforming), restoring the hash
  repartition above `SharedSubtreeExec`. Regression test:
  `dedupe_aggregate_rule::tests::q15_partitioned_join_survives_shared_subtree_collapse`
  (forces `Partitioned` via zeroed `hash_join_single_partition_threshold*`,
  data-independent).
- **Re-verification (2026-07-01, main @ `1724102`,
  `tpch_triangulation_bench`, 1 trial / 0 warmups)**: SF=10 default
  118 ms; SF=100 default 1255 ms (1 row — previously the crash config);
  SF=1 and SF=10 with `PARTITIONS={3,5,28,100}` all pass; forced
  `Partitioned` mode (`EMAT_COLLECT_LEFT_THRESHOLD_ROWS=0`) at
  SF=1 (all four partition counts) and SF=10 all pass; regression test
  green. Not partition-count-dependent post-fix. Note: SF=100
  `supplier.parquet` was re-emitted with 14 row groups (Σ.AH.4) after the
  original crash; the forced-Partitioned runs cover the old-layout
  (1-row-group build side) shape regardless.
