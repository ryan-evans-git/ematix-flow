# Σ.AN.1 — Per-operator partition routing for high-cardinality aggregates

**Status:** design draft, 2026-05-27.
**Prerequisite:** Σ.AN.0 finding (`[[sigma-an-partitions-shape-dependent]]`) + Σ.AM (`[[sigma-am-q18-diagnosis]]`).
**Budget estimate:** 3-5 days.

## Goal

Close ~30-50 ms of Q18 SF=10's remaining 100 ms gap to DuckDB by routing **only the RepartitionExec feeding a high-cardinality FinalPartitioned aggregate** to a higher partition count (target ~50K groups per partition), leaving all other operators at session default.

Σ.AN.0 measurement: Q18 with `PARTITIONS=112` (8× cores) globally: -50 ms (-15%). With per-operator routing the win is bounded somewhere between 0 and 50 ms — likely 30-40 ms because we don't get the parallel-speedup of the other operators in Q18 that benefited at P=112 globally.

## Formula

```
optimal_partitions = clamp(
    ceil(expected_groups / 50_000),  // ~50K groups per partition (L3 fit + prefetch)
    cores,                            // floor at core count (= session target_partitions)
    8 * cores                         // ceiling at coordination knee
)
```

50_000 derives from:
- L3 cache on M3 Pro = 12 MB shared
- Per-group overhead in DataFusion's hash agg ≈ 120 bytes (key + accumulator + slot metadata)
- 50_000 × 120 = 6 MB per partition — sits comfortably under L3 with headroom for shared overhead

8 derives from the P=112 vs P=224 inflection point in Σ.AN.0's Q18 sweep (P224 was worse than P112 due to coordination overhead).

For Q18 (15M groups): `clamp(ceil(15M/50K), 14, 112) = clamp(300, 14, 112) = 112` ✓ matches measured optimum.

## DataFusion plumbing — the partition-count propagation problem

A naive rule that just rewrites the Repartition's partition count from 14 → 112 breaks downstream operators. The next consumer (FilterExec, LeftSemi join) inherits its partitioning from its input. If we bump only one Repartition, the rest of the pipeline now has a 112-way stream where it expected 14-way.

In Q18's plan:
```
LeftSemi join (mode=Partitioned, expects both sides 14-way)
  ├─ orders side (14-way)
  └─ FilterExec(sum>300) (inherits input)
        AggregateExec FinalPartitioned (inherits input)
          RepartitionExec(Hash([l_orderkey]), 14)    ← rewrite target
```

If we rewrite to `RepartitionExec(Hash, 112)`:
- Filter/Agg now run 112-way
- LeftSemi RIGHT side now 112-way but LEFT is 14-way → join breaks or repartitions implicitly

**Solution:** insert a *second* Repartition AFTER the boosted aggregate to bring the count back to session default. This second repartition operates on the small post-agg output (~1M rows for Q18 = ~16 MB), so it should be cheap (~5-10 ms wall).

```
Before:
  ... → AggregateExec FinalPartitioned → RepartitionExec(Hash, 14) → AggregateExec Partial → ...

After Σ.AN.1:
  ... → AggregateExec FinalPartitioned (now 112-way)
       ← RepartitionExec(Hash, 112)  ← rewritten count
       ↑
  ... [downstream is still 112-way until session-restore]
       ↓
  RepartitionExec(RoundRobin, 14)   ← NEW node, restore session count
  FilterExec(sum>300)
  LeftSemi join
```

## Estimated win

Net Q18 SF=10 savings = `agg_cache_fit_savings` - `extra_repartition_cost`
- agg cache fit at 112 vs 14: ~30-40 ms wall (per Σ.AN.0)
- extra Repartition cost: ~5-10 ms wall
- **Net: 20-30 ms savings on Q18**

Other queries with similar shape (Q13, Q21 had wins at P56) may also benefit:
- Q13: 1.5M groups → 30 partitions (just above session default; small effect)
- Q21: large lineitem intermediates → similar potential

## Implementation phases

### Phase 1 — Matcher + cardinality estimation (1 day)

New file: `crates/ematix-flow-core/src/agg_partition_boost.rs`

```rust
pub struct AggPartitionBoostRule {
    target_groups_per_partition: usize,   // default 50_000
    max_partitions_multiplier: usize,      // default 8 (× cores)
}

impl PhysicalOptimizerRule for AggPartitionBoostRule {
    fn optimize(&self, plan, config) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            // Match: AggregateExec(FinalPartitioned) ← RepartitionExec(Hash)
            // Estimate: agg group cardinality
            // Compute: optimal partitions via formula
            // If diff from current: rewrite Repartition + insert restore Repartition
        })
    }
}
```

Cardinality estimate sources (in priority order):
1. `partition_statistics(None).num_rows` on the agg input (input rows is upper bound on groups)
2. `column_stats[group_col].distinct_count` if available
3. Fall back to a configurable estimate (e.g., 1M)

### Phase 2 — Tests (4-8 hours)

- Unit test: matcher recognizes `AggregateExec(FinalPartitioned) ← RepartitionExec(Hash)` pattern
- Unit test: formula produces expected values for cardinality {1K, 100K, 1M, 15M}
- Unit test: rule emits exactly two RepartitionExec replacements when firing
- Unit test: rule is no-op when cardinality < threshold
- Correctness: Q18 SF=10 row count matches baseline
- Correctness: Q13/Q21 SF=10 row count matches baseline

### Phase 3 — Bench (30 min)

Strict interleaved A/B 22q SF=10:
- A: baseline (rule OFF)
- B: rule ON

**Pass criteria:**
1. Q18 ≥ -20 ms wall above 2σ
2. No per-query regression > 5% above 2σ
3. 22q net Δ ≤ +1.5pp (any improvement also acceptable)

### Phase 4 — Decision (30 min)

- (a) Clean win → flip default ON
- (b) Partial win → narrower predicate
- (c) No win → close arc, revert

## Risk register

- **Partition propagation**: addressed via post-agg Repartition restore.
- **Cardinality estimate inaccuracy**: at planning time we don't always have distinct_count. Using `num_rows` as upper bound is conservative (over-estimates groups → over-shards → mild perf loss, not correctness). Tests should cover the case where estimate is 100× too high.
- **Codegen tax**: adding a PhysicalOptimizerRule has the `[[optimizer-codegen-sensitivity]]` risk. To mitigate: keep the rule MINIMAL (single match arm), gate via env var opt-in initially to avoid taxing 22q geomean.
- **Plan cache invalidation**: the cached plans may be the pre-boost shape. Need to verify the boost rule runs BEFORE plan_cache populates (probably already the case since plan_cache caches the post-optimization plan).

## Plan cache interaction

Per `[[sigma-ag-complete]]`: PlanCache caches the LogicalPlan + physical_template, hits replay via `with_new_children`. The Σ.AN.1 rule modifies the physical plan structurally (different Repartition partition count + extra node). If the cache is populated AFTER our rule fires, the rewritten plan is cached. So no special handling needed.

## References

- Σ.AN.0 measurement: `[[sigma-an-partitions-shape-dependent]]`
- Σ.AM diagnosis: `[[sigma-am-q18-diagnosis]]`
- Codegen tax precedent: `[[optimizer-codegen-sensitivity]]`
- Plan cache: `[[sigma-ag-complete]]`
- Q18 partition sweep: this session 2026-05-27 (P14=331, P56=289, P112=281, P224=320, P448=480, P896=1228)
