# Shape coverage gaps — TPC-H non-firing queries

**Date**: 2026-05-20
**Branch**: `feat/sigma-f-shape-catalog`
**Bench baseline**: HEAD geomean 0.9594 vs v0.3.0 (14 wins / 0 regressions / Q06 -18.7%).

Inventory tool (`/tmp/sigma_g_inventory.md`) reports **10/22 TPC-H queries fire no catalog rule** — Q09, Q12, Q13, Q14, Q15, Q16, Q17, Q18, Q19, Q22. Q14 has a bespoke FastParquet integration. Q19's gap was diagnosed as orchestration ([[project_q19_root_cause_orchestration]]), not shape. The other 8 are pure shape gaps.

This doc walks the physical plans (dumped via `sigma_h_plan_dump`) for the three largest unsold queries — **Q13 (42ms), Q17 (35ms), Q18 (49ms) = ~125ms / 38% of total bench time** — and names the exact catalog gap that blocks each.

## Q13 — nested aggregation (outer agg over inner agg's output)

```
ProjectionExec
  AggregateExec(Final, gby=[c_count], aggr=[count(*)])
    RepartitionHash([c_count])
      AggregateExec(Partial, gby=[c_count])
        ProjectionExec(c_count)
          AggregateExec(SinglePartitioned, gby=[c_custkey], aggr=[count(o_orderkey)])  ← body is itself an aggregate
            HashJoinExec(Left, on=[c_custkey = o_custkey])
              EmatixFastParquetExec(customer)
              FilterExec(o_comment NOT LIKE ...) → EmatixFastParquetExec(orders)
```

**Gap**: `filter_multi_agg_shape()`'s body matcher (`is_supported_body`) accepts leaves, single-child wrappers, and `HashJoinExec`, but **rejects `AggregateExec` as a body node**. Q13's outer agg's body is the inner agg's output — body is a `SinglePartitioned` AggregateExec.

**Widening cost**: medium. We can't fuse the outer agg with the inner agg's plan (different group keys), so the inner agg would have to remain as-is and the outer agg's fused exec just consumes its output. The body matcher could allow `AggregateExec(SinglePartitioned)` as a leaf-equivalent. Risk: the JIT-compiled inner of the fused exec would need to handle batches arriving from a synchronous aggregate, not a partitioned stream — different concurrency model.

## Q17 — empty GROUP BY + CoalescePartitions

```
ProjectionExec(sum/7)
  AggregateExec(Final, gby=[], aggr=[sum(l_extendedprice)])  ← empty group keys
    CoalescePartitionsExec                                    ← NOT RepartitionHash
      AggregateExec(Partial, gby=[], aggr=[sum(l_extendedprice)])
        HashJoinExec(Inner, on=[p_partkey = l_partkey], filter=l_quantity < 0.2 * avg(l_quantity))
          ...self-correlated subquery (avg over lineitem grouped by partkey)
```

**Gap**: two issues.
1. The catalog shape requires `RepartitionHash` between Partial and Final, but with empty group keys DataFusion emits `CoalescePartitions` instead (no shuffling needed).
2. The HashJoin carries a runtime `filter=` expression referencing aggregate output from the right side. Even if shape matched, no current rule fuses join+filter.

**Widening cost**: low for (1) — add an `Optional`-style branch in the shape: `RepartitionHash | CoalescePartitions` between Partial and Final. Risk: codegen perturbation in the multi-agg hot path (the Σ.H.1d failure mode). Mitigation: do it as a **separate new rule** with its own `try_build_replacement`, leaving the string-keyed Partial+Final-with-RepartitionHash path byte-for-byte unchanged.

## Q18 — SinglePartitioned top agg + LeftSemi join

```
SortPreservingMergeExec → SortExec
  AggregateExec(SinglePartitioned, gby=[c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice], aggr=[sum(l_quantity)])  ← top is SinglePartitioned
    HashJoinExec(LeftSemi, on=[o_orderkey = l_orderkey])
      HashJoinExec(Inner) → HashJoinExec(Inner) → scans
      FilterExec(sum > 300)
        AggregateExec(Final, gby=[l_orderkey], aggr=[sum(l_quantity)])    ← anti-subquery agg
          RepartitionHash → AggregateExec(Partial, ...)
```

**Gap**: the top aggregate is `SinglePartitioned` mode, not the Partial+Final pair the catalog matches. DataFusion uses SinglePartitioned when an upstream operator (here: the LeftSemi HashJoin's partitioning) already establishes the group keys' hash distribution, so a separate Final stage is redundant.

**Widening cost**: same as Q13's body issue. The catalog needs a separate top-aggregate shape variant for `SinglePartitioned`, and the matched body would be the HashJoin chain instead of a Partial+RepartitionHash+Final triple. Construction-side: the fused spec would have to do single-pass aggregation instead of two-stage.

## Net assessment

Two distinct gaps account for the three largest non-firing queries:

| Gap | Queries unlocked (TPC-H SF=1 time) | Widening risk |
|---|---|---|
| **A. SinglePartitioned-mode top agg** | Q13 (42ms) + Q18 (49ms) = 91ms | Medium-high — new spec lowering, new fused-exec wiring. Implementable as a *separate* rule to avoid Σ.H.1d-style codegen perturbation. |
| **B. CoalescePartitions in place of RepartitionHash** | Q17 (35ms) | Low-medium — same shape catalog + same spec, just one optional alternation in the matcher. Same separate-rule mitigation applies. |

**Other non-firing queries (smaller time budget):**
- Q09 (29ms) — multi-table join body
- Q12 (15ms) — CASE WHEN aggregates (`sum(case when l_shipmode in (...) then 1 else 0 end)`); the agg-spec extractor in [[fused_aggregate_filter_multi_agg_rule]] doesn't recognise CASE WHEN as a counted-condition.
- Q15 (16ms) — CTE + scalar subquery (`with revenue as (...) ... where total_revenue = (select max(total_revenue) from revenue)`)
- Q16 (9ms) — COUNT(DISTINCT)
- Q22 (8ms) — EXISTS subquery rewritten as anti-join + complex predicates

**TPC-DS coverage** (0/22) is structurally similar — most TPC-DS queries top with SortPreservingMerge or Projection over multi-stage aggregations, but the body is invariably a multi-table join chain (frequently 5–8 joins) which the matchers don't currently walk into. Investigating Σ.K.A (SinglePartitioned) would surface whether TPC-DS would light up alongside.

## Recommendation

**Bite Σ.K.A**: build a new `InjectSinglePartitionedAggRule` as a separate `PhysicalOptimizerRule` (don't touch existing multi-agg rule body), targeting Q13 + Q18 (combined 91ms = 28% of bench time). The separate-rule pattern explicitly avoids the Σ.H.1d codegen-perturbation failure mode.

Bench gate: full 5-run × 20-trial vs current HEAD before landing. Pass = geomean improvement + no individual query regresses > +3%.
