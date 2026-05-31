# Σ.U — Agg-Side LeftSemi Pushdown

**Status:** Phase 1 LANDED 2026-05-26 as opt-in via `EMAT_AGG_SEMI=1`
**Owner:** ryan-evans-git
**Opened:** 2026-05-26 (continued from Σ.T audit)

---

## Why this lever

Σ.T's 22-query plan audit (committed at [`1a14007`](../crates/ematix-flow-core/src/join_reorder.rs)) ranked Q17 SF=10 as the #1 inefficiency — the correlated subquery shape produces an `Aggregate` over **all 60M lineitem rows** producing 2M partkey groups, when only the ~200 partkeys matching the outer `p_brand='Brand#23' AND p_container='MED BOX'` filter actually contribute to the final result.

DuckDB closes this with `LEFT_DELIM_JOIN` ("magic set" / dependent-subquery decorrelation): the part-filter is pushed into the aggregate's lineitem scan so the agg only sees the ~6K rows whose partkey will survive the outer join.

## Pattern matched

After DataFusion's `scalar_subquery_to_join` runs, Q17's optimized LogicalPlan looks like:

```
Inner Join: part.p_partkey = __scalar_sq_1.l_partkey, filter: l_quantity < 0.2*avg
  ├─ Projection                                          ← main branch
  │   └─ Inner Join: lineitem.l_partkey = part.p_partkey
  │       ├─ TableScan: lineitem
  │       └─ Projection: part.p_partkey
  │           └─ Filter: brand+container
  │               └─ TableScan: part
  └─ SubqueryAlias: __scalar_sq_1                        ← agg branch (target)
      └─ Projection: 0.2*avg, l_partkey
          └─ Aggregate(group_by=[l_partkey], avg)
              └─ TableScan: lineitem                     ← target node
```

Σ.U rewrites this to:

```
... unchanged outer InnerJoin and main branch ...
  └─ SubqueryAlias: __scalar_sq_1
      └─ Projection: 0.2*avg, l_partkey
          └─ Aggregate(group_by=[l_partkey], avg)
              └─ LeftSemi (lineitem.l_partkey = part.p_partkey)
                  ├─ TableScan: lineitem
                  └─ Filter(brand+container) → TableScan: part   ← cloned from main branch
```

After DataFusion physical planning, the LeftSemi becomes a `HashJoinExec` with `mode=Partitioned, join_type=RightSemi` — the small filtered-part build (200 rows) probed by lineitem (60M rows), output ~6K rows feeding the aggregate. The aggregate downstream becomes `mode=SinglePartitioned` (no shuffle, since partition is already on `l_partkey`).

## Implementation

[`crates/ematix-flow-core/src/agg_filter_pushdown.rs`](../crates/ematix-flow-core/src/agg_filter_pushdown.rs) — ~300 LOC pre-plan walker.

Key design decisions:

1. **Pre-plan walker, not OptimizerRule** — matches the [`dict_routing`](../crates/ematix-flow-core/src/dict_routing.rs) and [`join_reorder`](../crates/ematix-flow-core/src/join_reorder.rs) precedent, avoiding the [optimizer-codegen tax](../~/.claude/projects/-Users-ryanevans-RustroverProjects-ematix-flow/memory/project_optimizer_codegen_sensitivity.md) that's burned 5–8 pp geomean on three prior rules.
2. **Column-name matching, not full Column equality** — DataFusion's `scalar_subquery_to_join` wraps the agg subtree in a `SubqueryAlias` (e.g. `__scalar_sq_1`), so the outer join's column references use that alias while the agg's group-by retains the original (`lineitem.l_partkey`). The rule looks up the un-aliased column from the agg's own group-by and uses THAT inside the LeftSemi's join condition.
3. **Filter subtree cloning** — we duplicate the `Filter → TableScan` chain from the main branch into the agg's input. This costs an extra small scan (200K-row `part` scan with a selective filter). The duplicate scan is cheap relative to the saved work, and Σ.O.c's RG cache shares decoded row-groups across multiple scans of the same file anyway.
4. **Narrow pattern** — Σ.U Phase 1 matches exactly the Q17 shape. Generalization (multi-key agg group-by, more complex filter subtrees, fan-out chains) is out of scope until the basic pattern ships.

## Bench result — Q17 SF=10 (correctness-first framing)

| | Baseline | EMAT_AGG_SEMI=1 | Δ |
|---|---:|---:|---:|
| Q17 SF=10 ematix (5 trials × 2 warmups) | 178.31 ms ± 16.14 | 180.53 ms ± 16.09 | +1.2% (in noise) |
| Q17 SF=10 vs DuckDB | 1.09× | 1.12× | in noise |
| 22q SF=10 geomean (3-trial × 1-warmup A/B) | 0.714 | 0.732 | +1.8pp (Q01/Q18 flipped — all run-to-run noise) |

Rule-fire scan across 22 queries: **only Q17 fires** — confirms the 22q noise is system-level, not rule-induced.

### Where the saved work went

The structural improvement is real (LeftSemi pre-prunes lineitem to ~6K rows before the agg) but wall-time is flat. Three factors absorb the theoretical savings:

1. **[Σ.O.c.2 RG decode cache](../~/.claude/projects/-Users-ryanevans-RustroverProjects-ematix-flow/memory/project_sigma_oc2_provider_landed.md)** already amortises the duplicate lineitem scan — the 60M-row decode is shared between the main-branch and agg-branch lineitem refs. Before Σ.U, the agg branch's "duplicate scan" was logical only.
2. **`GroupValuesPrimitive`** makes the original 60M-row → 2M-group hash agg cheaper than naive estimates suggest. Single-int group key + SIMD-vectorised batch insert ≈ 5 ns/row.
3. **The LeftSemi probe** runs over the same 60M lineitem rows (post-decode), checking l_partkey against the 200-row filtered-part bloom. ~5 ns/row × 60M ≈ 300 ms serial / 14 threads = ~22 ms. That's approximately equal to the saved hash-agg work.

Net: the rewritten plan does the same amount of physical work, just in a different shape. The wins would materialise IF (a) the underlying decode could be skipped (page-index pruning), or (b) the LeftSemi probe were significantly cheaper than the agg insert (it isn't at this row count). Documented for future investigation.

## Disposition

- **Land as opt-in** via `EMAT_AGG_SEMI=1`. Default OFF.
- The rewrite is structurally correct and matches DuckDB's pattern.
- Future work on Q17's perf should focus on **page-index pruning** for the agg's lineitem scan (skip RGs that don't contain any of the 200 matching partkeys) — that would skip decode, not just probe.

## Phase 2 — possible follow-ups (NOT scoped)

- Generalize the pattern to match Q02's `(SELECT MIN(ps_supplycost) ...)` shape — needs verification that DF's decorrelation produces the same Inner-Join-over-Aggregate structure.
- Detect Q22's `c_acctbal > (SELECT AVG ...)` pattern.
- CSE pass to dedup the cloned filter subtree (Σ.P SharedSubtreeExec extension).
