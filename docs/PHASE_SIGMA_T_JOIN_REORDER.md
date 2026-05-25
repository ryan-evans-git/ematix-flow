# Σ.T — Cost-Based Join Reorder

**Status:** Phase 0 (discovery)
**Owner:** ryan-evans-git
**Opened:** 2026-05-25
**Est. effort:** 3-4 weeks

---

## Why this lever, why now

After the Q01-Q22 SF=10 stage-profiling survey (2026-05) and four rejected levers (#1 BridgeFilter, #2 SIMD LIKE, #3 L9 cascading, #4 compound-key Robin Hood), the **22q SF=10 geomean sits at 0.74** (ematix/DuckDB) with five queries — Q05, Q07, Q08, Q17, Q18 — losing on join order rather than kernel speed.

Quoting [project_q18_sf10_duckdb_plan_diff.md](../~/.claude/projects/-Users-ryanevans-RustroverProjects-ematix-flow/memory/project_q18_sf10_duckdb_plan_diff.md):

> Gap is join order (we materialise 60M intermediate, DuckDB filters orders to 3M first); not the agg kernel.

Σ.Q.L10 (push-down LeftSemi, [PR landed in commit 50825c9](../crates/ematix-flow-core/src/push_down_left_semi_rule.rs)) closed Q18 from +153% → +6.2% by routing one semi-join through a path DataFusion's planner couldn't reach. The other four queries need full multi-join reorder, not single-rule tricks.

---

## What's there today

From the Phase-0 survey (2026-05-25):

| Surface | Status |
|---|---|
| DataFusion 53.1 cost-based join reorder | **Absent** — `JoinSelection` only swaps L/R per binary join |
| Custom reorder in ematix-flow | **Absent** |
| Σ.Q.L10 (push-down LeftSemi) | Present — opt-in via `EMAT_PUSH_SEMI=1` |
| `SwapSemiJoinBuildSideRule` | Present — Q18 build-side fixup |
| Bloom cascade (Σ.S.B) | Present — opt-in |
| TableProvider::statistics() | **`num_rows` exact only**, `ColumnStatistics::new_unknown()` for everything else |
| Parquet row-group min/max → DF | **Not surfaced** |
| NDV / histograms | None |
| Pre-plan SQL walker | dict_routing (Σ.K.2) — viable host pattern |

---

## The concrete gap

Plan dumps captured 2026-05-25 for all 5 worst queries. Reading right-to-left (DataFusion) and bottom-up (DuckDB):

### Q05 — JOIN-REORDER PROBLEM ✓

**Ematix order:**
`customer ⋈ orders(1994) → ⋈ lineitem(60M)` produces ~24M intermediate, then `⋈ supplier (2-key) → ⋈ nation → ⋈ region(ASIA)`.

**DuckDB order:**
`region(ASIA) → ⋈ nation → ⋈ customer → ⋈ orders(1994) → ⋈ lineitem` produces ~2.4M intermediate — 10× smaller because the region/nation funnel is applied **before** the lineitem join.

**Gap:** Ematix delays the region filter to the end of the join chain. Pulling region/nation to the front would shrink the customer pool by ~5× before lineitem multiplies it out.

### Q07 — MIXED (some reorder, mostly predicate)

**Both** planners materialise `nation × nation` and apply the `(n_name=FRANCE AND n_name=GERMANY) OR (n_name=GERMANY AND n_name=FRANCE)` filter post-join — a logically tautological-pair shape that needs predicate-splitting, not reorder, to win further.

**Reorder slice:** Ematix builds `supplier ⋈ lineitem(1995-96)` first (~12M); DuckDB starts with the nation filter and routes inward. Win is ~2-3× on the intermediate, not 10×.

### Q08 — JOIN-REORDER PROBLEM ✓ (smaller win than Q05)

**Ematix order:** `part(filtered) ⋈ lineitem(60M) → ⋈ supplier → ⋈ orders(1995-96) → ⋈ customer → ⋈ nation × 2 → ⋈ region(AMERICA)`.

**DuckDB order:** Similar shape, but funnels through nation→region earlier. Both keep `part ⋈ lineitem` early because part is the most selective starting filter (~400 rows). Win is ~1.5-2× on intermediate cardinality.

### Q17 — **NOT a join-reorder problem** ✗

**Ematix:** computes `avg(l_quantity) GROUP BY l_partkey` for **all 60M** lineitem rows (10.8M groups), then joins back to filtered `part`.

**DuckDB:** uses **`LEFT_DELIM_JOIN`** (Delim Join, Index 1) — scalar-subquery decorrelation that runs the avg only for partkeys matching `Brand#23 AND MED BOX` (~400 parts).

**Lever needed:** scalar-subquery decorrelation, not join reorder. Task #15 ("Lever 4: scalar-subquery decorrelation (DELIM_JOIN-equivalent)") attempted this earlier — outcome unclear from completion alone. Σ.R.2 separately tried to fix Q17 at the kernel level and was rejected ([memory](../~/.claude/projects/-Users-ryanevans-RustroverProjects-ematix-flow/memory/project_sigma_r2_rejected.md)).

### Q18 — ALREADY MOSTLY CLOSED ✓

Σ.Q.L10 push-down LeftSemi + Σ.N.f3 RobinHoodSumF64 + Σ.Q.L9 bloom sideband have closed Q18 from +153% (May baseline) to +6.2% (current). The residual gap is **already smaller than the bench noise band**.

---

## Revised scope

The original framing — "five queries lose on join order" — was wrong. Honest decomposition:

| Query | Lever needed | Estimated impact |
|---|---|---|
| Q05 | **Join reorder** | -50% (60M intermediate → ~6M) |
| Q08 | **Join reorder** | -20% (smaller win, FK shape doesn't enumerate as well) |
| Q07 | OR-predicate splitting | Separate lever, ~-10% |
| Q17 | Scalar-subquery decorrelation | Separate lever (Σ.U?) |
| Q18 | Already closed | (noise band) |

**Join reorder helps 2 queries clearly (Q05, Q08) plus marginal wins across 3-4 other join-heavy queries that currently fall back to FROM-order (Q09/Q10?).** The earlier "3-4 wk for 5-query unlock" framing was overoptimistic — the realistic call is **2-3 wk for 2-4 query unlock**, with Q07/Q17 needing **separate** levers.

---

## Design

### Where the rule lives

Two options:
1. **Logical optimizer rule** — rewrite `LogicalPlan::Join` trees before physical planning. Pro: cleanest fit, predicates still attached. Con: physical-side info (build cost, partitioning) not visible yet.
2. **Pre-plan SQL walker** (dict_routing pattern) — sits outside DataFusion's optimizer rules, avoids the [codegen sensitivity tax](../~/.claude/projects/-Users-ryanevans-RustroverProjects-ematix-flow/memory/project_optimizer_codegen_sensitivity.md) that's burned us 5-8% on three prior rules.

**Decision:** Pre-plan SQL walker. Rewrite the parsed `LogicalPlan` before `ctx.sql(...)` returns the DataFrame; DF's existing rules then run on the reordered tree. Matches the dict_routing precedent.

### Cost model (Phase 2)

**Sources of cardinality:**
1. `TableProvider::statistics().num_rows` — exact, already available
2. Parquet row-group min/max → predicate selectivity per column (new infra in Phase 1)
3. NDV estimates from parquet `BOUNDARY_ORDER` + bloom-filter metadata where present (Phase 1)
4. Hardcoded TPC-H FK cardinality table as a fallback floor for Phase 2 — strip later

**Cost function (left-deep, conservative):**
```
cost(join_seq) = Σᵢ build_card(seqᵢ) + probe_scan(seqᵢ)
where  build_card(joinᵢ) = output_card(left_subtree(joinᵢ))
       probe_scan(joinᵢ) = num_rows(right_table(joinᵢ)) × probe_per_row_cost
```

**Output cardinality:**
```
out(A ⋈ B on k) = |A| × |B| / max(NDV(A.k), NDV(B.k))      # equi-join FK heuristic
× selectivity(filter_predicates_on_output_cols)
```

For TPC-H this devolves into well-understood FK ratios (e.g. `orders × lineitem → ~4× orders` via `o_orderkey`). The cost function only has to **rank** alternatives, not estimate absolute ms.

### Enumeration (Phase 2)

- **DP for ≤8 tables** (Selinger left-deep, 2^N × N² table). All 22 TPC-H queries fit.
- **Greedy fallback ≥9 tables** for safety on user workloads.
- Skip Cartesian intermediates (no edge in join graph).
- Keep DataFusion's `JoinSelection` downstream — it still picks build vs probe per binary join after we've fixed the sequence.

---

## Phasing

### Phase 0 — Discovery (this doc, ~3 days)
- Plan dumps Q05/Q07/Q08/Q17/Q18 (ematix + DuckDB)
- Fill the "concrete gap" section above
- Lock the host decision (pre-plan walker vs PhysicalOptimizerRule)
- Lock the cost-model interface

### Phase 1 — Statistics scaffold ✓ LANDED 2026-05-25

Turned out to be a 2-line wire-up rather than a week of work — the aggregation infra (`aggregate_column_statistics`) and per-table cache (`column_stats`) were already in place since Σ.E5 (2026-05-18) and used by `partition_statistics` on the Exec. Both `TableProvider::statistics()` impls ([ematix_fast_parquet.rs:1568](../crates/ematix-flow-core/src/ematix_fast_parquet.rs), [fast_parquet.rs:684](../crates/ematix-flow-core/src/fast_parquet.rs)) just returned `new_unknown` for column_statistics and ignored the cached values.

Fix: return `(*self.column_stats).clone()` in both. Now `null_count`, `min_value`, `max_value` flow into the logical planner.

Tests added (all pass):
- `ematix_fast_parquet::tests::table_provider_statistics_exposes_typed_column_stats`
- `fast_parquet::tests::table_provider_statistics_exposes_typed_column_stats`
- `fast_parquet::tests::column_stats_aggregate_across_row_groups` (3-RG fold)

**Not in Phase 1, deferred to Phase 2 cost model:**
- `distinct_count` (NDV) — for TPC-H FK joins, NDV is derivable from the parent table's `num_rows`, so we don't need parquet-level extraction yet. Compound joins (`s_suppkey + s_nationkey` in Q05) would need it; revisit when the cost model surfaces a use case.
- Bloom-filter NDV metadata extraction (parquet `BLOOM_FILTER_NDV`)
- Integer-range NDV estimation (`max - min + 1`)

### Phase 2 — Cost model + reorder rule ✓ MVP LANDED 2026-05-25

New crate module [`crates/ematix-flow-core/src/join_reorder.rs`](../crates/ematix-flow-core/src/join_reorder.rs).

Public entry: `pub fn reorder_inner_joins(plan: LogicalPlan) -> DfResult<LogicalPlan>`.

What the MVP does:
- `transform_down + TreeNodeRecursion::Jump` walks the LogicalPlan, processing each Inner Join chain atomically from its top (not bottom-up, which would lose the chain shape after partial rebuilds).
- Flattener descends through Projection nodes (DataFusion's projection-pruning optimizer inserts them between joins) but stops at SubqueryAlias / Filter / TableScan as leaves.
- Extracts equi predicates from both `Join::on` and `Join::filter` slots — DataFusion 53.1's SQL parser routes `ON col=col` conditions through either depending on shape.
- **Connectivity-aware greedy ordering**: at each step pick the smallest unplaced leaf that has a predicate connecting to the current chain. Pure smallest-first fails on Q05 because `orders` (~112K post-filter) is smaller than `customer` but doesn't connect directly to `region+nation+supplier`.
- Rebuilds left-deep using `LogicalPlanBuilder::join_on` (predicates land in `filter` slot — DataFusion's HashJoin still treats column-pair filters as equi-keys for HashJoinExec construction).
- Bails (returns the original plan) if any leaf can't be connected (cross-product detected), if any equi predicate can't be re-attached, or if the chosen order matches the input order.

5 tests pass:
1. `no_op_on_single_table` — `SELECT COUNT(*) FROM lineitem` passes through unchanged
2. `no_op_on_two_table_join` — 2-leaf chains are no-ops (MVP threshold ≥3)
3. `reorders_three_table_chain_smallest_first` — `lineitem JOIN orders JOIN customer` rewrites to put `customer` (smallest) as leftmost
4. `reorders_q05_shape_against_real_data` — Q05 SF=1 reorders to `region → nation → supplier → customer → orders → lineitem`, exactly matching DuckDB's plan
5. `rewrite_preserves_query_result` — end-to-end correctness: original plan and rewritten plan produce the same result rows on a 3-table aliased GROUP BY

**Phase 2 NOT yet covered (deferred to Phase 2.b or 3):**
- Selinger DP enumeration. The greedy heuristic is order-graph-aware and gets Q05's optimal order, but for shapes with more than one cardinality-equivalent join graph (cycles, multi-way bushy plans) DP could outperform it. Defer until a benchmark shows a case where greedy loses.
- FK-aware output cardinality. The cost model uses leaf-side estimated rows (table size × filter selectivity from min/max). Output cardinality after join (`|A|×|B|/NDV`) isn't modeled — for left-deep TPC-H chains this is fine because the join graph is a tree and NDV ≈ smaller table's row count.
- Q07/Q08-shape OR predicate splitting (`n_name=FRANCE AND n_name=GERMANY OR ...`). Separate Phase.

### Phase 3 — Bench validation (~1 week)
- Single-query A/B Q05/Q07/Q08/Q17/Q18 (15-trial, 3-warmup)
- 22q SF=10 geomean check
- **Bench gate:** 22q geomean ≤ 0.74 AND zero query-level regression > +5%. Anything worse and we revert before merging.

### Phase 4 — Harden + integrate (~1 week)
- Opt-in flag `EMAT_REORDER=1`
- Default-on after gate passes
- Integration tests with extreme join shapes
- Documentation + memory entries

---

## Risks

1. **Cost-model garbage** — bad cardinality estimates produce worse plans than DF's default left-deep-by-FROM-order. Mitigation: TPC-H FK floor as fallback; bench-gate every change.
2. **Codegen tax** — three prior rules silently regressed 5-8% from LLVM perturbation ([memory](../~/.claude/projects/-Users-ryanevans-RustroverProjects-ematix-flow/memory/project_optimizer_codegen_sensitivity.md)). Mitigation: pre-plan walker (no PhysicalOptimizerRule).
3. **CSE doesn't share Join outputs** ([memory](../~/.claude/projects/-Users-ryanevans-RustroverProjects-ematix-flow/memory/project_sigma_qm_slice2_rejected.md)) — reordering may expose duplicate join subtrees with no sharing. Mitigation: explicit CSE pass after reorder (deferred to Phase 4 if observed).
4. **Σ.Q.L10 interaction** — push-down LeftSemi and join reorder both rewrite the LogicalPlan. Must compose. Phase 0 dump confirms whether L10 fires first or after.

---

## Decision points (Phase 0 must resolve)

- [ ] Logical-plane vs pre-plan walker — confirm pre-plan walker
- [ ] Reorder INPUT: `LogicalPlan` (post-parse pre-optimize) vs `Plan` (post-optimize) — confirm post-parse pre-optimize
- [ ] Compose order with Σ.Q.L10 — confirm L10 runs **before** reorder (it shrinks the tree)
- [ ] Statistics path — extend ematix-parquet to surface row-group stats vs scrape per-call from parquet metadata
- [ ] Bench gate threshold — currently set at 0.74 geomean / zero +5% regression; confirm
