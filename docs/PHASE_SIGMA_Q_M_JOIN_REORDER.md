# Σ.Q.M — Synthetic LeftSemi join-reorder lever

**Mission**: close the remaining SF=10 star-join gaps (Q05 1.42×, Q07 1.11×,
Q08 1.09×, Q03 ~parity, Q09 already a win but a candidate accelerator) by
synthesising redundant `LeftSemi` joins above `Inner` joins where the dim
side has been measurably filtered. Σ.Q.L10 (`PushDownLeftSemiRule`) then
takes over and pushes the synthetic semi down to wrap the fact-table
`TableScan`, replicating DuckDB's dynamic-filter propagation as a static
plan rewrite.

**Branch**: `perf/sigma-q-single-node-parity`
**Scope start**: 2026-05-23, post Σ.Q.L16 (commit `bf43dc0`).
**Predecessors**: Σ.Q.L10 (the consumer), Σ.Q.L9 (runtime sideband, complementary), Σ.Q.L17 (declared "remaining gaps need structural work" — this IS that structural work).

---

## Background

DuckDB closes Q05 / Q07 / Q08 by propagating filters from filtered dim
tables into fact-table scans as **dynamic filters**. Once `region.r_name =
'ASIA'` filters `region` to 1 row, the chain
`region ⋈ nation ⋈ customer ⋈ orders ⋈ lineitem` lets DuckDB compute (at
runtime) the set of `o_orderkey` / `l_orderkey` values that can possibly
survive and apply that set as a probe-side filter to the lineitem scan.

ematix-flow has two adjacent mechanisms but neither does this:

1. **Σ.Q.L9** (`runtime_bloom_sideband_rule.rs`) — captures hash-join build
   keys at runtime and ships them sideband to the probe scan. Works only
   when the build side is a HashJoinExec that fires before the probe
   scan polls. Doesn't help when the dim subtree is *several joins above*
   the fact scan in the original plan (L9's sideband attaches one level
   up at a time).

2. **Σ.Q.L10** (`push_down_left_semi_rule.rs`) — pushes existing
   `LeftSemi` / `LeftAnti` joins down through `Inner` joins to wrap the
   target `TableScan`. Triggered today only by `IN` / `EXISTS`
   subqueries that DataFusion's `decorrelate_predicate_subquery` produces
   (e.g. Q18, Q21). Star joins like Q05 don't have any subquery, so no
   `LeftSemi` ever exists for L10 to push.

**Σ.Q.M is the producer rule that creates the LeftSemi for L10 to push.**

The composition is:

```text
┌────────────────────────┐         ┌─────────────────────────┐
│ Σ.Q.M (logical)        │         │ Σ.Q.L10 (logical)       │
│ detect filtered dim ⋈  │  ───►   │ push synthetic LeftSemi │
│ fact pattern;          │         │ down to fact TableScan  │
│ wrap with LeftSemi     │         │                         │
└────────────────────────┘         └─────────────────────────┘
```

The Σ.Q.M-emitted plan is semantically a no-op (LeftSemi above an Inner
on the same keys produces the same rows the Inner already does). Σ.Q.L10
then physically relocates the semi-join to filter the fact scan early.
The combined effect is the static analogue of DuckDB's runtime dynamic
filter.

---

## 1. Detection logic

### What we detect

The target pattern in logical-plan form, at any depth in the tree:

```text
Inner Join(left, right, on = [(L.K, R.FK) | (L.FK, R.K)])
   ├── left  subtree S_L  → reaches base table T_L
   └── right subtree S_R  → reaches base table T_R
```

Where:

- One of `(S_L, S_R)` is a **dim subtree** — a subtree that has
  measurable selectivity vs its underlying base table.
- The other is a **fact subtree** — a subtree whose base table is the
  large fact (lineitem, orders).
- The join's equi-key on the dim side resolves to a column of T_dim;
  the equi-key on the fact side resolves to a column of T_fact.

When we match, we wrap the *fact subtree's TableScan* (lazily — actually
we wrap the join's parent context, see Synthesis below) with:

```text
LeftSemi(fact_subtree, dim_subtree, fact.FK = dim.K)
```

Then we leave the original `Inner Join` intact above it. Σ.Q.L10 (which
runs later in the optimizer pass list) walks the LeftSemi down to the
fact `TableScan`.

### "Measurable selectivity" — concrete definition

A dim subtree S has **measurable selectivity** vs its base table T_dim
iff at least one of:

1. **Filter visible in subtree.** Walking S downward from its root,
   there exists a `LogicalPlan::Filter` node whose predicate references
   only columns of T_dim. The presence of a Filter directly above /
   between scans is the strongest signal — DataFusion's `PushDownFilter`
   guarantees Filters land adjacent to their scans, so the existence of
   a Filter in S means it has at least some narrowing.

2. **Subtree contains a Join with another already-filtered dim.** S
   might be `nation ⋈ region` where `region` has the actual Filter. The
   `nation ⋈ region` result narrows nation indirectly. Recursive
   detection: a subtree counts as filtered if it (a) directly contains
   a Filter on T_dim columns, or (b) is the result of joining T_dim
   against a filtered subtree on an equi-key.

3. **Statistical narrowing from leaf-scan partition stats.** If
   `S.partition_statistics().num_rows().get_value()` is meaningfully
   smaller than the base table cardinality (ratio ≥ 2×), the subtree
   has reduced from base — likely a filter has been pushed into the
   ParquetScan's pruning logic.

We start with **rule 1 only** in Phase 1 (Filter-in-subtree is the
cheapest, highest-precision signal and covers Q05 / Q07 / Q08 / Q03).
Rule 2 (recursive — covers Q05's `region ⋈ nation` cascade) is added
in Phase 2. Rule 3 (statistical) is Phase 3, deferred until we have a
test query that needs it; it carries a regression risk because DataFusion's
partition_statistics are not always populated and we'd need a sane
fallback.

### Leaf-table-size source

Two options:

- **`LogicalPlan::TableScan::source.statistics()`** returns
  `Option<Statistics>` whose `num_rows` may be `Exact` or `Inexact`.
  For Emat/FastParquet providers this is populated from the file
  metadata at registration time. **Primary source.**

- **Walk count.** Cheaper, no-stats-needed fallback: if a subtree
  contains *any* `Filter` on dim columns, treat the subtree as
  "selectivity ≥ 2×" without quantifying. The cost gate (§4) then
  consumes the binary "is_filtered_dim / is_unfiltered_dim" answer
  rather than a ratio.

We use option 2 (Filter-presence walk) as primary because (a) it doesn't
depend on stats accuracy and (b) the cost-gate signal really is binary
(see §4 — unfiltered dims are net-negative). Option 1 (numerical
statistics) is a fallback for the Phase 3 statistical-narrowing rule.

### What we DO NOT detect (initial scope)

- **Non-equi joins.** The Σ.Q.M rule fires only on `Inner` joins with
  `on.len() ≥ 1` and at least one equi-key whose sides resolve cleanly
  to single columns of the two base tables. Joins with `filter:` post-
  conditions (non-equi predicates) are skipped per Σ.Q.L10's existing
  policy.

- **Multi-fact patterns.** If both sides resolve to fact tables
  (lineitem⋈lineitem self-join in Q21), neither is "dim." Skip.

- **Cross joins.** Don't fire.

- **Joins where neither side has a measurable filter.** This is the
  unfiltered-dim case from §4 — pure performance cost with no win.

---

## 2. Rule placement in the optimizer pass order

DataFusion's logical-optimizer pass list (relevant ones, default order):

```text
1. type_coercion
2. simplify_expressions
3. unwrap_cast_in_comparison
4. replace_distinct_aggregate
5. eliminate_join
6. decorrelate_predicate_subquery     ← emits LeftSemi from IN/EXISTS
7. scalar_subquery_to_join
8. extract_equijoin_predicate
9. eliminate_duplicated_expr
10. eliminate_filter
11. eliminate_cross_join
12. common_sub_expression_eliminate
13. eliminate_limit
14. propagate_empty_relation
15. eliminate_one_union
16. filter_null_join_keys
17. eliminate_outer_join
18. push_down_limit
19. push_down_filter                  ← Filters land adjacent to scans
20. single_distinct_to_groupby
21. simplify_expressions  (rerun)
22. (custom rules)
```

### Where Σ.Q.M needs to run

Σ.Q.M needs to run **AFTER `push_down_filter`** because:

- Detection rule 1 ("Filter visible in subtree") relies on Filters
  having been pushed adjacent to their dim TableScans. Before
  `push_down_filter`, Filters sit at the top of the query, so the
  Filter-in-dim-subtree signal isn't reliable.
- Filter placement is what makes the dim subtree small. The whole
  premise of "measurable selectivity" is post-PushDownFilter.

Σ.Q.M needs to run **BEFORE `PushDownLeftSemiRule`** (Σ.Q.L10) because:

- Σ.Q.M produces synthetic LeftSemi joins; L10 consumes them.
- L10 currently runs at the tail of the custom rules. Σ.Q.M slots
  between PushDownFilter and L10.

**Concrete placement**: Σ.Q.M registers as a custom optimizer rule
installed via `SessionStateBuilder::with_optimizer_rule(...)` exactly
the way L10 does. Custom rules run after built-in rules in DataFusion's
default order, with multiple custom rules running in registration order.
Install order:

```rust
builder
    .with_optimizer_rule(Arc::new(SyntheticLeftSemiRule))       // Σ.Q.M
    .with_optimizer_rule(Arc::new(PushDownLeftSemiRule))         // Σ.Q.L10
```

### Interaction with `PushDownFilter`

If Σ.Q.M fires and emits a LeftSemi, will `PushDownFilter` see the new
join and try to push something through it? Answer: PushDownFilter runs
ONCE before Σ.Q.M (per the pass order above), so it doesn't see the
synthetic join. The optimizer runs a fixed-point loop by default though
— if Σ.Q.M produces a transformation, the loop may re-run earlier
passes. We mitigate by:

1. Making Σ.Q.M idempotent: re-running it on a plan that already
   contains the synthetic LeftSemi must produce no further change.
   Detection condition: if the Inner Join already has a sibling
   LeftSemi with the same equi-keys above it, skip.

2. Tagging emitted joins with a marker (a `LogicalPlan` extension
   property or a side-table of `join_ptr -> "synthetic"`) so the
   detector treats them as "already done."

Idempotency is the cleaner solution and is what we'll start with. The
sibling-check uses the same `Arc::ptr_eq` / equi-key matching logic L10
already implements.

### Interaction with `extract_equijoin_predicate`

This earlier pass converts join filters like `Inner Join ON p.x = q.y`
into `on: [(p.x, q.y)]`. It runs at pass 8, well before Σ.Q.M. So by
the time Σ.Q.M walks the plan, every relevant join already has its
equi-keys in the canonical `on` vec. Good — we don't need to parse
join `filter:` expressions.

### Interaction with Σ.Q.L9 (runtime sideband)

L9 fires on physical `HashJoinExec` nodes, after JoinSelection during
physical planning. Σ.Q.M's plan transformation is logical (above
physical planning), so by the time L9 runs, the synthetic LeftSemi has
been physically planned as a `HashJoinExec(LeftSemi)` already pushed
to its target — L10 has done its work. **L9 sees the post-Σ.Q.M plan
and may attach a sideband to the LeftSemi's HashJoinExec or to any
remaining Inner joins.** No coupling needed; the two rules don't
conflict.

---

## 3. Correctness

### Core argument

`LeftSemi(L, R, K_L = K_R)` outputs every row of `L` for which there
exists at least one row of `R` satisfying `K_L = K_R`. Its output
schema equals `L`'s schema. Its output is a subset of `L`'s rows.

`Inner Join(L, R, K_L = K_R)` outputs every (l, r) pair satisfying
`K_L = K_R`, projecting both schemas. Critically, every `l` that
appears in the Inner output also satisfies "∃ r ∈ R: K_L(l) = K_R(r)"
— the LeftSemi predicate.

**Therefore**: for any Inner Join with equi-keys `[(K_L_i, K_R_i)]`,
inserting a `LeftSemi(L, R, [(K_L_i, K_R_i)])` on the same left
subtree and the same right subtree produces *exactly the same rows*
on the left side as the Inner Join would let through. The Inner
operating on the post-LeftSemi left is identical to the original
Inner operating on the pre-LeftSemi left, because every row the
Inner would have joined on still survives the semi (the semi only
drops rows the Inner would have dropped anyway).

This is **provably semantics-preserving** in the no-NULL case.

### Edge cases

#### NULLs in join keys

`Inner Join` on `K_L = K_R` drops rows where either key is NULL
(SQL standard `NULL = NULL` is unknown, not true). `LeftSemi` follows
the same convention by default in DataFusion (per `null_equality:
NullEquality::NullEqualsNothing`). So if the original `Inner` had
NullEqualsNothing (the default), the synthetic `LeftSemi` must also
use NullEqualsNothing, and equivalence holds.

**Rule**: copy `null_equality` and `null_aware` fields from the
detected Inner Join to the synthetic LeftSemi. Don't construct a
LeftSemi if the Inner's `null_equality` is `NullEqualsNull` and we
can't prove (cheaply) that the dim key is non-nullable — in TPC-H
all FK columns are non-nullable so this should be a rare bail.

#### Multi-key joins

Inner Joins can have `on.len() > 1`. The semi-pushdown's correctness
argument is per-key independent: if all `n` equi-keys are preserved
on the LeftSemi, semantics are preserved. We just copy the entire
`on` vec.

Σ.Q.L10's existing implementation already handles multi-key LeftSemi
(via `resolve_single_target_table` which checks all equi-keys reference
the same base table). Our synthetic emission will, by construction,
satisfy this: we identify the fact-side base table T_fact, and every
fact-side equi-key is by definition a column of T_fact.

#### Non-equi `filter:` post-conditions

If the original Inner Join has a `filter:` Expression (post-equi
predicate), the synthetic LeftSemi has two choices:

- **Include the filter.** Correctness preserved, but L10 bails on
  LeftSemi with `filter.is_some()`, so the push-down won't happen.
  Net: rule fires but doesn't propagate.

- **Drop the filter.** WRONG — the LeftSemi would now pass through
  rows that the Inner would later drop, but the LeftSemi predicate
  is weaker than the Inner's. This is incorrect semantics, because
  while the LeftSemi above the Inner still produces correct rows
  (the Inner filters them downstream), the *pushed* LeftSemi below
  would let extra rows through that may matter for later joins.

**Rule**: skip Inner Joins with non-empty `filter:`. This is the same
policy Σ.Q.L10 uses. Q07 and Q05 don't have post-conditions on the
relevant joins, so we don't lose coverage.

#### Multi-instance dim subtrees (sharing)

If the dim subtree `S_R` is expensive to compute and Σ.Q.M wraps it
into a LeftSemi *and* leaves it referenced in the Inner Join, that's
two evaluations of S_R. For raw `Filter → TableScan(nation)` this is
cheap. But for `customer ⋈ nation` (a multi-join dim subtree), running
it twice is a real cost.

**Mitigation options**:

1. **Use a CTE / SubqueryAlias.** Wrap S_R in a logical equivalent
   of a CTE so both consumers share the result. DataFusion's
   `SharedSubtreeExec` (Σ.P CSE) already does this at the physical
   level. We can either rely on Σ.P to catch the duplication or
   produce SubqueryAlias in the logical plan.

2. **Build a small bloom from S_R execution and use that instead of
   re-running the subtree.** This is what Σ.Q.L9 does at runtime;
   it's not what Σ.Q.M does (Σ.Q.M is a static plan rewrite).

3. **Restrict to S_R = single TableScan or `Filter → TableScan`.**
   Multi-join dim subtrees are not eligible. This is the safe
   starting point — sacrifices Q05's full chain but covers Q07
   (nation has a simple filter).

We start with option 3 (restrict to shallow dim subtrees) in Phase 1
and lift the restriction in Phase 2 once we've measured cost
correctly. Note that Σ.Q.L10's existing `is_shallow_build_subtree`
gate from the Σ.Q.L4' work is the precedent.

---

## 4. Cost gate (when NOT to fire)

The defining failure mode (from Σ.Q.L4' and Σ.Q.L9 history): **firing
on an unfiltered dim wastes work**. If `supplier` has no filter, then
`supplier ⋈ lineitem` already passes ~100% of lineitem's supplier-FK
values through the bloom (every supplier key is in the bloom set), so
the LeftSemi pushed down to the lineitem scan does no row elimination
yet incurs bloom-test cost downstream.

This is the same pattern Σ.Q.L15 captured with `EMAT_RT_BLOOM_RATIO=
1024`: the L9 selectivity gate had to be widened from 64× to 1024×
to gate out the "supplier is unfiltered" case. For Σ.Q.M we want a
static analogue.

### The concrete gate

Fire the rule on `Inner Join(left=L, right=R)` with equi-keys `K_L=K_R`
only if **both** of:

#### Gate A — Dim side has a Filter

The dim subtree (the one with the smaller base table — for TPC-H
these are part, supplier, customer, orders, nation, region) MUST
contain a `LogicalPlan::Filter` node anywhere between its root and
the TableScan of its base table.

This is a binary, structural check. The walk is cheap (linear in
subtree size). Implementation:

```text
fn has_filter_on_path_to_scan(subtree: &LogicalPlan, target_table: &str) -> bool {
    // Walk subtree. If a Filter node is encountered before reaching
    // a TableScan of target_table, return true. Bails on Join nodes
    // (those introduce non-monotonic narrowing semantics).
}
```

Edge cases:

- **Filter on a *different* table within the subtree** (e.g., the
  dim subtree is `nation ⋈ region` and the Filter is on `region`):
  this counts as filtering the dim. The phase-1 simple walker should
  follow only direct chains (`Filter → TableScan` or `Filter →
  Projection → TableScan`). Phase 2 adds Join-aware recursion for
  Q05.

- **Filter predicate is trivially true** (e.g. `1 = 1`): assume
  DataFusion's `simplify_expressions` has removed it; the unlikely
  residual case fires the rule once and Σ.Q.L10 may push, but the
  bloom is the full dim key set so impact is neutral-to-slightly-
  negative. Acceptable.

#### Gate B — Fact side is large

The fact side must be a "fact-class" table — TPC-H lineitem (60M @
SF=10) or orders (15M @ SF=10). Concrete test: the fact subtree's
base table has `Statistics.num_rows() >= FACT_ROW_THRESHOLD`. We
pick `FACT_ROW_THRESHOLD = 1_000_000` for now (SF=1's smallest fact
is lineitem at 6M; orders is 1.5M but we still consider it fact-
sized at SF=10's 15M).

If both subtrees are large, the rule doesn't fire (the symmetric
fact⋈fact case is not the dim-propagation pattern).

### Why not estimate selectivity directly?

We considered numeric selectivity gates (e.g., "fire if filtered_dim
rows / base_dim_rows ≤ 0.5"). Rejected because:

- DataFusion's `Filter.statistics().num_rows()` is often `Inexact` or
  missing for parquet sources after some Filter rewrites.
- The threshold's right value depends on join cardinality (a 50%
  filter is selective enough on a 10K-row dim but not on a 100-row
  dim).
- A binary "Filter present" gate is sound for TPC-H and avoids
  brittleness.

### Optionally fire on FactA⋈FactB (Phase 4)

Q07's worst case is `lineitem ⋈ orders` where one side may have been
date-filtered. Treating orders as a fact (it is) but recognising that
`o_orderdate >= '1995-01-01'` cuts orders by 80% means Σ.Q.M could
synthesise a semi from orders→lineitem too. Phase 4 lifts the
"FactA⋈FactB skip" once we've measured Phase 1-3 cleanly.

---

## 5. Phasing

Each slice is an independently shippable PR. Each slice is gated by
`EMAT_SYNTHETIC_LEFT_SEMI=1` (env opt-in) and validated by:

- All 14 existing `push_down_left_semi_rule.rs` tests still pass.
- New rule-specific unit tests pass (TDD per memory rules).
- `cargo run --example tpch_validate --release` passes at SF=1 and
  SF=10 (cell-by-cell value match against DuckDB).
- 22q `tpch_triangulation_bench` at SF=10 with 11×3 trials shows
  the targeted query move without >10% regression on any other
  query.

### Slice 1: Detection + emission, single-table dim subtree

**Scope**: Implement `SyntheticLeftSemiRule` that walks the plan,
detects `Inner Join` nodes where one direct child is a
`Filter → TableScan(dim)` and the other is a `TableScan(fact)`
(possibly behind a single Projection). Emit `LeftSemi(fact_side,
dim_side, fact.FK = dim.K)` above the Inner.

Idempotency check: skip if there's already a LeftSemi above with the
same on-keys and same left/right subtree pointers.

**Code touched**:
- `crates/ematix-flow-core/src/synthetic_left_semi_rule.rs` (new file).
- `crates/ematix-flow-core/src/lib.rs` (re-export + install function).
- Optionally `crates/ematix-flow-core/src/preset.rs` (default-off opt-in
  via env, default-on for `EMAT_RULES=v_sigma_q_m`).

**Tests** (TDD):
1. `fires_on_filtered_dim_inner_join` — small synthetic case mirroring
   Q07's `s_suppkey = l_suppkey` with a supplier filter. Assert that
   after optimization the plan contains a `LeftSemi` above the lineitem
   `TableScan` (because Σ.Q.L10 will have pushed it).
2. `does_not_fire_on_unfiltered_dim` — same shape but no Filter on
   supplier. Assert no LeftSemi appears.
3. `does_not_fire_when_both_sides_fact` — lineitem⋈lineitem (synthetic
   self-join). Assert no LeftSemi.
4. `bails_on_non_equi_filter_postcondition` — Inner with `filter:`
   set. Assert no LeftSemi.
5. `idempotent_on_repeated_application` — Run the rule twice via
   `transform_up`; assert second pass produces no change.
6. `null_equality_copied` — Set Inner's `null_equality` to a
   non-default; assert synthetic LeftSemi copies it.

**Expected query moves**: Q07 SF=10 (s_suppkey⋈l_suppkey with filtered
supplier), Q03 SF=10 (c_custkey⋈o_custkey with filtered customer).

**Pass criteria**:
- Q07 SF=10 ≤ 145 ms (current 151 ms; target -4% to land in DuckDB
  parity range).
- 22q SF=10 geomean within ±2% of pre-slice baseline (the codegen tax
  is real — we measure once on landing).
- All 22 tpch_validate values match.

**Estimated effort**: 8-12 hours.

### Slice 2: Recursive dim-side detection (Filter under one join)

**Scope**: Extend dim-subtree detection to follow one level of Inner
Join descent. Pattern:

```text
S_dim = Inner Join(dim_base, other_dim_with_filter, on = (...))
      | Filter → TableScan(dim_base)
      | TableScan(dim_base)
```

The recursion stops at depth 2 in this slice (we don't recurse further
yet). Q05's `region ⋈ nation` chain matches at depth 1: `nation ⋈
(Filter(r_name=ASIA) → region)` is detected as filtered-dim.

Multi-instance protection: if the deeper subtree is referenced
elsewhere in the plan (would double-execute), check for `Σ.P
SharedSubtreeExec` opportunities. Initial implementation: just rely
on DataFusion's `common_subexpression_elimination` pass and accept
that S_dim may be evaluated twice — measure the cost in the bench.

**Code touched**: Extend the walker in
`synthetic_left_semi_rule.rs`. Keep the gate-A simple-Filter case as
fast-path, only fall through to recursive detection when shallow
detection fails.

**Tests**:
1. `descends_through_inner_join_to_filtered_table` — `nation ⋈
   filtered_region` shape; assert LeftSemi emitted.
2. `does_not_descend_into_subtree_with_join_and_no_filter` — pure
   `nation ⋈ region` with no filter on either side; assert no emission.
3. `descends_to_depth_2_only` — depth-3 chain (region⋈nation⋈customer
   with filter on region); assert behaviour matches design (we'd
   start with no emission at depth 3 in Phase 2; lift in Phase 3).
4. `cse_avoids_double_evaluation` — emit a synthetic; verify the
   physical plan post-Σ.P shows a `SharedSubtreeExec` over the dim
   subtree, indicating sharing.

**Expected query moves**: Q05 SF=10 (region→nation→customer→orders→
lineitem chain), Q08 SF=10 (similar shape with region filter).

**Pass criteria**:
- Q05 SF=10 ≤ 165 ms (currently 202 ms; target -18% to land at
  ~1.15× DuckDB, closing most of the 1.42× gap).
- Q08 SF=10 ≤ 190 ms (currently 202 ms; target -6% to land at parity).
- 22q SF=10 geomean within ±2% of pre-slice baseline.
- All 22 tpch_validate values match.

**Estimated effort**: 8-12 hours.

### Slice 3: Deep dim-chain detection (depth ≥ 3)

**Scope**: Lift the depth-2 cap from Slice 2. Walk arbitrary nested
Inner joins on the dim side as long as every joined subtree contains
a Filter (Gate A) or is leaf-table-sized (small dim base table, e.g.
nation has 25 rows and is always "selective enough").

Add a small-dim-base bypass: if a TableScan's base table cardinality
is < `SMALL_DIM_THRESHOLD = 10_000` rows, the subtree counts as
"filtered" even without a Filter node. Justified because nation (25
rows) and region (5 rows) join keys form tight FK sets that even
unfiltered would prune the fact side substantially.

**Code touched**: Extend recursive walker; add small-dim-base check
via `TableScan.source.statistics()`.

**Tests**:
1. `q05_pattern_4_table_chain` — synthetic test mirroring Q05's
   region→nation→customer→orders chain. Verify the synthetic
   LeftSemi covers the orders side and pushes to lineitem.
2. `small_unfiltered_dim_counts_as_filtered` — nation (25 rows)
   without a Filter; assert emission fires.
3. `large_unfiltered_dim_skipped` — synthetic 200K-row dim without
   filter; assert no emission.

**Expected query moves**: Q09 SF=10 (`p_name LIKE '%green%'` → part,
into supplier, into lineitem — already a 0.84× win but the lever may
amplify it to 0.75× via more aggressive lineitem pruning).

**Pass criteria**:
- Q09 SF=10 ≤ 240 ms (currently 272 ms).
- 22q SF=10 geomean ≤ pre-slice baseline.
- All 22 tpch_validate values match.

**Estimated effort**: 4-8 hours.

### Slice 4: Date-filtered orders → lineitem (FactA→FactB)

**Scope**: Lift the "FactA⋈FactB skip" from §4 specifically for
`orders ⋈ lineitem` when orders has a date-range Filter pushed to
its scan. The cost-benefit specific to this case: orders is 15M @
SF=10, but a date filter cuts it to ~2.3M (Q05) or ~3M (Q07). A
synthetic semi from filtered_orders → lineitem prunes lineitem to
the matching subset.

This is structurally Σ.Q.L9's win path applied statically, and it's
the highest-EV slice for Q05 (the only remaining 1.4× SF=10 loss).

**Code touched**: Add a "fact-with-filter" branch to the detector;
mark Gate B as not strictly "fact side is unfiltered" but "fact
side base is fact-class AND (filter or post-filter rows >
FACT_THRESHOLD)."

**Tests**:
1. `orders_with_date_filter_propagates_to_lineitem` — Q05 minimal:
   `orders WHERE o_orderdate >= '1994-01-01' AND < '1995-01-01'`
   joined to lineitem. Assert LeftSemi above lineitem TableScan.
2. `lineitem_without_filter_does_not_propagate_to_orders` — reverse
   direction; assert no emission (lineitem is the larger fact, no
   reason to push lineitem keys into orders).

**Expected query moves**:
- Q05 SF=10 close to DuckDB (143 ms target from current 202 ms).
- Q07 SF=10 to DuckDB (136 ms target from current 151 ms — likely
  achieved by Slice 1 already).

**Pass criteria**:
- Q05 SF=10 ≤ 150 ms (1.05× DuckDB).
- 22q SF=10 geomean ≤ pre-slice baseline.
- All 22 tpch_validate values match.

**Estimated effort**: 4-6 hours.

### Slice 5 (optional): Default-on under a shape profile

**Scope**: If Slices 1-4 land cleanly with no regressions and
positive geomean shift, flip Σ.Q.M from env-opt-in to default-on
when the shape catalog detects a star-join shape. Otherwise leave
opt-in.

**Estimated effort**: 2-4 hours, gated on bench data.

---

## 6. Expected wins per query (SF=10)

Based on current per-query timings (§ Baseline of
`SIGMA_Q_SINGLE_NODE_PARITY.md`) and the structural analogue to
DuckDB's plan shape:

| Query | Current ematix ms | DuckDB ms | Current ratio | Σ.Q.M target | Target ratio | Slice |
|-------|------------------:|----------:|--------------:|-------------:|-------------:|------:|
| Q03   | 148               | 149       | 0.99×         | 135          | 0.91×        | 1     |
| Q05   | 202               | 143       | 1.42×         | 150          | 1.05×        | 2, 4  |
| Q07   | 151               | 136       | 1.11×         | 138          | 1.01×        | 1     |
| Q08   | 202               | 185       | 1.09×         | 185          | 1.00×        | 2     |
| Q09   | 272               | 323       | 0.84×         | 240          | 0.74×        | 3     |

Detailed reasoning:

### Q05 — biggest win expected (Slice 2 + 4)

Current dominant op: `HashJoin(orders ⋈ lineitem on l_orderkey) =
852 ms` (per session memory). Build = 2.28M date-filtered orders, probe
= 60M lineitem.

Post-Σ.Q.M Slice 4: synthetic LeftSemi(lineitem, filtered_orders,
l_orderkey = o_orderkey) pushed to lineitem scan. Lineitem decoded
rows drops from 60M to ~12M (the 2.28M orders × ~5 lineitems/order
chain ≈ 11.4M). HashJoin cost drops 5×. Expected wall-time saving:
500-600 ms.

Slice 2 additionally propagates the ASIA → customer → orders chain
via region (5 rows) → nation (5 rows, ASIA) → customer (300K post-
filter via c_nationkey). Orders is further narrowed by the customer
semi to ~600K. Cascade applies to lineitem via Slice 4. Combined
expectation: Q05 ≈ 145-160 ms (-25% to -28%).

### Q07 — clean Slice 1 win

Current: `s_suppkey ⋈ l_suppkey` and `o_orderkey ⋈ l_orderkey` are the
two big joins. Supplier itself unfiltered in Q07 (no filter on
supplier columns), but `nation ⋈ supplier on s_nationkey =
n_nationkey` where `nation` has the FRANCE/GERMANY filter creates the
selective dim subtree. Slice 1 fires on `supplier ⋈ lineitem on
s_suppkey` if supplier has a Filter, which it doesn't directly —
Slice 1 misses Q07.

**Correction**: Q07's win comes from Slice 2 (recursive detection
through nation→supplier). Slice 1 covers the simpler shape but Q07
needs depth-2 walk.

Slice 2 should fire on `(nation_filtered ⋈ supplier) ⋈ lineitem`.
Pushed: LeftSemi(lineitem, nation_filtered ⋈ supplier, l_suppkey =
s_suppkey). Lineitem prunes from 60M to ~10M (50% × supplier ⋈
nation_filter ≈ 4K suppliers out of 100K, then each supplier ~1K
lineitems = 4M rows post-prune). Expected saving: ~13 ms (151 ms →
138 ms).

### Q08 — modest Slice 2 win

Current: 7-way join with region='AMERICA'. Same shape as Q05 but
narrower filter. Slice 2 (cascade) fires on the chain
`region→nation→customer→orders`. Slice 4 propagates to lineitem.

Expected: 202 → 185 ms (-8%, lands at DuckDB parity 185 ms).

### Q03 — small Slice 1 win

Current: `c_custkey = o_custkey` with customer filter `c_mktsegment
= 'BUILDING'` (filters to ~30%). Lineitem joined via l_orderkey.
Slice 1 fires on customer-orders direct, then Slice 4 propagates to
lineitem.

Expected: 148 → 135 ms (-9%). Already at near-parity; this slice
gives a margin.

### Q09 — Slice 3 amplifies an existing win

Current: `part ⋈ lineitem` via `l_partkey = p_partkey` with
`p_name LIKE '%green%'` filter. Slice 1 should fire on direct
filtered-part to lineitem. Q09 may already be a 0.84× win because
DataFusion's existing pushdown does some of this work; Σ.Q.M would
amplify.

Expected: 272 → 240 ms (-12%).

### Net 22q SF=10 geomean expectation

Pre-Σ.Q.M baseline (post-L16): geomean = 0.80 (14 wins / 6 DuckDB
/ 2 Polars).

Post-Σ.Q.M target: geomean = 0.74-0.76 (16-17 wins / 4-5 DuckDB
/ 1-2 Polars). Q05, Q08 should flip from DuckDB-wins to ematix-wins.

---

## 7. Risks and mitigation

### R1 — Codegen perturbation (the "any new rule costs ~7%" tax)

**Risk**: per `optimizer-codegen-sensitivity` and the Σ.Q.L1b retry
data point, adding any new optimizer rule has historically cost +5-8%
SF=1 geomean even when the rule never fires on the affected queries.

**Mitigation**:

- **Phase 1 ships behind `EMAT_SYNTHETIC_LEFT_SEMI=1` (default off).**
  This is the standard Σ.Q rule install pattern (L9, L10, etc.) and
  is what made those rules ship-clean. Production paths don't pay
  the tax until they explicitly opt in.

- **Bench at 11+ trials, both standalone and 22q sweep.** Don't claim
  a win until both standalone target query AND in-sweep results
  agree. The Σ.Q.L1b retry caught a 12-point swing standalone-vs-
  sweep; we'd miss the tax at 3-5 trials.

- **Bench at SF=10, not SF=1.** SF=10 amortises optimizer overhead;
  the codegen tax shrinks as a percentage of total time. Per the
  codegen-sensitivity memo, "Move the bench to SF=10+ — the
  regression magnitude shrinks." Σ.Q.L10 confirmed this: SF=10
  geomean -6.4%, SF=1 geomean neutral.

- **If the tax hits at SF=10 too**: consider merging Σ.Q.M into the
  Σ.Q.L10 rule body instead of registering as a separate rule. One
  rule, two transformations, single codegen footprint. This is what
  the codegen-sensitivity memo recommends ("extend the existing
  operator, don't add a new rule"). Cost: more complex rule logic;
  benefit: amortised codegen cost.

### R2 — Plan explosion (rule fires too often)

**Risk**: on a 6-way join like Q05, multiple Inner joins match Gate
A simultaneously and we emit multiple synthetic LeftSemis. If each
fires, the resulting plan can have N synthetic semis stacking, each
of which Σ.Q.L10 pushes; the net effect may be redundant work even
though semantically equivalent.

**Mitigation**:

- **Idempotency check on emission.** Before emitting, walk parents to
  see if an equivalent LeftSemi is already in place (same on-keys,
  same target table). If so, skip.

- **Single emission per (fact_table, dim_subtree) pair per query.**
  Track emitted pairs in the rule's `rewrite` context. The optimizer
  fixed-point loop may run Σ.Q.M multiple times; we want stable
  output.

- **Bench-driven sanity.** If a synthetic test like Q05 produces an
  explain-plan with 5+ LeftSemi nodes pushed to lineitem, it's a
  warning sign. Cap synthetic emissions per query at 3 in Phase 1.

### R3 — Regression on simple non-star queries

**Risk**: A query like Q06 (single-table scan, no joins) shouldn't
trigger Σ.Q.M at all, but the rule walks the tree on every plan. If
the walker is buggy and emits on Q06, the plan goes from "scan +
filter" to "scan + filter + redundant LeftSemi" — pure cost.

**Mitigation**:

- **Unit-test no-emission cases explicitly** (test 3 in Slice 1).
  Cover Q06 shape (single table), Q22 shape (CTE), Q01 shape
  (no joins).

- **Visibility via env trace** (`EMAT_SYNTHETIC_LEFT_SEMI_TRACE=1`)
  prints to stderr each emission's target Inner Join and chosen
  fact/dim sides. Easy to spot when the rule misfires on a query
  that shouldn't trigger.

- **Per-query bench guard.** The triangulation bench captures all 22
  query timings; any query >10% slower than baseline is a hard
  block on landing.

### R4 — Interaction with existing rules (L9, L10, Σ.P)

**Risk**: L9 (runtime sideband) might attempt to attach a sideband to
the synthetic LeftSemi's HashJoinExec. If that fails (e.g., col_idx
mismatch from the new pushed scan), L9 silently no-ops and we lose
the lever Σ.Q.M was meant to enable.

**Mitigation**:

- **Test L9 + Σ.Q.M combo explicitly.** Run Q07 with both
  `EMAT_RT_BLOOM_SIDEBAND=1 EMAT_SYNTHETIC_LEFT_SEMI=1` and verify
  per-partition lineitem scan output rows in `EMAT_L9_TRACE`.
  Expectation: both rules apply additively — Σ.Q.M prunes lineitem
  to ~10M, L9 prunes further to ~2M.

- **If they conflict, prefer Σ.Q.M (static) over L9 (runtime).** A
  static plan rewrite is cheaper at runtime than the sideband. We
  can detect "this HashJoinExec's probe is already wrapped by a
  pushed LeftSemi" in L9's rule and skip attaching the sideband.

- **Σ.P CSE interaction.** If Σ.Q.M emits two synthetic semis sharing
  a dim subtree, Σ.P should fold them into a `SharedSubtreeExec`.
  Test the cseunion case.

### R5 — Multi-instance dim subtrees double-evaluated

**Risk**: Phase 2 emits synthetic semis from multi-join dim subtrees.
The original Inner Join still references the same subtree. Without
Σ.P's CSE catching it, we run the dim subtree twice.

**Mitigation**: Already covered in §3 edge cases (use SubqueryAlias
or rely on Σ.P CSE). Verify in Slice 2's test 4.

### R6 — Q19 / Q21 / Q18 don't benefit but might regress

**Risk**: Σ.Q.M's primary targets are star joins (Q03/Q05/Q07/Q08/Q09).
Q18 and Q21 already have subquery-decorrelated LeftSemis (Q18) or
anti-joins (Q21) and rely on Σ.Q.L10 directly. The new rule
shouldn't fire on them but if it does (e.g., misclassifies a
Q21 join as a dim chain), it could cause regression.

**Mitigation**: Per-query bench guard from R3.

---

## 8. Success criteria

### Per-slice success criteria

Each slice must hit (no negotiation):

1. **All 22 tpch_validate cells match DuckDB.** Both SF=1 and SF=10.
2. **No query regresses >10% at either SF=1 or SF=10.**
3. **The slice's target query moves by ≥5% in the predicted
   direction.** If it doesn't, the slice isn't pulling its weight
   and we re-investigate before continuing.

### Cumulative program success criteria

**Hard targets** for declaring Σ.Q.M shipped:

- **22q SF=10 geomean(ematix/DuckDB) ≤ 0.76** (was 0.80 pre-Σ.Q.M).
- **22q SF=1 geomean(ematix/DuckDB) ≤ 0.58** (currently 0.559;
  drift of +0.02 acceptable — the codegen-tax tolerance).
- **Q05 SF=10 ≤ 1.10× DuckDB.** This is the one remaining big star-
  join gap; closing to <1.10× is the headline win.
- **Q07 SF=10 ≤ 1.05× DuckDB.** Tighter target because the SF=10
  Q07 win was the proof of life for the L9 sideband too; we want
  both mechanisms cumulative.
- **Q03 / Q08 SF=10 at parity or better.**

**Soft targets** (nice-to-have but not blocking):

- Q09 SF=10 ≤ 0.78× DuckDB (already 0.84×; lever should push it).
- SF=10 outright ematix wins ≥ 16/22 (currently 14/22).

### Stop conditions (ship-vs-keep-iterating)

**Ship** (after Slice 4 lands) when:
- All hard targets above met.
- 22q geomean has not regressed at SF=1 (within ±2%).
- `EMAT_SYNTHETIC_LEFT_SEMI=1` clean for ≥1 week of perf tests across
  SF=1 and SF=10 (including incidental runs in other Σ.Q work).

**Keep iterating** if:
- Q05 stays above 1.20× DuckDB after Slice 4. Probably means the gap
  isn't pure join-order propagation; consider Q05-specific physical
  plan investigation.
- Codegen tax exceeds 5% at SF=10 (no precedent for this, but if it
  happens consider rolling Σ.Q.M into Σ.Q.L10's body).

**Abandon** if:
- After Slice 1, the predicted Q07 / Q03 wins don't materialise even
  in standalone (not sweep) benches. This would suggest the static
  semi-pushdown isn't replacing enough work — possibly because
  DataFusion's PushDownFilter or join reordering is already doing
  it. Don't push to later slices.
- After Slice 2 and 4, Q05 stays above 1.30× DuckDB. The gap then
  isn't structural-plan but executor-physical and Σ.Q.M doesn't
  carry. Roll back and investigate via Q05 EXPLAIN ANALYZE on
  ematix vs DuckDB.

---

## Open assumptions

These are calls made without data; flag if any look wrong:

1. **Σ.Q.L10 will correctly push the synthetic LeftSemi without
   modification.** L10's existing tests use IN/EXISTS-derived
   LeftSemis but the structural shape (LeftSemi → InnerJoin chain →
   target TableScan) is identical. We may discover edge cases when
   the synthetic semi's right subtree is multi-table; if so, Slice 2
   may need to extend L10's `push_through_subtree`.

2. **DataFusion's optimizer fixed-point loop converges in 1-2
   iterations on Σ.Q.M + L10 composition.** Worst case is N
   iterations for N synthetic semis stacking; the idempotency check
   should cap this. If runtime is observably slow on a 22q sweep,
   investigate.

3. **DataFusion's `Inner Join` `null_equality` is reliably the
   default (`NullEqualsNothing`) on TPC-H plans.** If a query
   somehow gets a `NullEqualsNull` Inner Join (e.g., from a CTE or
   subquery rewrite), Σ.Q.M will still emit a matching semi. We
   trust the DataFusion default and don't extra-validate.

4. **The codegen tax from adding Σ.Q.M is bounded by the same ±5%
   range observed in Σ.Q.L9/L10.** Phase 1 will measure; if much
   worse (say +12% geomean) we re-strategise per R1.

5. **No regression in Σ.P CSE coverage.** Σ.P's
   `DedupeAggregateForFloatDeterminism` handles aggregates; Σ.P CSE
   handles subtree dedup. Neither should be affected by our
   structural rewrite, but we verify in Slice 2.

---

## Mechanical milestones (what "done" looks like at each slice)

| Slice | LOC | Test count | Bench check | Code lands at |
|-------|-----|-----------|-------------|----------------|
| 1     | ~300 | 6 unit + 1 integration | 22q SF=10 + tpch_validate | new commit on `perf/sigma-q-single-node-parity` |
| 2     | +200 | +4 unit | 22q SF=10 + tpch_validate | follow-up commit |
| 3     | +100 | +3 unit | 22q SF=10 + tpch_validate | follow-up |
| 4     | +150 | +2 unit | 22q SF=10 + tpch_validate | follow-up |
| 5     | +50 | 0 (config only) | 22q SF=1 + SF=10 + tpch_validate | follow-up if shape catalog ready |

Total estimated effort: 25-35 hours across slices, plus 5-10 hours of
bench/analysis between slices. Calendar-time: 1.5-2 weeks at typical
session cadence.

---

## Decision log

(none yet — this is a fresh planning doc)
