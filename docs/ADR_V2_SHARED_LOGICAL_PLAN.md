# ADR: v2 shared logical-plan layer — one plan for SQL and DataFrame (S0.1)

**Status:** Accepted (design). Sprint S0.1 of
[`V2_SPRINT_PLAN.md`](V2_SPRINT_PLAN.md); the linchpin foundation
([`V2_TARGET.md`](V2_TARGET.md) §3.1, §S0). Depends on the API shape in
[`ADR_V2_DATAFRAME_API.md`](ADR_V2_DATAFRAME_API.md).

## Context

The DataFrame API (`ematix.frame`) must lower to the **same** DataFusion
`LogicalPlan` as SQL, so the ematix CBO, fused-aggregate kernels,
narrow-key decode, join-reorder walkers, and the distributed mesh all
apply to DataFrame queries for free. If the two surfaces forked the
engine, the "one query engine" promise (`V2_TARGET.md`) would break and
every optimization would have to be built twice.

**What the architecture map (S0.1 investigation) established:**

- There is **no bespoke parser**. SQL is planned by stock DataFusion via
  `SessionContext::sql(&str)` → a DataFusion `DataFrame` wrapping a
  `LogicalPlan`.
- Every production query converges on one choke point:
  `SessionState::create_physical_plan()` → the ematix
  **`FlowQueryPlanner`** (`crates/ematix-flow-core/src/flow_query_planner.rs:120`),
  which runs the post-optimization walker pipeline (`rewrite` at
  `flow_query_planner.rs:62`), re-optimizes, and does physical planning.
- All ematix behavior is bound to a single `SessionState` assembled by
  **`preset::with_optimizer_rules_overridden`**
  (`crates/ematix-flow-core/src/preset.rs:308`) — "THE single
  chain-assembly function." Logical + physical rules + the
  `FlowQueryPlanner` query planner all register there.
- `LogicalPlanBuilder` is **already** used internally by the rewrite
  rules (e.g. `agg_filter_pushdown.rs`, `push_fusion_rule.rs`), proving
  the team already composes plans programmatically — just not from a
  user surface.
- The Python binding (`crates/ematix-flow-py/src/lib.rs`) is SQL/TOML
  only today; it never builds a `LogicalPlan`.
- **One outlier:** the streaming transform path
  (`crates/ematix-flow-core/src/transform.rs:328`) builds a plain
  `SessionContext::new()` and does **not** install the ematix preset.
  Every other path (CLI shard, distributed, bench) goes through the
  preset. This bypass must be reconciled or explicitly scoped out.

## Decision

**The DataFrame API builds a DataFusion `LogicalPlan` and wraps it as
`datafusion::dataframe::DataFrame::new(session_state.clone(), plan)`
using the identical `SessionState` that SQL uses.** Because that state
carries the whole ematix stack, a plan entering via DataFrame gets
byte-identical treatment to one from `ctx.sql()` — they merge at
`SessionState::create_physical_plan` → `FlowQueryPlanner`.

Concretely, S0.1 delivers:

1. **A canonical shared context constructor.** Introduce
   `preset::session_context()` (and/or `preset::session_state()`) as the
   ONE entry both SQL and DataFrame lowering call, wrapping
   `preset::with_optimizer_rules(SessionStateBuilder::new()…)` +
   `SessionContext::new_with_state`. Today callers re-assemble the
   builder inline (`run_shard.rs:82`, `distributed/src/lib.rs:282`) —
   collapse them onto the shared constructor so the DataFrame surface
   can't drift from the SQL surface. The `production_chain_matches_
   pinned_names` tripwire (`preset.rs:720`) guards against drift.

2. **A stub `ematix.frame` lowering** (Rust side in
   `ematix-flow-py` / a new `ematix-flow-frame` module) that builds a
   `LogicalPlan` via `LogicalPlanBuilder` / `ctx.read_*` and hands it to
   the shared state. Scope for S0: enough surface to *prove plan
   identity*, not the full API (that's S4).

3. **The S0.1 exit demo / test:** a trivial frame op and its SQL
   equivalent produce **identical optimized `LogicalPlan`s** (compare
   `DataFrame::into_optimized_plan()` output). This is the gate that
   unblocks Track B.

4. **Reconcile the streaming bypass.** Decide + document whether
   `transform.rs:328` adopts the shared preset or is explicitly scoped
   out of the shared-plan guarantee for v2. (Streaming per-batch SQL has
   different constraints; a scoped-out decision is acceptable if
   recorded.)

`FlowQueryPlanner::rewrite` (`flow_query_planner.rs:62`) already operates
on a bare optimized `LogicalPlan` and assumes nothing about SQL
provenance, so no changes are needed there — a strong signal the choke
point is genuinely surface-agnostic.

## Consequences

- **Track B unblocked once the demo is green** — the whole DataFrame API
  (S4/S5) then rides the existing optimizer/physical/mesh stack with no
  engine fork.
- **Low engine risk.** S0.1 is mostly a *constructor-consolidation +
  wrapping* exercise over machinery that already exists; the map found
  no architectural blocker. The risk is drift, which the pinned-chain
  tripwire already guards.
- **The streaming outlier is now a tracked decision**, not a latent
  inconsistency.
- **Distributed comes for free:** the mesh gate is just another physical
  rule on the shared state (`distributed/src/lib.rs:338`), so a
  DataFrame query with `engine="distributed"` meshes exactly like SQL —
  no extra S0.1 work, validated later in S8.

## Key files

- `crates/ematix-flow-core/src/preset.rs:308` — shared state assembly
  (the constructor to expose).
- `crates/ematix-flow-core/src/flow_query_planner.rs:62,120` — the
  surface-agnostic post-optimization choke point.
- `crates/ematix-flow-core/src/transform.rs:328` — the preset-bypass to
  reconcile.
- `crates/ematix-flow-py/src/lib.rs` — current SQL/TOML-only Python
  surface; where the frame lowering attaches.
