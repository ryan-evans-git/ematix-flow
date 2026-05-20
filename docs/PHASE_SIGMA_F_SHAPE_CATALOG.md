# Σ.F — Unified shape catalog: declarative rule matcher

**Status:** scoped 2026-05-20, not started. Orthogonal to Σ.E
(scan-layer dict-awareness); this phase is an optimiser-layer
architectural refactor.

**One-line goal:** replace the N per-rule walkers
(`InjectFilterMultiAggRule`, `InjectFilterSumRule`,
`EnableDictGroupCountRule`, `EnableDictFilterRule`, future Σ.E6
additions) with a single declarative shape catalog that drives all
plan-tree rewrites.

**Why now:** the Σ.E6 work plus the Appendix B shape-extension
follow-ups (B1–B5) plus the eventual Σ.E7 join+filter+agg rule each
require writing a new walker. Each walker is ~150 LOC of
PhysicalExpr extraction, CSE resolution, scan-column type checking,
ScalarValue literal extraction, etc. A unified catalog can be ~30
LOC per shape after the DSL is built once.

## Why this isn't another generic auto-router

We retired `EnableFusedJitRule` in #534 because it tried to fire on
anything and was hard to reason about. The shape catalog is
*opposite*: every entry is a precise shape with explicit priority,
and the rewriter still routes to monomorphic template-specialised
executors. The DSL is for the **matcher**, not the executor.

## Today's pain point

Four optimiser rules in the workspace today, each with its own
walker:

| Rule | Matcher LOC | Walks |
|---|---:|---|
| `InjectFilterMultiAggRule` | ~180 | Aggregate → Projection(CSE) → Filter → Scan |
| `InjectFilterSumRule` | ~150 | Aggregate(sum) → CoalescePartitions → Aggregate(Partial) → Filter → Scan |
| `EnableDictGroupCountRule` | ~120 | Aggregate(count) → Aggregate(Partial) → Scan with dict col |
| `EnableDictFilterRule` | ~140 | Filter on dict col |

Each walker re-implements the same primitives:

- Extract scalar literal from `BinaryExpr` (with column-on-either-side flipping)
- Flatten AND chain
- Resolve CSE through `__common_expr_N` aliases
- Cross-check scan column names + types
- Detect `RepartitionExec` placement between `Partial` and `Final` aggregate
- Walk past `CoalescePartitions` / `Coalesce` wrappers
- Recurse through `ProjectionExec` passthroughs

The Σ.D3 phase D refactor (memory: `project_sigma_d_phase_d_checkpoint`)
landed a `AggregateShapeConfig` walker that cut per-rule code from
~150 → ~30 lines. Σ.F is the next step on that arc: move from
"shared walker primitives" to "declarative pattern → matched
sub-plan".

## Proposed architecture

A static catalog of `ShapeEntry`:

```rust
pub struct ShapeEntry {
    pub name: &'static str,
    pub pattern: Shape,
    pub rewrite: fn(MatchedSubtree, &ExecutionPlanRef) -> Result<ExecutionPlanRef>,
    // Order in the catalog = match priority. Most specific first.
}

pub static SHAPE_CATALOG: &[ShapeEntry] = &[
    ShapeEntry {
        name: "filter_join_multi_agg",          // Σ.E7 — covers Q3/Q5/Q10
        pattern: shape!(
            Aggregate(Final, group_by, aggs) >
            Projection(cse?) >
            HashJoin(probe_dict?) >
            Filter(and_chain) >
            Scan
        ),
        rewrite: FilterJoinMultiAggSpec::from_matched,
    },
    ShapeEntry {
        name: "filter_multi_agg",               // Σ.G.2f.3 — covers Q1/Q4/Q22
        pattern: shape!(
            Aggregate(Final, small_card_group, aggs) >
            Projection(cse?) >
            Filter(and_chain) >
            Scan
        ),
        rewrite: FilterMultiAggSpec::from_matched,
    },
    ShapeEntry {
        name: "filter_sum",                     // Σ.G.2f — covers Q6
        pattern: shape!(
            Aggregate(Final, sum_only, no_group) >
            CoalescePartitions >
            Aggregate(Partial) >
            Filter(and_chain) >
            Scan
        ),
        rewrite: FusedFilterSumExec::from_matched,
    },
    ShapeEntry {
        name: "dict_group_count",               // Σ.E3b — covers Q22-shape
        pattern: shape!(
            Aggregate(Final, count_only, single_dict_key) >
            Aggregate(Partial) >
            Scan
        ),
        rewrite: DictGroupCountExec::from_matched,
    },
    ShapeEntry {
        name: "dict_filter",                    // Σ.E3a — covers any dict-col filter
        pattern: shape!(
            Filter(dict_predicate) > Scan
        ),
        rewrite: DictFilterExec::from_matched,
    },
];
```

The optimiser rule becomes a one-shot dispatcher:

```rust
fn match_and_rewrite(plan: &ExecutionPlanRef) -> Option<ExecutionPlanRef> {
    for entry in SHAPE_CATALOG {
        if let Some(matched) = entry.pattern.try_match(plan) {
            return Some((entry.rewrite)(matched, plan));
        }
    }
    None
}
```

## Pattern DSL design

The `shape!` macro produces a `Shape` AST. Each node carries:

- An operator class (`Aggregate`, `Filter`, `Projection`, `HashJoin`,
  `CoalescePartitions`, `RepartitionExec`, `Scan`, etc.)
- Operator-specific attribute matchers (`Final`/`Partial`,
  predicate shape, aggregate function set, group-by arity / type)
- A list of child shapes (recursive)
- An optional capture binding for the rewriter to read

Two essential capabilities:

1. **Optional wrappers** — DataFusion sometimes wraps a subtree in
   `CoalescePartitions`, `RepartitionExec`, or `ProjectionExec`
   without changing semantics. The DSL needs `wrappers?(...)` or
   per-node `optional` flags.
2. **Equivalence under reorder** — the order of AND-clauses in a
   `Filter` predicate doesn't matter for matching. Same for the
   order of aggregates in an `AggregateExec`. The DSL needs
   set-equality for these.

## Catalog ordering + ambiguity

Two shapes can match the same subtree. Catalog order is the
tiebreaker: the FIRST entry that matches wins. This is the same
rule DataFusion's own rules use internally; we just make it
declarative.

For correctness, the catalog is **manually ordered most-specific
first**. A `filter_join_multi_agg` entry must precede
`filter_multi_agg` because the former is a superset of the latter's
shape.

Drift risk: as the catalog grows, the ordering becomes load-bearing.
Mitigation: each entry's `pattern` includes a `specificity_score`
auto-derived from depth + attribute count; CI test asserts the
catalog is ordered by descending specificity.

## Comparison to alternatives

### (A) Per-rule, per-shape Inject* (today)

**Pros:** familiar; each rule is self-contained; bit-equality testing
straightforward per rule.

**Cons:** N walkers, all re-implementing the same primitives;
adding a shape = writing 150 LOC of walker + a rule registration;
ordering is implicit (rule registration order).

### (B) Declarative shape catalog (this phase)

**Pros:** add a shape = ~30 LOC catalog entry; ordering explicit;
DSL is the single source of truth for what shapes ematix-flow
optimises; cross-shape composability (a filter_join_multi_agg
entry can reference the same Filter pattern as filter_multi_agg).

**Cons:** the DSL itself is real work (~1-2 weeks); patterns
must compile to efficient matchers (no per-match tree walking
interpreter); plan-tree evolution requires DSL updates.

### (C) JIT code-gen per plan signature (Photon-style)

**Status:** previously rejected (#534 retired `EnableFusedJitRule`).
But — see "Why JIT might be worth a clean revisit" below. The
prior rejection was made under noisy conditions.

**Pros (if it works):** the optimiser doesn't pick a template; it
GENERATES the optimal executor at query-plan time. Covers every
plan shape, not a finite catalog. Photon (Behm et al. VLDB '22)
gets near-DB-internal perf this way.

**Cons:** the maintenance surface is enormous. Code generation
needs a stable IR (we picked Cranelift, then deleted it). Debug
story for runtime-generated code is hard.

### (D) DuckDB-style vectorise-per-operator (no fusion)

**Pros:** simplest mental model; no specialised executors at all.

**Cons:** gives up the per-shape fusion wins we already have on
Q01 / Q06 / Q14. Net regression on the benches we tune for.

## Why JIT might be worth a clean revisit

The previous JIT rejection (Σ.G.2f.2, memory:
`project_sigma_g2f2_template_specialization`) was based on a bench
comparison where:

- `InjectFusedQ1Rule` existed (TPC-H-hardcoded; retired in #533)
- The JIT-based `EnableFusedJitRule` was a secondary matcher
- The bench couldn't isolate "JIT vs templates" because the
  hardcoded `Q1Spec` template was getting force-fired first
- The full `#533/#534` retire-hardcoded-rules cleanup happened
  AFTER that bench

In other words, the JIT path was being measured while a different
template was sitting on the only query that mattered. The JIT
numbers looked bad partly because it was doing work to navigate
around interfering rules — not necessarily because JIT itself was
slower than templates.

What changed post-#533/#534:

- All Q-specific rules are gone. Only generic rules remain.
- The codebase is small enough now that a JIT-vs-template head-to-head
  on Q01 wouldn't be confused by leftover hardcoded paths.
- ematix-parquet's NEON+AVX2 SIMD kernels exist; JIT could
  compose them, giving a path templates don't have.
- Cranelift is mature enough that the IR-design tax is lower
  than it was when we tried in 2024.

**Concrete proposal:** as part of the Σ.F spike (see below), include
a JIT track that re-benches the templates-vs-JIT comparison on a
clean main branch. If JIT meaningfully wins on at least one of
Q01/Q06/Q19, it's worth pursuing as Σ.F's executor strategy.

This isn't a commitment to JIT. It's an honest reopening of a
decision that was made with messy data.

## Spike plan

A 1-week spike with three tracks, run in parallel by a single
engineer or split across two:

### Track 1 — Shape DSL + 3-rule migration

1. Design `shape!` macro syntax. Start minimal: just enough
   expressiveness for the 3 existing rules.
2. Implement `Shape::try_match(plan) -> Option<MatchedSubtree>`.
   Use a depth-first match against the operator tree.
3. Migrate `InjectFilterMultiAggRule`, `InjectFilterSumRule`,
   `EnableDictGroupCountRule` onto catalog entries.
4. Run the 22-query SF=1 bench. Must be bit-identical perf to
   today's per-rule path (same executors, just new matcher).

### Track 2 — Add a 4th shape via the catalog only

Pick the cheapest of B1–B5 from Σ.E6 Appendix B —
**B1 (nested ANDs + Filter chain)**. Express it as a catalog
entry only; do NOT write a new walker.

**Acceptance gate:** the 4th shape costs ≤ 50 LOC in the catalog
(vs ~150 LOC for a new walker), and it unlocks at least one
TPC-H query that was previously falling through (likely Q19's
OR-of-AND once we extend to OR — but B1 alone may not flip
anything; a more aggressive ≤100 LOC budget for B2 OR-of-AND
might be the better gate).

### Track 3 — JIT-vs-templates clean revisit

1. Restore a minimal `EnableFusedJitRule` from #534's revert
   on a side branch.
2. Bench against the current templates on Q01 + Q06 + Q19, on a
   main with NO hardcoded rules.
3. **Acceptance gate:** if JIT lands within ±5% of templates on
   all three queries OR wins on one of them, JIT becomes Σ.F's
   executor strategy candidate. If JIT loses on all three by
   > 5%, we close this door definitively.

## Acceptance criteria for the phase

To ship Σ.F as a refactor:

1. **No-regression** — the 22-query SF=1 bench is bit-identical
   pre vs post per-query (same executors fire, just dispatched
   through the catalog).
2. **Add-a-shape velocity** — adding the 4th shape costs ≤ 50 LOC
   in the catalog. Adding shape #5 onwards costs ≤ 30 LOC each
   (after the DSL is mature).
3. **Catalog is discoverable** — `cargo doc -p ematix-flow-core
   --document-private-items` produces a readable catalog
   reference; an LLM or new engineer can look at the catalog and
   know what optimisations the project does.
4. **Tooling unlock** — a `flow explain` mode shows which catalog
   entry matched (if any) for a given SQL query. Concrete
   actionable signal when no shape matches.

If Track 3 (JIT) hits its gate, an additional criterion:

5. **JIT track demonstrates ≤ 5% perf loss or any win** on the
   clean head-to-head. Otherwise JIT stays closed.

## Risks

- **DSL design rabbit hole.** Pattern languages tend to grow
  features (negation, fallback, multi-match) until they become
  Turing-complete. Box the DSL aggressively: 3 existing shapes
  define the v1 surface; v2 expands only when a real shape needs
  something the v1 can't express.
- **Pattern proliferation.** Photon's pattern space exploded;
  that's why they did code-gen. We may hit the same wall. The
  spike's 4th-shape gate is the early-warning test.
- **Plan-tree drift.** DataFusion changes physical plans across
  versions. Catalog patterns can break silently — a previously
  matching shape stops matching after a DF upgrade. Mitigation:
  per-shape integration tests against real SQL.
- **JIT track resurrecting dead substrate.** If Track 3 fails,
  the side-branch code must be deleted clean — no half-revival
  shapes lingering. Same hygiene as the `#534` cleanup.

## Effort + sequencing

| Slice | Estimate | Gating |
|---|---|---|
| Track 1 — DSL + 3-rule migration | ~5 days | — |
| Track 2 — Catalog adds 4th shape | ~1 day | Track 1 |
| Track 3 — JIT revisit bench | ~3 days | — (parallel) |
| Spike write-up + go/no-go | ~1 day | All three |
| Total spike | ~1.5 wk | |
| **If go:** migrate remaining rules + ship | ~1-2 wk | Spike passes |
| **If JIT wins on Track 3:** scope Σ.F.JIT | TBD | — |

## What this is not

- Not a new executor type. The catalog dispatches to the same
  template-specialised executors we already have.
- Not a query-plan optimiser. The catalog rewrites physical plans
  one-pass; it doesn't reorder joins or pick aggregate strategies.
  Those stay with DataFusion's planner.
- Not a commitment to JIT. Track 3 reopens a previously-rejected
  question; the spike's gate decides whether to invest further.
- Not a replacement for Σ.E6. The shape catalog and dict-aware
  scan are independent levers. Σ.E6 makes the executors faster;
  Σ.F makes the matcher smaller and the catalog discoverable.

## Predecessors

- `AggregateShapeConfig` walker (memory:
  `project_sigma_d_phase_d_checkpoint`) — the shared-walker
  refactor that made this phase thinkable.
- `#533` / `#534` — retired hardcoded TPC-H rules + JIT substrate.
  This phase reopens the JIT question with the cleanup done.
- Σ.E6 — orthogonal; lands first because it directly addresses a
  measurable regression. Σ.F is architecture; Σ.E6 is perf.
