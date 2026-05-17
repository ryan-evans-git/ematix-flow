# Σ.G.2e — Predicate extraction for the generic `FilterSum` shape

**Status:** scoped, not started.
**Prereqs:** Σ.G.3 (#476) — without a JIT path in `AggregateSpec`, a runtime-configured spec is materially slower than the fixed-shape `Q6Spec`/`Q1Spec` and net-regresses real SQL.
**Unblocks:** any single-table SUM-over-Filter SQL that isn't byte-equivalent to TPC-H Q6/Q1.

## Today: what is and isn't extracted

The existing `InjectFusedQ6Rule` / `InjectFusedQ1Rule` (in `fused_jit_rule.rs`) already extract literal *values* from the `PhysicalExpr` AST. What's hardcoded today is the **shape** — which columns, how many clauses, which operators — not the literal constants.

| Capability | Q6 rule | Q1 rule |
|---|---|---|
| Literal value extraction (`>= V`, `< V`, etc.) | ✓ | ✓ |
| `flip_op` mirror canonicalisation (`5 < col` → `col > 5`) | ✓ | ✓ |
| AND-chain flattening (handles nested `(a AND b) AND c`) | ✓ | n/a (Q1 is 1 clause) |
| `BETWEEN a AND b` (DataFusion lowers to `>= AND <=`) | ✓ (via AND-chain) | n/a |
| Different *columns* than the canonical TPC-H set | ✗ | ✗ |
| Subset of expected clauses (e.g. Q6 without `l_quantity`) | ✗ | n/a |
| Extra residual clauses on un-fused columns | ✗ | ✗ |
| Variable aggregate (e.g. `SUM(qty)` instead of `SUM(price * (1-discount))`) | ✗ | n/a |

So the meaningful extension isn't "extract literals" — it's: lift the shape itself from the plan instead of pattern-matching against a fixed shape.

## Why this is gated on Σ.G.3

A runtime-configured `FilterSumSpec` looks like:

```rust
pub struct FilterSumSpec {
    pub clauses: Vec<FilterClause>,
    pub agg_col_idx: usize,
    pub agg_expr: AggExpr,           // SUM(col), SUM(col * (1-other)), etc.
    pub output_schema: SchemaRef,
}
pub struct FilterClause {
    pub col_idx: usize,
    pub op: Operator,
    pub literal: ScalarValue,
}
impl AggregateSpec for FilterSumSpec { ... }
```

The hand path would have to iterate `Vec<FilterClause>` per row — death by clause-count for a 5-clause Q6:

```rust
for i in 0..batch.num_rows() {
    let mut keep = true;
    for c in &self.clauses {     // <-- can't unroll; clause set is data-driven
        if !c.eval(batch, i) { keep = false; break; }
    }
    if keep { acc += ...; }
}
```

`Q6Spec` is fast because it has a fixed-arity inner loop body (one branch per clause, hoisted out, auto-vectorised). `FilterSumSpec`'s vec loop wouldn't auto-vectorise — bench would regress 3-10× for Q6 SQL.

The right answer is to let the **JIT path** do the heavy lifting: a `FilterSumSpec` with a JIT kernel built once at planning has the same fast hot loop the hand operators get today. The Cranelift IR emitter (`FusedFilterAggJit::try_build`) already supports arbitrary-clause specs (it's how Q6Spec and Q1Spec lower to JIT IR today). What's missing is the trait-side plumbing — that's exactly Σ.G.3.

So the slice ordering is:
1. Σ.G.3 — JIT in `AggregateSpec`
2. Σ.G.2e (this doc) — generic `FilterSumSpec` + recognition rule, JIT-only fast path

Doing Σ.G.2e first would either ship a slow generic spec or duplicate Q6/Q1 specialisation indefinitely.

## Slice plan (post-Σ.G.3)

### Σ.G.2e-1 — `FilterSumSpec` (JIT-only)

1. New module `fused_aggregate_filter_sum.rs`:
   - `FilterSumSpec` with the fields above
   - `Accumulator = f64` (single-bucket SUM; multi-bucket / multi-agg comes later)
   - `process_batch` REQUIRES the JIT (no fallback) — fail loudly at construction if JIT build failed
   - `finalize` produces a one-column batch matching `output_schema`
2. Construction helper `FilterSumSpec::try_new(clauses, agg_col_idx, agg_expr, child_schema) -> DfResult<Self>` builds the JIT eagerly.
3. Tests: 3 unit tests against a known fixture (5-clause, 3-clause, 1-clause shapes).
4. **No bench gate** at this slice — the spec is unreachable until the rule lands.

### Σ.G.2e-2 — `InjectFilterSumRule`

1. Walk the physical plan tree, recognise:
   ```text
   AggregateExec(Final[Partitioned], one SUM aggregate, no group-by)
     CoalescePartitionsExec
       AggregateExec(Partial, same SUM, no group-by)
         FilterExec(AND-chain of Column ⊕ Literal)
           [optional Projection]
             scan
   ```
2. Generic extraction: shared `decompose_filter_chain(expr) -> Vec<(col_idx, Operator, ScalarValue)>` reusing the existing helpers from `fused_jit_rule.rs` (lifted into a shared module).
3. Build a `FilterSumSpec`, wrap in `FusedAggregateExec<FilterSumSpec>`, return.
4. **Bench gate:** `examples/sigma_g2e_filter_sum_vs_q6.rs` — same methodology as Σ.G.2c (41 trials × 3 rounds MIN-of-K interleaved). Q6 SQL through `FusedAggregateExec<FilterSumSpec(JIT)>` must come within **3 %** of `FusedAggregateExec<Q6Spec(JIT)>`. The JIT path is the same in both, so this gate validates the planner-side overhead is negligible.

### Σ.G.2e-3 — retire `InjectFusedQ6Rule`

Once Σ.G.2e-2 lands and the gate passes for ≥1 week:
1. Delete `InjectFusedQ6Rule` and `try_match_q6_plan` / `extract_q6_predicate`.
2. The generic rule handles Q6 SQL natively.
3. Migrate `examples/tpch_q6_inject_bench.rs` to use the generic rule's name.
4. Update `docs/FUSED_AGGREGATE_SHAPES.md`.

`InjectFusedQ1Rule` follows the same pattern but lifts into a separate `FilterMultiAggSpec` because Q1 is a multi-aggregate + group-by shape — that's Σ.G.2f.

## What this phase deliberately doesn't do

1. **Post-join shapes** (Q3 / Q5 / Q12 / Q14). Those go through `FusedPostJoinExec` and live behind the Σ.G.4 trait surface (two-input aggregates).
2. **Computed-column aggregates beyond the Q6/Q1 family.** `SUM(col1 * (1 - col2) * (1 + col3))` patterns work for Q1/Q6 specifically because the JIT IR is hand-built for them. A general `AggExpr` IR emitter is its own multi-day chunk; for Σ.G.2e the spec carries a closed `enum AggExpr { Sum(Col), SumDiscPrice(Col, Col), … }` matching what the JIT already knows.
3. **Residual-clause splitting.** SQL of the form `WHERE Q6_clauses AND extra_condition` — for now, return `None` and let DataFusion's default plan run. Splitting into `FusedFilterSumExec` + residual `FilterExec` is a follow-up.
4. **`OR` chains.** AND-only. `OR` chains rarely compile to a single-pass kernel anyway; users wanting fused execution can re-write as a `UNION ALL`.

## Risks

1. **JIT build cost on the planner hot path.** The existing rules already pay ~5 ms per JIT build at planning; the generic rule pays the same. Cached against the spec's literals-and-shape if it ever shows up in profiles.
2. **Spec equivalence under `with_new_children`.** Cloning a `FilterSumSpec` with a baked JIT is an `Arc` bump (fine), but if `with_new_children` is called with a schema where `col_idx` shifted, the cached JIT IR (which uses absolute positions) is wrong. Σ.G.3 already needs to solve this for `Q6Spec` / `Q1Spec`; the fix carries over.
3. **Coverage cliff.** The current Q6 rule rejects any unrecognised column or extra clause — silently falling back to DataFusion's default plan. The generic rule will accept *more* shapes, which means a SQL pattern that today silently doesn't optimise will start hitting the JIT path. That's the win, but it also means a regression in any of those JIT kernels for some new shape will surface immediately. Recommendation: ship behind a `SessionConfig` opt-in for one release before turning it on by default.

## Sizing

- Σ.G.2e-1 (`FilterSumSpec`): ~1 day
- Σ.G.2e-2 (`InjectFilterSumRule` + shared `decompose_filter_chain`): ~1 day
- Σ.G.2e-3 (retire `InjectFusedQ6Rule`): ~0.5 day

Total: ~2.5 days after Σ.G.3 lands.

## Companion: how to find the next gap

When the rule is live, the way to identify the next SQL pattern that should JIT is:
1. `cargo run --example tpch_run_all` against the full 22-query suite
2. `EXPLAIN ANALYZE` each query
3. Any plan that still shows `AggregateExec(Final) → … → FilterExec → Scan` (no fused exec) is a gap
4. Match the gap against this doc's "deliberately doesn't do" list — if not covered, file a follow-up
