# Σ.G.3 — JIT into `AggregateSpec`, then retire hand operators

**Status:** scoped, not started.
**Prereqs:** Σ.G.2c (#94 → reopened as #96 — `FusedAggregateExec<S>`) + Σ.G.2d (auto-routing rule, same PR).
**Unblocks:** deletion of `FusedFilterSumExec`, `FusedFilterMultiAggExec`, `InjectFusedQ{6,1}Rule` (today's TPC-H-hardcoded substrate).
**Owner:** TBD.

## What this phase does

Make `FusedAggregateExec<S>` capable of running the Cranelift-JIT'd inner loop, so the generic operator subsumes every capability of the two hand operators. Once it does, the hand operators (and their per-query injection rules) can be deleted: their last reason to exist is being the JIT substrate.

Not in scope:
- `FusedPostJoinExec(Q3/Q5/Q12/Q14)`. That's the post-join shape; separate phase (Σ.G.4) builds the trait surface for two-input aggregates.
- Predicate extraction from `PhysicalExpr` ASTs — that's the SQL-surface generalization, orthogonal to JIT.

## Today's JIT plumbing

`crates/ematix-flow-core/src/fused_jit.rs`:

- `FusedFilterAggSpec` — data-driven IR description. `inputs`, `clauses` (predicate), `aggregates`, optional `group`. Constructors `::q6(...)`, `::q1(...)`, `::q14_post_join()`.
- `FusedFilterAggJit::try_build(spec)` — Cranelift module + function `fn(n: i64, inputs: *const *const u8, outputs: *mut f64)`. One slot per (group × aggregate) cell.
- The hand operators carry `jit: Option<Arc<FusedFilterAggJit>>` and dispatch in their `execute()` worker:

  ```rust
  match &jit_p {
      Some(j) => process_q1_batch_jit(&batch, idx_p, j, &mut cells),
      None => process_q1_batch_hand(&batch, predicate_p, idx_p, &mut groups),
  }
  ```

  `process_q{6,1}_batch_jit` is a free function in `fused.rs` / `fused_multi_agg.rs` that calls the JIT FFI and writes the output cells.

## Target shape — Σ.G.3 design

The cleanest port keeps the `AggregateSpec` trait **unchanged**. Each spec carries its own optional JIT kernel; `process_batch` dispatches internally; the operator stays JIT-unaware.

```rust
pub struct Q6Spec {
    pub predicate: Q6Predicate,
    pub indices: Q6ColumnIndices,
    pub output_schema: SchemaRef,
    jit: Option<Arc<FusedFilterAggJit>>,    // NEW
}

impl Q6Spec {
    pub fn try_new(predicate, child_schema) -> Self { /* jit: None */ }

    pub fn try_new_jit(predicate, child_schema) -> DfResult<Self> {
        let mut spec = Self::try_new(predicate, child_schema)?;
        let jit_spec = FusedFilterAggSpec::q6(
            predicate.date_lo, predicate.date_hi,
            predicate.disc_lo, predicate.disc_hi, predicate.qty_hi,
        );
        spec.jit = Some(Arc::new(
            FusedFilterAggJit::try_build(&jit_spec)
                .map_err(|e| DataFusionError::Plan(format!("Q6Spec JIT: {e}")))?,
        ));
        Ok(spec)
    }
}

impl AggregateSpec for Q6Spec {
    type Accumulator = f64;

    #[inline(always)]
    fn process_batch(&self, batch: &RecordBatch, acc: &mut f64) -> DfResult<()> {
        match &self.jit {
            Some(j) => *acc += process_q6_batch_jit(batch, self.indices, j),
            None    => *acc += process_q6_batch_hand(batch, self.predicate, self.indices),
        }
        Ok(())
    }
    // finalize / merge / output_schema / validate_input_schema unchanged.
}
```

`Q1Spec` mirrors this with `[Q1Aggs; 5]` accumulator and the JIT writing into a 30-cell scratch (existing pattern — same conversion done in `FusedFilterMultiAggExec::execute()`).

### Why this shape over alternatives

| Option | Trait change | Operator change | Spec change | Verdict |
|---|---|---|---|---|
| **Internal JIT in spec** (this doc) | none | none | +1 field, +1 ctor, +1 match arm | ✓ smallest blast radius |
| Trait gets `enable_jit(self)` | +1 default method | one extra call site | one impl per spec | acceptable but doesn't avoid the field |
| Separate `Q6JitSpec` type | none | none | doubles spec count | clean but bloats the API |
| Operator owns JIT | none | +1 field on operator | none | spec/operator inversion — operator would need to know JIT IR shape |

The match in `process_batch` is invariant across iterations once the spec is constructed, so LLVM should hoist the branch out of the hot loop in monomorphized code. Validated by the Σ.G.3 bench gate (below).

## Slice plan

Each slice ships behind a bench gate, mirroring Σ.G.2.

### Σ.G.3a — Q6 JIT in spec

1. Add `jit: Option<Arc<FusedFilterAggJit>>` to `Q6Spec`.
2. `Q6Spec::try_new_jit` constructor.
3. `process_batch` match — non-JIT branch unchanged.
4. **Bench:** `examples/sigma_g3a_q6_spec_jit_vs_hand_jit.rs`. Same methodology as Σ.G.2c (41 trials × 3 rounds × MIN-of-K, interleaved, 3 warmups). Compares `FusedAggregateExec<Q6Spec(JIT)>` vs `FusedFilterSumExec(JIT)`. Gate at 3 %.
5. Tests: 2 unit tests asserting the JIT path produces the same sum as the hand path on a known fixture.

### Σ.G.3b — Q1 JIT in spec

Symmetric. The 30-cell scratch → `[Q1Aggs; 5]` conversion stays in `process_q1_batch_jit`; spec just owns the JIT handle.

### Σ.G.3c — extend the planner rule

`EnableFusedAggregateExecRule` (Σ.G.2d) currently leaves JIT instances alone:

```rust
if !e.has_jit() { /* lift to generic */ }
```

Change to: always lift, choosing the JIT-or-non-JIT spec constructor based on the source exec's mode:

```rust
let spec = if e.has_jit() {
    Q6Spec::try_new_jit(e.predicate(), &input.schema())?
} else {
    Q6Spec::try_new(e.predicate(), &input.schema())?
};
```

This subsumes the JIT path of `EnableFusedJitRule` for these two shapes. Bench-gate: rerun the Σ.G.3a/b benches with the rule in the pipeline (not just direct construction) to confirm planner-level perf.

### Σ.G.3d — retire `FusedFilterSumExec` + `FusedFilterMultiAggExec`

After Σ.G.3a/b/c land + bench-passes hold for a week (catch flake-driven regressions):

1. Update `InjectFusedQ6Rule` / `InjectFusedQ1Rule` to construct `FusedAggregateExec<Q{6,1}Spec(JIT)>` directly when they detect their SQL pattern. Skip the intermediate hand-exec construction.
2. Update `EnableFusedJitRule` — drop the `FusedFilterSumExec` / `FusedFilterMultiAggExec` arms (now no source nodes). Keep the `FusedPostJoinExec(Q14)` arm; that's still Σ.G.4 territory.
3. Delete `FusedFilterSumExec` (`crates/ematix-flow-core/src/fused.rs`), `FusedFilterMultiAggExec` (`fused_multi_agg.rs` operator portion), `EnableFusedAggregateExecRule` (no source nodes left — pure migration aid).
4. Migrate every `examples/*.rs` and integration test that constructs these directly. Most should already be replaceable; the JIT-bench in `examples/tpch_fused_jit_bench.rs` will need a parallel rewrite.
5. Update `docs/FUSED_AGGREGATE_SHAPES.md` (post-Σ.G.2 reality check) to remove the hand-operator references.

This is the slice that closes the open `[#476]` task.

## Risks and unknowns

1. **JIT path perf through the trait.** Σ.G.2c showed the hand-path goes through the trait without measurable cost; the JIT path is an FFI call into a 50-µs Cranelift-compiled kernel, so trait overhead is even less relevant proportionally. But: the spec's `match &self.jit` introduces a branch that wasn't in either of the existing hand operators (which match on a captured `jit_p` local). LLVM should hoist it; the Σ.G.3a bench confirms.

2. **Spec equality semantics for `with_new_children`.** `FusedAggregateExec<S: AggregateSpec + Clone>::with_new_children` does `spec.clone()`. Cloning a `Q6Spec` with a JIT handle is an `Arc<FusedFilterAggJit>` refcount bump — fine. But the JIT was built against the *original* child schema; if `with_new_children` is called with a schema where column indices shifted, the cached `indices` are stale and the JIT IR (which references those columns by absolute position) is silently wrong. The current `validate_input_schema` would catch a column-list mismatch but not a column-order swap. Fix: in `with_new_children`, rebuild the spec via `try_new_jit` / `try_new` when the new child schema differs from the cached `output_schema` of the input.

3. **JIT build time on the planner hot path.** Cranelift codegen is ~5 ms per spec. Today the hand operators eagerly build JIT at exec construction; that's already on the planner hot path so this isn't a regression. If it ever needs to be moved off, the lever is `lazy_static`-style caching keyed on the spec's literal-encoded constants — but that's a separate optimization.

4. **`FusedPostJoinExec` not in scope.** Q3/Q5/Q12/Q14 keep going through the legacy operator. The JIT integration for them is the harder Σ.G.4 piece (two-input shape requires a different trait surface for post-join aggregates). Σ.G.3 deliberately scopes to the filter-agg shape so we get a complete deletion of the two simpler hand operators without waiting on Σ.G.4.

## Bench-gate checklist (before any deletion in Σ.G.3d)

- [ ] Σ.G.3a — Q6 spec-JIT within 3 % of hand-JIT on TPC-H SF=1, MIN-of-41 trials × 3 rounds
- [ ] Σ.G.3b — Q1 spec-JIT within 3 % of hand-JIT on TPC-H SF=1, same methodology
- [ ] Σ.G.3c — re-run Σ.G.3a/b through `SessionContext::sql()` with the updated rules registered, gate still passes
- [ ] No bench in the existing suite regresses by > 3 % between the rule-only and the post-retirement commits

## Sizing

- Σ.G.3a: ~1 day (spec field + ctor + match + bench + 2 tests)
- Σ.G.3b: ~1 day (mirror of a; Q1 30-cell conversion is the only extra wrinkle)
- Σ.G.3c: ~0.5 day (rule change + integration test)
- Σ.G.3d: ~1 day (rewrite Inject*Rules + delete operators + migrate examples + doc updates)

Total: ~3.5 days of focused work, fully bench-gated at each slice.
