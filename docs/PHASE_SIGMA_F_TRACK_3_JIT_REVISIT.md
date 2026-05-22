# Σ.F Track 3 — JIT revisit (fresh integration, not restored)

**Status:** scoped 2026-05-20, not started. Decision-gated.

**One-line goal:** decide whether wiring the existing `fused_jit.rs`
substrate into `FilterMultiAggSpec`'s hot loop produces a measurable
win over the current template-specialized path, on a clean main with
zero hardcoded TPC-H rules.

---

## State of the world (what the previous plan got wrong)

The Σ.F plan doc framed Track 3 as "JIT vs templates" as if no JIT
were running today. That's incorrect.

**Already JIT'd (Cranelift, in production):**

- `FilterSumSpec` — the Q06 family (single-bucket SUM over an
  AND-chain). Built via `FusedFilterAggJit::try_build` at construction
  time. The substrate is `crates/ematix-flow-core/src/fused_jit.rs`
  (`FusedFilterAggSpec` IR, `FusedFilterAggJit` codegen, Cranelift
  workspace deps still present in `Cargo.toml`).

**Not JIT'd (template-specialized in Rust):**

- `FilterMultiAggSpec` — the Q01 family (multi-aggregate group-by).
  Hot loop calls `eval_predicate_typed` / `eval_agg_typed` /
  `combine_cell`, each of which has a per-row `match` on a small enum.
- Per-shape monomorphic templates (`process_batch_dict_single`,
  `process_batch_two_key_utf8view`, `process_batch_perfect_hash_dict`)
  pick a fast hash path at the outer level but still go through the
  generic per-row eval functions.

**Previously retired (deleted in #533 / #534):**

- `EnableFusedJitRule` — the auto-routing optimizer rule that
  matched plans and routed them to a `FusedAggregateExec` over the
  JIT'd path. Removed because it was a noisy general-purpose router.
- `Q1Spec` / `Q6Spec` / `FusedPostJoinSpec` — TPC-H-hardcoded
  wrappers around `FusedFilterAggJit`. Removed because the generic
  `FilterSumSpec` / `FilterMultiAggSpec` matchers subsume them
  without the per-query hardcoding.

**Constraint from the user (2026-05-20):** the Σ.F Track 3 spike
must NOT restore the deleted code. `EnableFusedJitRule` stays gone;
no `Q1Spec` wrapper resurrection. A fresh integration on top of the
live `fused_jit.rs` substrate is in scope.

## What "JIT vs templates" actually means here

The remaining open question, post-correction, is narrower than the
plan doc implied:

> Does emitting a single JIT'd function — predicate +
> per-row aggregate update + combine — beat the template path's
> dispatch-per-row loop in `FilterMultiAggSpec::process_batch_generic`
> (and its specialized siblings)?

If yes: wire it in, gate on bench parity for the existing JIT
queries (Q06) and a measurable win on at least one multi-agg query.

If no: close the question definitively, document why, and stop.

The bar is the same as the original plan doc: ≤5% loss across the
three target queries OR any single-query win > 5%.

## Why this might still be worth doing

`FilterMultiAggSpec` hot loop per-row work, for a query like Q01 with
1 clause + 4 aggregates:

```text
for row in 0..n_rows {
    if !eval_predicate_typed(typed_cols, row) {          // 1× match dispatch
        continue;
    }
    // ... group-by hash + cell lookup ...
    for ai in 0..4 {                                      // 4× iterations
        let row_value = eval_agg_typed(agg, cols, row);  // 1× match dispatch
        cells[ai] = combine_cell(agg, cells[ai],         // 1× match dispatch
                                  row_value);
    }
}
```

That's roughly **9 dispatch branches per row** for Q01-shape input.
On lineitem SF=1 (6M rows post-filter) that's ~54M branches that an
optimal JIT would have inlined as straight-line code with the
specific clause op, agg expression, and combine op baked in.

The branch predictor handles these well (same target every iteration
in steady state), so the cost isn't catastrophic — but the i-cache
pressure and the loss of opportunities to fuse the predicate +
aggregate evaluation (e.g. read each f64 once instead of twice) is
real. Order of magnitude: 5-30% on Q01.

JIT also unlocks two doors that templates don't:

- **Predicate ops the JIT IR doesn't have today** — Eq, NotEq, BETWEEN
  range fusion. Extending the JIT IR is mechanical; extending the
  template requires another `ClauseOp` variant + match arm.
- **Aggregate kernels the template doesn't have today** — `SUM(CASE WHEN ...)`,
  `STDDEV`, `SUM(expr * something_runtime_computed)`. Same point.

The architectural lever Track 3 actually opens is: **what's faster
to extend, the JIT IR or the template's enum?** Both can implement
any operation, but JIT's per-plan cost amortizes when the operation
space grows. Templates' per-plan cost is zero but the enum scales
poorly past ~20 variants.

## Pre-spike gating profile (1 day)

Before any implementation, measure what fraction of FilterMultiAggSpec
hot-loop time the dispatch actually costs. If it's <5%, JIT can't
help even at theoretical peak.

Profiles to run:

1. **`samply` or Instruments time-profiler on Q01** — running the
   triangulation bench's Q01 in a loop. Identify the cost split:
     - predicate `match` dispatch
     - aggregate `match` dispatch
     - combine_cell `match` dispatch
     - group-by hash probe
     - column-typed-access (`f64_at`, `i32_at`)
     - allocator (HashMap entry, key_buf clear/push)
   This is gross, but the time-shares tell us where JIT could even
   help in principle.

2. **Microbench: dispatch overhead in isolation.** Run two functions
   on identical synthetic data:
     - A: `process_batch_generic` exactly as today.
     - B: a hand-monomorphized version where the clause op + agg
       kind are const-generic parameters, eliminating dispatch.
   The A-vs-B delta is the upper bound on what JIT could close.

3. **Existing FilterSumSpec JIT vs a hypothetical template version
   on Q06** — for calibration. We know JIT wins on Q06; the size
   of the win sets expectations for the multi-agg case.

**Decision gate:** if (1) and (2) together estimate the JIT-recoverable
fraction at <5% of Q01 total time, close the question. If ≥10%,
proceed to the implementation spike.

## Implementation spike (3 days, gated on the profile)

Assuming the profile justifies it:

### Phase A — minimal JIT predicate (1 day)

Extend `FusedFilterAggJit::try_build` to support a "predicate-only"
mode that emits a function:

```text
fn predicate(
    n: i64,
    typed_col_ptrs: *const *const u8,   // one per input column
    out_mask: *mut u8,                  // bitmap, n bits
);
```

Wire `FilterMultiAggSpec` to compile this at `try_new` time and
call it once per batch instead of looping `eval_predicate_typed`.
Bench Q06 (parity, since FilterSum is JIT'd already) and Q01 (the
multi-agg query whose predicate is non-trivial only on a few of the
22-query set).

Acceptance: Q01 doesn't regress; if anything, marginally faster.

### Phase B — full inner-loop JIT (1.5 days)

If Phase A is positive, extend the JIT to emit the entire predicate
+ aggregate + combine loop as one function. The group-by hash probe
stays in Rust (it's the same logic across all specs, and Cranelift
doesn't help with HashMap probes).

Bench Q01 + Q03 + Q04 + Q10 (the multi-agg queries from the SF=1
suite). Acceptance: per-query Δ within ±5% with at least one query
> 5% faster.

### Phase C — bench gate + decision (0.5 day)

Run the 3-run multi-bench head-to-head (Σ.F.2's methodology).
Geomean must stay within ±2%. Document outcome in this file +
update memory.

## Why not LLVM, or hand-written asm, or a new IR

- **LLVM (inkwell):** longer compile times per plan (we'd hit
  100-200 ms per query plan, vs Cranelift's <10 ms). Better generated
  code in principle but the wins don't justify the latency for OLAP
  queries that run in 10-50 ms total.
- **Hand-written x86_64 / ARM64 assembly:** unmaintainable. Two
  architectures, two SIMD vocabularies; the workspace already has
  the right factoring (ematix-parquet for portable SIMD decode,
  cranelift for per-plan codegen).
- **A new IR layer above Cranelift:** the `FusedFilterAggSpec` IR
  in `fused_jit.rs` is the IR. Adding another layer is gold-plating.

Cranelift is the right tool. The constraint is the integration, not
the backend.

## What this is NOT

- Not a restoration of `EnableFusedJitRule`. The Σ.F shape catalog
  takes the place of the auto-router; if `FilterMultiAggSpec` grows
  a JIT path, the existing `InjectFilterMultiAggRule` instantiates it
  the same way it instantiates the template path today.
- Not a new operator type. `FusedAggregateExec<FilterMultiAggSpec>`
  stays; `FilterMultiAggSpec` grows an internal JIT path that takes
  the place of `eval_predicate_typed` / `eval_agg_typed` /
  `combine_cell`.
- Not a perf push for the sake of JIT. The deliverable is data: a
  go/no-go decision backed by a profile + microbench + head-to-head
  bench, not "we tried JIT again, here's how much code we wrote."

## Risks

- **The profile says <5% recoverable.** Then we close the question
  and the spike was 1 day of investigation work. Acceptable.
- **The profile says >10% recoverable but the implementation only
  recovers half.** Common with JIT — codegen quality varies and
  Cranelift isn't LLVM. Document the gap and decide whether to
  push further or shelve.
- **The new JIT path regresses FilterSumSpec.** If Phase A's
  refactor of `FusedFilterAggJit` accidentally breaks the existing
  Q06 path, that's a critical regression. Mitigation: keep the
  current `try_build` working unmodified; add new entry points
  for the multi-agg shape.
- **i-cache / branch-prediction pessimism.** Some optimizations
  win in isolation but lose at workload mix when other code paths
  share the cache. Mitigation: bench the full 22-query suite, not
  just the target queries.

## Predecessors + sequencing

- Σ.F.1 / Σ.F.2 / Σ.F.3 — the shape catalog (this PR series).
  Track 3 is independent of the catalog work but stacks naturally
  after it: if the JIT path lands, `InjectFilterMultiAggRule`'s
  rewriter constructs the JIT-mode spec from the captured shape
  same as it constructs the template-mode spec today.
- `fused_jit.rs` substrate landed in Σ.D3. Continues to ship for
  `FilterSumSpec`. Track 3 extends it; doesn't replace it.

## Open questions for the user before implementation

These don't need to be answered to do the gating profile, but
should be answered before any wire-up work:

1. **Compile latency budget per query plan.** Cranelift's emit-and-link
   is ~5-10 ms per function for plans this size. Acceptable for a
   30-50 ms query but obvious at <10 ms. Should the integration
   cache JIT artifacts per `FilterMultiAggSpec` signature?
2. **Fallback strategy.** If the JIT compile fails for a given
   spec (unsupported clause op, unsupported aggregate variant),
   does the rule fall back to the template path or refuse the plan?
   Existing FilterSumSpec returns `Err`; FilterMultiAggSpec could
   fall back gracefully.
3. **Acceptance for queries that don't hit the path.** The 22-query
   suite has ~5 multi-agg queries (Q01, Q03, Q04, Q10, Q22-ish);
   the rest can't benefit. The geomean gate is fine for these;
   the per-query gate matters for the 5.
