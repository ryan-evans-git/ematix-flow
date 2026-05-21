# Σ.K.A — Numeric-keyed FilterMultiAgg as a separate rule

**Date**: 2026-05-20
**Predecessor**: Σ.H.1d (rejected — see [[project_sigma_h1d_rejected]] and `docs/PHASE_SIGMA_H1D_BENCH_RESULT.md`).

## What changed since Σ.H.1d.6

Re-reading Q13's physical plan (`docs/SHAPE_COVERAGE_GAPS.md`) at the byte level revealed the actual gap is **not** SinglePartitioned mode (as the earlier hypothesis claimed):

```
ProjectionExec
  AggregateExec(Final, gby=[c_count], aggr=[count(*)])              ← Partial+Final pair, c_count is Int64
    RepartitionHash([c_count])
      AggregateExec(Partial, gby=[c_count])
        ProjectionExec(c_count)                                      ← single-child wrapper (CSE proj)
          AggregateExec(SinglePartitioned, gby=[c_custkey], aggr)   ← single-child wrapper (body)
            HashJoinExec(Left)
              ...
```

Q13's **outer** aggregate is Partial+Final with RepartitionHash — the exact shape the existing matcher accepts. The body matcher (`is_supported_body`) accepts AggregateExec(SinglePartitioned) since it has one child. The thing that bails the rule is **resolution of `c_count` as a `GroupKeyKind`**: the existing resolver only accepts `Utf8View` and `Dictionary(UInt32)`. `c_count: Int64` falls through.

So Q13's gap is identical to what Σ.H.1d set out to fix. The Σ.H.1d fix shipped the spec (`fused_aggregate_filter_multi_agg_numeric.rs`, still dormant in-tree) but the wiring approach — adding a dispatch branch inside the existing rule's `try_build_replacement` — perturbed LLVM codegen on the string hot path and broke Q02/Q15.

## Σ.K.A design

Rebuild the dispatch as a **completely separate `PhysicalOptimizerRule`**. The existing `InjectFilterMultiAggRule` file stays byte-for-byte unchanged.

### Files

- New: `crates/ematix-flow-core/src/inject_numeric_filter_multi_agg_rule.rs`
- New struct: `pub struct InjectNumericFilterMultiAggRule;`
- New `impl PhysicalOptimizerRule for InjectNumericFilterMultiAggRule`
- Reuses existing types from `fused_aggregate_filter_multi_agg_numeric` (already in-tree, dormant since Σ.H.1d.2).
- Touches `lib.rs` (new `pub mod ...;`) and `preset.rs` (register the new rule alongside the string rule).

### Shape

Identical to `filter_multi_agg_shape()` — `Projection → Aggregate(Final) → RepartitionHash → Aggregate(Partial) → optional Projection → body`, wrapped for Sort/Limit. The new rule **does not** import the existing rule's `filter_multi_agg_shape` function (to avoid any cross-module call site that LLVM might inline into both rules). Instead it locally constructs the same `Shape` via the catalog builders. Slightly duplicated, intentionally — codegen isolation matters more than DRY here.

### Matcher behavior

```
fn try_match_numeric_filter_multi_agg_plan(node) -> DfResult<Option<...>> {
    if EMAT_DISABLE_NUMERIC_FILTER_MULTI_AGG is set → return Ok(None).
    if shape().try_match(node).is_none() → return Ok(None).
    extract_group_key_names → ...
    resolve_numeric_group_keys → if any key is non-numeric, return Ok(None).
                                  (the string rule will fire on the same node)
    enforce: numeric_keys.len() == 1 (Σ.H.1d.2's spec only handles single-key).
    extract_aggregates, extract_filter_clauses → ...
    build FilterMultiAggSpecNumeric, wrap in FusedAggregateExec, wrap in alias projection.
}
```

### Ordering vs the existing rule

DataFusion's `PhysicalOptimizerRule`s run sequentially in registration order. Both rules try to match the same shape; the keys-resolve check makes them mutually exclusive on a given node:

- String keys → string rule succeeds, replaces the node, the numeric rule sees the rewritten plan (not a match).
- Numeric (1-key) → string rule's `resolve_group_keys` returns None, returns Ok(None), the numeric rule then matches.
- Numeric (≥2 keys) → both bail, DataFusion default plan runs.
- Mixed → both bail.

Registration order: numeric **before** string. Why: with the existing transform_down traversal, the first matcher to succeed wins, and either order works because the key-type check is exclusive. We use numeric-first to keep the new rule's behavior the canonical entry for numeric shapes (so if a future change accidentally widens the string rule's acceptor, the numeric rule still owns numeric-keyed cases by default).

### Why this can't repeat the Σ.H.1d.4 failure

The Σ.H.1d.4 break (Q02/Q15 with `Int64 == Float64` runtime error) traced to LLVM emitting a different inlining for the string spec's hot loop after we added a sibling `match resolved_keys` branch in the same `try_build_replacement`. With Σ.K.A:

1. The existing rule's `try_build_replacement` is **literally not edited**. Same source bytes → same LLVM IR → same JIT output.
2. The numeric rule lives in a separate module. Its hot path is the FilterMultiAggSpecNumeric, which is already shipping (5 unit tests pass, never exercised by any TPC-H query so codegen is irrelevant on the string path).
3. No shared mutable state, no shared inline functions on the hot path.

The same `EMAT_DISABLE_FILTER_MULTI_AGG=1` repro that confirmed Σ.H.1d.4 was the cause will be available as `EMAT_DISABLE_NUMERIC_FILTER_MULTI_AGG=1` for the new rule.

## Tests (TDD)

Before writing the rule body:

1. **`rule_fires_on_q13_shape`** — synthesize a plan with `Projection → Final(gby=[Int64]) → RepartitionHash → Partial → wrappers → HashJoin`. Run the rule. Assert the rewritten plan contains `FusedAggregateExec` and no `AggregateExec`.
2. **`rule_does_not_fire_on_string_keyed_shape`** — synthesize the same shape with `Utf8View` group key. Assert the rule returns Ok(None) (string rule will catch it).
3. **`rule_does_not_fire_on_multi_key_numeric`** — `gby=[Int64, Int32]`. Assert Ok(None) (Σ.H.1d.3 follow-up).
4. **`rule_does_not_fire_on_mixed_keys`** — `gby=[Utf8View, Int64]`. Assert Ok(None).
5. **`disable_env_var_skips_rule`** — set EMAT_DISABLE_NUMERIC_FILTER_MULTI_AGG, assert Ok(None).

All five test bodies look exactly like the existing rule's tests; only the schema differs. Copy-modify, don't share.

## Bench gate

Same protocol as Σ.H.1d.6 (5 runs × 20 trials × 22 TPC-H SF=1):

- Pass: geomean strictly better than current HEAD, no individual query regresses >+3%, all current Σ.H.1 wins (Q01/Q03/Q04/Q05/Q21) preserve within ±3%, **Q02/Q15 do not break**.
- Expected unlock: Q13 from 42ms → ~25ms (analogous to Q01's path). That's a ~40% query-local improvement, ~3% geomean.

The bench-gate doc explicitly verifies the Σ.H.1d failure mode by checking Q02/Q15 succeed in 5/5 runs.

## Scope clarification

Σ.K.A unlocks Q13 only. Q18's SinglePartitioned-mode top is a genuinely different shape; that's Σ.K.B. Q17's CoalescePartitions instead of RepartitionHash is Σ.K.C. Both are deferred — Σ.K.A first, bench gate, then decide.

## Estimated effort

- New rule file with TDD tests: half a day
- Bench gate (build + 5×20 trials × 2 sides): one hour
- Doc + commit: 30 min

Total: ~1 day if the bench passes cleanly.
