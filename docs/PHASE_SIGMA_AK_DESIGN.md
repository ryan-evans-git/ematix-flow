# Σ.AK — Q10 shape-predicate dim-push

**Status:** **CLOSED POSITIVE, branch (a) clean win, 2026-05-27** — predicate gate landed, `EMAT_DIM_PUSH` flipped default-ON. 22q SF=10 net -74.98 ms (-2.39%), Q10 -66.38 ms WIN, all hard gates pass. Phase 0+1+2 completed in ~2 hours instead of the 1-2 day budget because both the structural difference and the predicate pattern were straightforward.
**Prerequisite:** Σ.AJ.1 Lever C + harness fix landed (commits `8bd3f39`, `41201b4`). DIM_PUSH re-validation result: `[[sigma-ad-dim-push-revalidated]]`.
**Budget estimate:** 1-2 days for a phase-gated spike; 3-5 days if Phase 1 needs cardinality estimation.

## Outcome (2026-05-27)

### Phase 0 — Plan-diff identification ✓
Structural difference visible in the first plan dump. Q07 post-rewrite physical plan shows `CoalescePartitionsExec` between two `CollectLeft` joins (the exact pathology Σ.AD's module header documented). Q10's post-rewrite has no such inversion; instead gains `AggregateExec: mode=SinglePartitioned`.

### Phase 1 — Predicate + tests ✓
- `fact_side_has_competing_filtered_dim(fact_side, target_table) -> bool` added to `dim_join_pushdown.rs:229`
- `dim_subtree_leaf_table` helper to walk SubqueryAlias / Filter / Projection chains
- Wired into `try_rewrite` right after `target_table` is determined
- 4 unit tests pass: `predicate_blocks_q07_shape`, `predicate_allows_q10_shape`, `predicate_blocks_q21_shape`, `predicate_blocks_q03_shape`
- SF=10 e2e correctness: `rewrite_preserves_q10_result_sf10` passes (row count match)
- The prior `fires_on_q07_shape` test renamed to `predicate_blocks_q07_shape` with inverted assertion — Σ.AK changes the semantic from "Σ.AD must fire on Q07" to "predicate must refuse Q07"

### Phase 2 — Re-bench ✓

| Query | DIM_PUSH OLD (no predicate) | DIM_PUSH NEW (Σ.AK predicate) |
|---|---|---|
| **Q10** | -62.25 ms WIN | **-66.38 ms WIN** (-24.56%, bar 3.48) |
| Q07 | +43.04 ms regression | **+1.63 ms noise** ✓ |
| Q21 | +26.21 ms regression | **-3.30 ms noise** ✓ |
| Q03 | +13.79 ms regression | **+3.90 ms noise** ✓ |
| Q09 | noise | **+7.54 ms regression (+2.79%, bar 1.94)** ⚠️ |
| **Net** | +44.56 ms (+1.41%) | **-74.98 ms (-2.39%)** |

Σ.AD doesn't fire on Q09 at all under the predicate (`[Σ.AD] fires=0`). Q09's +7.5 ms is likely thermal drift or second-order interaction with the harness's `dim_push_actually_fires` check (extra `into_optimized_plan` on first trial). The 2.79% magnitude is well under the 5% hard gate; Q10 win (-66 ms / -24.56%) dwarfs it.

### Phase 3 — Decision branch (a) clean win ✓

All hard gates pass:
- ✅ Q10 wins ≥ -40 ms above 2σ (actual -66.38 at bar 3.48)
- ✅ No per-query regression > 5% above 2σ (max is Q09 at 2.79%)
- ✅ 22q geomean Δ ≤ +1.5pp (actual -2.39% improvement)

**Actions taken:**
1. `fact_side_has_competing_filtered_dim` predicate landed in `dim_join_pushdown.rs`
2. `EMAT_DIM_PUSH` default flipped from opt-in to ON in `tpch_triangulation_bench.rs` (both env-var read sites + comment block update)
3. SF=10 correctness test added
4. Existing `fires_on_q07_shape` test inverted to `predicate_blocks_q07_shape` (Σ.AK changes the semantic)

### Combined cumulative SF=10 gains (today's session)

- Lever C (Σ.AJ.1): -225.9 ms (-6.64%)
- Σ.AK Q10 shape-predicate dim-push: -74.98 ms (-2.39%) **on top of Lever C**
- Total: roughly **-300 ms (-9%)** from no-defaults baseline

### Follow-ups

- Q09 +7.5 ms — investigate under a second 22q bench to confirm thermal vs real. Bench-harness inspection might reveal whether `dim_push_actually_fires` adds measurable per-trial cost on non-firing queries. Low priority given the 2.79% magnitude.
- Q10 + Σ.AH.X Lever G compound test — Lever G also moves Q10 (-21 ms). Combined effect could be additive or absorptive. Not in scope here.

## Why this arc exists

`EMAT_DIM_PUSH=1` re-validation (commit `byjssy5qq` bench at `/tmp/strict-ab-dim-push-revalidation/diff.md`) showed:

| Query | Δ ms | Verdict |
|---|---:|---|
| **Q10** | **-62.25 (-22.92%)** | **WIN >2σ** |
| Q07 | +43.04 (+27.71%) | regression >2σ |
| Q21 | +26.21 (+8.13%) | regression >2σ |
| Q03 | +13.79 (+9.67%) | regression >2σ |
| Net | +44.56 (+1.41%) | mild negative |

The Q10 win is **larger than anything we've shipped in any other lever this session** (Σ.U/Lever C delivered Q17 -104 ms, which was the biggest single-query win — Q10 -62 ms is the next-biggest). If we can isolate Q10's shape from Q07/Q21/Q03, this is a major default-flip candidate.

## What's already documented about the regression

The Σ.AD module's own header (`dim_join_pushdown.rs:78-91`) identifies the mechanism:

> The structurally-correct plan REGRESSES Q07/Q21 wall-time because the new shape inserts a `CoalescePartitionsExec` between two CollectLeft joins. The first CollectLeft (nation⋈supplier in Q07) is fine — small build, small probe — but the resulting filtered supplier stream gets coalesced before becoming the build side of the next CollectLeft (supplier⋈lineitem). The coalesce + small-build-side broadcast overhead exceeds the savings.

So the regression has a known root cause: **CollectLeft-after-Coalesce inversion**. The fix is either:
- **(Σ.AK-A) Predicate gate** — only fire when the rewrite would NOT introduce a CollectLeft-after-Coalesce. Cheap, narrow, ~1-2 days.
- **(Σ.AK-B) Physical fix** — emit the inner CollectLeft as Partitioned to skip the coalesce. Or push the dim filter as a static IN-list predicate at the FK scan (true magic-set rewriting). 1-2 weeks, structurally cleaner.

This spike scopes Σ.AK-A; Σ.AK-B is the longer-term option if the predicate proves too narrow.

## Hypothesis (CONFIRMED in Phase 0, 2026-05-27)

There exists a logical-layer predicate (no physical plan inspection needed) that:
- Returns `true` on Q10's chain shape (customer ⋈ orders ⋈ lineitem ⋈ nation — dim push reaches a deep TableScan(lineitem) leaf cleanly)
- Returns `false` on Q07's chain shape (nation-IN ⋈ supplier ⋈ lineitem ⋈ orders ⋈ customer — dim push lands BETWEEN supplier and lineitem)
- Returns `false` on Q21/Q03's chains

**Concrete predicate** (Phase 0 finding):

> Refuse to push the outer dim if the **fact_side subtree contains another `Inner Join (Filter→TableScan, X)`** in the path between the outer join and the target TableScan.

The "another Inner Join with a filtered-dim side" is exactly what becomes a CollectLeft in the physical plan. Pushing the outer dim through it forces a `CoalescePartitionsExec` between the two CollectLeft layers — the pathology Σ.AD's module header documents.

### Phase 0 evidence

Q07 post-rewrite physical plan (excerpt):
```
HashJoinExec: mode=CollectLeft, on=(s_suppkey, l_suppkey)
  BuildSideBloomEmitterExec
    CoalescePartitionsExec        ← THE BAD COALESCE
      ProjectionExec
        HashJoinExec: mode=CollectLeft, on=(n_nationkey, s_nationkey)   ← inner CollectLeft
```

Q10 post-rewrite physical plan: no CollectLeft inversion. Instead gains `AggregateExec: mode=SinglePartitioned` (skips Partial+Repartition) — that's where Q10's -62 ms comes from.

| Query | fact_side has another `Inner Join(Filter→TableScan, X)`? | Predicate | Expected bench result |
|---|---|---|---|
| Q07 | YES — `supplier ⋈ Filter(n_name)→nation` inside fact_side | refuse | avoid +27.71% regression |
| Q10 | NO — `customer ⋈ orders ⋈ lineitem` (no filtered-dim Inner Join nested) | allow | preserve -22.92% win |
| Q21/Q03 | Likely yes (parallel shape to Q07) | refuse | avoid regressions |

## Spike plan — 1-2 days

### Phase 0 — Plan diff Q10 vs Q07 (4-6 hours)

Dump the optimized LogicalPlan for Q10 and Q07 with and without `EMAT_DIM_PUSH=1` via `sigma_q_explain_plan`. Identify:
1. What does the rewrite produce for Q10? Where does the dim land?
2. What does the rewrite produce for Q07? Where does the dim land?
3. Structural diff at the LogicalPlan level (NOT physical) — what surrounds the rewritten subtree?

Specifically: does Q07's fact_side subtree contain another `Filter(P) → TableScan` chain near the TableScan(T) target?  If yes → don't fire.

**Output:** a written predicate function signature like `fn destination_has_competing_filtered_dim(plan: &LogicalPlan, target_table: &str) -> bool`.

**Gate:** if Phase 0 reveals no clean structural predicate (i.e., Q10 and Q07 have the same logical shape and the difference is purely cardinality-driven), close Σ.AK-A and bump to Σ.AK-B (physical CollectLeft fix). Time-box at 6 hours.

### Phase 1 — Implement predicate + tests (1 day)

1. Add `should_push_dim_join(outer_join, target_table) -> bool` to `dim_join_pushdown.rs`
2. Gate `try_rewrite` to call the predicate before applying the rewrite
3. Add unit tests:
   - `predicate_allows_q10_shape`
   - `predicate_blocks_q07_shape`
   - `predicate_blocks_q21_shape`
   - `predicate_blocks_q03_shape`
   - SF=10 end-to-end correctness for all four queries
4. Build clean

**Gate:** all 4 unit tests pass with the predicate returning the expected `Allow/Block` for each query's plan.

### Phase 2 — Re-bench (30 min)

Same methodology as the harness re-validations:
```bash
./scripts/bench/strict_ab.sh --env-b "EMAT_DIM_PUSH=1" --invocations 4 --out /tmp/strict-ab-sigma-ak
```

**Pass criteria:**
1. **Hard:** Q10 wins ≥ -40 ms above 2σ (preserve most of the -62 ms; some loss is OK from gating overhead)
2. **Hard:** No per-query regression > 5% above 2σ (Q07/Q21/Q03 must return to noise)
3. **Hard:** 22q geomean Δ ≤ +1.5pp (improvement expected; gate is the noise floor)

### Phase 3 — Decision (30 min)

- **(a) Clean win** — flip `EMAT_DIM_PUSH` default to ON. Re-bench triangulation. Update docs.
- **(b) Predicate too narrow / over-broad** — return to Phase 0, refine predicate. 4-hour time-box per `[[no-quick-reject]]`.
- **(c) Predicate fundamentally can't isolate Q10** — close Σ.AK-A, escalate to Σ.AK-B physical fix.

## Risk register

- **Codegen tax: zero.** The rule and the predicate are in the existing pre-plan walker; only the runtime behavior changes. No new OptimizerRule per `[[optimizer-codegen-sensitivity]]`.
- **Σ.AD's original SF=10 5-trial bench (2026-05-25) showed Q02 -26%, Q05 -8%, Q08 -5% as wins.** Those are NOW noise under current defaults — the default stack ate the wins. We may LOSE those four small wins (Q02/Q05/Q08/Q11) when we gate. That's OK; they're all within 2σ noise today.
- **Cardinality-driven Q10 vs Q07 distinction.** If the difference between Q10 win and Q07 regression is purely about table sizes (supplier 100K → 8K vs customer different ratios), a static logical predicate may not cleanly separate them. Phase 0 result decides this; if cardinality-driven, escalate to Σ.AK-B.
- **Compounding with Σ.AH.X Lever G.** Both rules independently move Q10 (Lever G -21 ms, dim-push -62 ms). They may compound, or one may subsume the other. Not in spike scope; tested separately.

## Why this is potentially the highest-impact lever after Σ.AJ.1 Lever C

Lever C delivered -225.9 ms (-6.64%) net 22q. The Q10 -62 ms portion of Σ.AK alone is ~30% of that magnitude. If clean, this is a major one-day win.

## What we keep regardless

- The Σ.AD module's existing matcher is correct (structurally row-set preserving)
- Tests pass on SF=1 + SF=10 already
- The bench-harness fix means we can A/B accurately now (was impossible under broken harness)

## Inputs already available

| Artifact | Path |
|---|---|
| Σ.AD rule implementation | `crates/ematix-flow-core/src/dim_join_pushdown.rs` (595 LOC) |
| Σ.AD original tests | `dim_join_pushdown.rs` `#[tokio::test]` block |
| Bench wire-up | `tpch_triangulation_bench.rs` (with harness fix already) |
| Strict A/B harness | `scripts/bench/strict_ab.sh` |
| DIM_PUSH baseline | `/tmp/strict-ab-dim-push-revalidation/diff.md` |
| `sigma_q_explain_plan` | for Phase 0 plan dumps |
| Prior arc memory | `[[sigma-ad-dim-push-revalidated]]` |

## References

- Re-validation: `[[sigma-ad-dim-push-revalidated]]`
- Σ.AD original: task #91, `dim_join_pushdown.rs`
- Σ.AH.X Lever G (compounds on Q10): `[[sigma-ah-x-lever-g-revalidated]]`
- Methodology: `[[sigma-ai-1-strict-bench-landed]]`
- Codegen tax: `[[optimizer-codegen-sensitivity]]`
- Don't quick-reject: `[[no-quick-reject]]`
- No TPC-H hardcoding: `[[no-tpch-hardcoding]]` (predicate must be shape-based, not table-name-based)
