# Σ.F Track 2 — single-pass dispatcher bench gate (FAIL)

**Date**: 2026-05-20
**Branch**: `feat/sigma-f-shape-catalog`
**Baseline (pre-T2)**: HEAD at `b31344d` (post-Σ.K.A revert)
**Tested HEAD**: pre-T2 + `shape_catalog_dispatcher.rs` (~150 LOC) + `pub(crate)` visibility on two `try_match_*` functions + preset.rs rewiring (2 separate rules → 1 dispatcher rule with 2 matchers)

## Methodology

3 runs × 20 timed trials × 22 TPC-H SF=1 queries per side.
Σ.F Track 2 had a high-noise run (Q05 21→32 ms, Q08 22→34 ms, Q07 28→34 ms — looks like a thermal/scheduler hiccup on one trial), so I cross-checked against **min-of-runs** as the noise-resistant stat.

## Result: **FAIL**

| Stat | Value |
|---|---|
| Geomean (med-of-med) | **1.0540** |
| Geomean (min-of-runs) | **1.0479** |
| Wins (<-3%) | 0 |
| Regressions (>+3%, by median) | 15 |
| Σ.H.1 preserved-win gate | FAIL (Q01 +3.6%, Q03 +3.3%, Q04 +3.1%, Q21 +6.8%) |

Worst regressions: Q08 +18.7%, Q17 +10.8%, Q09 +10.6%, Q06 +9.5%, Q10 +9.3%, Q14 +9.7%.

## Why this overturns the Σ.K.A walk-cost hypothesis

Σ.K.A failed with +7.5% geomean and the writeup attributed it to **per-rule `transform_down` walk overhead** (adding a rule costs ~1.5 ms × 22 queries ≈ +35 ms ≈ +7.5%).

Σ.F Track 2 **removes** a rule walk (3 rules → 2 rules: 1 dict rule + 1 dispatcher containing 2 matchers). By the walk-cost hypothesis it should *gain* ~7%. Instead it regressed ~5%.

So the dominant factor in Σ.K.A's regression was **not** walk cost. The walk-cost theory looked plausible because the magnitude was right, but it predicted the wrong sign here.

## Revised hypothesis: codegen perturbation is the dominant cost

Both Σ.K.A (+7.5%) and Σ.F Track 2 (+5%) added/changed code in the optimizer-rule reachability graph. Both regressed system-wide, including queries that the new code can't possibly fire on (Σ.K.A: Q06 had empty group keys → can't match the numeric rule; Σ.F Track 2: same queries regressed even though the dispatcher has *less* per-query work than the two separate rules it replaced).

This matches the **Σ.H.1d.4 failure mode** more than the walk-cost mode. Adding *any* new compilation unit reachable from `with_optimizer_rules` appears to perturb LLVM's inlining/scheduling decisions on the existing hot paths.

**The dominant constraint is now clear: the ematix-flow-core crate's existing optimizer hot paths are unusually sensitive to ANY new code in the optimizer module reachability graph.** This was already known from Σ.H.1d.4 (intra-function modification) and Σ.H.1d's spec-module addition; Σ.K.A (new rule file) and Σ.F Track 2 (refactoring rule list) both confirm it operates at the module-reachability level, not just the function-body level.

## Implication for the "any-query" vision

The user's stated vision — "a recursive processor to optimize any SQL query" — currently has a structural barrier:

- **Adding new SHAPES to the catalog costs ~5–8% geomean per addition** (the bites tried so far).
- **Refactoring the dispatcher to remove rule-walk overhead doesn't recover the cost.**
- **The codegen sensitivity isn't local** — it's at the LLVM module-reachability level.

Without addressing this constraint, expanding query-shape coverage via the existing rule-registration mechanism is a losing strategy at the SF=1 query budget where the bench currently runs.

## Mitigations worth trying (future)

1. **PGO (profile-guided optimization)** — feed LLVM bench-driven profile info so it makes inlining decisions based on actual hot paths instead of static heuristics. Should be much more stable across additions.

2. **Move the bench upmarket to SF=10+** — at larger data scale, query time is dominated by scan/decode (kernel work), not optimizer overhead. The 5-8% regressions would shrink to <1% and become invisible relative to the work itself.

3. **Continue chasing kernel-level wins instead** — the ematix-parquet v0.13.0 bump delivered +4% geomean with **zero regressions**. Kernel changes don't perturb the optimizer's codegen because they live in a separate crate.

## State

Reverted in working tree. All four files restored (`fused_aggregate_filter_multi_agg_rule.rs`, `fused_aggregate_filter_sum_rule.rs`, `lib.rs`, `preset.rs`) and the new `shape_catalog_dispatcher.rs` deleted. 737/737 lib tests pass on pre-T2 HEAD.

## Files

- Bench data: `/tmp/sigma_f_track2/run_{1..3}.md`
- Pre-T2 baseline: `/tmp/sigma_emat_v013/run_{1..3}.md`
- Variance probe: `/tmp/sigma_f_track2/variance.py`
