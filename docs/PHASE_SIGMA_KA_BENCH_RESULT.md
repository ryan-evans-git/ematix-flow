# Σ.K.A bench gate — **FAIL**

**Date**: 2026-05-20
**Branch**: `feat/sigma-f-shape-catalog`
**Baseline (pre-K.A)**: HEAD at `252d0db` (post-Σ.H.1d.4 revert + ematix-parquet v0.13.0)
**Tested HEAD**: pre-K.A + `inject_numeric_filter_multi_agg_rule.rs` (~800 LOC, fully isolated; no edits to existing rule file) + preset.rs registration

## Methodology

3 runs × 20 timed trials × 22 TPC-H SF=1 queries per side. Per-run
median ms → median-of-medians. Both sides built from the same revert
commit; only `lib.rs`, `preset.rs`, and the new rule file differ.

## Result: **FAIL — across-the-board regression**

| Metric | Value |
|---|---|
| Geomean Σ.K.A / pre-K.A | **1.0743** |
| Mean Δ | +7.54% |
| Wins (<-3%) | **0** |
| Regressions (>+3%) | **17 of 22** |
| Worst regression | Q06 +24.0% (10.46 → 12.97 ms) |
| Q13 (the supposed target) | +1.5% (no win) |

Σ.H.1 preserved-win queries all regress:
- Q01: +10.5% (FAIL gate ±3%)
- Q03: +10.4%
- Q04: +4.7%
- Q05: +5.9%
- Q21: +12.4%

Q02 / Q15 (the Σ.H.1d.4 failure-mode queries) **both ran** in 5/5 trials,
so Σ.K.A is not a repeat of Σ.H.1d.4's specific failure — but it's a
*worse* outcome because every query regresses.

## Root-cause hypothesis

The design assumed that "fully isolated rule, no edits to existing
hot path" would preserve the existing string rule's codegen. Empirically,
this is not the cost.

The likely cause is **per-rule optimizer-walk overhead**. DataFusion
runs every registered `PhysicalOptimizerRule` over every plan via
`transform_down`. Adding a new rule that visits every node and tries
shape-match adds a constant per-query CPU cost. With 22 queries averaging
~21 ms each, an extra ~1.5 ms per query of optimizer overhead is
exactly the ~7.5% geomean we observed. Small queries (Q11=7.5ms,
Q22=8.3ms) take a 10–13% hit; the largest (Q21=40ms) takes ~12% because
the matcher walks every node of Q21's wide plan, including all the
multi-table-join children.

The existing string rule does the same walk but is **counted in the
baseline** — adding a *second* rule with the same shape adds a roughly
equal walk to every plan. The shape-catalog matcher in particular has
to descend through projection / repartition / sort wrappers checking
every Optional branch.

Two corollaries:

1. **The "separate isolated rule" pattern doesn't avoid system-wide perturbation.** The Σ.H.1d.4 lesson about codegen sensitivity may have been real, but it's not the dominant cost here. The dominant cost is walking the plan tree once per registered rule.

2. **Q13 didn't win** even though the rule fires on it. The +1.5% delta on Q13 means whatever speedup the FilterMultiAggSpecNumeric path provides is washed out by the per-query optimizer overhead added to all 22 queries.

## What did *not* fail

- All 4 unit tests pass — the rule fires correctly on Int64 keys, bails
  on string keys, bails on multi-key, bails on mixed keys.
- The build is clean, no compiler warnings.
- 738/738 lib tests pass.

So Σ.K.A is *correct* — it just has nowhere near enough speedup on its
target query to offset its registration cost.

## Recommendation: reverted in working tree

Σ.K.A was implemented + tested + benched + reverted in this session.
Working tree: clean.

The `fused_aggregate_filter_multi_agg_numeric` spec module remains
in-tree as dormant (originally landed in Σ.H.1d.2 via `12da4f6`).
There's no path to lighting it up via a top-level optimizer rule
without paying the per-query walk cost.

**Possible future paths:**

1. **In-tree, gated**: ship the rule but only register it when an env
   var or config flag is set. Default off → no perf cost.

2. **Internal dispatch (rejected by Σ.H.1d)**: re-attempt the dispatch
   inside the existing rule, but this time accept that the LLVM
   codegen risk is *real* (Σ.H.1d.4 specifically broke Q02/Q15) and
   instrument the build to verify the string path's binary footprint
   doesn't change. Materially harder.

3. **Combine multiple rules into one walk**: refactor the shape catalog
   so a single optimizer pass tries all rules in one transform_down
   traversal. Amortises the walk cost across rules. Σ.F's catalog is
   meant for this; Σ.F Track 2 (never started) would implement the
   one-pass dispatcher.

(3) is the most aligned with the "any query a user may write" vision —
adding more rules shouldn't keep paying linear walk cost. **Σ.K.A
should not be revisited until Σ.F Track 2 lands.**

## Files

- Bench data: `/tmp/sigma_ka/run_{1..3}.md`
- Pre-K.A baseline: `/tmp/sigma_emat_v013/run_{1..3}.md`
- Design (now obsolete): `docs/PHASE_SIGMA_KA_DESIGN.md`
