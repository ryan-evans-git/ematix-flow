# Σ.H.1d.6 — deep-bench gate (FAIL)

**Date**: 2026-05-20
**Branch**: `feat/sigma-f-shape-catalog`
**Σ.H.1d HEAD**: `793016f` — `feat(Σ.H.1d.4+5): wire String/Numeric dispatch + 1-key numeric gate`
**Baseline**: v0.3.0 (`268ab96`), `crates/ematix-flow-core/src/` checked out, everything else current

## Methodology

22 TPC-H queries at SF=1, 20 timed trials after 3 warmups per query per engine.
Repeat 5 times → 5 × 20 = 100 trials per side per query. Per-run median ms → 5
medians per query per side → **median-of-medians** as the headline statistic.

Source: `/tmp/sigma_h1d_deep/{h1d,v0}_run_{1..5}.md`.

## Result: **FAIL**

Two of the planned acceptance gates fail:

1. **Σ.H.1 wins must survive (Q04 / Q05 / Q21 within ±3% of v0.3.0)** —
   Q21 regresses **+6.0%** (40.88 → 43.33 ms). Q04 and Q05 hold.
2. **No new broken queries** — Σ.H.1d breaks **Q02** and **Q15** entirely
   (5/5 runs return Err where v0.3.0 runs them in ~10 ms / ~16 ms).

Geometric mean over the 20 comparable queries is 1.0054 (~parity at face
value), but the headline number hides two query-level failures.

## Per-query table

| Q | v0.3.0 (med-of-med ms) | Σ.H.1d (med-of-med ms) | Δ% | Note |
|---|---:|---:|---:|---|
| Q01 | 28.80 | 29.37 | +2.0% | |
| Q02 | 9.98 | **BROKEN** | — | Arrow: Invalid comparison Int64 == Float64 |
| Q03 | 14.34 | 14.28 | −0.4% | |
| Q04 | 13.38 | 13.26 | −0.9% | |
| Q05 | 22.30 | 22.06 | −1.1% | |
| Q06 | 12.86 | 12.80 | −0.5% | |
| Q07 | 28.95 | 29.47 | +1.8% | |
| Q08 | 21.58 | 21.46 | −0.6% | |
| Q09 | 30.43 | 28.61 | **−6.0%** | win |
| Q10 | 30.28 | 30.04 | −0.8% | |
| Q11 | 7.53 | 7.65 | +1.6% | |
| Q12 | 14.55 | 14.62 | +0.5% | |
| Q13 | 41.71 | 42.19 | +1.2% | |
| Q14 | 11.67 | 11.79 | +1.0% | |
| Q15 | 16.18 | **BROKEN** | — | |
| Q16 | 9.10 | 9.20 | +1.1% | |
| Q17 | 38.30 | 38.11 | −0.5% | |
| Q18 | 51.32 | 52.69 | +2.7% | |
| Q19 | 17.29 | 17.90 | **+3.5%** | regression |
| Q20 | 16.41 | 16.34 | −0.4% | |
| Q21 | 40.88 | 43.33 | **+6.0%** | Σ.H.1 win regression |
| Q22 | 8.36 | 8.45 | +1.1% | |

Geomean Σ.H.1d / v0.3.0 over 20 comparable queries: **1.0054** (≈parity).

## Diagnosis

### Q02 / Q15 break

- Reproduces deterministically at HEAD (`793016f`).
- Disappears with `EMAT_DISABLE_FILTER_MULTI_AGG=1` (rule disabled).
- Disappears at `5183024` (the commit immediately before Σ.H.1d.2 / Σ.H.1d.4)
  even with the *same* `fused_aggregate_filter_multi_agg_numeric.rs` file
  on disk (the scaffold from 118b468 is reachable from 5183024).

So the regression is introduced by **Σ.H.1d.4's dispatch wiring**
(`resolve_keys_unified` + `match ResolvedKeys` in `try_build_replacement`),
not by the spec module itself. Inspection of the diff doesn't reveal an
obvious behavioral change on the String path — both 5183024 and 793016f
build a `FilterMultiAggSpec` with identical inputs for the String case
— but empirically the runtime emits an `Int64 == Float64` comparison
error for Q02's subquery shape (`select min(ps_supplycost) from ...`
with no GROUP BY).

### Q19 / Q21 regressions

Above the ±3% noise floor; pattern-consistent with Σ.H.1b's earlier
regressions on the *string* path (Q19 +3.5%, Q21 +6.0%). The Phase A
"isolation" of the numeric module evidently did not preserve the string
path's codegen byte-for-byte in practice — even though the spec module
file is untouched, adding the dispatch wiring + a second `try_new` call
site in the same function appears to perturb LLVM's inlining decisions
on the hot path.

## Recommendation

**Revert Σ.H.1d.4** (commit 793016f). Keep Σ.H.1d.1 / Σ.H.1d.2 (the
scaffold + spec module) — they are dormant and harmless, and parking
them avoids re-doing the work if a future bite revives the numeric path.

Rationale:
- Σ.H.1d.4 has **zero** measured positive impact on the 22 TPC-H queries
  at SF=1 (no query exercises the numeric path — Q11's would, but its
  aggregate-input type rejects the rule on a separate path).
- It introduces **two** broken queries and **one** real string-path
  regression (Q21 +6.0%).
- The diagnosis (sub-microsecond JIT side-effects vs. LLVM inlining)
  is not cheaply addressable.

Future numeric-keyed agg work should either:
1. Carry a non-TPC-H micro-benchmark that proves the numeric path is
   worth landing before the wiring, or
2. Rebuild the dispatch as a separate `PhysicalOptimizerRule` so the
   string rule's body remains untouched.
