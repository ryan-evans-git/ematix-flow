# Σ.AH.X re-validation complete 2026-05-27 — only Lever A ships

**Status:** All in-scope opt-in mechanisms re-validated under interleaved A/B strict protocol. **Only `EMAT_L9_FUSED_PROBE` (Lever A) is now default-on.** Lever G + Story 2a remain opt-in.

## Final per-lever findings under interleaved A/B (Σ.AI.2)

| Lever | Net Δ | Clear wins | Clear regressions | Decision |
|---|---:|---|---|---|
| **Lever A** (`EMAT_L9_FUSED_PROBE`) | -45 to -82 ms (-1.3 to -2.3%) | Q21 -40 to -54ms, Q11 -0.8ms | none | **DEFAULT-ON 2026-05-27** |
| Lever G (`EMAT_REORDER_SHAPE_GATED`) | +26 ms (+0.7%) | Q19 -20.6ms | none (high variance) | opt-in |
| Story 2a (`EMAT_L9_MAX_EXPECTED_KEYS=25000`) | +97 ms (+2.92% slower) | none | **Q05 +8.4, Q10 +16, Q12 +7.9** | opt-in (net-negative alone; only useful as safety net for EMAT_REORDER) |

## What we learned about Story 2a

The original AH.3 Story 2a closure documented: "the gate is redundant with `require_filtered_build` for the baseline plan." Today's interleaved A/B confirmed this empirically: enabling the gate on the baseline plan rejects L9 emits that **were doing useful work** on Q05/Q10/Q12. The gate only makes sense as a **safety net** composed with `EMAT_REORDER` (which can create wasteful 50000-key L9 emits when reorder happens).

## What we learned about Lever G

Lever G alone has Q21 +22% catastrophic regression (when its reorder over-fires L9 emits on lineitem.l_orderkey). Composing with Story 2a as safety net brings net to ~0%, but several queries still mildly regress. **Q19 -20.6 ms is a NEW finding** that only appeared in this session's interleaved A/B — wasn't measured in any prior loose/sequential bench. Could be a real Q19-specific win worth deeper investigation, OR could be one-bench noise that's still above the (high-variance) 2σ bar.

## Banked infrastructure (cumulative)

Default-ON:
- **`EMAT_L9_FUSED_PROBE`** (2026-05-27) — opt-out via `=0`

Opt-in:
- `EnableRuntimeBloomSidebandRule::max_expected_keys_per_partition` — `EMAT_L9_MAX_EXPECTED_KEYS=N` (default 0)
- `reorder_inner_joins_shape_gated()` + `ReorderOpts` — `EMAT_REORDER=1 EMAT_REORDER_SHAPE_GATED=1`
- `EMAT_L9_TIGHT_CARDINALITY=1` (AH.2 Story 1'.3)

Bench harness:
- `scripts/bench/strict_22q.sh` — single-mode strict
- `scripts/bench/strict_ab.sh` — interleaved A/B strict
- `scripts/bench/strict_summarize.py` + `strict_diff.py`

## What's next

The Σ.AH.X salvage arc is fully validated. Three remaining directions:

1. **Investigate Q19 -20.6 ms win** under Lever G — if reproducible, design a shape predicate that captures Q19's win cleanly (much narrower than the existing Lever G shape gate). ~1-2 hours wall.
2. **Re-validate Σ.AH.2 Stage 6 findings** under interleaved A/B — the Q01 -7 / Q03 -6 / Q06 -5 wins from the original loose measurement should be re-checked. May reveal additional default-on candidates.
3. **Pivot to operator-level work** — Q17 HashJoin + AVG kernel, the actual structural bottleneck per AH.1 stage profile. Higher-ceiling, ~1-2 weeks.

Recommendation: **option 1 (Q19 deep-dive)** is fastest and tests a single concrete hypothesis (Q19 might benefit from EMAT_REORDER specifically). If Q19 reproduces, we have a candidate for narrow shape-predicate shipping.

## Σ.AH meta-arc status (final-final)

| Arc | Status |
|---|---|
| Σ.AH.1 (decode-skip) | REJECTED 2026-05-27 |
| Σ.AH.2 (Partitioned-mode L9) | CLOSED 2026-05-26 |
| Σ.AH.3 (build-vs-probe side-swap) | CLOSED 2026-05-27 |
| Σ.AH.4 (partition-count generalization) | Completed 2026-05-26 |
| **Σ.AH.X Lever A** | **DEFAULT-ON 2026-05-27 (the ship)** |
| Σ.AH.X Lever G | opt-in (interleaved A/B confirmed net-neutral; Q19 win deserves follow-up) |
| Σ.AH.X Story 2a (L9 gate) | opt-in (interleaved A/B shows net-slower alone; useful only as EMAT_REORDER safety net) |
| Σ.AH.X Levers B/C/D/E/F | deferred |
| Σ.AI.1 (strict bench protocol) | LANDED 2026-05-27 |
| **Σ.AI.2 (interleaved A/B harness)** | **LANDED 2026-05-27** |

## Archived plans

- [`2026-05-27-sigma-ah-3.md`](archive/2026-05-27-sigma-ah-3.md)
- [`2026-05-27-sigma-ah-1.md`](archive/2026-05-27-sigma-ah-1.md)
- [`2026-05-27-sigma-ah-2.md`](archive/2026-05-27-sigma-ah-2.md)
- [`2026-05-26-sigma-ah-survey.md`](archive/2026-05-26-sigma-ah-survey.md)
- [`2026-05-25-sigma-t-v5-tier-1.md`](archive/2026-05-25-sigma-t-v5-tier-1.md)

## Deferred plans

- [`sidecar-deferred.md`](sidecar-deferred.md) — V5 Tier 5 sidecar read + adaptive work.
