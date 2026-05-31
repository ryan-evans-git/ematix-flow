# Σ.AH.X — Shape-aware L9 enablement (salvage arc)

**Status:** drafted, opportunistic (not active)
**Parent finding:** Σ.AH.1 + Σ.AH.2 arc closures left ~30-40 ms of measured per-query wall savings on the table — opt-in flags that win on specific queries but can't be flipped default-on because losers cancel winners.
**Predecessors:** [Σ.AH.1 (rejected)](archive/2026-05-27-sigma-ah-1.md), [Σ.AH.2 (closed)](archive/2026-05-27-sigma-ah-2.md). Memories `[[sigma-ah-1-arc-rejected]]`, `[[sigma-ah-2-arc-closed]]`.

## Hypothesis

Six small, deferred levers each show **real per-query wins** that we couldn't bank because we lacked a per-query selection mechanism. Combined, they represent ~40-60 ms of measured wall savings across ~6-8 queries (~1.5-2 pp 22q SF=10 geomean).

The unsolved problem is **shape-aware enablement**: a planning-layer predicate that selects the right opt-in lever per query. Σ.AH.2 Stage 5 attempted this with the build/probe_distinct ratio and went the wrong direction. Σ.AH.X is the correct framing: profile-driven shape predicates, one per lever, gated by per-query opt-in until soak.

## Inventory of deferred wins

### A. EMAT_L9_FUSED_PROBE per-query (Σ.AH.2 Stage 6 deferred, Σ.AH.1 Mode B confirmed)

22q Stage 6 sweep + AH.1 Phase 0 spike both measured per-query deltas with fused-probe ON:

| Query | Δ wall | Source | Notes |
|---|---:|---|---|
| Q01 | -7 ms | AH.2 Stage 6 | clean win |
| Q03 | -6 ms | AH.2 Stage 6 | clean win |
| Q06 | -5 ms | AH.2 Stage 6 | clean win |
| Q07 | -7.9 ms | AH.1 spike mode B | also +9.5 in Stage 6 → at noise edge |
| Q17 | -6.9 ms | AH.1 spike mode B | clean win |
| Q21 | -14 ms | AH.2 Stage 6 | biggest single win |
| Q18 | +13 ms | AH.2 Stage 6 | regression; loser |
| Q07 | +9.5 ms | AH.2 Stage 6 | regression in this trial |

**Net unbanked**: ~30-40 ms wall across 5-6 reliable winners. Can't flip default-on because Q18 and Q07's bad-trial cancel.

**Shape predicate (hypothesis)**: fused-probe wins when the bloom-filtered scan is small (Q06, Q17 lineitem outer scan) or when the join key is dict-encoded with high reuse (Q01, Q03, Q21). Loses when the scan is large AND has high downstream join cost (Q18 60M lineitem → orders join domination).

### B. EMAT_L9_TIGHT_CARDINALITY for Q17-class shapes (Σ.AH.2 Story 1'.3 + AH.1 Mode C)

AH.1 spike mode C (fused + tight): **Q17 -32 ms** — biggest single deferred win. But Q07 +11, Q08 +55 (Σ.AH.2), Q18 +40.

**Shape predicate (hypothesis)**: tight cardinality wins when the build-side is a small Eq-filtered dim hitting a narrow lineitem column (Q17 `l_partkey` ←  `part` filtered to ~200 rows). Loses when the probe-side scan is large *and* the post-filter pass rate is still ≥ 0.1% (Q08, Q18 — bloom probe + filter cost > saved join work).

### C. Stage 4 filter-once-per-RG (Σ.AH.2, REVERTED `e5222d7`)

ROI was marginal (~1-2 ms / L9-firing partition) but the **underlying bug fix (StringView/DictUtf8 full-RG slice conversion) is reusable infra**. Σ.AH.2 closure called this out as a future lever.

**What's needed**: correct `slice_decoded(&col, 0, total_pre, target)` for StringView and DictUtf8 columns at the full-RG boundary. Test fixture: any of Q07/Q08/Q12/Q21 (the four that broke when Stage 4 landed).

### D. Dict-aware bloom probe (Σ.AH.1 Outcome B lever, untested)

Stage profile refuted the *Q17* application (per-row probe is not dominant there). But the lever could win on queries where:
- The filter column IS dict-encoded (`column_is_dict_encoded` returns true), AND
- The bloom-filterable scan IS a meaningful fraction of total compute (≥ 20%), AND
- The probe column has high reuse (dict cardinality << row count)

Candidates: Q08 `p_type` (Inexact(150) per Σ.AH.2 Story 1'.2), Q09 `n_name` if it survives planner-time. **Needs measurement.**

### E. Σ.AH.2 Story 1'.2 dict-distinct (banked, but underutilized)

Already banked at `956d65f` — `column_stats.distinct_count = Inexact(max_per_rg)` from parquet dict pages. **Currently consumed only by `estimate_filter_selectivity_via_emat_stats` (the tight-cardinality rule).** Any other rule that wants accurate distinct counts can use it: Σ.AH.3 build-vs-probe cardinality estimator, hash-table presizing, dict-grouping rules. Σ.AH.X should track which downstream rules pick it up.

### G. Shape-detected EMAT_REORDER for cust⋈orders (Σ.AH.3 sliver, banked 2026-05-27)

Σ.AH.3 spike measured `EMAT_REORDER=1` across the 5 target queries (Q07/Q08/Q09/Q10/Q14). Q10 cleanly wins -25 ms (-9%) on `cust ⋈ orders` shape across multiple trials. The other 4 target queries regress (Q07 +30%, Q09 +32%) because the estimator can't model their multi-join shapes. A shape-detector that fires EMAT_REORDER ONLY on single-equijoin-Inner `cust ⋈ orders`-like patterns captures Q10's win cleanly.

**Pre-requisites**: `crates/ematix-flow-core/src/join_reorder.rs::reorder_inner_joins` already implements the rewrite. Need to add a shape predicate that runs before the existing 3+ leaf chain detection: detect when the chain reduces to a 2-leaf `customer ⋈ orders` (single Inner, single equi-key, both leaves are TableScan or FilterExec on TableScan).

**Effort**: 1-2 days.

**Predicted impact**: -25 ms wall on Q10 cleanly. No other query affected.

**Risk**: low — shape predicate is tight; rule is opt-in until soak.

### F. Q07 noise-band lever (separate session, not in inventory)

Q07 appears with both signs in different trials (-7.9 in AH.1 mode B, +9.5 in AH.2 Stage 6). The ±8 ms σ confirms noise, but it also means we can't tell whether Q07 is a true winner or a true loser without **higher-trial-count A/B isolation**. Lever F: re-bench Q07 standalone, 20 trials, both modes, to settle the question.

## Predicted impact (combined)

| Lever | Best-case per-query | Confidence | Comments |
|---|---:|:---:|---|
| A. Per-query fused-probe allowlist | 30-40 ms across 5 queries | high | direct measurement exists |
| B. Tight-cardinality for Q17-class | 30 ms on Q17 | medium | needs shape predicate |
| C. Stage 4 fix | 5-10 ms across L9-firing partitions | low | infra value > direct perf |
| D. Dict-aware bloom probe | unknown, 5-30 ms band | low | untested; needs spike |
| E. Dict-distinct downstream consumers | indirect | n/a | enables other arcs |
| F. Q07 noise resolution | ±8 ms | n/a | settles a measurement question |
| G. Shape-detected EMAT_REORDER for cust⋈orders (Σ.AH.3 sliver) | -25 ms on Q10 | high | direct AH.3 spike measurement |

**Combined target: 75-105 ms wall = ~2.5-3.5 pp 22q SF=10 geomean.** Lower confidence than any individual arc, but the floor (Lever A + Lever G alone) is already ~1.5 pp with direct measurement evidence.

## Effort estimate

**1-2 weeks per lever, total 3-4 person-weeks for the full salvage.** Most levers are small (a few-hundred-line shape predicate + bench-gate). The exception is Lever C (Stage 4 fix) which requires careful StringView/DictUtf8 work.

## Risk level

**L-M.** Each lever is independent and gated by a per-query shape predicate, so a single bad lever can't regress the bench. Risk is mostly the **codegen-sensitivity tax** (memory `[[optimizer-codegen-sensitivity]]`) from installing the shape walker — mitigated by gating each lever behind its own env var until soak.

## Hard constraints (inherited)

- **No new PhysicalOptimizerRule.** Implement levers as pre-plan walkers + per-query env-var or shape predicates.
- **No TPC-H-specific hardcoding.** Per `[[feedback-no-tpch-hardcoding]]`, the shape predicates must be generalised — e.g., "small build × dict-encoded i64 probe column" is OK; "Q17-specific" is not.
- **TDD per `[[feedback-tdd]]`.** Each shape predicate gets unit tests on synthetic LogicalPlans before bench gating.
- **Bench gate at the lever level** — each lever ships independently when its 22q A/B clears: per-query no-regression bar 3%, geomean ≥ baseline.

## Story skeleton (no tasks yet)

- **Story 1 — Lever A: per-query fused-probe enablement.** Write a shape walker that detects "bloom-filtered scan is small relative to total query compute" — likely a heuristic on `partition_statistics` post-filter row counts. Gate via `EMAT_L9_FUSED_PROBE_AUTO=1`. Confirm Q01/Q03/Q06/Q17/Q21 fire, Q18 doesn't. 22q SF=10 bench-gate.
- **Story 2 — Lever B: tight-cardinality shape predicate.** Detect "small Eq-filtered dim + narrow lineitem column" shape. Don't fire on Q08-shape (lineitem 60M with 0.1%+ pass rate). 22q SF=10 bench-gate, especially Q07/Q08/Q17/Q18 watch.
- **Story 3 — Lever C: Stage 4 StringView/DictUtf8 fix.** Reproduce the Σ.AH.2 Stage 4 correctness bug on Q07/Q08/Q12/Q21. Fix `slice_decoded` for StringView (offsets + buffers must rebase) and DictUtf8 (key array slice + dict reuse). Land filter-once-per-RG behind `EMAT_FILTER_ONCE_PER_RG=1`. Bench-gate.
- **Story 4 — Lever D: dict-aware bloom probe spike.** 2-day spike on Q08 and Q09 dict-encoded columns. Implement `dict_pass_bits` array for dict-encoded i64 filter columns under bloom probe. Measure. Ship if either query gains ≥ 10 ms.
- **Story 5 — Lever E: dict-distinct downstream consumers audit.** No code; survey what rules could use `column_stats.distinct_count`. File follow-up arcs.
- **Story 6 — Lever F: Q07 noise resolution.** 20-trial × 2-mode Q07 bench. Decide whether Q07 stays in the Lever A allowlist or not.

## What this arc is NOT

- **Not Σ.AH.3.** Σ.AH.3 is build-vs-probe side-swap — a single mechanism. Σ.AH.X is a salvage portfolio of 6 small mechanisms.
- **Not a "fix Σ.AH.2"-style rework.** Σ.AH.2 closed honestly; its banked opt-in infra (`EMAT_L9_FUSED_PROBE`, dict-distinct) is the substrate Σ.AH.X uses.
- **Not a default-on flip.** Each lever stays opt-in even after ship; only the *shape predicate* fires automatically.

## Sequencing recommendation

**Run after Σ.AH.3.** Σ.AH.3 is higher-impact per arc (60-80 ms predicted) and uses overlapping infra (post-filter cardinality estimator). Σ.AH.X can compose with Σ.AH.3 — once side-swap is correct, some of Lever A's loser queries (Q18) might become winners under a different join order.

If Σ.AH.3 also gets rejected, Σ.AH.X becomes the primary follow-up — it has lower predicted impact but higher confidence (each sub-lever has direct measurement evidence).

## References

- Σ.AH.1 spike numbers: [docs/PHASE_SIGMA_AH_1_DESIGN.md](../PHASE_SIGMA_AH_1_DESIGN.md) § 4
- Σ.AH.2 closure: memory `[[sigma-ah-2-arc-closed]]`
- Σ.AH.1 rejection: memory `[[sigma-ah-1-arc-rejected]]`
- Stage 6 22q sweep numbers: see `[[sigma-ah-2-arc-closed]]` § 7
- Codegen-tax precedent: `[[optimizer-codegen-sensitivity]]`
- Stage 4 bug origin: Σ.AH.2 Story 1'.4 Stage 4, commit `e5222d7` (reverted)
- Dict-distinct infra: commit `956d65f` (Σ.AH.2 Story 1'.2)
- Fused-probe infra: commit `8c9a3c2` (Σ.AH.2 Story 1'.4 Stage 1)
