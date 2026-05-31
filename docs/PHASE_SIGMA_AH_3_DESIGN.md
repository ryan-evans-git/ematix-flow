# Σ.AH.3 — Build-vs-probe side-swap: design

**Status:** **CLOSED 2026-05-27** with banked defensive infra; original arc framing rejected, but the dig-in surfaced one real (small) win and ruled out the L9-over-fire hypothesis. Memory: `[[sigma-ah-3-arc-closed]]`.
**Arc shell:** [`docs/plans/sigma-ah-arc-3.md`](plans/sigma-ah-arc-3.md).
**Active plan:** [`docs/plans/CURRENT.md`](plans/CURRENT.md).
**Predecessor lesson:** [Σ.AH.1 rejected](plans/archive/2026-05-27-sigma-ah-1.md) via 2-day spike that saved 3-4 weeks. Same discipline here.

## 1. Decision summary

**Phase 0 (spike, 2 days) before committing to the full arc.** Σ.AH.3's mechanism is more straightforward than AH.1's (pure planner rewrite, no decode-time work), but the predicted impact (60-80 ms wall across 5 queries) depends on **two assumptions** that need empirical confirmation before we spend 2-3 weeks:

1. **The post-filter cardinality estimator (Σ.AE.1) correctly identifies the 5 build-side-too-big cases** — Q07, Q08, Q09, Q10, Q14. If the estimator misjudges, the rule fires wrong (false positives = regressions; false negatives = no gain).
2. **Manually swapping sides on one of the 5 queries actually saves wall time.** The DataFusion HashJoin probe-side is parallel-iterating against a build-side hash table. Swapping just exchanges which side is parallel — the *win* requires that the smaller side be both faster to build AND the larger side benefits from being probe (cache locality, partial-agg interaction).

The spike has three honest outcomes:

- **(A) Both assumptions hold + manual swap on Q09 saves ≥ 20 ms wall.** Commit to the full arc; Stories 2-5 as drafted.
- **(B) Estimator works but manual swap saves < 10 ms.** Narrow the arc to a "guard" rule that prevents existing wins from being undone in future planner changes — defensive, not offensive. Effort drops to ~1 week.
- **(C) Estimator is unreliable on these queries, OR manual swap doesn't move wall on Q09.** Reject the arc, capture finding, pivot to Σ.AH.X (salvage arc) or operator-level Q17 work.

## 2. Why this needs a spike (lessons from AH.1)

The AH.1 arc shell predicted Q17 -80 ms by "skipping decoding entirely" — a prediction based on a structural cost model (60M rows × per-row decode cost) that turned out to be wrong (the Q17 outer scan is only 155 ms of 6970 ms total compute, and most of that isn't bloom-filterable decode).

The same risk exists here. The AH.3 prediction of ~60-80 ms wall is based on:

- **Cardinality counts** (build 1.46M > probe 117k on Q07 depth 13) — these are correct.
- **Implicit cost model** that swap → wall savings proportional to (build − probe) row delta.

But the actual wall cost of HashJoin building is **not linear in row count**. It includes:
- Hash table allocation (proportional to row count)
- Probe-side iteration (proportional to row count, regardless of which side)
- Cache locality during probe (the smaller side, when probe, gets better hit rate)
- Parallelism behaviour (Partitioned mode hashes both sides; CollectLeft serialises the build)

A 1.46M vs 117k swap on Q07 might save ~30 ms (if hash-build dominates) or ~5 ms (if probe iteration dominates). The spike answers this empirically.

## 3. Phase 0 — the spike

### 3.1 What to measure

**Step 1: Estimator audit (1 day).** For each of Q07, Q08, Q09, Q10, Q14, dump the optimized LogicalPlan and identify the target Inner-join node. For each side, get `partition_statistics` → `num_rows` via the existing Σ.AE.1 machinery. Confirm the build > probe cardinality differential the arc shell claims.

| Query | Join node | Expected build | Expected probe | Estimator says |
|---|---|---:|---:|:---|
| Q07 | depth 13 nation+supp ⋈ lineitem | 1.46M | 117k | TBD |
| Q08 | depth 13 nation+region+supp ⋈ lineitem | 1.5M | 122k | TBD |
| Q09 | depth 9 partsupp ⋈ lineitem | 15M | 3.26M | TBD |
| Q10 | depth ? cust ⋈ orders | 1.5M | 573k | TBD |
| Q14 | depth ? part ⋈ lineitem-filt | 2M | 749k | TBD |

**Hard-fail criterion**: if even ONE of the 5 estimator values is off by > 50%, the rule isn't safely shippable. The Σ.AH.2 closure already noted that DataFusion's `FilterExec.statistics()` doesn't consult `distinct_count` for string-Eq selectivity — so any join sitting downstream of a string-LIKE filter (Q09 `p_name LIKE '%green%'`) gets the 0.2 default. That's a known issue. **Q09 specifically is high-risk** for estimator under-count.

**Step 2: Manual-swap wall test (1 day).** Pick **Q09** as the highest-impact candidate (15M build → 3.26M build is the biggest absolute swap). Two methods to manually force the swap:

- **Method A**: rewrite the SQL to invert the join order. Less clean but doesn't require code changes.
- **Method B**: write a one-off `PhysicalOptimizerRule` (in a test-only feature flag) that pattern-matches Q09's specific HashJoinExec and swaps its `left`/`right` children + flips the join keys.

Run 10 trials × 2 warmups at SF=10. Compare swapped vs unswapped wall time on Q09.

### 3.2 Spike pass / fail criteria

- **Pass (Outcome A)**: Step 1 estimator audit passes for ≥ 4 of the 5 queries (Q09 may legitimately fail; others must pass). Step 2 manual Q09 swap saves ≥ **20 ms** wall. Commit to full arc.
- **Partial (Outcome B)**: estimator works but manual Q09 swap saves < 10 ms. Pivot to defensive guard-rule scope. Maybe 1-week arc.
- **Fail (Outcome C)**: estimator under-counts on ≥ 2 queries OR Q09 manual swap shows no wall movement. Reject arc.

### 3.3 Phase 0 deliverable

`/tmp/sigma-ah-3-spike/estimator-audit.md` with the 5-row table from Step 1 plus the Q09 swap A/B numbers from Step 2. Decision documented in § 4 of this file.

## 4. Phase 0 result (2026-05-27): Outcome C — arc REJECTED

**Spike collapsed from 2 days to 2 hours after discovering the mechanism already ships as opt-in `EMAT_REORDER=1` (Σ.T Phase 3, commit `1a14007`).** Stories 2-5 would have rebuilt what `crates/ematix-flow-core/src/join_reorder.rs` already does. The right spike question became: "Does the existing `EMAT_REORDER=1` deliver AH.3's 60-80 ms gain on Q07/Q08/Q09/Q10/Q14?"

### Spike result (SF=10, 10 trials × 2 warmups)

| Query | A (baseline) | B (`EMAT_REORDER=1`) | Δ ms | Δ % | Gate verdict |
|---|---:|---:|---:|---:|:---|
| Q01 | 243.87 ± 14.83 | 240.48 ± 9.59 | -3.4 | -1.4% | noise |
| Q03 | 147.79 ± 3.88 | 149.40 ± 6.08 | +1.6 | +1.1% | noise |
| **Q07** | 155.57 ± 4.61 | 201.49 ± 6.83 | **+45.9** | **+29.5%** | **fail (>3%)** |
| Q08 | 192.16 ± 5.92 | 196.64 ± 8.55 | +4.5 | +2.3% | borderline |
| **Q09** | 283.15 ± 13.24 | 374.39 ± 16.27 | **+91.2** | **+32.2%** | **fail (>3%)** |
| **Q10** | 278.18 ± 11.59 | 253.00 ± 10.47 | **-25.2** | **-9.1%** | **win** |
| Q14 | 91.30 ± 3.00 | 91.05 ± 4.71 | -0.3 | -0.3% | flat |
| Q17 | 278.17 ± 6.27 | 273.34 ± 4.81 | -4.8 | -1.7% | noise |
| Q18 | 337.70 ± 5.74 | 337.87 ± 6.56 | +0.2 | 0% | flat |

### Gate evaluation

- **Q09 ≥ 20 ms drop required**: Actual **+91 ms regression**. ❌
- **Q07+Q08+Q10+Q14 collectively ≥ -30 ms**: Actual **+24.9 ms net** (Q07 +45.9, Q08 +4.5, Q10 -25.2, Q14 -0.3). ❌
- **No query regresses > 3%**: Q07 +29.5%, Q09 +32.2% both violate by an order of magnitude. ❌

### Why the cardinality estimator fails on these queries

The Σ.T Phase 3 archive (commit `1a14007`) already documented this: "Q08 still regresses because string-equality predicate selectivity defaults to 0.1 (too coarse for `p_type='ECONOMY...'` which is ~0.001 selective)."

This spike confirms the same pattern at AH.3's specific target set:
- **Q07**: nation×nation self-join + multi-table chain. The estimator can't accurately model nation-fanout intermediate sizes.
- **Q09**: 15M partsupp ⋈ lineitem + LIKE filter on `p_name`. DataFusion's default 0.2 selectivity for LIKE means the estimator over-estimates the post-filter cardinality and picks a wrong join order.
- **Q08**: similar to Q07 (nation/region + supplier chain).

Q10's clean win (cust ⋈ orders, simple 2-table side-swap with integer Eq filter) is the **only shape** where the estimator is reliable enough.

### Decision

**Reject Σ.AH.3.** The arc's premise ("5 queries have build > probe; swap fixes all 5") is wrong. Only Q10's shape is reliable; Q07/Q08/Q09/Q14 either don't improve or regress catastrophically. The estimator is structurally inadequate for the multi-join shapes the arc targets — same finding the Σ.T arc reached 2026-05-25 at broader scope.

### Q10 sliver — what gets banked

Q10 -25 ms (-9%) is real and is captured by **the existing opt-in `EMAT_REORDER=1` flag**. No new code needed. The flag stays opt-in. Users who specifically want Q10's win can enable it; the default keeps the other queries safe.

Optional future work: add Q10's shape detector to Σ.AH.X as a new lever ("auto-fire `EMAT_REORDER` on cust⋈orders shape only"). This is mechanically straightforward but is opt-in lever, not arc-level work.

### Pivot

Per the Σ.AH meta-arc: pivot to **Σ.AH.X (salvage arc)**, which now has 7 levers (the original 6 + a "Q10 shape-detect EMAT_REORDER" sliver). Combined target moves from ~40 ms to ~65 ms.

Alternative pivot candidates:
- **Q17 HashJoin probe optimization** (operator-level, per AH.1 stage profile) — direct attack on the actual Q17 bottleneck.
- **Σ.AH.7 (LIKE selectivity via dict pages)** — would unlock Q09 by giving the estimator accurate selectivity for `p_name LIKE '%green%'`. **Could also retroactively fix this AH.3 spike's Q09 regression** if the estimator gets accurate LIKE selectivity.

### Artefacts

- `/tmp/sigma-ah-3-spike/A-baseline.md`, `B-reorder.md` — full bench tables

## 4a. Story 2a — L9 selectivity-threshold gate (2026-05-27)

The user pushed back on the surface "reject" finding: *"Could this be exposing a different inefficiency that was previously hidden?"* That triggered a dig-in. The Q07 reorder plan-diff vs baseline showed **2 emits → 5 emits**, with the new ones at `expected_keys_per_partition = 3571 / 3571 / 50000` — far larger than baseline's two 64 emits. Hypothesis: L9 is over-firing on non-selective FK columns; a per-partition build-size ceiling would gate it.

### Implementation

Added `max_expected_keys_per_partition: usize` field to `EnableRuntimeBloomSidebandRule`. After computing `expected_keys`, computes `(expected_keys / build.output_partitioning().partition_count().max(1)).max(64)` and rejects emit if it exceeds the threshold. Env override: `EMAT_L9_MAX_EXPECTED_KEYS=N`. Two new unit tests (`gate_skips_when_build_exceeds_max_expected_keys`, `gate_allows_emit_when_build_below_max_expected_keys`) verify behavior.

### Bench result (SF=10, 10 trials × 2 warmups, 10 queries each)

Four-corner table:

| Query | A (baseline) | B (gate 25k) | D (reorder) | C (reorder + gate 25k) |
|---|---:|---:|---:|---:|
| Q01 | 239.46 | 235.93 | 240.48 | 246.25 |
| Q03 | 145.84 | 145.86 | 149.40 | 165.06 |
| Q07 | 159.37 | 161.41 | 201.49 | 205.91 |
| Q08 | 185.45 | 192.31 | 196.64 | 210.33 |
| Q09 | 282.71 | 283.78 | 374.39 | 412.42 |
| Q10 | 283.23 | 275.95 | 253.00 | 272.20 |
| Q14 | 90.37 | 84.06 | 91.05 | 94.81 |
| Q17 | 275.11 | 275.30 | 273.34 | 319.15 |
| Q18 | 323.57 | 331.89 | 337.87 | 368.57 |
| Q21 | 333.97 | 344.42 | n/a | 389.23 |

**B vs A**: all deltas within 1σ (typical σ = 6-15 ms). The "Q14 -6 ms" looked like a win but plan dump confirms Q14's plan is **identical** with gate ON vs OFF → trial noise.

**C vs D**: gate ON + reorder is WORSE than reorder alone (Q07 +4, Q08 +14, Q09 +38, Q17 +46, Q18 +31). The gate is rejecting useful emits in the reorder plan.

### What the dig-in surfaced (real findings)

**Finding 1: The hypothesis was wrong.** Decomposing Q07 reorder regression: removing 2 emits via the gate adds ~5 ms (i.e., those emits were collectively **saving** 5 ms). The other 42 ms of reorder regression comes from the join-order pick itself, not from L9 over-firing.

**Finding 2: The new gate is redundant with `require_filtered_build`.** Trace shows Q08 baseline has **zero** L9 emits — all rejected by `min_probe_to_build_ratio` or `require_filtered_build` before reaching the new gate. The new gate only sees Inner joins where the build IS pre-filtered (e.g. Q07's supplier-after-nation-chain), and those bloom emits turn out to be net-positive (~5-10% pass rate, not 100% FK-bloom pattern).

**Finding 3: `[[sigma-q-l9-bloom-consumer-findings]]`'s "bloom-on-FK net-negative" pattern is structurally distinct from "large build."** The right gate is `require_filtered_build` (already in place) — checking whether the build subtree has a `FilterExec`. Absolute build size isn't a reliable signal.

### Decision: bank gate as opt-in infra, default 0 (disabled)

Reasons to keep the implementation:
- Defensive insurance if a future planner change exposes a shape the existing gates miss (e.g. LeftSemi/RightSemi with very large builds — different code path)
- Trace + tests document the structural finding for future contributors
- Reverts cleanly if someone tries the same hypothesis

Why opt-in only (not default-on):
- Story 2a bench measured **no baseline wall-time win** at 25k threshold
- Risks regression under EMAT_REORDER (and any future planner shapes) where pre-filtered large builds are useful
- `require_filtered_build` already covers the FK-bloom pattern this gate was meant to catch

### Q10 sliver

Across all four modes, the `cust ⋈ orders` shape (Q10) shows the cleanest reorder behavior: A=283, D=253 (-30 with reorder), B=276, C=272. **Q10 alone gains ~25-30 ms from EMAT_REORDER consistently.** If we wanted to bank just Q10's win, a shape-detector that fires EMAT_REORDER only on `cust ⋈ orders`-like patterns would do it. That's the Σ.AH.X candidate ("Q10 shape-detect" lever in the salvage arc).

### Story 2b / 2c not started

Story 2b (Q10 shape-detect EMAT_REORDER) and Story 2c (combined 22q bench-gate) are no longer needed for AH.3 itself — the Story 2a finding eliminated the AH.3 framing. Q10 sliver moves to the Σ.AH.X salvage arc.

## ~~4. If Outcome A — what shipping looks like~~ (NOT TAKEN)

Stories 2-5 from the arc shell, unchanged. The pre-plan walker scans LogicalPlan Inner joins, queries `partition_statistics` for both sides, swaps if `build_rows > probe_rows × 2.0`. Behind `EMAT_SIDE_SWAP=1` until soak completes.

Tight per-query no-regression bar (3%) is the critical safety net — multiple queries already have hand-tuned plans, and the swap rule must not undo them.

## 5. If Outcome B — defensive guard scope

If the estimator works but the wall delta is < 10 ms per query, the rule isn't worth shipping for direct perf — but it still has value as a **regression guard**. Today's "good" plans (where DataFusion happens to pick the small side as build) could break in a future DataFusion bump if their cost model changes. A guard rule that explicitly forces small-build ordering provides insurance.

Smaller scope: implement only the guard, no offensive swap. Effort drops to ~1 week. Risk: codegen tax for installing yet another walker (per `[[optimizer-codegen-sensitivity]]`).

## 6. If Outcome C — what we capture

The post-filter cardinality estimator from Σ.AE.1 is structurally inadequate for join-side decisions. Both Σ.AH.3 (offensive) and Σ.AH.X Lever B (tight-cardinality shape predicate) depend on it. If it can't be trusted, both arcs become much harder.

Pivot candidates:
- **Σ.AH.X (salvage arc)** — direct measurements; doesn't depend on estimator accuracy.
- **Q17 HashJoin probe optimization** — the AH.1 Phase 0 stage profile identified this as Q17's actual bottleneck. Direct operator-level work.
- **Σ.T resumption** — if cost-based join reorder is needed broadly, this arc's failure is a signal to revisit Σ.T from the archived plan.

## 7. Composition with Σ.AH.X (salvage arc)

Σ.AH.X is sequenced after Σ.AH.3 explicitly because:

- If Σ.AH.3 ships, Q18 (an AH.X Lever A loser) might become a winner under a different join order — composition advantage.
- If Σ.AH.3 fails, AH.X becomes the primary follow-up. Both arcs would compete for "next-up", but AH.X has higher floor confidence because each sub-lever has direct measurement.

The two arcs do not share infrastructure conflicts: AH.3 is planner-level (LogicalPlan walker), AH.X is mostly env-var-gated shape predicates against `partition_statistics`. They co-exist cleanly.

## 8. Open questions

- **OQ-AH.3-A**: does Q09's `p_name LIKE '%green%'` filter survive into the optimized LogicalPlan, and does the estimator return Inexact(0.2) or something tighter? — Step 1 audit answers.
- **OQ-AH.3-B**: does DataFusion's `HashJoinExec::with_swapped_inputs()` (if it exists) preserve the join's output schema, or does the swap require projection rewiring downstream? — Step 2 implementation discovers.
- **OQ-AH.3-C**: does the swap rule need to compose with `PushDownLeftSemiRule` (Σ.Q.L10) or `EnableContextBloomRule` (Σ.J.2.b.vi)? Both fire post-LogicalPlan but pre-physical. — Story 2 audits.

## 9. References

- Arc shell: [docs/plans/sigma-ah-arc-3.md](plans/sigma-ah-arc-3.md)
- Phase C source: [docs/PERF_REVIEW_2026_05.md](PERF_REVIEW_2026_05.md) § Σ.AH.3
- Σ.AE.1 estimator: memory `[[sigma-ae-complete]]`
- Σ.T archived (parent superset): `docs/plans/archive/2026-05-25-sigma-t-v5-tier-1.md`
- AH.1 spike-first precedent: memory `[[sigma-ah-1-arc-rejected]]`
- Salvage arc that's queued after: memory `[[sigma-ah-x-salvage]]`
- Codegen-tax constraint: memory `[[optimizer-codegen-sensitivity]]`
