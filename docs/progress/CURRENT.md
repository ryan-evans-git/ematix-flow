# Current progress

**Active plan:** [`docs/plans/CURRENT.md`](../plans/CURRENT.md) — **Σ.AH.X FROZEN 2026-05-27**
**Status:** Lever G banked opt-in (Q10 -20ms lost in 22q noise); Lever A REJECTED via 3-invocation validation (fused-probe net +2.9% slower; 7 consistent regressions vs 2 wins). Remaining levers B/C/F all derived from similarly noisy single-invocation measurements — deferred.
**Major methodological finding banked:** SF=10 22q noise floor at single-invocation × 5-trial is high enough that most per-query wins < 5-10 ms are below detection. AH.2 Stage 6's "net-zero" was biased ~40× low. Memory `[[bench-methodology-3-invocations]]`.
**Next direction options:** (a) SF=100 measurement, (b) stricter bench protocol, (c) operator-level pivot (Q17 HashJoin + AVG).

**Prior arcs:** Σ.AH.3 closed 2026-05-27, Σ.AH.1 rejected 2026-05-27, Σ.AH.2 closed 2026-05-26 (note: its Stage 6 "net-zero" finding is now known to be biased low).
**Branch policy:** local commits only on this plan. PR only after combined 22q bench gate clears.

**Prior arcs:** Σ.AH.3 closed 2026-05-27 `[[sigma-ah-3-arc-closed]]`; Σ.AH.1 rejected 2026-05-27 `[[sigma-ah-1-arc-rejected]]`; Σ.AH.2 closed 2026-05-26 `[[sigma-ah-2-arc-closed]]`.

---

## Σ.AH meta-arc status

The Σ.AH survey (Phases A-D) completed 2026-05-26 and archived to [`docs/plans/archive/2026-05-26-sigma-ah-survey.md`](../plans/archive/2026-05-26-sigma-ah-survey.md).

- **Phase A** (audit): 6 VERIFIED / 1 STALE / 5 UNVERIFIED constants. A.2 skipped.
- **Phase B** (22q sweep): all 22 PERF_Q*.md re-verified. 15/22 at-floor; 7 with Q-specific waste; 3 methodology corrections captured (per-col Snappy, projection-aware FilterExec, Q05 dominant-stage).
- **Phase C** (synthesis): [`docs/PERF_REVIEW_2026_05.md`](../PERF_REVIEW_2026_05.md) — 6 ranked arcs, 4 rejection re-look flags, ~8-12 pp total geomean potential.
- **Phase D** (arc shells): 3 shells drafted (Σ.AH.1, AH.2, AH.3); AH.2 promoted to active.

**Σ.AH.1 (L9 scan-level integration)** **rejected 2026-05-27** via Phase 0 spike. Original shell at [`docs/plans/sigma-ah-arc-1.md`](../plans/sigma-ah-arc-1.md); archived design at [docs/PHASE_SIGMA_AH_1_DESIGN.md](../PHASE_SIGMA_AH_1_DESIGN.md) § 4.

**Σ.AH.3 (build-vs-probe side-swap)** drafted at [`docs/plans/sigma-ah-arc-3.md`](../plans/sigma-ah-arc-3.md). Now the most-promising remaining Σ.AH arc after AH.1 rejection.

**Σ.AH.X (shape-aware L9 enablement / salvage arc)** drafted 2026-05-27 at [`docs/plans/sigma-ah-arc-X.md`](../plans/sigma-ah-arc-X.md). Bundles 6 deferred per-query wins from AH.1+AH.2 closures (~30-40 ms unbanked wall). Runs after Σ.AH.3.

**Σ.AH.4 (partition-count generalization)** completed 2026-05-26 as code-only commit; the original parquet re-emit was reverted to preserve the captured baseline.

**Σ.AH.5/AH.6** parallel-track candidates; not currently active.

---

## Archived plans

- [`2026-05-27-sigma-ah-1.md`](../plans/archive/2026-05-27-sigma-ah-1.md) — Σ.AH.1 rejected via Phase 0 spike 2026-05-27
- [`2026-05-27-sigma-ah-2.md`](../plans/archive/2026-05-27-sigma-ah-2.md) — Σ.AH.2 closed with net-zero 2026-05-26
- [`2026-05-26-sigma-ah-survey.md`](../plans/archive/2026-05-26-sigma-ah-survey.md) — Σ.AH survey (Phase A-D complete)
- [`2026-05-25-sigma-t-v5-tier-1.md`](../plans/archive/2026-05-25-sigma-t-v5-tier-1.md) — Σ.T V5 Tier 1 (L13 custom hash join). **Superseded** by Σ.AH Phase B evidence: DataFusion's HashJoinExec is at-or-below kernel floor on every measured query, so L13's "the kernel is slow" premise doesn't hold. L13 moves to rejection-re-look pile.

## Deferred plans

- [`sidecar-deferred.md`](../plans/sidecar-deferred.md) — V5 Tier 5 sidecar read + adaptive work.
