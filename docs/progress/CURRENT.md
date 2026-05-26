# Current progress

**Active plan:** [`docs/plans/CURRENT.md`](../plans/CURRENT.md) — **Σ.AH.2: L9 emitter Partitioned-mode extension**
**Started:** 2026-05-26 (promoted from Σ.AH Phase D arc shell)
**Active phase:** Pre-work
**Active story:** Σ.AH.4 chore — customer.parquet re-emit with 14 row groups (30-60 min). Then Story 1 (partition-aware bloom merge).

**Predicted impact:** ~150-200 ms wall across Q05/Q07/Q08/Q09 = 3-6 pp SF=10 geomean. Solo target; cascade with Σ.AH.1 adds another 2-3 pp.

**Effort estimate:** 2-3 person-weeks. Risk: M (partition-aware merge serialisation).

**Branch policy:** local commits only on this plan. PR only after Story 4 wall-time gate clears.

---

## Σ.AH meta-arc status

The Σ.AH survey (Phases A-D) completed 2026-05-26 and archived to [`docs/plans/archive/2026-05-26-sigma-ah-survey.md`](../plans/archive/2026-05-26-sigma-ah-survey.md).

- **Phase A** (audit): 6 VERIFIED / 1 STALE / 5 UNVERIFIED constants. A.2 skipped.
- **Phase B** (22q sweep): all 22 PERF_Q*.md re-verified. 15/22 at-floor; 7 with Q-specific waste; 3 methodology corrections captured (per-col Snappy, projection-aware FilterExec, Q05 dominant-stage).
- **Phase C** (synthesis): [`docs/PERF_REVIEW_2026_05.md`](../PERF_REVIEW_2026_05.md) — 6 ranked arcs, 4 rejection re-look flags, ~8-12 pp total geomean potential.
- **Phase D** (arc shells): 3 shells drafted (Σ.AH.1, AH.2, AH.3); AH.2 promoted to active.

**Σ.AH.1 (L9 scan-level integration)** drafted at [`docs/plans/sigma-ah-arc-1.md`](../plans/sigma-ah-arc-1.md). Activates after Σ.AH.2 lands.

**Σ.AH.3 (build-vs-probe side-swap)** drafted at [`docs/plans/sigma-ah-arc-3.md`](../plans/sigma-ah-arc-3.md). Opportunistic; activates after AH.1+AH.2.

**Σ.AH.4 (customer.parquet re-emit chore)** queued as pre-work in AH.2's CURRENT.md.

**Σ.AH.5/AH.6** parallel-track candidates; not currently active.

---

## Archived plans

- [`2026-05-26-sigma-ah-survey.md`](../plans/archive/2026-05-26-sigma-ah-survey.md) — Σ.AH survey (Phase A-D complete)
- [`2026-05-25-sigma-t-v5-tier-1.md`](../plans/archive/2026-05-25-sigma-t-v5-tier-1.md) — Σ.T V5 Tier 1 (L13 custom hash join). **Superseded** by Σ.AH Phase B evidence: DataFusion's HashJoinExec is at-or-below kernel floor on every measured query, so L13's "the kernel is slow" premise doesn't hold. L13 moves to rejection-re-look pile.

## Deferred plans

- [`sidecar-deferred.md`](../plans/sidecar-deferred.md) — V5 Tier 5 sidecar read + adaptive work.
