# Current progress

**Active plan:** [`docs/plans/CURRENT.md`](../plans/CURRENT.md) — Σ.AH: clean-slate Q01-Q22 SF=10 execution-plan inefficiency review (per-stage waste survey on post-Σ.AG.7 numbers)
**Started:** 2026-05-26
**Active phase:** Σ.AH COMPLETE — ready to archive (Phase D drafted 2026-05-26)
**Phase D summary:** 3 arc shells written (`docs/plans/sigma-ah-arc-{1,2,3}.md`). Sequencing memo appended to `PERF_REVIEW_2026_05.md`. **Recommended next CURRENT.md: Σ.AH.2 (L9 Partitioned-mode extension)** — biggest aggregate impact + unblocks Σ.AH.1 cascade. Pre-work: Σ.AH.4 (customer.parquet re-emit) as a chore.
**Phase C summary:** Synthesis written to `docs/PERF_REVIEW_2026_05.md`. 6 arc candidates ranked (Σ.AH.1-6); 4 rejection re-look flags surfaced (Σ.S.B cascade, Σ.Q.L11, Σ.R.2, Σ.T). Combined wall-time impact estimate: 400-500 ms across 8-10 queries = ~8-12 pp SF=10 geomean.
**Phase B summary:** All 22 PERF_Q*.md files updated. Methodology corrections discovered mid-sweep: projection-cost-aware FilterExec floor (B.4), per-column Snappy floor (B.1 redo). Major correction: Q05 dominant operator was misidentified in 2026-05-25 — corrected B.5.

**Phase A summary (commit `0821002`):** Audit appendix appended to `STAGE_PROFILING_METHODOLOGY.md`. 6 VERIFIED, 1 STALE (hash agg 10K-1M groups moved from 5-15 ns/row → 3-7 ns/row post-Σ.N.f), 5 UNVERIFIED (accept as published). A.2 skipped (threshold ≥3 STALE not reached).

Phase A estimate: 0.5–1 day. Phase B estimate: 3 person-days (22 queries × 30–60 min, hard 2-hour cap each). Phase C estimate: 1 day. Phase D estimate: 1 day.

**Branch policy:** local commits only on this plan; no PRs until a Phase D arc is sized and gated.

**Previous active plan (archived 2026-05-26):** [`docs/plans/archive/2026-05-25-sigma-t-v5-tier-1.md`](../plans/archive/2026-05-25-sigma-t-v5-tier-1.md) — Σ.T V5 Tier 1 (L13 custom hash join, Story 2.1 not started). May resume after this survey's Phase D depending on the arc ranking.

**Deferred:** [`docs/plans/sidecar-deferred.md`](../plans/sidecar-deferred.md) — V5 Tier 5 sidecar read+adaptive work.
