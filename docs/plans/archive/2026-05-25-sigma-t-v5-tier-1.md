# Σ.T V5 Tier 1 — universal amplifying-with-scale levers

**Archived:** 2026-05-26 to make room for the post-Σ.AG.7 clean-slate stage-profiling review (`docs/plans/CURRENT.md`). Status at archive time: Phase 1 dropped, Phase 2 ACTIVE on Story 2.1 (kernel scaffold not started). L13 / L14 implementation remains a viable downstream arc; the new review will independently surface candidates and may or may not re-prioritise this work.

---

# Σ.T V5 Tier 1 — universal amplifying-with-scale levers

**Status:** active
**Created:** 2026-05-25
**Branch policy:** one PR per story unless a sub-bite is gated on perf / data / external dependency, per `feedback_fewer_prs.md`.
**Roadmap parent:** [`docs/PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE_V5.md`](../PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE_V5.md) — V5 §1 Tier 1 plus §2 sequencing rationale; lever descriptions in [`V2`](../PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE_V2.md) §2.2 L3, §2.3 L13, §2.3 L14.
**Predecessor plan:** [`docs/plans/sidecar-deferred.md`](./sidecar-deferred.md) — L1/L15 sidecar work (V5 Tier 5, scale-specific) deferred. Resumes after this plan's Phase 2 completes (Phase 1 dropped 2026-05-25 — see `docs/PHASE_PGO_BASELINE.md`), or earlier if a dedicated engineer picks it up in parallel.

---

## Summary

V5 re-scored V2's 18 levers by **scale-universality** — which levers benefit SF=1, SF=10, and SF=100 in proportion. Tier 1 levers amplify with scale (SF=100 gains > SF=10 gains > SF=1 gains, all positive). This plan now ships V5 Tier 1 in two phases after the Phase 1 PGO empirical result fell below the acceptance bar (see `docs/PHASE_PGO_BASELINE.md`):

1. ~~**Phase 1 — L3 PGO release build**~~ **[DROPPED 2026-05-25]** — Linux x86_64 measurement (minipc) showed only +1.44pp geomean improvement vs. the ≥3pp acceptance gate, with Q14 at +2.06% per-query regression (just over the ≤2% bar). Most of the workload (the multi-join queries Q03 / Q05 / Q07–Q09) is memory-bandwidth-bound on SF=10 per V5 §5.2 — codegen quality is not the bottleneck. PGO scripts and pipeline (`scripts/pgo/` on branch `feat/pgo-instrumented-build`) are unmerged but available if hardware mix changes warrant revisiting.
2. **Phase 2 — L13 custom hash join** (2-3 person-quarters, blast radius 3). Sibling-crate kernel + ExecutionPlan wrapper. Reuses Σ.N.f.3 RobinHood substrate. Amplifying-with-scale: 2-4pp at SF=1, 10-13pp at SF=10, 18-25pp at SF=100. **Promoted to ACTIVE** after Phase 1's drop.
3. **Phase 3 — L14 dict-preserved end-to-end** (4-6 weeks, blast radius 2). Σ.K.2 dict-routing extended to default-on for low-cardinality strings; per-shape persistence via Σ.L workload log; speculative-race probe avoids the `project_sigma_k_dict_arrival_ab.md` regression regime. 1-2pp at SF=1, 3-5pp at SF=10, 5-10pp at SF=100.

**Full archived content** — for the full text of this plan as it stood at archive time, see git history at branch `feat/l13-bloom-emitter` HEAD~ or earlier (commit 9f597a4 includes the full unaltered plan body). Truncated here to keep the archive directory readable.
