# v2.0.0 — S0 Foundations (working plan)

Live working doc for **Sprint S0** of [`../V2_SPRINT_PLAN.md`](../V2_SPRINT_PLAN.md).
Design source: [`../V2_TARGET.md`](../V2_TARGET.md). This is the doc that
tracks S0 day-to-day; it gets updated in place as stories move.

## Branch strategy — one long-running `v2` branch

All v2 work (S0–S10) lands on a **single long-running `v2` integration
branch**, not a stream of PRs to `main`.

```
main (protected: PR required, released as v1.x)
  └─ v2  (long-running integration branch — all S0–S10 work)
       ├─ direct commits for small/atomic work (v2 is unprotected)
       └─ short-lived sub-branches for risky work, merged into v2
```

Rules:
- **Commit S0–S10 work directly onto `v2`** (it's unprotected; matches
  the "fewer PRs" preference). Use a short-lived sub-branch only when a
  change is risky enough to want an isolated CI dispatch first.
- **Keep `v2` current with `main`:** periodically `git merge origin/main`
  into `v2` so v1.x hotfixes/hardening flow forward and the eventual
  back-merge is small. Do this at each sprint boundary at minimum.
- **CI is tag-only** (v1 policy). For real CI signal on `v2` without a
  release tag, use `gh workflow run ci.yml --ref v2`. Do this before any
  sprint-closing checkpoint. Never assume `v2` is green from local runs
  alone.
- **Release path:** when v2 is ready, one PR `v2 → main` (main requires a
  PR), then tag `v2.0.0` → PyPI. That final merge is the only mandatory
  PR in the whole v2 arc.
- **Versioning:** `v2` does NOT bump the workspace version to `2.0.0`
  until the S10 release cut — keeping it `1.x` avoids confusing any
  interim wheel builds. (Decision revisit at S10.)

## S0 goal

One plan layer both SQL and the DataFrame API lower to, plus a TPC-DS
harness that makes every later sprint measurable. Exit: shared-plan demo
green; `tpcds_validate` runs 99 queries at SF=1 (pass/fail matrix, not
yet all-green); S0.4 ADR merged.

## Task board

| Story | What | Status | Notes |
|---|---|---|---|
| **S0.1** | Shared logical plan (SQL + DataFrame → same `LogicalPlan`) | 📐 design done | Map complete; design in [`../ADR_V2_SHARED_LOGICAL_PLAN.md`](../ADR_V2_SHARED_LOGICAL_PLAN.md). Next: `preset::session_context()` constructor + stub frame lowering + plan-identity demo. Low engine risk — no architectural blocker found. |
| **S0.2** | TPC-DS data (`dsdgen`) + `tpcds_validate` harness, CI SF=1 behind a flag | ⬜ todo | Mirror `tpch_validate` / `scripts/bench/` |
| **S0.3** | SQL-surface gap audit → tracked items (from V2_TARGET §2.1) | ⬜ todo | Land as a checklist doc on `v2` (not GH issues — repo is kept issue-light) |
| **S0.4** | Decide open questions → ADR (namespace, laziness, index depth) | ✅ done | Decided 2026-07-18: `ematix.frame`, lazy-by-default, **strict pandas index**. [`../ADR_V2_DATAFRAME_API.md`](../ADR_V2_DATAFRAME_API.md). ⚠ strict index makes M2 heavier — budget it in S4/S5. |

Legend: ⬜ todo · ⏳ in progress · 🔬 investigating · 📐 design done · ✅ done

**S0.4 landed a scope flag:** strict pandas index (owner's call over the
lighter option) adds a real index layer to M2 (S4/S5) — alignment,
`MultiIndex`, index preservation through transforms. Budget it into the
S4/S5 estimates; re-open with the owner if it threatens the M2 timeline
at the S4 midpoint (per the ADR's revisit trigger).

## Immediate next actions

1. Land the S0.1 architecture map → write the shared-plan design as an
   ADR + a stub demo (a trivial frame op and its SQL equivalent produce
   identical optimized plans).
2. Get owner decisions on the S0.4 open questions; write the ADR.
3. Stand up `tpcds_validate` (S0.2) — data-gen + oracle harness.
4. Write the S0.3 SQL-surface gap checklist.

## Open questions (S0.4) — RESOLVED 2026-07-18

Decisions recorded in [`../ADR_V2_DATAFRAME_API.md`](../ADR_V2_DATAFRAME_API.md):

- **DataFrame namespace:** `ematix.frame`.
- **Default laziness:** lazy-by-default (terminal op executes through the
  shared plan; eager mode opt-in).
- **pandas index model:** **strict pandas index** — full compatibility
  (alignment, `MultiIndex`), accepted as a heavier M2 investment.
