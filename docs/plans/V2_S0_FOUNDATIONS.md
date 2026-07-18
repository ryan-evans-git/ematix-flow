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
| **S0.1** | Shared logical plan (SQL + DataFrame → same `LogicalPlan`) | 🔬 mapping | Architecture map in flight (the linchpin — nothing in Track B starts until its demo is green) |
| **S0.2** | TPC-DS data (`dsdgen`) + `tpcds_validate` harness, CI SF=1 behind a flag | ⬜ todo | Mirror `tpch_validate` / `scripts/bench/` |
| **S0.3** | SQL-surface gap audit → tracked items (from V2_TARGET §2.1) | ⬜ todo | Land as a checklist doc on `v2` (not GH issues — repo is kept issue-light) |
| **S0.4** | Decide open questions → ADR (namespace, laziness, index depth) | ⏳ deciding | Blocks S0.1 shape + S4; awaiting owner calls, then adr-writer |

Legend: ⬜ todo · ⏳ in progress · 🔬 investigating · ✅ done

## Immediate next actions

1. Land the S0.1 architecture map → write the shared-plan design as an
   ADR + a stub demo (a trivial frame op and its SQL equivalent produce
   identical optimized plans).
2. Get owner decisions on the S0.4 open questions; write the ADR.
3. Stand up `tpcds_validate` (S0.2) — data-gen + oracle harness.
4. Write the S0.3 SQL-surface gap checklist.

## Open questions (S0.4 — needs owner sign-off)

- **DataFrame namespace:** `ematix.frame` (explicit) vs a top-level
  first-class surface. The `import ematix.pandas as pd` shim (S7) is
  separate either way.
- **Default laziness:** lazy-by-default (build a plan, execute on a
  terminal op — matches the engine + Polars) vs eager (pandas notebook
  feel).
- **pandas index model depth:** ship a lightweight optional index and
  document the delta, vs invest in strict-pandas-index compatibility.
