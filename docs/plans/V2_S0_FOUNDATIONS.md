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
| **S0.1** | Shared logical plan (SQL + DataFrame → same `LogicalPlan`) | ✅ done | `preset::session_context()`/`session_state()` shared constructors + `frame` module (lowering seam) + **plan-identity gate GREEN** (2 shapes: filter+agg, filter+project — frame ≡ SQL optimized plan). CLI `run_shard` consolidated onto the shared constructor; streaming path scoped out (recorded in the ADR). [`../ADR_V2_SHARED_LOGICAL_PLAN.md`](../ADR_V2_SHARED_LOGICAL_PLAN.md). |
| **S0.2** | TPC-DS data (`dsdgen`) + `tpcds_validate` harness, CI SF=1 behind a flag | ⬜ todo | Mirror `tpch_validate` / `scripts/bench/` |
| **S0.3** | SQL-surface gap audit → tracked items (from V2_TARGET §2.1) | ⬜ todo | Land as a checklist doc on `v2` (not GH issues — repo is kept issue-light) |
| **S0.4** | Decide open questions → ADR (namespace, laziness, index depth) | ✅ done | Decided 2026-07-18: `ematix.frame`, lazy-by-default, **index-light core** (revised from strict — positional alignment, Polars-style). [`../ADR_V2_DATAFRAME_API.md`](../ADR_V2_DATAFRAME_API.md). |

Legend: ⬜ todo · ⏳ in progress · 🔬 investigating · 📐 design done · ✅ done

**S0.4 positioning:** ematix.frame is a *faster alternative to pandas*,
not an exact syntax/semantics match. Index-light core keeps DataFrame
plans byte-identical to SQL (protects the S0.1 gate) and avoids the
alignment-join tax. The one behavioral delta to document loudly (S5.5):
binary ops align **positionally**, not by label. Faithful-pandas index
semantics live in the S7 `ematix.pandas` shim + `.to_pandas()`, opt-in.

## Immediate next actions

1. ~~S0.1 shared-plan constructor + stub frame + plan-identity demo.~~ ✅
2. ~~S0.4 owner decisions + ADR.~~ ✅ (index-light revision landed.)
3. **S0.2** — stand up `tpcds_validate`: `dsdgen` data-gen + oracle
   harness (row-parity vs DuckDB), CI SF=1 behind a feature flag.
4. **S0.3** — write the SQL-surface gap checklist from `V2_TARGET.md`
   §2.1 (grouping sets / window / set-ops / decorrelation), pinned to
   the TPC-DS queries that need each.
5. Grow the `frame` stub toward S4 only as later stories need it — S0
   keeps it minimal (it exists to prove the seam, not to be the API).

## Open questions (S0.4) — RESOLVED 2026-07-18

Decisions recorded in [`../ADR_V2_DATAFRAME_API.md`](../ADR_V2_DATAFRAME_API.md):

- **DataFrame namespace:** `ematix.frame`.
- **Default laziness:** lazy-by-default (terminal op executes through the
  shared plan; eager mode opt-in).
- **pandas index model:** **index-light core** (revised from strict
  same day) — optional positional `RangeIndex`, positional alignment,
  no label/`MultiIndex` in core. Faithful-pandas via the S7 shim +
  `.to_pandas()`. Goal = faster-than-pandas, not exact match.
