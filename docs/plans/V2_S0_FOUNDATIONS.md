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
| **S0.2** | TPC-DS data (`dsdgen`) + `tpcds_validate` harness, CI SF=1 behind a flag | ✅ done | `tpcds_generate` (DuckDB tpcds ext → parquet) + `tpcds_validate` (runs all 99 on `preset::session_context()`, DuckDB row-parity oracle, gated skip). **First SF=1 run: ematix executes 97/103, row-parity OK 89/97.** |
| **S0.3** | SQL-surface gap audit → tracked items (from V2_TARGET §2.1) | ✅ done | [`V2_SQL_SURFACE_GAPS.md`](V2_SQL_SURFACE_GAPS.md). Key finding: all 99 TPC-DS *plan* via DataFusion today — the gap is **native/fused execution** (grouping sets→S1, window+set-ops→S2, decorrelation→S3), not correctness. |
| **S0.4** | Decide open questions → ADR (namespace, laziness, index depth) | ✅ done | Decided 2026-07-18: `ematix.frame`, lazy-by-default, **index-light core** (revised from strict — positional alignment, Polars-style). [`../ADR_V2_DATAFRAME_API.md`](../ADR_V2_DATAFRAME_API.md). |

Legend: ⬜ todo · ⏳ in progress · 🔬 investigating · 📐 design done · ✅ done

**S0.4 positioning:** ematix.frame is a *faster alternative to pandas*,
not an exact syntax/semantics match. Index-light core keeps DataFrame
plans byte-identical to SQL (protects the S0.1 gate) and avoids the
alignment-join tax. The one behavioral delta to document loudly (S5.5):
binary ops align **positionally**, not by label. Faithful-pandas index
semantics live in the S7 `ematix.pandas` shim + `.to_pandas()`, opt-in.

## S0.2 first-run results (SF=1, 2026-07-18)

`cargo run --release -p ematix-flow-core --example tpcds_validate` over
DuckDB-generated SF=1:

- **ematix executes: 97/103** query files (99 canonical + 4 variants).
- **Row-parity OK: 89/97** of the executing queries (vs DuckDB over the
  same Parquet).
- **6 ematix EXEC_FAIL** — all correlated-subquery **alias-scoping**
  errors (q69, q94, q95, …: "No field named `c.c_current_addr_sk`" /
  "`ws1.ws_ship_date_sk`"). These are the S3 decorrelation / correlated-
  subquery surface (see [`V2_SQL_SURFACE_GAPS.md`](V2_SQL_SURFACE_GAPS.md)
  row 4) — expected gaps, now with concrete query IDs to target.
- **2 parity MISMATCH** (e.g. q73 ematix=0 vs duck=1) — to investigate;
  could be a real correctness delta or a translation/parameter diff.
- **6 ORACLE_SKIP** — DuckDB rejects backtick identifiers in the
  translated SQL (`syntax error at or near "\`"`). This is an oracle-path
  limitation (row-parity via the DataFusion-translated SQL), **not** an
  ematix failure. Refinement: strip backticks for the DuckDB side, or
  switch the oracle to DuckDB's own `tpcds_answers()` reference set.

Harness caveats (documented, not hidden): parity is **row-count** only in
v1 (value-level parity is a follow-on); the validate path uses DataFusion's
default parquet reader (correctness), not the ematix fast provider
(that's the S6 benchmark path).

## Immediate next actions

1. ~~S0.1 shared-plan constructor + stub frame + plan-identity demo.~~ ✅
2. ~~S0.4 owner decisions + ADR (index-light).~~ ✅
3. ~~S0.3 SQL-surface gap checklist.~~ ✅
4. ~~S0.2 `tpcds_generate` + `tpcds_validate` (SF=1 matrix: 97/103).~~ ✅

**S0 is functionally complete.** Remaining polish (optional, can trail
into S1): dispatch `ci.yml` on `v2` for a full-matrix checkpoint; wire a
gated `tpcds_validate` CI job (needs a data-gen step — extension +
network); improve the oracle (backtick strip / `tpcds_answers()`).

**Next sprint: S1 — grouping sets** ([`../PHASE_V2_S1_GROUPING_SETS.md`](../PHASE_V2_S1_GROUPING_SETS.md)),
now with concrete SF=1 data + the validate harness to gate it.

## Open questions (S0.4) — RESOLVED 2026-07-18

Decisions recorded in [`../ADR_V2_DATAFRAME_API.md`](../ADR_V2_DATAFRAME_API.md):

- **DataFrame namespace:** `ematix.frame`.
- **Default laziness:** lazy-by-default (terminal op executes through the
  shared plan; eager mode opt-in).
- **pandas index model:** **index-light core** (revised from strict
  same day) — optional positional `RangeIndex`, positional alignment,
  no label/`MultiIndex` in core. Faithful-pandas via the S7 shim +
  `.to_pandas()`. Goal = faster-than-pandas, not exact match.
