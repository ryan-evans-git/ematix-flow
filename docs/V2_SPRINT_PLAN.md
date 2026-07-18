# ematix-flow — v2.0.0 Sprint Plan

Execution plan for [`V2_TARGET.md`](V2_TARGET.md). Decomposes the six
milestones into two-week sprints, each ending in a **demoable
increment**. Every SQL-surface and DataFrame story is **RED-test-first**
against a pandas/DuckDB oracle before the kernel lands (project TDD
discipline).

- **Cadence:** 2-week sprints. ~11 sprints (S0–S10) ≈ 3 PIs ≈ 5–6 months.
- **Tracks:** **A = engine/SQL** (M1, M3, M6-eng), **B = DataFrame/DX**
  (M2, M4). A and B share the logical-plan layer (S0) and can interleave
  when capacity allows; the table marks the critical path.
- **Definition of Done (every story):** RED oracle test → kernel/impl →
  green in CI gate → row-parity/benchmark sentinel where applicable →
  docs updated → clippy+fmt clean. No published benchmark number without
  a banked, reproducible run.

---

## Sprint map

| Sprint | Milestone | Theme | Track | Exit / demo |
|---|---|---|---|---|
| **S0** | M1+M2 | Foundations: shared logical plan, TPC-DS harness, open-Q decisions | A+B | SQL & a stub DataFrame lower to the **same** plan; `dsdgen` SF=1/10 data + runner in repo |
| **S1** | M1 | Grouping sets / ROLLUP / CUBE / `GROUPING()` — vectorized | A | TPC-DS Q18/Q22/Q67/Q77 native, row-parity vs DuckDB SF=1 |
| **S2** | M1 | Window frames + set ops (`INTERSECT`/`EXCEPT [ALL]`) | A | Q44/Q47/Q49/Q51 + Q8/Q38/Q87 native, parity SF=1 |
| **S3** | M1 | Subquery decorrelation + remainder → **99/99 native** | A | Full TPC-DS row-parity clean SF=1 **and** SF=10 *(M1 exit)* |
| **S4** | M2 | DataFrame core I: readers, indexing, `groupby().agg()`, interop | B | pandas script (filter→groupby→agg) runs native, row-identical |
| **S5** | M2 | DataFrame core II: `merge`/join, window/rolling, pivot/melt, lazy | B | **Top-50 pandas ops** native; zero-copy `to_pandas/polars` *(M2 exit)* |
| **S6** | M3 | TPC-DS benchmark campaign | A | SF=100 single-node vs DuckDB **charted on ematix.dev** *(M3 exit)* |
| **S7** | M4 | Migration on-ramps: `ematix.pandas` shim + `ematix migrate` | B | Existing pandas script runs via shim; scorecard on a dbt project *(M4 exit)* |
| **S8** | M5 | Interop: Flight SQL server + ADBC + Iceberg read | A | **DBeaver connects**, lists tables, runs a TPC-DS query *(M5 exit)* |
| **S9** | M6 | Hardening: CBO / spill / caching + regression sentinels | A | No query >threshold vs DuckDB; no OOM at target scales |
| **S10** | M6 | **v2.0.0 cut**: breaking-change batch, migration guide, release | A+B | Tag `v2.0.0` → PyPI; site repositioned *(M6 exit)* |

Critical path: **S0 → S1 → S2 → S3 → S6 → S9 → S10**. Track B (S4, S5,
S7) runs against the S0 plan layer and merges in wherever capacity opens;
S7 needs S5, S8 needs S3.

---

## S0 — Foundations *(critical path; unblocks everything)*

**Goal:** one plan to rule both surfaces, and a TPC-DS harness that makes
every later sprint measurable.

- **S0.1 Shared logical plan.** Ensure SQL and the (stub) DataFrame API
  lower to the **same** `LogicalPlan` so the CBO, spilling, narrow-key
  decode, and distributed execution apply to both. *Demo:* a trivial
  frame op and its SQL equivalent produce identical optimized plans.
- **S0.2 TPC-DS data + runner.** `dsdgen` integration, SF=1/10 fixtures,
  a `tpcds_validate` harness mirroring `tpch_validate` (row-parity vs
  DuckDB), wired into CI (SF=1) behind a feature flag.
- **S0.3 SQL-surface gap audit.** Turn §2.1's table into tracked issues,
  each pinned to the TPC-DS query that needs it, ranked by frequency.
- **S0.4 Decide open questions (§11).** DataFrame namespace
  (`ematix.frame`), lazy-vs-eager default, pandas-index depth. Record as
  an ADR so S4 isn't blocked on debate.

**Exit:** shared-plan demo green; `tpcds_validate` runs 99 queries at
SF=1 (pass/fail matrix, not yet all-green); ADR merged.

---

## S1 — Grouping sets / ROLLUP / CUBE *(M1)*

**Goal:** the aggregation shapes analysts use most, vectorized on the
push engine (not a scalar DataFusion fallback).

> **Full design:** [`PHASE_V2_S1_GROUPING_SETS.md`](PHASE_V2_S1_GROUPING_SETS.md).

- **S1.1** RED parity tests for `GROUPING SETS`, `ROLLUP`, `CUBE`,
  `GROUPING()` against DuckDB (Q18, Q22, Q67, Q77).
- **S1.2** Vectorized grouping-set aggregation kernel in the push engine.
- **S1.3** Spillable grouping-set state (carry the Q09-cliff lesson — no
  livelock, must spill).

**Exit:** Q18/Q22/Q67/Q77 native + row-parity clean at SF=1; kernels are
vectorized, benchmarked to confirm no scalar fallback.

---

## S2 — Window frames + set operators *(M1)*

- **S2.1** RED tests: `RANGE`/named windows, `RANK/DENSE_RANK/NTILE/
  LEAD/LAG` (Q44/Q47/Q49/Q51/Q57); `INTERSECT`/`EXCEPT [ALL]`
  (Q8/Q14/Q38/Q87).
- **S2.2** Vectorized window-frame execution.
- **S2.3** Native set-operator execution + large `IN`-list handling
  (Q33/Q56/Q60).

**Exit:** all listed queries native + parity at SF=1.

---

## S3 — Decorrelation → 99/99 native *(M1 exit)*

- **S3.1** RED tests for correlated / `EXISTS`/`NOT EXISTS` at depth
  (Q10, Q35, Q41) + recursive CTE.
- **S3.2** Subquery-decorrelation pass in the optimizer.
- **S3.3** Close the long tail; drive `tpcds_validate` to **99/99** at
  SF=1 **and** SF=10.

**Exit (M1):** 99/99 TPC-DS execute natively, row-parity clean at
SF=1/10; grouping-set + window aggregation confirmed vectorized.

---

## S4 — DataFrame core I *(M2)*

**Goal:** the read→explore→aggregate loop, native and pandas-shaped.

- **S4.1** Readers: `read_parquet/csv/sql`; Arrow-backed `Frame`/`Series`
  (C Data Interface, no object columns).
- **S4.2** Selection: `[]`/`.loc`/`.iloc`, boolean masks, `assign`,
  `astype`, `fillna`/`dropna`.
- **S4.3** `groupby().agg(...)`, `sort_values`, `head`/`tail`; terminal
  ops (`.collect()`, `__repr__`) execute through the S0 plan.
- **S4.4** Zero-copy `to_pandas()`/`from_pandas()`.

**Exit:** a real pandas script (filter → groupby → agg) runs on the
engine and returns row-identical results, faster than pandas.

---

## S5 — DataFrame core II → top-50 ops *(M2 exit)*

- **S5.1** `merge`/`join` (all how=), `concat`.
- **S5.2** `rolling`/`expanding`, window expressions, datetime accessors.
- **S5.3** `pivot_table`/`melt`; `apply` (vectorized; UDF path via
  existing `@udf` when not).
- **S5.4** Lazy-by-default plan build + eager mode per the S0.4 ADR;
  `to_polars()` interop.
- **S5.5** Honest-delta docs: index model, NaN-vs-null, object columns.

**Exit (M2):** top-~50 pandas ops native; join+groupby+window+pivot
script row-identical vs pandas; zero-copy interop proven.

---

## S6 — TPC-DS benchmark campaign *(M3 exit)*

Reuse the TPC-H rig ([`DISTRIBUTED_TPCH_BENCHMARK_PLAN.md`](DISTRIBUTED_TPCH_BENCHMARK_PLAN.md)).

- **S6.1** Harness: sum-of-per-query-medians, 5×2 warmups, row-parity
  guard, AUTO-arm memo; SF=100 single-node.
- **S6.2** Head-to-head vs DuckDB (and Polars where expressible); bank
  results.
- **S6.3** `Sf*Benchmarks`-style charts on ematix.dev + reproducer in
  repo. *(SF=1000 = stretch / fast-follow per §11.)*

**Exit (M3):** SF=100 banked, reproducible, charted vs DuckDB.

---

## S7 — Migration on-ramps *(M4 exit; needs S5)*

- **S7.1** `import ematix.pandas as pd` drop-in shim over the S4/S5
  engine; unsupported calls raise clear "rewrite as X" errors, never
  wrong answers.
- **S7.2** `ematix migrate` scorecard: ingest a pandas script / SQL
  folder / dbt project → % supported + per-line rewrite suggestions.
  Reuses/extends the Σ.A2 dialect translator with a pandas-expr front end.
- **S7.3** Cheat-sheets: pandas→ematix, Spark→ematix, dbt→ematix, each
  with a runnable before/after.
- **S7.4** *(stretch)* `dbt-ematix` "run compiled SQL" path.

**Exit (M4):** an existing supported pandas script runs unchanged via the
shim; `ematix migrate` gives an accurate scorecard on a real dbt project.

---

## S8 — Interop *(M5 exit; needs S3)*

- **S8.1** Arrow Flight SQL server (reuse the distributed Flight
  plumbing) + `SHOW TABLES`/`information_schema`.
- **S8.2** ADBC driver validation; Flight-SQL JDBC path for legacy tools.
- **S8.3** Land Iceberg read ([`ICEBERG_PLAN.md`](ICEBERG_PLAN.md),
  [`SIDECAR_INDEXES_PLAN.md`](SIDECAR_INDEXES_PLAN.md)) so analytics runs
  over existing tables without copy-in.

**Exit (M5):** DBeaver (or equivalent) connects via Flight SQL, lists
tables, runs a TPC-DS query interactively.

---

## S9 — Hardening *(M6)*

- **S9.1** CBO for wide joins / snowflake schemas + grouping sets
  (builds on `PHASE_PSI_COLUMN_STATS_AND_CBO.md`).
- **S9.2** Spill correctness for large grouping-set + window state.
- **S9.3** Result/plan caching for the interactive loop.
- **S9.4** Benchmark regression sentinels in CI (TPC-DS + DataFrame).

**Exit:** no TPC-DS query regresses vs DuckDB beyond the defined
threshold; no OOM at target scales.

---

## S10 — v2.0.0 cut *(M6 exit)*

- **S10.1** Batch breaking changes (§8): API renames, DataFrame
  namespace, default-behavior changes.
- **S10.2** v1→v2 migration guide.
- **S10.3** Site repositioning ("analyze *and* move data on one
  runtime"): Analytics section, DataFrame reference, TPC-DS charts.
- **S10.4** Version bump + CHANGELOG `[2.0.0]`; tag `v2.0.0` → PyPI
  (release gates on `ci.yml`); deploy site after PyPI confirms.

**Exit (M6 / v2):** `2.0.0` on PyPI; docs + site live; migration guide
published.

---

## Risks & sequencing notes

- **S0.1 is the linchpin.** If SQL and DataFrame don't share the plan,
  Track B forks the engine and the "one query engine" promise breaks.
  Do not start S4 until the S0.1 demo is green.
- **Decorrelation (S3.2) is the hardest engine work** — highest chance
  of slipping. If it does, ship M1 as "97/99 native, 2 via translation"
  and fast-follow rather than blocking S6.
- **Vectorized grouping-set/window kernels (S1/S2)** are what keep the
  TPC-DS *speed* story honest; a scalar fallback would pass correctness
  but lose the benchmark. Gate S6 on them being vectorized.
- **SF=1000 TPC-DS** is explicitly out of the v2 cut (fast-follow), like
  the TPC-H SF=1000 arc.
- **Solo-capacity reality:** the table is dependency-ordered, not
  parallel-team-sized; when B stories interleave into an A sprint,
  they extend that sprint rather than run truly concurrently.

---

*Living document — re-scope at each sprint boundary. Detailed per-story
designs land in `PHASE_*` / `docs/plans/` as each sprint opens.*
