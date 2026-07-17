# ematix-flow — v2.0.0 Target

**Theme: from pipeline runtime to the analytics runtime.**

v1.0.0 proved ematix-flow is a production data-pipeline runtime — one
`pip install`, a query engine that beats DuckDB/Polars single-node and
Trino/PySpark on a mesh, orchestration, data-quality, and an operator
UI. It wins the *movement* of data.

v2.0.0 makes ematix-flow the place you **analyze** data too. The pitch
becomes: *the analyst writes pandas or SQL, the engineer writes
pipelines, and both run on the same Rust engine on one machine — no
cluster, no second tool, no rewrite to productionize.*

- **Status:** planning. Target cut date TBD.
- **SemVer:** v2 is a **major** — breaking API changes are permitted and
  will be batched here (see [§8 Compatibility & versioning](#8-compatibility--versioning)).
- **Owner:** Ryan Evans.
- **Supersedes/extends:** [`ROADMAP.md`](ROADMAP.md), [`PHASE_SIGMA_PLAN.md`](PHASE_SIGMA_PLAN.md),
  [`SQL_TRANSFORMS_PLAN.md`](SQL_TRANSFORMS_PLAN.md).

---

## 1. Why v2 exists (positioning)

Today an analytics team runs a stack: pandas/Polars for exploration →
dbt/Spark SQL for transforms → a warehouse for scale → a BI tool on top.
Each hop is a rewrite and a new system to operate. ematix already
collapses the *pipeline* half of that stack. v2 collapses the
*analytics* half onto the same runtime.

Three things have to be true for an analytics team to adopt ematix:

1. **It runs their queries.** The analytical SQL surface must be
   complete and fast — proven the way we proved TPC-H: natively
   executed and benchmarked at scale. **TPC-DS is that proof.**
2. **It speaks their language.** Analysts live in pandas. A Rust-backed,
   pandas-compatible DataFrame API lets them keep their muscle memory
   and get engine speed, with a zero-rewrite path from existing scripts.
3. **Migrating is cheap and reversible.** Compatibility shims, dialect
   translation, BI connectivity, and honest coverage scorecards make the
   switch a low-risk trial, not a bet-the-quarter rewrite.

Pillars 1–3 below map to those three. Pillars 4–5 make them fast and
connected.

---

## 2. Pillar 1 — Native TPC-DS + analytical SQL completeness

**Where we are:** TPC-DS (99 queries, ×2 variants ≈ 103) *passes the
dialect audit* — the Spark and DuckDB translators emit
DataFusion-compatible SQL that plans and runs
([`examples/tpcds_dialect_audit.rs`](../crates/ematix-flow-core/examples/tpcds_dialect_audit.rs)).
That proves **translation correctness**, not native execution or
performance. TPC-DS has never been run through the ematix push /
vectorized engine at scale, and never benchmarked.

**v2 target:** TPC-DS becomes a first-class, natively-executed,
benchmarked analytics proof — the TPC-H story, redone for the harder,
analyst-shaped workload.

### 2.1 SQL surface completeness

TPC-DS exercises shapes TPC-H never touches. Each is a concrete work
item, gated by a query that needs it:

| Shape | Needed by (examples) | Status target |
|---|---|---|
| `GROUPING SETS` / `ROLLUP` / `CUBE` + `GROUPING()` | Q18, Q22, Q67, Q77 | native, vectorized |
| Advanced window frames (`RANGE`, named windows, `RANK/DENSE_RANK/NTILE/LEAD/LAG`) | Q44, Q47, Q49, Q51, Q57 | native |
| `INTERSECT` / `EXCEPT` (+ `ALL`) | Q8, Q14, Q38, Q87 | native |
| Correlated + `EXISTS`/`NOT EXISTS` subqueries at depth | Q10, Q35, Q41 | decorrelation in optimizer |
| Recursive CTEs | (out-of-suite, common in analytics) | native |
| `INTERSECT`/`UNION`-heavy set pipelines, large `IN`-lists | Q33, Q56, Q60 | native |

Anything that only executes today because DataFusion's own operators
catch it must be pulled onto the ematix push engine to keep the
performance story intact — otherwise we win TPC-H and lose TPC-DS on the
exact operators analysts use most (window + grouping-set aggregation).

### 2.2 The benchmark campaign

Mirror the TPC-H rig ([`BENCHMARKS.md`](BENCHMARKS.md),
[`DISTRIBUTED_TPCH_BENCHMARK_PLAN.md`](DISTRIBUTED_TPCH_BENCHMARK_PLAN.md)):

- **Scales:** SF=1/10/100 single-node; SF=1000 one-box + 4-node mesh.
- **Protocol:** sum-of-per-query-medians, 5 trials × 2 warmups,
  row-parity guarded, AUTO-mode arm memo.
- **Head-to-head:** DuckDB, Polars (where it can express the query),
  Trino/PySpark on the mesh.
- **Deliverable:** a `Sf*Benchmarks` chart set on ematix.dev alongside
  TPC-H, plus a reproducer harness in the repo. **No published number
  without a banked, reproducible run** (per project discipline).

### 2.3 Exit criteria

- 99/99 TPC-DS queries execute natively on the push engine, row-parity
  clean at SF=1/10/100.
- At least SF=100 single-node benchmarked and charted vs DuckDB.
- Grouping-sets and window aggregation are vectorized, not scalar
  fallbacks.

---

## 3. Pillar 2 — Rust-backed pandas-compatible DataFrame API

**The headline feature.** A DataFrame API with a pandas-shaped surface,
executed entirely on the ematix Arrow/push engine — *no pandas
dependency, no NumPy object columns, no Python-loop per row*. Think
"Polars speed, pandas ergonomics, ematix engine."

### 3.1 Shape of the API

```python
import ematix.frame as ef        # the migration alias; also `from ematix import frame`

df = ef.read_parquet("s3://bucket/orders/*.parquet")
out = (
    df[df["amount_cents"] > 10_000]           # boolean mask indexing
      .assign(amount=lambda d: d["amount_cents"] / 100)
      .groupby("customer_id")
      .agg(total=("amount", "sum"), n=("order_id", "count"))
      .sort_values("total", ascending=False)
      .head(20)
)
out.to_pandas()      # zero-copy escape hatch, only if the user wants real pandas
```

Design commitments:

- **Familiar surface first.** `read_csv/parquet/sql`, `[]`/`.loc`/`.iloc`
  indexing, boolean masks, `groupby().agg()`, `merge`, `join`,
  `pivot_table`, `melt`, `concat`, `rolling`/`expanding`, `apply`
  (vectorized where possible; UDF path via the existing `@udf` dispatch
  when not), `fillna`/`dropna`/`astype`, datetime accessors.
- **Lazy by default, eager on demand.** Operations build a plan; a
  terminal op (`.to_pandas()`, `.collect()`, `.head()`, `__repr__`)
  executes it through the same optimizer TPC-DS uses. An eager mode
  mirrors pandas semantics for notebook feel.
- **Arrow all the way down.** Columns are Arrow arrays; interop with
  pandas/Polars/PyArrow is zero-copy via the C Data Interface. No hidden
  materialization to Python objects.
- **One engine.** The DataFrame lowers to the **same** logical plan as
  SQL, so a query is a query — the CBO, spilling, narrow-key decode, and
  distributed execution all apply. A DataFrame can be a SQL CTE and
  vice-versa.

### 3.2 Where pandas semantics fight the engine

Be explicit and honest about the deltas (this is a migration doc, not a
promise of bit-identical pandas):

- **Index model.** pandas' row index is semantically heavy. v2 supports
  a lightweight optional index; a strict-pandas-index mode is a
  stretch goal, not a launch gate. Document the difference loudly.
- **dtype coverage.** Map pandas dtypes → Arrow; object columns of mixed
  Python types are the one thing we *don't* accelerate — surface a clear
  error/opt-out rather than silently degrading.
- **Ordering & NaN vs null.** Arrow `null` ≠ NumPy `NaN`; define and
  document one consistent rule.

### 3.3 Unification with pipelines

The DataFrame is not a side-car. A frame can be a **job target** and a
**job source**, so exploratory analysis productionizes without a
rewrite:

```python
@ematix.job(name="daily_cohorts", target=CohortTable, target_connection="warehouse", mode="merge", keys=("cohort_id",))
def daily_cohorts(conn):
    df = ef.read_sql("SELECT * FROM analytics.orders", conn)
    return df.groupby("cohort_id").agg(...)   # a frame *is* a valid job return
```

### 3.4 Exit criteria

- The pandas cheat-sheet's top ~50 operations run natively.
- A representative real pandas analytics script (join + groupby + window
  + pivot) runs faster than pandas and returns row-identical results.
- Zero-copy `to_pandas()`/`from_pandas()`/`to_polars()` proven.

---

## 4. Pillar 3 — Migration & adoption on-ramps

Adoption is a funnel; each item lowers a specific barrier.

- **`import ematix.pandas as pd` drop-in shim.** A compatibility module
  exposing pandas' top-level API (`pd.read_csv`, `pd.DataFrame`,
  `pd.merge`, `pd.concat`) backed by the Rust engine. Change one import
  line, run the script, get speed. Unsupported calls raise a clear
  "not accelerated — falls back / rewrite as X" error, never a wrong
  answer.
- **`ematix migrate` / compatibility scorecard.** A CLI that ingests a
  pandas script, a folder of SQL, or a dbt project and reports: % of
  operations natively supported, which lines need changes, and the
  suggested rewrite. Turns "will it work?" into a 30-second answer.
- **dbt adapter.** `dbt-ematix` so existing dbt models run on the ematix
  engine (single-machine warehouse). Meets analytics teams where they
  already are.
- **Dialect translation, extended.** The Σ.A2 translator already accepts
  Spark and DuckDB SQL. v2 adds a **pandas-expression translator** used
  by `migrate`, and tightens Spark/DuckDB coverage to the TPC-DS surface
  so translated analytics workloads execute, not just parse.
- **Interactive analytics.** A REPL / Jupyter kernel with `.explain()`,
  query profiling, and progress on long scans — the notebook loop
  analysts expect, on the fast engine.
- **Migration docs as a product.** "pandas → ematix", "Spark → ematix",
  "dbt → ematix", and "warehouse → ematix" cheat-sheets, each with a
  runnable before/after. Honesty about gaps is the trust-builder.

**Exit criteria:** a new user can take an existing pandas script or dbt
project, run `ematix migrate`, get an accurate scorecard, and (for a
supported script) run it unchanged via the shim.

---

## 5. Pillar 4 — Interop & BI connectivity

Analysts don't only write code — they point Tableau/Power BI/DBeaver at
a warehouse. Give ematix a front door.

- **Arrow Flight SQL server + ADBC driver.** ematix already speaks Arrow
  Flight internally (distributed engine). Expose a Flight SQL endpoint so
  any ADBC/Flight-SQL client connects. This is the highest-leverage BI
  integration and reuses existing plumbing.
- **JDBC/ODBC via Flight SQL** (through the Arrow Flight SQL JDBC driver)
  for tools that can't speak ADBC yet.
- **Lakehouse read.** Land the in-flight Iceberg work
  ([`ICEBERG_PLAN.md`](ICEBERG_PLAN.md),
  [`SIDECAR_INDEXES_PLAN.md`](SIDECAR_INDEXES_PLAN.md)) so analytics can
  run over existing Iceberg/Delta tables without copying data in.
- **Catalog awareness.** `SHOW TABLES`/`information_schema` so BI tools
  can introspect.

**Exit criteria:** DBeaver (or equivalent) connects via Flight SQL,
lists tables, and runs a TPC-DS query interactively.

---

## 6. Pillar 5 — Analytics performance & correctness at scale

Analytical queries stress the optimizer differently from ETL. v2
hardens the engine for the new shapes.

- **CBO maturity for wide joins & grouping sets.** TPC-DS has snowflake
  schemas and many-way joins; join-reorder and cardinality estimation
  (building on `PHASE_PSI_COLUMN_STATS_AND_CBO.md`) must handle them.
- **Vectorized grouping-set & window aggregation** — the operators
  §2.1 introduces must be push-engine kernels, not scalar fallbacks.
- **Result / plan caching** for the interactive loop (re-running a
  notebook cell shouldn't re-scan cold).
- **Spill correctness** for large grouping-set and window states (the
  Q09 memory-cliff lesson — HashJoin/agg must spill, not livelock).

**Exit criteria:** the §2.2 campaign shows no query regressing vs DuckDB
by more than a defined threshold, and no OOM at the target scales.

---

## 7. Cross-cutting

- **Docs & site:** new "Analytics" section; DataFrame API reference;
  TPC-DS benchmark charts on ematix.dev; migration cheat-sheets;
  homepage repositioning ("analyze and move data on one runtime").
- **CI:** TPC-DS correctness suite in the gate; DataFrame API test matrix
  against a pandas oracle; benchmark regression sentinels.
- **TDD discipline:** every SQL-surface and DataFrame op ships with a
  RED-first parity test (pandas/DuckDB oracle) before the kernel.

---

## 8. Compatibility & versioning

v2.0.0 is where accumulated breaking changes land. Candidate breaks to
batch here (decide per item before the cut):

- Any decorator/API renames deferred during v1.
- DataFrame API namespace decisions (`ematix.frame` vs top-level).
- Default-behavior changes (e.g. lazy-by-default) that would surprise v1
  users.

Everything not explicitly broken keeps its v1 semantic-version guarantee.
Publish a **v1 → v2 migration guide** with the release.

---

## 9. Non-goals for v2

- **Not** a distributed OLAP cluster product — the thesis stays
  single-machine-first with opt-in mesh. Scale-out is proven, not the
  headline.
- **Not** bit-identical pandas emulation — compatible surface + honest
  deltas, not a reimplementation of every pandas edge case.
- **Not** a BI *visualization* tool — we provide the SQL front door, not
  the dashboards.
- **Not** a managed cloud service.

---

## 10. Milestones (sequencing, not dates)

1. **M1 — TPC-DS native correctness.** SQL surface (§2.1) lands; 99/99
   execute natively row-parity clean at SF=1/10. *Proves the engine.*
2. **M2 — DataFrame API core.** Read/filter/groupby/join/window +
   zero-copy interop; the top-50 pandas ops (§3.4). *Proves the surface.*
3. **M3 — TPC-DS benchmark campaign.** SF=100 (+SF=1000 stretch) charted
   vs DuckDB; on ematix.dev. *Proves the speed.*
4. **M4 — Migration on-ramps.** `import ematix.pandas as pd` shim +
   `ematix migrate` scorecard + cheat-sheets. *Proves it's cheap to try.*
5. **M5 — Interop.** Flight SQL server + ADBC + Iceberg read; DBeaver
   connects. *Proves it fits the existing stack.*
6. **M6 — Hardening + v2.0.0 cut.** CBO/spill/caching (§6), migration
   guide, breaking-change batch, release.

M1 and M2 are the critical path and can run in parallel (shared logical
plan). M4/M5 depend on M2/M1 respectively.

---

## 11. Open questions

- **DataFrame namespace & default laziness** — `ematix.frame` alias vs a
  first-class top-level; lazy-by-default vs eager-by-default for the
  notebook feel. (Decide before M2.)
- **How far to chase pandas' index model** — launch with lightweight
  optional index, or invest in strict-index compatibility?
- **dbt adapter scope** — full adapter vs a documented "run your compiled
  SQL" path for v2.0, adapter in v2.x.
- **Which BI clients are tier-1** for the Flight SQL launch (DBeaver +
  one of Tableau/Power BI)?
- **TPC-DS benchmark ceiling** — is SF=1000 in-scope for the v2 cut, or a
  fast-follow like the TPC-H SF=1000 arc was?

---

*Draft — feedback and re-scoping expected. This sets the target; the
per-pillar phase plans (`PHASE_*`) will carry the detailed designs.*
