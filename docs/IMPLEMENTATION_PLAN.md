# ematix-flow — Implementation Plan (v0.1)

Companion to `docs/PRD.md`. Phases are ordered for fastest path to a working
end-to-end happy path, then breadth of strategies, then production polish.

Each phase ends in a green `cargo test` + `pytest` run and a working demo on
real Postgres.

---

## Repo layout (target)

```
data-eng-framework/                 (rename to ematix-flow before publish)
├── Cargo.toml                      workspace root
├── crates/
│   ├── ematix-flow-core/           pure-Rust library (Postgres engine)
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── types.rs            type catalogue (Postgres dialect)
│   │       ├── ddl.rs              CREATE TABLE planner
│   │       ├── hash.rs             row hash function
│   │       ├── pg.rs               tokio-postgres pool + helpers
│   │       ├── source.rs           Source resolution + same-DB detection
│   │       ├── stage.rs            COPY-based staging
│   │       ├── meta.rs             ematix_flow.{watermarks,run_history}
│   │       ├── strategy/
│   │       │   ├── mod.rs
│   │       │   ├── append.rs
│   │       │   ├── truncate.rs
│   │       │   ├── merge.rs        SCD1
│   │       │   └── scd2.rs
│   │       └── plan.rs             PipelineSpec → ExecutionPlan
│   └── ematix-flow-py/             PyO3 bindings crate
│       ├── Cargo.toml
│       └── src/lib.rs              wraps core types for Python
├── python/
│   └── ematix_flow/                pure-Python package (depends on built ext)
│       ├── __init__.py
│       ├── types.py                Column, Integer, String, ...
│       ├── table.py                ManagedTable
│       ├── source.py               Source.postgres_query, ...
│       ├── strategy.py             AppendOnly, MergeUpsert, SCD2 wrappers
│       ├── pipeline.py             Pipeline, pipeline.sync
│       ├── cli.py                  `flow` entry point
│       └── _core.pyi               type stubs for the Rust extension
├── pyproject.toml                  maturin build config
├── tests/
│   ├── rust/                       in-crate `cargo test`
│   └── python/                     pytest, integration via testcontainers
└── docs/
    ├── PRD.md
    ├── IMPLEMENTATION_PLAN.md
    └── (mkdocs site later)
```

---

## Phase 0 — Repo & build scaffolding (≈1 day)

Goal: a workspace that builds, lints, and ships an empty wheel.

- [ ] Convert `Cargo.toml` to a workspace; move existing stub into
      `crates/ematix-flow-core`.
- [ ] Add `crates/ematix-flow-py` with `pyo3 = { version = "0.22", features = ["extension-module"] }`
      and `crate-type = ["cdylib"]`.
- [ ] Add `pyproject.toml` with `[build-system] requires = ["maturin>=1.5"]`,
      configured to find the `ematix-flow-py` crate and emit a wheel exposing
      `ematix_flow._core`.
- [ ] Create the empty `python/ematix_flow/` package with a stub `__init__.py`
      that re-exports nothing yet.
- [ ] `maturin develop` succeeds; `python -c "import ematix_flow"` succeeds.
- [ ] CI: GitHub Actions workflow running `cargo fmt --check`, `cargo clippy
      -- -D warnings`, `cargo test`, `pytest`, on macOS-latest and ubuntu-latest.
- [ ] Decide: rename repo / package directory from `data-eng-framework` to
      `ematix-flow` now or after v0.1? **Recommend now** to avoid rewriting
      paths and CI configs later.

Exit criteria: green CI on a no-op PR.

---

## Phase 1 — Rust ↔ Python bridge smoke test (≈0.5 day)

- [ ] Define a minimal `PipelineSpec` struct in `ematix-flow-core` with
      `serde` derives.
- [ ] Expose a `parse_spec(json: &str) -> PyResult<PipelineSpec>` to Python.
- [ ] Python builds spec from `Pipeline(...)` args, ships JSON to Rust,
      Rust echoes a normalized version back. No DB yet.

Exit: round-trip test passing.

---

## Phase 2 — Type system + ManagedTable (≈1–2 days)

- [ ] Rust `types.rs`: enum of supported types, each with `to_postgres_sql()`.
- [ ] Python `types.py`: `Column`, `Integer`, `BigInt`, `SmallInt`, `String`,
      `Text`, `Boolean`, `Float`, `Double`, `Numeric(precision, scale)`, `Date`,
      `Timestamp`, `TimestampTZ`, `JSON`, `JSONB`, `UUID`, `Bytes`.
- [ ] Python `table.py`: `ManagedTable` metaclass that:
  - Collects `Column` attributes in declaration order.
  - Validates `__tablename__` and `__schema__` exist.
  - Computes a stable `schema_fingerprint` (hash of name + ordered columns + types).
  - Exposes `_to_spec()` returning a JSON-serializable dict.
- [ ] Tests: declare a `CustomerDim` class, assert spec round-trips through
      Rust unchanged, assert primary-key inference works.

Exit: a `ManagedTable` declaration can be lowered to a Rust `TableSpec`.

---

## Phase 3 — Postgres adapter + connection management (≈1–2 days)

- [ ] Rust `pg.rs`: `tokio-postgres` + `deadpool-postgres` pool.
- [ ] Connection-string parsing with the `postgres` crate's URL parser.
- [ ] `same_database(a, b)` returning true iff `(host, port, dbname, user)`
      match (port-normalized, default 5432).
- [ ] Async runtime: `tokio::runtime::Runtime` owned by the core, all
      async work driven through `runtime.block_on()` from PyO3 entry points.
- [ ] Test against a `testcontainers::Postgres` instance: connect, run
      `SELECT 1`, run a transaction.

Exit: `ematix_flow._core.connect(url)` works from Python.

---

## Phase 4 — DDL planner (≈1–2 days)

- [ ] Rust `ddl.rs::create_table_sql(table_spec) -> String`.
- [ ] Reflection: `read_existing_columns(conn, schema, table)` returning
      a vec of `(name, postgres_type, nullable, is_pk)`.
- [ ] Drift comparator: declared vs reflected → enum
      `{ Match, Missing, Drift(Vec<Difference>) }`.
- [ ] Python wires `target.ensure(conn, on_drift="error")` into a
      pre-flight step run before any strategy.
- [ ] Tests: create from scratch; reflect a matching table; reflect a
      drifted table; confirm error message lists differences.

Exit: targets can be created and validated.

---

## Phase 5 — Strategy: AppendOnly (≈1 day)

The simplest strategy; proves the planner → executor pipeline end-to-end.

- [ ] `strategy/append.rs::plan(spec) -> ExecutionPlan` emitting an
      `INSERT INTO target (cols...) SELECT cols... FROM (<source query>) src`
      for the same-DB path.
- [ ] Cross-DB path: `COPY ... TO STDOUT BINARY` from source, `COPY ... FROM
      STDIN BINARY` into a staging temp table, then INSERT...SELECT into
      target.
- [ ] Auto-add `_loaded_at`, `_batch_id` columns if not declared.
- [ ] Run history record written.
- [ ] Tests: same-DB and cross-DB integration tests against testcontainers.

Exit: `pipeline.sync(..., mode="append")` works.

---

## Phase 6 — Strategy: TruncateReplace (≈0.5 day)

- [ ] Same-DB: `BEGIN; TRUNCATE; INSERT...SELECT; COMMIT`.
- [ ] Cross-DB: stage, then `BEGIN; TRUNCATE; INSERT...SELECT FROM stage; COMMIT`.
- [ ] Tests: correct row count, atomicity (kill-mid-load leaves old data).

Exit: `mode="truncate"` works.

---

## Phase 7 — Strategy: MergeUpsert / SCD1 (≈1–2 days)

- [ ] Same-DB: `INSERT INTO target (cols) SELECT cols FROM (<source>) src
      ON CONFLICT (keys) DO UPDATE SET col = EXCLUDED.col, ... WHERE
      target.* IS DISTINCT FROM EXCLUDED.*` (the WHERE makes the affected
      row count meaningful).
- [ ] Cross-DB: stage → `ON CONFLICT` from staging table.
- [ ] Configurable `update_columns`, default = all non-key declared columns.
- [ ] Returns `rows_inserted`, `rows_updated`, `rows_unchanged`.
- [ ] Tests: insert-only, update-only, mixed, idempotency (run twice → second
      run reports 0/0).

Exit: `mode="merge"` / `mode="scd1"` works.

---

## Phase 8 — Strategy: SCD2 (≈3–4 days, biggest single phase)

- [ ] Hash function (`hash.rs`):
  - Canonical encoding: per-column `coalesce(col::text, '\x00NULL\x00')`
    joined with a `\x01` separator (chosen to be vanishingly unlikely in
    real text data).
  - SHA-256 of UTF-8 bytes; stored as 32-byte `bytea` in `row_hash`.
  - Postgres-side equivalent: `digest(coalesce(col1::text,'\x00NULL\x00') || '\x01' || ..., 'sha256')`
    using `pgcrypto` (auto-`CREATE EXTENSION` on first run).
- [ ] DDL: SCD2 ensures `valid_from TIMESTAMPTZ NOT NULL`, `valid_to
      TIMESTAMPTZ`, `is_current BOOLEAN NOT NULL`, `row_hash BYTEA NOT NULL`,
      and a partial index `WHERE is_current` keyed on the natural key.
- [ ] Same-DB SQL plan (single transaction):
  ```sql
  WITH src AS (
    SELECT *, digest(...) AS row_hash FROM (<user query>) q
  ),
  changed AS (
    SELECT src.* FROM src
    LEFT JOIN target t ON t.<keys> = src.<keys> AND t.is_current
    WHERE t.row_hash IS DISTINCT FROM src.row_hash
  )
  UPDATE target SET valid_to = now(), is_current = false
    WHERE is_current AND <keys> IN (SELECT <keys> FROM changed);
  INSERT INTO target (<cols>, valid_from, valid_to, is_current, row_hash)
    SELECT <cols>, now(), NULL, true, row_hash FROM changed;
  ```
- [ ] Cross-DB: stage source rows + their hashes into a temp table, then
      run the same `changed`/`UPDATE`/`INSERT` flow against the staging table.
- [ ] Configurable column names: `effective_from`, `effective_to`,
      `current_flag`, `hash_column`.
- [ ] Tests:
  - First load creates one current version per natural key.
  - Re-running with no source changes → 0 changes.
  - Updating a `compare_columns` value → previous version closed out, new
    version current.
  - Updating a non-`compare_columns` value → no new version (and the
    target row is *not* updated — SCD2 is immutable beyond close-out).
  - Order independence: hash is stable across column physical order.
  - NULL handling: NULL → 'x' is detected; 'x' → NULL is detected.

Exit: `mode="scd2"` works end-to-end.

---

## Phase 9 — Source abstraction polish (≈1 day)

- [ ] `Source.postgres_query(conn, query)`: arbitrary SELECT.
- [ ] `Source.postgres_table(conn, schema, table, columns=None)`: sugar that
      builds the SELECT.
- [ ] Source-side column projection: only fetch declared `compare_columns +
      keys + incremental_column + non-key columns`.
- [ ] Auto-detection of same-DB vs cross-DB path (Phase 3 helper).
- [ ] Override: `pipeline.sync(..., force_path="staging")` for tests.
- [ ] Tests covering both paths for at least Append + SCD2.

Exit: source resolution is robust and override-able.

---

## Phase 10 — Watermarking + metadata schema (≈1–2 days)

- [ ] Rust `meta.rs`: lazy creation of `ematix_flow` schema +
      `watermarks`, `run_history`, `schema_history` tables.
- [ ] `incremental_column` argument plumbed end-to-end.
- [ ] Watermark read before run, watermark write after success only.
- [ ] Run history populated for every run (including failures).
- [ ] Tests:
  - Watermark advances on success.
  - Watermark does not advance on failure (simulated via post-load error).
  - First run with watermark mode treats `last_value = -infinity`.

Exit: incremental loads work and are restart-safe.

---

## Phase 11 — Delete handling opt-in (≈1 day)

- [ ] `handle_deletes` argument; mutual-exclusion check vs.
      `incremental_column`.
- [ ] `"soft"` semantics for SCD2 (close out missing keys).
- [ ] `"soft"` semantics for SCD1 (set `is_deleted=true`, auto-add column).
- [ ] `"hard"` semantics for SCD1 (DELETE missing keys).
- [ ] Tests: insert, then drop a key from source; verify expected outcome.

Exit: delete handling opt-in works.

---

## Phase 12 — Scheduling + `flow` CLI (≈1 day)

- [ ] Cron-string parser: `croniter`-style; in Rust use `cron` crate.
- [ ] `Pipeline(schedule=...)` registration in a module-level registry.
- [ ] `python -m ematix_flow.cli` entry point + `flow` console script in
      `pyproject.toml`.
- [ ] Subcommands:
  - `flow list --module my_pipelines`
  - `flow run --module my_pipelines <name>`
  - `flow run-due --module my_pipelines [--now ISO8601]`
- [ ] Tests: `flow run-due` with mocked `--now` triggers expected pipelines.

Note: `flow daemon` (long-lived scheduler with internal cron loop, per-pipeline
locks, SIGTERM-safe shutdown) is deferred to v0.2. v0.1 users invoke
`flow run-due` from external cron / k8s CronJob / GitHub Actions schedule.

Exit: `flow run-due` demonstrably runs from cron / k8s CronJob.

---

## Phase 13 — Test infrastructure (continuous, but polish here, ≈1 day)

- [ ] `tests/python/conftest.py` provides a `pg_url` fixture using
      `testcontainers[postgres]`.
- [ ] Mark long integration tests `@pytest.mark.integration`; default
      `pytest` runs unit only, CI runs both.
- [ ] Rust integration tests in `crates/ematix-flow-core/tests/` use the
      same Docker image via `testcontainers` Rust crate.
- [ ] Coverage report (Codecov optional).

---

## Phase 14 — Docs, packaging, release (≈2 days)

- [ ] README quickstart (10-minute path: install → declare → SCD2 sync).
- [ ] `mkdocs` site with: concepts, strategies, sources, scheduling, CLI,
      API reference (autogen via `mkdocstrings`).
- [ ] `examples/` directory with one example per strategy + one full
      cron-driven setup.
- [ ] GitHub Actions wheel build matrix:
  - macOS x86_64, macOS aarch64
  - Linux x86_64, Linux aarch64 (manylinux2014 + musllinux)
  - Python 3.10, 3.11, 3.12, 3.13
- [ ] Trusted publishing to PyPI via `pypa/gh-action-pypi-publish`.
- [ ] Tag `v0.1.0`, publish.

---

## Risk register

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| `pgcrypto` not installed in user's DB | Medium | Medium | Detect on first run, auto-`CREATE EXTENSION`, fall back to `md5()` if forbidden. |
| Same-DB / cross-DB detection is fooled by pgbouncer / connection proxies | Low | Medium | Allow explicit `force_path` override; document the heuristic. |
| Hash collisions silently mask changes | Very low (SHA-256) | High | Document; consider HMAC-keyed hash later. |
| Wide tables (>200 cols) blow up the canonical-encoding string | Low | Medium | Stream the digest in Rust; batch column groups in Postgres. |
| Users expect SQLAlchemy interop | Medium | Low | Document clearly; consider thin SA-compat shim in v0.2. |
| Cron daemon is the wrong abstraction at scale | Medium | Low | It's a v0.1 convenience; v0.2 likely adds k8s-CronJob first-class story. |

---

## Estimate

Rough total for one engineer working steady: **~3 weeks of focused work** to
v0.1 PyPI release. Phase 8 (SCD2) is the largest single chunk; Phases 0–4 are
the prerequisites that feel slow but enable everything else.

Critical path: Phase 0 → 1 → 2 → 3 → 4 → 5 → 7 → 8 → 14. Phases 6, 9, 10,
11, 12 can fan out in parallel after Phase 7 lands.
