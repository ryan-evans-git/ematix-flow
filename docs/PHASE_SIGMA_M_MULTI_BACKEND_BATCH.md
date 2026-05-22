# Σ.M — multi-backend batch target support

**Status:** design — pre-implementation
**Owner:** TBD
**Target release:** v0.6.0
**Predecessor work:** Σ.B (Backend trait spike), Phase 30c (cross-backend Arrow streaming)

## Problem

`@ematix.pipeline` (batch decorator) writes Postgres only. The
v0.5.0 surface matrix on ematix.dev claims MySQL / SQLite / DuckDB
support — that's accurate for `@ematix.streaming_pipeline`, **not**
for the batch decorator. Users hitting

```python
@ematix.pipeline(target=OrdersTable, target_connection="mysql_target", mode="merge")
```

get a deep-stack `ValueError: invalid connection URL: invalid connection
string` from `_core.connect()`. The decorator's resolution chain
(`config.connect()` → `_core.connect()`) is hardcoded to construct
`PgPool`, regardless of URL scheme. MySQL / SQLite / DuckDB DSNs
all fail at the same point.

This is a wiring gap, not a missing-feature gap.

## Existing substrate (what's already done)

The Rust core (`crates/ematix-flow-core`) already has:

- A `Backend` trait (`backend.rs:323`) with the full method surface:
  `ping`, `execute`, `read_arrow_stream`, `write_arrow_stream`,
  `ensure_table`, `run_append`, `run_truncate`, `run_merge`, `run_scd2`,
  `read_watermark` / `write_watermark`, `same_database`, plus
  cross-db helpers.
- Concrete impls for every relevant backend kind:
  - `PostgresBackend` (`backend.rs:1304`) — wraps `PgPool`
  - `MySQLBackend` (`mysql_backend.rs:472`)
  - `SQLiteBackend` (`sqlite_backend.rs:408`)
  - `DuckDBBackend` (`duckdb_backend.rs:393`)
  - Plus DeltaBackend, ObjectStoreBackend, KafkaBackend, KinesisBackend,
    PubSubBackend, RabbitMQBackend (streaming + lake targets, out of
    scope for Σ.M).
- The cross-backend Arrow streaming bridge already exists
  (`crates/ematix-flow-py/src/lib.rs:673` `cross_backend_arrow_sync`),
  proving the trait works at the Python boundary.

## What's missing

The Python binding's `Connection` struct
(`crates/ematix-flow-py/src/lib.rs:160`) hardcodes:

```rust
struct Connection { pool: Arc<PgPool>, dsn: String }
```

And every `#[pymethods]` method on it calls into `PgPool` directly
(`self.pool.run_append_same_db(...)`, `self.pool.run_merge_cross_db(...)`,
`self.pool.ensure_table(...)`, `self.pool.read_watermark(...)`).

`_core.connect(url)` (line 835) calls `PgPool::connect(&url)` regardless
of scheme.

The `Connection.dialect()` method even acknowledges this:

```rust
fn dialect(&self) -> &'static str {
    // For Phase 30c the only backend is Postgres. Phase 31+ extend
    // this to MySQL/SQLite/DuckDB/etc.
    "postgres"
}
```

## Σ.M design

### Σ.M.1 — Python-side `Connection` refactor to trait dispatch

```rust
struct Connection {
    backend: Arc<dyn Backend>,
    dsn: String,
}
```

- `_core.connect(url)` dispatches on URL scheme:
  - `postgres://` / `postgresql://` → `PostgresBackend::from_dsn(url)`
  - `mysql://` / `mysql+pymysql://` → `MySQLBackend::from_dsn(url)`
    (strip the `+driver` qualifier the Python side adds)
  - `sqlite://` / `sqlite:///path` / bare path → `SQLiteBackend::from_dsn(url)`
  - `duckdb://` / `duckdb:///path` → `DuckDBBackend::from_dsn(url)`
  - Anything else → clear error pointing at the supported scheme list.
- Every `#[pymethods]` method routes through the `Backend` trait:
  - `self.backend.run_append(spec, source_query, ...)` — the trait
    method's signature already encapsulates same-db vs cross-db dispatch
    via `source: Option<&dyn Backend>`.
  - `self.backend.run_merge(...)`, etc.
- `dialect()` returns `self.backend.dialect()` (a `Dialect` enum with
  variants per backend, already defined in `dialect.rs`).
- `connection_info()` returns `self.backend.connection_info()` (already
  on the trait).
- `cross_backend_arrow_sync` (which already takes two `&Connection`)
  needs no change because the `Connection.backend` field is the same
  trait object it expects.

### Σ.M.2 — same-db perf parity gate

The refactor introduces dyn dispatch on every `Connection` method call.
Need a bench gate confirming the pg → pg path doesn't regress.

- Re-run the existing TPC-H 22-query SQL benchmark suite. Geomean
  baseline (v0.5.0): see `docs/PHASE_SIGMA_KA_BENCH_RESULT.md`. Gate at
  ±1% regression — within the 3 pp run-to-run noise window already
  documented in [[sigma-e5-geomean-ceiling]].
- Re-run the rich-history `record_run_record` micro-bench (the
  per-batch hot path that gets hit on every pipeline tick) — gate at
  ±2% since dyn dispatch overhead is most visible on tiny ops.

### Σ.M.3 — MySQL end-to-end gate

The Rust `MySQLBackend` already exists with `ensure_table`, `run_append`,
`run_merge`, etc. — but it's currently only exercised by the streaming
test surface. Need a batch-decorator end-to-end test:

- `tests/python/integration/test_batch_mysql.py` — pg source + mysql
  target, append + merge + scd2 modes. Cross-db (pg → mysql) and
  same-db (mysql → mysql). Validates schema drift detection.
- The local validation harness (`ematix-flow-local-validation`) needs
  `orders_to_mysql` un-commented and verified end-to-end.

Open gaps to confirm at impl time:

- Does `MySQLBackend::ensure_table` produce schema-drift output
  compatible with the framework's `EnsureOutcome::Drift` enum? (look
  at impl)
- Does the merge planner handle MySQL's `INSERT ... ON DUPLICATE KEY
  UPDATE` syntax? Check `crates/ematix-flow-core/src/dialect.rs` for
  the existing translator — Σ.A.2 PR 1 already shipped one for the
  streaming path.
- SCD2 against MySQL — `valid_from` / `valid_to` semantics with
  MySQL's TIMESTAMP type (no timezone) vs Postgres's TIMESTAMPTZ.

### Σ.M.4 — SQLite + DuckDB end-to-end gates

Same shape as Σ.M.3 but for SQLite and DuckDB. These are
simpler because:

- SQLite has no concept of separate databases — the "same-db" check
  is "same file path." The framework's same-DB optimization
  (`PostgresBackend::run_append_same_db` etc.) doesn't apply; everything
  is "same-db" by definition.
- DuckDB is in-process. The connection pool size of 8 (used by PgPool)
  is meaningless; DuckDB takes one writer per file. Need to either
  serialize writes or use the existing DuckDB advisory-lock helper.

Both backends already have full `Backend` impls in tree — what's
needed is the e2e validation, not new Rust code.

### Σ.M.5 — Python-side connection wrappers

`ematix_flow.connections.MySQLConnection` (and the others) currently
exist but are consumed only by the streaming dispatch. Need:

- Each connection class exposes a `_to_dsn()` method the batch path
  can call. MySQLConnection already has `url`; just standardise.
- `config.connect(name)` accepts any of the backend kinds (not just
  postgres). The resolution chain
  (`EMATIX_FLOW_DSN_<NAME>` env / `.ematix-flow.toml` / `~/.ematix-flow/connections.toml`)
  already returns an opaque DSN string; the scheme dispatch in
  `_core.connect()` does the rest.

## Slicing plan

| Slice | Scope | Risk | Tests added |
|---|---|---|---|
| Σ.M.1 | Connection struct refactor + scheme dispatch in connect() | High — touches every Connection method | All existing 1090 Python tests keep passing on pg path |
| Σ.M.2 | Perf parity gate (TPC-H + record_run_record micro-bench) | Low | Bench results captured in PHASE_SIGMA_M_BENCH_RESULT.md |
| Σ.M.3 | MySQL end-to-end (append + merge + scd2; same-db + cross-db) | Medium — dialect translation edge cases | +6 integration tests (3 modes × 2 paths) |
| Σ.M.4 | SQLite + DuckDB end-to-end | Medium — in-process / file-based quirks | +12 integration tests |
| Σ.M.5 | Local validation harness re-enables orders_to_mysql | Low | Harness `make run-batch` end-to-end passes against mysql |
| Σ.M.6 | Docs refresh — README + USER_GUIDE + ematix.dev | Low | — |

Each slice is its own PR with its own bench / test gate. No
all-at-once merge.

## Risks + open questions

1. **`Backend::as_postgres()` escape hatch** — used by
   `PostgresBackend`'s cross-db executors to take the COPY BINARY fast
   path on pg↔pg pairs. Need to verify the existing match-all-other-
   backends path uses the generic Arrow stream bridge correctly.
   Reference: `backend.rs:394` docs already flag this as a future
   refactor target.
2. **Schema-drift detection per backend** — `ensure_table` returns
   `EnsureOutcome::Drift(Vec<Difference>)`. The PG impl walks
   `information_schema`; MySQL has a different shape (still standard);
   SQLite uses `pragma table_info()`; DuckDB has `DESCRIBE`. All four
   already implement this in their respective `*_backend.rs` modules —
   need to verify the difference list is comparable across backends
   (e.g. a "column missing" difference reads the same way regardless
   of source).
3. **Cross-db arrow streaming pairs** — pg→pg uses COPY BINARY
   (`PostgresBackend::run_append_cross_db`). Every other pair
   (pg→mysql, mysql→sqlite, etc.) routes through
   `cross_backend_arrow_sync` → trait `read_arrow_stream` /
   `write_arrow_stream`. Need to confirm all 6 source / target combos
   (P×M×S×D ordered pairs minus the same-pair, divided 2) actually work
   without surprises. The streaming surface already exercises these on
   per-batch shape, but batch-shaped "drain all rows then write" may
   have transactional / commit semantics gaps.
4. **Watermarks across backends** — `read_watermark` / `write_watermark`
   on the trait. The PG impl stores in `ematix_flow.watermarks`; each
   non-pg backend needs an equivalent meta-table. All four backends
   appear to have this; need to verify the schema is consistent.
5. **SCD2 dialect** — `_core.plan_scd2_sql` (decorators.py:572)
   generates Postgres-specific SQL (`CREATE TEMP TABLE`, `UPDATE FROM`).
   Either the planner needs to take a `Dialect` parameter and emit
   the right shape per backend, or each backend needs its own SCD2
   strategy executor. The PG path uses the latter
   (`PostgresBackend::run_scd2`); confirm the other backends' impls
   do the same.

## What this is NOT

- Not a rewrite of the streaming surface — `@ematix.streaming_pipeline`
  already supports multi-backend targets via the dispatch in
  `streaming.py:1338`.
- Not a Rust-side feature add — every backend impl is already in tree.
- Not a SQL-dialect new build — `dialect.rs` shipped in Σ.A.2 PR 1
  and is consumed by the streaming + transform-SQL paths.

## Acceptance criteria for Σ.M as a milestone

1. Every test in `tests/python/` passes on the v0.5.0 baseline AND on
   the multi-backend tip. No regression.
2. TPC-H geomean within ±1% of v0.5.0 (caveat: kernel work lives in
   ematix-parquet, so v0.6.0 perf shouldn't move regardless — gate is
   "no regression," not "improvement").
3. `orders_to_mysql` in `ematix-flow-local-validation` runs green
   through `make run-batch`.
4. Equivalent SQLite + DuckDB pipelines run green.
5. ematix.dev backend matrix updated to reflect parity (no asterisks
   on batch vs streaming columns for the four target DBs).
6. README + USER_GUIDE batch examples include at least one non-pg
   target so the surface gets exercised by anyone copy-pasting from
   docs.
