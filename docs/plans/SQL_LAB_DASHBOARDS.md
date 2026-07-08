# SQL Lab + Dashboards (Superset-style analytics for ematix-flow)

Status: **PROPOSED** — awaiting go-ahead. Author: 2026-07-07.

## 1. Goal

Add a self-service analytics surface to the existing ematix-flow web UI (today: workflows +
job history), so users can:

- Write and run ad-hoc **SQL** in a browser editor, browse the schema, and see results in a grid.
- Turn a result into a **chart** (table / line / bar / area / pie / scatter / big-number).
- Pin charts onto grid-layout **dashboards** with filters, cross-filtering, and refresh.

All query execution goes through **ematix's engine**. Per product decision:

- **Query target = source picker.** Each query runs against a user-selected data source: either
  the **in-process ematix engine** (Arrow/DataFusion over registered tables + object-storage
  sources) *or* a **configured warehouse/DB backend** (Postgres/Snowflake/BigQuery/… — the same
  Backends pipelines already use), pass-through.
- **Scope = full Superset-like**, delivered in phases (0–4 below).
- **Charting = Apache ECharts** (what Superset uses), lazy-loaded into the Svelte bundle.

## 2. What exists today (grounding)

- **UI**: Svelte 4 + Vite 5, hash router, hand-rolled CSS (dark, teal accent), `fetch` wrapper in
  `web-ui/src/lib/api.js` against `/api`. Compiles to `python/ematix_flow/web/ui_dist/`. **No
  charting or editor library.** Single bundle, no code-splitting.
- **Server**: Python FastAPI in `python/ematix_flow/web/server.py`; optional bearer-token auth
  gates `/api/*` (except `/api/health`).
- **Engine**: Rust + Arrow via PyO3 (`crates/ematix-flow-py/src/lib.rs`). DataFusion 53.1
  in-process. Backend trait in `crates/ematix-flow-core/src/backend.rs` supports Postgres, MySQL,
  SQLite, DuckDB, Snowflake, BigQuery, Redshift, object storage, streaming.
- **Result path (KEY FINDING)**: The Arrow-streaming plumbing already exists —
  `Backend::read_arrow_stream(query)` (used by `cross_backend_arrow_sync`) and
  `arrow_iter::PyArrowBatchIter` / `iter_arrow_stream` already bridge `RecordBatch → pyarrow`.
  But it is **not exposed as a `Connection.query(sql) → rows`** method: today `Connection.execute()`
  returns only a `u64` rowcount, and `fetch_scalar_int()` returns a scalar. So the row-returning
  path is a *small binding addition over existing engine code*, not new engine work.
- **Persistence**: raw-SQL store, no ORM/migrations. Default `SqliteRunLog`
  (`python/ematix_flow/run_log/sqlite.py`); pluggable via `--run-log-url` (sqlite/postgres/mysql/
  duckdb/memory). We will add analytics tables alongside, with a tiny versioned-DDL migration helper.
- **Gaps (all greenfield)**: no `/api/query`, no catalog/schema API, no saved-query/chart/dashboard
  store, no charting lib, no editor lib, no user model.

## 3. Backend design

New FastAPI router module `python/ematix_flow/web/analytics.py`, mounted under `/api`, plus a new
store module `python/ematix_flow/analytics/store.py`.

### 3.1 Data sources & catalog
- `GET  /api/datasources` → list query targets: the in-process engine (`id="ematix"`) + configured
  backends. Configured backends come from a new lightweight **datasources config** (yaml/env), NOT
  from scraping pipeline internals — keeps credentials explicit and out of the ad-hoc surface.
- `GET  /api/datasources/{id}/schemas`
- `GET  /api/datasources/{id}/schemas/{schema}/tables`
- `GET  /api/datasources/{id}/tables/{schema}/{table}/columns` → name + type + nullable.
  In-process: enumerate registered tables / object-store sources. DB backends: `information_schema`.

### 3.2 Query execution
- `POST /api/query` → body `{datasource_id, sql, max_rows?}`. Returns
  `{columns:[{name,type}], rows:[[...]], stats:{row_count, bytes_scanned?, elapsed_ms, truncated}}`.
  - Runs via the engine: for `ematix`, DataFusion `SessionContext`; for a backend, the new
    `Connection.query()` Arrow path → serialized to JSON.
  - **Guards (required, see §5)**: read-only enforcement (reject DDL/DML by default), automatic
    `LIMIT`/row cap, statement timeout, single-statement only, block access to the run-log DB and
    to local-filesystem reads for the in-process engine.
- `POST /api/query/async` + `GET /api/query/{qid}` → deferred to Phase 4 for long queries
  (synchronous-with-timeout is fine for 0–3).

### 3.3 Saved queries / charts / dashboards (CRUD)
- `/api/saved-queries` — `{id, name, datasource_id, sql, owner, created_at, updated_at}`
- `/api/charts` — `{id, name, datasource_id, sql | saved_query_id, viz_type, encoding(JSON), owner}`
- `/api/dashboards` — `{id, name, layout(JSON grid), filters(JSON), owner}` + chart membership
- `POST /api/dashboards/{id}/query` — batch-execute all charts' queries for one dashboard load
  (applies dashboard filters as parameters).

### 3.4 Storage
New tables in the existing store (idempotent `CREATE TABLE IF NOT EXISTS` + a `schema_version`
row): `saved_queries`, `charts`, `dashboards`, `dashboard_charts`. Every row carries a nullable
`owner` column now so multi-user ownership (Phase 4) is a fill-in, not a migration.

### 3.5 Engine binding (Phase 0) — SHIPPED
Added to `crates/ematix-flow-py/src/lib.rs`:
```
fn query(&self, py, sql: String, max_rows=None) -> PyResult<PyDict>   # {columns, rows, truncated}
```
wrapping the trait-level `Backend::read_arrow_stream` (sqlite/duckdb/mysql/objectstore, not just
Postgres). Arrow → Python conversion happens **in Rust** (`crates/ematix-flow-py/src/arrow_to_py.rs`):
typed fast paths (ints→`int`, floats→`float`, utf8→`str`, bool→`bool`), `ArrayFormatter` string
fallback for temporal/decimal/binary/nested, nulls→`None`; `max_rows` caps in Rust and reports
`truncated`.

**Critical constraint — no pyarrow in the engine process.** `_core` installs mimalloc as Rust's
`#[global_allocator]` and pyarrow bundles *its own* mimalloc; two mimalloc runtimes in one process
corrupt each other's arenas → hard SIGSEGV (mid-run inside `pa.record_batch` and at teardown via
`mi_process_done`). So we return native Python objects, **not** `to_pyarrow` (the Arrow C-Data-Interface
FFI also ABI-crashes on pyarrow 24 / py3.14) and **not** Arrow IPC + pyarrow decode. This also keeps
pandas/numpy out of the API path. Full root-cause writeup in `memory/project_sql_lab_dashboards.md`.

## 4. Frontend design

New hash routes and components under `web-ui/src/routes/`:
- `#/sql` → `SqlLab.svelte` — CodeMirror 6 editor (`@codemirror/lang-sql` + autocomplete fed by the
  catalog API), datasource picker, schema browser sidebar, Run/Cancel, virtualized results grid,
  query tabs, Save. (Phase 1)
- `#/charts` + `ChartBuilder.svelte` — pick saved query or inline SQL, choose viz type, map columns
  → encodings (dimensions / metrics / series / aggregation), live ECharts preview, Save. (Phase 2)
- `#/dashboards`, `#/dashboards/{id}` → `Dashboards.svelte` / `Dashboard.svelte` — grid layout
  (drag/resize), pin charts, filter bar, auto-refresh. (Phase 3–4)
- `lib/echarts.js` wrapper + `lib/charts/*` per-viz option builders; extend `lib/api.js`.

New deps: `echarts`, `codemirror` v6 (`@codemirror/lang-sql`, `@codemirror/autocomplete`,
`@codemirror/view/state`), a grid layout lib (`svelte-grid` or hand-rolled CSS grid), a
virtualized-table helper. **Introduce route-level code-splitting** so ECharts + CodeMirror load
lazily and don't bloat the existing workflows UI (the memory notes the bundle is deliberately small).

## 5. Security (ad-hoc SQL is a real surface)
- **Read-only by default. ✅** `guard_readonly` allows only leading SELECT/WITH/EXPLAIN, single
  statement, comment-stripped.
- **File / network / cross-DB block. ✅** `_DANGEROUS_SQL` denylist in `guard_readonly` rejects engine
  table-functions that read the filesystem/network (`read_csv`/`read_parquet`/`read_json*`/`*_scan`/
  `glob`/`load_extension`/`st_read`/`delta_scan`/`iceberg_scan`/`postgres_scan`/`sqlite_scan`…, matched
  as `name(`) and `ATTACH`/`INSTALL` — closes arbitrary file-read + SSRF from inside a valid SELECT.
- **Resource limits.** Row cap ✅. **Query timeout ✅** (`EMATIX_FLOW_QUERY_TIMEOUT_S`, default 30s →
  504; bounds handler wait — true mid-query cancellation via sqlite `interrupt()` / PG
  `statement_timeout` is a follow-up). Per-query **memory ceiling: TODO** (backend-specific).
- **Isolation.** Metadata DBs (run-log / analytics store) are protected by (a) not being registered as
  datasources and (b) the ATTACH/`sqlite_scan` block. Note: if an operator registers the same sqlite
  file as a datasource, its tables are queryable — document as an operator concern.
- **AuthZ**: rides the existing bearer token (single-tenant). Per-user ownership + roles = Phase 4;
  schema already carries `owner`.
- Guards covered by `test_web_security.py` (21 tests).

## 6. Phasing (each phase = one reviewable PR unless gated)

- **Phase 0 — Foundations & spike. ✅ SHIPPED.** `Connection.query()` over `read_arrow_stream`
  (returns native Python `{columns, rows, truncated}` — no pyarrow, see §3.5); read-only guard + row
  cap; datasource registry; `POST /api/query` + `GET /api/datasources`; `AnalyticsStore` +
  `schema_version`. Root-caused & fixed a dual-mimalloc native segfault along the way.
- **Phase 1 — SQL Lab. ✅ SHIPPED.** Catalog endpoints (`/api/datasources/{id}/schemas|tables|columns`,
  dialect-aware sqlite + information_schema); warehouse/DB pass-through works via the datasource URL;
  saved-queries CRUD (`/api/saved-queries`); `run_server(datasources=, analytics_store=)` wiring;
  Svelte `SqlLab.svelte` — CodeMirror 6 editor (lazy-loaded chunk), schema browser, results grid,
  save/load. `#/sql` route + nav. 90 py tests green; verified end-to-end in the preview server.
  *Follow-ups: CLI flags for `--datasource`/`--analytics-db`; §5 isolation guards; push `LIMIT` down.*
- **Phase 2 — Charts. ✅ SHIPPED.** ECharts (lazy chunk via `EChart.svelte`); `charts` table +
  CRUD (`/api/charts`, `AnalyticsStore` schema v2); `ChartBuilder.svelte` (`#/charts`) — SQL editor →
  run → viz-type picker (table/number/bar/line/area/pie/scatter) → column→encoding mappers → live
  preview → save/load. Pure option builder in `lib/charts.js`. 8 chart tests green; verified
  end-to-end (bar/pie canvases, big-number sum, save→reload→load round-trip).
- **Phase 3 — Dashboards. ✅ SHIPPED.** `dashboards` table + CRUD (`/api/dashboards`, AnalyticsStore
  schema v3, layout JSON); batch `POST /api/dashboards/{id}/query` (runs every tile's chart SQL,
  guarded, returns per-chart viz_type/encoding/data or error). Frontend: hand-rolled drag/resize
  `DashboardGrid.svelte` (12-col snap grid, no dep), shared `ChartView.svelte`, `Dashboards.svelte`
  (`#/dashboards`) — list/new, edit toggle, add-chart picker, remove tile, auto-refresh interval.
  7 dashboard tests green; verified end-to-end (2 tiles render via batch query; drag + resize snap
  and persist to backend). ★ grid geometry must be a reactive `$:` derived array, not a helper fn —
  a fn hides the `cellStride` dependency and tiles stick at 0-width until first interaction.
- **Phase 4 — Superset parity. ✅ SHIPPED (except full auth — see below).**
  - **Filters + cross-filtering.** `build_filtered_sql` subquery-wraps + ANDs `IN (…)` for columns
    the chart outputs; `POST /api/dashboards/{id}/query` takes `{filters}`; filter bar (chips/add/clear)
    re-queries all tiles. Verified: region=west collapses bar 4→1 + pie to one ring.
  - **Result cache + async jobs.** TTL cache (`EMATIX_FLOW_CACHE_TTL_S`) used by dashboard tiles
    (`stats.cached`); `POST /api/query/async` + `GET /api/query/jobs/{id}` (`query_jobs.py`) for slow
    queries; `POST /api/cache/clear`.
  - **Alerts.** `alerts` table + CRUD; `POST /api/alerts/{id}/check` evaluates a chart column vs
    `op`/`threshold` (`evaluate_alert`). Cron wiring = call `/check` from the existing scheduler.
  - **No-SQL chart builder.** `VisualQueryBuilder.svelte` (table + dimensions + metrics/aggregation →
    GROUP BY SQL) behind a SQL/Build toggle in the Chart Builder. Verified: generates + runs.
  - **Drill-down.** Click a dashboard chart → modal of the underlying rows for that category + a
    "Filter dashboard by this" button.
  - **Ownership.** `owner` set from the bearer-token identity (`_identity`) on create across
    saved-queries/charts/dashboards/alerts. Verified.
  - **Deferred (needs a decision):** *full auth* — real login / SSO / per-user tokens + RBAC. The
    ownership plumbing is in and single-tenant identity works; the identity model is the operator's
    call. Also: per-query memory ceiling; true mid-query cancellation.
  - *Note: ECharts data-clicks (cross-filter + drill trigger) can't be synthetically driven in the
    preview harness — those paths are code-verified + share verified downstream pipelines.*

## 7. Open decisions (not blocking Phase 0)
- Datasources config format & where credentials live (reuse pipeline connection config vs. new file).
- Multi-user/auth model timing (bearer token now; full model Phase 4).
- Grid layout library choice (svelte-grid vs. hand-rolled) — decide at Phase 3.
- Async execution backend for long queries (thread pool vs. reuse run-log worker) — decide at Phase 4.

## 8. Testing
Follow TDD (house rule). Phase 0: pytest for `/api/query` (happy path, row cap, read-only rejection,
timeout, both datasource types via a sqlite/duckdb fixture) + a Rust unit test for the new binding.
Front-end phases: component tests + manual verification via the preview server.
