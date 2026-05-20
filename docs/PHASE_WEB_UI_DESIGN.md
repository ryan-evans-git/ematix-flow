# Phase 4 — Web UI for run history (design)

**Updated**: 2026-05-20 after the user's requirement that the UI must
support restart-from-failed-step and rerun-from-beginning. That
requirement shifts the surface from read-only to read + mutate, so
this doc captures the architecture before code lands.

## Goals

1. Replace `flow runs ...` (CLI-only today) with a browser UI that
   lists runs, drills into a single run's detail, and shows the
   per-pipeline summary.
2. Let operators **restart a failed run from the failed step** or
   **rerun a run from the beginning** via UI actions.
3. Match ematix.dev's Pip-Boy / Fallout phosphor-teal aesthetic.
4. Ship as a single command (`flow web --port 8080`) — no external
   reverse-proxy required to be useful.

## Why Python, not Rust

The RunLog backends (SQLite, Postgres, MySQL, S3, Azure, GCS, DuckDB)
all live in `python/ematix_flow/run_log/`. A Rust server would need
to re-implement seven backend drivers; a Python server can call the
existing Protocol implementations directly. Decision: server is
Python (FastAPI + uvicorn). The frontend SPA is build-system-agnostic
and works against either.

## Architecture

```
┌────────────────────────────────────────────────────────────────┐
│  Browser                                                        │
│  ──────────────────────────────────────────────────────────     │
│  Vite + Svelte SPA  (CSS matches ematix.dev phosphor theme)     │
│  Pages:                                                          │
│    /runs            — list, sortable + filterable                │
│    /runs/:id        — detail + restart / rerun action buttons    │
│    /pipelines       — per-pipeline summary                       │
└─────────────────────────┬──────────────────────────────────────┘
                          │ JSON over HTTP
┌─────────────────────────▼──────────────────────────────────────┐
│  flow-web (Python, FastAPI + uvicorn)                           │
│  ──────────────────────────────────────────────────────────     │
│  GET  /api/runs?pipeline=&status=&since=&limit=                  │
│  GET  /api/runs/:id                                              │
│  GET  /api/pipelines                                             │
│  POST /api/runs/:id/restart        (enqueue restart-from-step)   │
│  POST /api/runs/:id/rerun          (enqueue rerun-from-beginning)│
│  GET  /api/health                                                │
│  GET  /                            (serve embedded SPA bundle)   │
│  Bearer-token auth (optional in dev; required by default in prod)│
└─────────────────────────┬──────────────────────────────────────┘
                          │ via the existing Python Protocol
┌─────────────────────────▼──────────────────────────────────────┐
│  RunLog (any of: sqlite, postgres, mysql, s3, azure, gcs, duckdb)│
└─────────────────────────┬──────────────────────────────────────┘
                          │
            ┌─────────────┴─────────────┐
            │                            │
            ▼                            ▼
┌──────────────────────┐     ┌──────────────────────┐
│  flow scheduler      │     │  flow worker         │
│  picks up enqueued   │     │  executes runs that  │
│  restart / rerun     │     │  the scheduler claims│
│  rows                │     │                      │
└──────────────────────┘     └──────────────────────┘
```

Web UI never executes pipelines itself. Restart / rerun are written
to the RunLog as new rows with `status = "requested"` and one of
two flags:

- `restart_from_step = <step_name>` — picked up by the scheduler;
  worker resumes the DAG from that node, reusing prior steps'
  outputs.
- `rerun_full = true` — picked up by the scheduler; worker runs
  the whole pipeline from scratch.

The existing `flow scheduler` daemon (Ω.W.6) already polls the
RunLog for due runs and dispatches them via the executor (Ω.W.3-5).
Adding `restart_from_step` / `rerun_full` is a small extension to
its claim+dispatch loop.

## REST contract

All endpoints return JSON. Bearer-token auth is enforced when the
server is started with `--auth-token <T>`; without one, the server
starts in **anonymous mode** with a startup warning. Anonymous mode
also disables mutating endpoints — POST /restart and /rerun require
a token.

### `GET /api/runs?pipeline=&status=&since=&limit=&offset=`

```json
{
  "runs": [
    {
      "run_id": "01HQ...",
      "pipeline": "warehouse_etl",
      "status": "failed",
      "started_at": "2026-05-20T14:32:01Z",
      "finished_at": "2026-05-20T14:32:48Z",
      "duration_ms": 47000,
      "attempt": 2,
      "failed_step": "merge_payments"
    }
  ],
  "total": 1234,
  "next_offset": 50
}
```

### `GET /api/runs/:id`

```json
{
  "run_id": "01HQ...",
  "pipeline": "warehouse_etl",
  "status": "failed",
  "started_at": "...",
  "finished_at": "...",
  "attempts": [
    {
      "attempt": 1,
      "started_at": "...",
      "finished_at": "...",
      "status": "failed",
      "failed_step": "merge_payments",
      "error_summary": "...",
      "error_stack": "..."
    },
    { "attempt": 2, "status": "failed", ... }
  ],
  "steps": [
    { "name": "load_orders",     "status": "succeeded", "duration_ms": 12000 },
    { "name": "merge_payments",  "status": "failed",    "duration_ms": 35000, "error": "..." }
  ],
  "actions": {
    "restart_from_step": ["merge_payments"],
    "rerun_full": true
  }
}
```

`actions` is server-side gating: only failed runs get a non-empty
restart_from_step list; only finished runs (succeeded or failed)
permit rerun_full. Streaming pipelines, which don't have discrete
steps, get an empty `restart_from_step` and the UI hides that
button.

### `POST /api/runs/:id/restart`

```json
{ "from_step": "merge_payments" }
```

Response:

```json
{ "new_run_id": "01HQ...new..." }
```

### `POST /api/runs/:id/rerun`

```json
{}
```

Response:

```json
{ "new_run_id": "01HQ...new..." }
```

### `GET /api/pipelines`

```json
{
  "pipelines": [
    {
      "name": "warehouse_etl",
      "latest_run": { "run_id": "...", "status": "failed", "started_at": "..." },
      "failure_rate_7d": 0.13,
      "median_duration_ms": 47000
    }
  ]
}
```

## Frontend pages

### `/runs` — list

- Filter: pipeline (multi-select), status (multi-select), time range
- Sort: started_at desc (default), duration_ms, status
- Pagination: 50 per page, infinite scroll
- Each row: pipeline · status · started_at · duration · attempts
- Click → /runs/:id

### `/runs/:id` — detail

- Header: pipeline, run_id, status (colored), started/finished
- Step timeline (left rail): one row per step, color-coded by
  status, click to anchor that step's logs
- Attempts panel: collapsible per-attempt block with error stack
- **Action buttons** (top-right, only when permitted):
  - **Restart from failed step** — dropdown lists the failed
    steps; click → confirm modal → POST /restart → toast +
    redirect to the new run's /runs/:id
  - **Rerun from beginning** — single button → confirm modal →
    POST /rerun → toast + redirect
  - Both disabled in anonymous mode with tooltip "set
    EMATIX_FLOW_WEB_TOKEN to enable mutating actions"

### `/pipelines` — summary

- One panel per pipeline, sorted by failure_rate_7d desc
- Each panel: name, sparkline of recent runs, failure rate,
  median duration, click → /runs?pipeline=name

## Auth

- `--auth-token <T>` or `EMATIX_FLOW_WEB_TOKEN` env var enables
  bearer-token auth. All endpoints require `Authorization: Bearer
  <T>`; reject with 401 otherwise.
- Without a token, the server starts in **anonymous mode**:
  - GET endpoints serve.
  - POST endpoints return 403 with a clear message ("server is
    running without auth; mutating actions are disabled").
  - Startup logs a warning.
- HTTPS is operator's job (reverse proxy). Documented in
  DEPLOYMENT.md Recipe 10.

## Slice plan

This is a large bite. Splitting:

- **Phase 4a (this slice)**: Python server + GET endpoints + the
  Svelte SPA's read-only pages. Buttons are present but disabled
  with a "Phase 4b" tooltip. Embed the bundled SPA into a Python
  wheel. CLI: `flow web` runs uvicorn.
- **Phase 4b**: POST endpoints, action buttons wired,
  scheduler-side pickup of `restart_from_step` / `rerun_full`.
  Requires touching the scheduler claim loop in Ω.W.6.

Phase 4a delivers the visual replacement for `flow runs ...` (the
ematix.dev claim). Phase 4b delivers the restart/rerun controls.
Both ship under one PR per the user's batching preference.

## Estimated effort

- Phase 4a: 3-4 days. SPA scaffolding, three pages, FastAPI server
  with three GET endpoints, embed-into-Python-wheel build, CLI
  wiring, smoke tests, theme matching.
- Phase 4b: 2-3 days. POST endpoints, action UI, scheduler
  extension, end-to-end test.

## Open questions for the user

1. **Auth**: bearer-token-via-flag is the cheapest first cut.
   Want OIDC / OAuth2 from day one, or is the flag fine to start?
2. **Frontend hosting**: embed the Vite bundle into the Python
   wheel (single install, no extra build step for the operator),
   or ship the SPA as a separate npm package and have `flow web`
   serve it as a path arg? The plan above assumes embed.
3. **Streaming pipeline restart**: streaming jobs don't have
   discrete "steps" in the DAG sense — they have a watermark.
   Restart-from-failed-step on a streaming run probably means
   "resume from the last committed watermark" (which the
   scheduler already does on its own). For the UI, I propose
   we show restart-from-failed-step **only for DAG / batch
   pipelines** and hide it for streaming. OK?
