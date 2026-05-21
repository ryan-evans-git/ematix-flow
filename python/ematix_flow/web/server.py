"""FastAPI app + uvicorn launcher.

Phase 4a-1 (server skeleton + stub data).
Phase 4a-2: ``create_app`` takes an optional :class:`RunHistoryStore`
  (see :mod:`ematix_flow.run_log.history`); when provided, the GET
  endpoints return real records instead of the stub list.

When the user runs ``flow web`` without configuring a store, the
server falls back to the stub data so the placeholder UI is still
useful for trying out the surface.
"""
from __future__ import annotations

import logging
import sys
from importlib import resources
from pathlib import Path
from typing import Any

from ematix_flow.run_log.history import RunHistoryStore, RunRecord

logger = logging.getLogger(__name__)


# ---- Stub data -----------------------------------------------------

# Replaced in the next slice by real RunLog.list_runs() / .get_run()
# results. Lets the SPA and CLI be developed against a fixed shape.
_STUB_RUNS: list[dict[str, Any]] = [
    {
        "run_id": "01HQSTUB000000000000000001",
        "pipeline": "warehouse_etl",
        "status": "succeeded",
        "started_at": "2026-05-20T14:30:00Z",
        "finished_at": "2026-05-20T14:30:47Z",
        "duration_ms": 47000,
        "attempt": 1,
        "failed_step": None,
    },
    {
        "run_id": "01HQSTUB000000000000000002",
        "pipeline": "warehouse_etl",
        "status": "failed",
        "started_at": "2026-05-20T15:30:00Z",
        "finished_at": "2026-05-20T15:30:35Z",
        "duration_ms": 35000,
        "attempt": 2,
        "failed_step": "merge_payments",
    },
    {
        "run_id": "01HQSTUB000000000000000003",
        "pipeline": "events_stream",
        "status": "running",
        "started_at": "2026-05-20T16:00:00Z",
        "finished_at": None,
        "duration_ms": None,
        "attempt": 1,
        "failed_step": None,
    },
]

_STUB_PIPELINES: list[dict[str, Any]] = [
    {
        "name": "warehouse_etl",
        "kind": "batch",
        "latest_run": _STUB_RUNS[1],
        "failure_rate_7d": 0.13,
        "median_duration_ms": 47000,
        # Last 10 oldest→newest (left-to-right). Mix of durations so
        # the height-encoding reads as variance, plus the failed run.
        "recent_runs": [
            {"run_id": f"01HQSTUB-WH-{i:02d}", "status": "succeeded",
             "started_at": "2026-05-19T00:00:00Z", "duration_ms": d}
            for i, d in enumerate([
                42_000, 45_000, 51_000, 47_000, 110_000,
                46_000, 48_000, 89_000, 43_000,
            ])
        ] + [
            {"run_id": _STUB_RUNS[1]["run_id"], "status": "failed",
             "started_at": _STUB_RUNS[1]["started_at"],
             "duration_ms": _STUB_RUNS[1]["duration_ms"]},
        ],
        "next_run_at": "2026-05-21T17:00:00Z",
        "timezone": None,
    },
    {
        "name": "events_stream",
        "kind": "streaming",
        "latest_run": _STUB_RUNS[2],
        "failure_rate_7d": 0.0,
        "median_duration_ms": None,
        # Streaming: prior daemon sessions ran for varied durations
        # (manual restarts, deployments, etc.); current session is open.
        "recent_runs": [
            {"run_id": f"01HQSTUB-EV-{i:02d}", "status": "succeeded",
             "started_at": "2026-05-20T00:00:00Z", "duration_ms": d}
            for i, d in enumerate([
                7_200_000, 1_800_000, 3_600_000, 14_400_000, 600_000,
                3_600_000, 2_400_000, 10_800_000, 5_400_000,
            ])
        ] + [
            {"run_id": _STUB_RUNS[2]["run_id"], "status": "running",
             "started_at": _STUB_RUNS[2]["started_at"], "duration_ms": None},
        ],
        "next_run_at": None,
        "timezone": None,
        # v0.5.0: streaming pipelines surface throughput + batch cycle
        # (live snapshots from the daemon's metrics endpoint), shown in
        # the UI footer in place of "Median duration".
        "streaming_stats": {
            "snapshot_at": "2026-05-21T16:00:30Z",
            "rows_consumed_total": 124_500,
            "rows_written_total": 124_500,
            "batches_total": 415,
            "errors_total": 0,
            "stats_1m": {
                "rows_consumed_per_sec": 412.0,
                "rows_written_per_sec": 412.0,
                "batches_per_sec": 1.4,
                "errors_per_sec": 0.0,
                "avg_batch_cycle_ms": 714.3,
                "span_seconds": 60.0,
            },
            "stats_5m": {
                "rows_consumed_per_sec": 398.5,
                "rows_written_per_sec": 398.5,
                "batches_per_sec": 1.35,
                "errors_per_sec": 0.0,
                "avg_batch_cycle_ms": 740.7,
                "span_seconds": 300.0,
            },
        },
    },
]


def _action_buttons_for(run: dict[str, Any]) -> dict[str, Any]:
    """Server-side gating of which action buttons the UI may show.

    Returns a dict describing the actions allowed on this run's
    current status. The SPA renders buttons based on this output —
    keeps the policy in one place.

    Accepts a dict (stub fallback path) or, by callers, the output
    of :func:`_detail_payload_from_record` which has the same
    shape.
    """
    status = run["status"]
    kind = run.get("kind") or (
        "streaming" if run["pipeline"] == "events_stream" else "batch"
    )
    is_streaming = kind == "streaming"
    actions: dict[str, Any] = {
        "pause": False,
        "resume": False,
        "restart_from_step": [],
        "rerun_full": False,
        "resume_from_watermark": False,
    }
    if status == "running":
        actions["pause"] = True
    elif status == "paused":
        actions["resume"] = True
    elif status == "failed":
        if is_streaming:
            actions["resume_from_watermark"] = True
        else:
            actions["restart_from_step"] = (
                [run["failed_step"]] if run.get("failed_step") else []
            )
        actions["rerun_full"] = True
    elif status in {"succeeded", "cancelled"}:
        actions["rerun_full"] = True
    return actions


def _detail_payload_from_record(record: RunRecord) -> dict[str, Any]:
    """Convert a :class:`RunRecord` into the
    ``GET /api/runs/:id`` JSON shape, including the synthetic
    ``attempts`` and ``steps`` arrays the UI renders.

    For Phase 4a-2 we synthesize a single-attempt history from the
    record (real per-attempt history needs a separate
    ``AttemptRecord`` model — Phase 4c follow-up). The ``steps``
    array is empty for runs that don't carry per-step state; the UI
    handles the empty case gracefully.
    """
    detail = record.to_detail_dict()
    detail["attempts"] = [
        {
            "attempt": record.attempt,
            "started_at": detail["started_at"],
            "finished_at": detail["finished_at"],
            "status": record.status,
            "failed_step": record.failed_step,
            "error_summary": record.error_summary,
            "error_stack": None,  # not captured in RunRecord today
        }
    ]
    # Surface per-step DAG state if a backend (or demo data) stashed it
    # in extras["steps"]. Shape: ``list[dict]`` with at least ``name`` +
    # ``status``; optional ``depends_on: list[str]`` + ``duration_ms``.
    detail["steps"] = list(record.extras.get("steps") or [])
    detail["actions"] = _action_buttons_for(detail)
    return detail


# ---- App factory ---------------------------------------------------


def create_app(
    *,
    history: RunHistoryStore | None = None,
    ui_dist_dir: Path | None = None,
    bearer_token: str | None = None,
):
    """Build the FastAPI app.

    Parameters:

    - ``history`` — optional :class:`RunHistoryStore` (Phase 4a-2).
      When provided, ``/api/runs`` and ``/api/runs/:id`` return real
      records via ``history.list_runs()`` / ``history.get_run()``.
      When ``None``, the GET endpoints fall back to a stub list so
      the server is still useful for trying out the API surface.
    - ``ui_dist_dir`` overrides where the SPA bundle is served
      from. Default behavior is to load from the package's
      ``ematix_flow.web.ui_dist`` data dir, which is populated by
      the Vite build at wheel-build time. If absent, the server
      serves a friendly placeholder HTML page.
    - ``bearer_token`` — when set, ``/api/*`` routes require an
      ``Authorization: Bearer <token>`` header that matches. Lets
      operators bind the server to non-loopback addresses safely.
      ``/api/health`` stays open so load-balancer / readiness probes
      keep working.
    """
    try:
        from fastapi import Body, FastAPI, HTTPException, Request
        from fastapi.responses import HTMLResponse
        from fastapi.staticfiles import StaticFiles
    except ImportError as exc:
        raise ImportError(
            "fastapi is required for the ematix-flow web UI; install with "
            "`pip install ematix-flow[web]`"
        ) from exc

    app = FastAPI(
        title="ematix-flow Web UI",
        version="0.4.0",
        docs_url="/api/docs",
        redoc_url=None,
    )

    # Task #6: bearer-token middleware. Applied as an HTTP-level
    # middleware (not a per-route Depends) so SPA static files +
    # docs stay reachable without auth, while every /api/* route
    # except /api/health is gated. Using constant-time compare so
    # a timing attack can't fingerprint valid tokens.
    if bearer_token is not None:
        import hmac

        @app.middleware("http")
        async def _bearer_token_gate(request: Request, call_next):
            path = request.url.path
            # Static / SPA paths + /api/health are open; everything
            # else under /api/ requires the bearer token.
            if not path.startswith("/api/") or path == "/api/health":
                return await call_next(request)
            header = request.headers.get("authorization", "")
            if not header.startswith("Bearer "):
                from fastapi.responses import JSONResponse

                return JSONResponse(
                    {"detail": "missing Authorization: Bearer <token> header"},
                    status_code=401,
                )
            presented = header[len("Bearer "):].strip()
            if not hmac.compare_digest(presented, bearer_token):
                from fastapi.responses import JSONResponse

                return JSONResponse(
                    {"detail": "invalid bearer token"}, status_code=401,
                )
            return await call_next(request)

    @app.get("/api/health")
    def health() -> dict[str, str]:  # type: ignore[unused-function]
        return {"status": "ok"}

    @app.get("/api/runs")
    def list_runs(
        pipeline: str | None = None,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:  # type: ignore[unused-function]
        if history is not None:
            records, total = history.list_runs(
                pipeline=pipeline, status=status, limit=limit, offset=offset
            )
            return {
                "runs": [r.to_summary_dict() for r in records],
                "total": total,
                "next_offset": offset + limit if offset + limit < total else None,
            }
        # Stub fallback (no history store configured).
        rows = _STUB_RUNS
        if pipeline:
            rows = [r for r in rows if r["pipeline"] == pipeline]
        if status:
            rows = [r for r in rows if r["status"] == status]
        total = len(rows)
        sliced = rows[offset : offset + limit]
        return {
            "runs": sliced,
            "total": total,
            "next_offset": offset + limit if offset + limit < total else None,
        }

    @app.get("/api/runs/{run_id}")
    def get_run(run_id: str) -> dict[str, Any]:  # type: ignore[unused-function]
        if history is not None:
            record = history.get_run(run_id)
            if record is None:
                raise HTTPException(
                    status_code=404, detail=f"run {run_id} not found"
                )
            return _detail_payload_from_record(record)
        # Stub fallback.
        for r in _STUB_RUNS:
            if r["run_id"] == run_id:
                return {
                    **r,
                    "attempts": [
                        {
                            "attempt": r["attempt"],
                            "started_at": r["started_at"],
                            "finished_at": r["finished_at"],
                            "status": r["status"],
                            "failed_step": r.get("failed_step"),
                            "error_summary": None
                            if r["status"] != "failed"
                            else "stub error",
                            "error_stack": None,
                        }
                    ],
                    "steps": [
                        {"name": "load", "status": "succeeded", "duration_ms": 12000},
                        {
                            "name": r.get("failed_step") or "transform",
                            "status": r["status"],
                            "duration_ms": (r["duration_ms"] or 0) - 12000,
                        },
                    ],
                    "actions": _action_buttons_for(r),
                }
        raise HTTPException(status_code=404, detail=f"run {run_id} not found")

    @app.get("/api/pipelines")
    def list_pipelines() -> dict[str, Any]:  # type: ignore[unused-function]
        if history is not None:
            # Aggregate from the rich-history store. Pull a generous
            # window (1000 most recent) and bucket by pipeline name.
            records, _ = history.list_runs(limit=1000, offset=0)
            by_pipeline: dict[str, list[RunRecord]] = {}
            for r in records:
                by_pipeline.setdefault(r.pipeline, []).append(r)
            pipelines = []
            for name, runs in by_pipeline.items():
                runs.sort(key=lambda r: r.started_at, reverse=True)
                latest = runs[0]
                durations = [
                    r.duration_ms for r in runs if r.duration_ms is not None
                ]
                failed = sum(1 for r in runs if r.status == "failed")
                # Last 10 executions, oldest → newest for left-to-right
                # rendering. A run that retried-then-succeeded has
                # status="succeeded" already (the final attempt is
                # what counts), so attempt-count isn't surfaced here.
                recent = [r.to_summary_dict() for r in runs[:10]][::-1]
                # `next_run_at` is the most-recent forecast the scheduler
                # produced (set via record.extras["next_run_at"]). For
                # streaming pipelines we omit it and let the UI render
                # "LIVE STREAMING" instead. If no extra is present, ask
                # the in-process registry to forecast from the cron +
                # timezone — this keeps the panel useful even on the
                # first boot before any scheduler tick has run.
                next_run_at = None
                pipeline_tz = None
                if latest.kind != "streaming":
                    next_run_at = latest.extras.get("next_run_at")
                    pipeline_tz = latest.extras.get("timezone")
                    if next_run_at is None or pipeline_tz is None:
                        sp = _registry_lookup(name)
                        if sp is not None:
                            if pipeline_tz is None:
                                pipeline_tz = sp.timezone
                            if next_run_at is None and sp.schedule is not None:
                                from ematix_flow.pipeline import forecast_next_run

                                forecast = forecast_next_run(
                                    sp.schedule, timezone=sp.timezone,
                                )
                                if forecast is not None:
                                    next_run_at = forecast.isoformat()
                # v0.5.0: surface streaming live-stats from extras. The
                # daemon writes throughput / cycle-time into the running
                # record's extras every ~30s; we just pass them through
                # so the UI can render them in place of "Median duration".
                streaming_stats: dict[str, Any] | None = None
                if latest.kind == "streaming":
                    e = latest.extras or {}
                    if any(
                        k in e
                        for k in (
                            "snapshot_at", "stats_1m", "stats_5m",
                            "rows_consumed_total",
                        )
                    ):
                        streaming_stats = {
                            "snapshot_at": e.get("snapshot_at"),
                            "rows_consumed_total": e.get("rows_consumed_total"),
                            "rows_written_total": e.get("rows_written_total"),
                            "batches_total": e.get("batches_total"),
                            "errors_total": e.get("errors_total"),
                            "stats_1m": e.get("stats_1m"),
                            "stats_5m": e.get("stats_5m"),
                        }
                pipelines.append(
                    {
                        "name": name,
                        "kind": latest.kind,
                        "latest_run": latest.to_summary_dict(),
                        "failure_rate_7d": (failed / len(runs)) if runs else 0.0,
                        "median_duration_ms": (
                            _median(durations) if durations else None
                        ),
                        "recent_runs": recent,
                        "next_run_at": next_run_at,
                        "timezone": pipeline_tz,
                        "streaming_stats": streaming_stats,
                    }
                )
            # Sort pipelines so the most recently active is on top.
            pipelines.sort(
                key=lambda p: p["latest_run"]["started_at"] or "",
                reverse=True,
            )
            return {"pipelines": pipelines}
        return {"pipelines": _STUB_PIPELINES}

    # ---- Cross-pipeline DAG view (task #7) -------------------------
    @app.get("/api/dag")
    def pipeline_dag() -> dict[str, Any]:  # type: ignore[unused-function]
        """Cross-pipeline DAG: nodes = registered pipelines, edges =
        ``depends_on`` declarations. Renders in the SPA as a tree to
        spot bottlenecks (deep chains, fan-out hot spots)."""
        try:
            from ematix_flow.pipeline import _DEPENDS_ON, _REGISTRY
        except Exception:
            return {"nodes": [], "edges": []}

        nodes = []
        for name, sp in sorted(_REGISTRY.items()):
            nodes.append(
                {
                    "name": name,
                    "schedule": sp.schedule,
                    "timezone": getattr(sp, "timezone", None),
                }
            )
        edges = []
        for name, upstreams in _DEPENDS_ON.items():
            for upstream in upstreams:
                # Edge direction: upstream → downstream (upstream
                # produces, downstream consumes).
                edges.append({"from": upstream, "to": name})
        return {"nodes": nodes, "edges": edges}

    # ---- Mutating actions (Phase 4b) -------------------------------
    #
    # All four endpoints require a configured history store; without
    # one the server only has stub data and there's nothing real to
    # mutate. Anonymous mode (the default Phase 4a auth choice) is
    # *not* gated here — the server already binds 127.0.0.1 by
    # default, so OS-level user boundary is the trust boundary.
    # Operators who pass --bind <non-loopback> see the loud warning
    # at startup.

    def _require_history():
        if history is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    "mutating actions require a configured RunHistoryStore; "
                    "this server was started without one (stub mode)"
                ),
            )

    @app.post("/api/runs/{run_id}/restart")
    def post_restart(
        run_id: str,
        body: dict[str, Any] = Body(default_factory=dict),  # noqa: B008
    ) -> dict[str, Any]:  # type: ignore[unused-function]
        _require_history()
        assert history is not None  # narrowed by _require_history
        from_step = body.get("from_step")
        try:
            new_id = history.enqueue_restart(run_id, from_step)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"new_run_id": new_id}

    @app.post("/api/runs/{run_id}/rerun")
    def post_rerun(run_id: str) -> dict[str, Any]:  # type: ignore[unused-function]
        _require_history()
        assert history is not None
        try:
            new_id = history.enqueue_rerun(run_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"new_run_id": new_id}

    @app.post("/api/runs/{run_id}/pause")
    def post_pause(run_id: str) -> dict[str, Any]:  # type: ignore[unused-function]
        _require_history()
        assert history is not None
        try:
            history.set_pause(run_id, True)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"status": "pause_requested"}

    @app.post("/api/runs/{run_id}/resume")
    def post_resume(run_id: str) -> dict[str, Any]:  # type: ignore[unused-function]
        _require_history()
        assert history is not None
        try:
            history.set_pause(run_id, False)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"status": "resume_requested"}

    # ---- SPA bundle ------------------------------------------------

    resolved_ui = _resolve_ui_dir(ui_dist_dir)
    if resolved_ui is not None and resolved_ui.is_dir() and any(resolved_ui.iterdir()):
        app.mount(
            "/",
            StaticFiles(directory=str(resolved_ui), html=True),
            name="ui",
        )
        logger.info("Web UI serving SPA bundle from %s", resolved_ui)
    else:
        # No bundle present — serve a placeholder so `flow web` is
        # still useful (the JSON API still works at /api/*).
        @app.get("/", response_class=HTMLResponse)
        def root_placeholder() -> str:  # type: ignore[unused-function]
            return _PLACEHOLDER_HTML

    return app


def _median(xs: list[int]) -> int:
    """Integer median. ``xs`` must be non-empty; callers gate."""
    s = sorted(xs)
    mid = len(s) // 2
    if len(s) % 2 == 0:
        return (s[mid - 1] + s[mid]) // 2
    return s[mid]


def _registry_lookup(name: str):
    """Look up a registered ScheduledPipeline by name, returning ``None``
    if the registry is empty in this process. The web server runs in a
    process where the user's pipelines may or may not be imported; we
    do not want a missing import to make the API panel blank."""
    try:
        from ematix_flow.pipeline import _REGISTRY  # type: ignore[attr-defined]
    except Exception:
        return None
    return _REGISTRY.get(name)


def _resolve_ui_dir(override: Path | None) -> Path | None:
    if override is not None:
        return override
    # Try the in-package location populated by the Vite build at
    # wheel-build time.
    try:
        with resources.as_file(
            resources.files("ematix_flow.web") / "ui_dist"
        ) as path:
            return Path(path)
    except (FileNotFoundError, ModuleNotFoundError):
        return None


_PLACEHOLDER_HTML = """\
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>ematix-flow Web UI</title>
<style>
  body {
    background: #060a08;
    color: #33dccc;
    font-family: "IBM Plex Mono", ui-monospace, monospace;
    margin: 0;
    padding: 4rem;
    min-height: 100vh;
  }
  h1 {
    color: #b6f0e8;
    font-family: "VT323", monospace;
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }
  code {
    color: #ffb000;
    background: rgba(255, 176, 0, 0.08);
    padding: 0.05em 0.35em;
    border: 1px solid #b07800;
  }
  a { color: #ffb000; }
</style>
</head>
<body>
  <h1>▸ ematix-flow Web UI</h1>
  <p>
    The JSON API is live at <code>/api/runs</code>, <code>/api/runs/&lt;id&gt;</code>,
    and <code>/api/pipelines</code>. Interactive Swagger docs at
    <a href="/api/docs">/api/docs</a>.
  </p>
  <p>
    The SPA bundle wasn't found in this install. To build it from a source
    checkout: <code>cd web-ui &amp;&amp; npm install &amp;&amp; npm run build</code>.
    Wheel installs (<code>pip install ematix-flow[web]</code>) bundle it at
    build time so this page won't appear.
  </p>
</body>
</html>
"""


# ---- uvicorn launcher ----------------------------------------------


def run_server(
    *,
    host: str = "127.0.0.1",
    port: int = 8080,
    log_level: str = "info",
    bearer_token: str | None = None,
    history: RunHistoryStore | None = None,
) -> None:
    """Launch the uvicorn server.

    Defaults to ``127.0.0.1`` so the UI is unreachable off-host
    without an explicit ``--bind <addr>`` opt-in. Binding to
    ``0.0.0.0`` (or any non-loopback) prints a loud warning **unless
    bearer-token auth is configured** — once a token is set, mutating
    actions require it and the non-loopback bind is safe.
    """
    try:
        import uvicorn  # type: ignore[import-not-found]
    except ImportError as exc:
        raise ImportError(
            "uvicorn is required for the ematix-flow web UI; install with "
            "`pip install ematix-flow[web]`"
        ) from exc

    is_loopback = host in {"127.0.0.1", "localhost", "::1"}
    if not is_loopback and bearer_token is None:
        print(
            f"WARNING: ematix-flow web is binding to {host!r} with no "
            "bearer-token auth. Anyone who can reach this port can trigger "
            "restart / rerun / pause actions on your pipelines. Pass "
            "--token <secret> (or set EMATIX_FLOW_WEB_TOKEN env var) before "
            "exposing this off-host.",
            file=sys.stderr,
        )

    app = create_app(bearer_token=bearer_token, history=history)
    uvicorn.run(app, host=host, port=port, log_level=log_level)
