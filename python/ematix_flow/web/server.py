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
    },
    {
        "name": "events_stream",
        "kind": "streaming",
        "latest_run": _STUB_RUNS[2],
        "failure_rate_7d": 0.0,
        "median_duration_ms": None,
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
    """
    try:
        from fastapi import Body, FastAPI, HTTPException
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
                pipelines.append(
                    {
                        "name": name,
                        "kind": latest.kind,
                        "latest_run": latest.to_summary_dict(),
                        "failure_rate_7d": (failed / len(runs)) if runs else 0.0,
                        "median_duration_ms": (
                            _median(durations) if durations else None
                        ),
                    }
                )
            return {"pipelines": pipelines}
        return {"pipelines": _STUB_PIPELINES}

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
        body: dict[str, Any] = Body(default_factory=dict),
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
) -> None:
    """Launch the uvicorn server.

    Defaults to ``127.0.0.1`` so the UI is unreachable off-host
    without an explicit ``--bind <addr>`` opt-in. Binding to
    ``0.0.0.0`` (or any non-loopback) prints a loud warning since
    Phase 4a ships without bearer-token auth — anyone who can reach
    the port can trigger restart / rerun / pause actions.
    """
    try:
        import uvicorn  # type: ignore[import-not-found]
    except ImportError as exc:
        raise ImportError(
            "uvicorn is required for the ematix-flow web UI; install with "
            "`pip install ematix-flow[web]`"
        ) from exc

    if host not in {"127.0.0.1", "localhost", "::1"}:
        print(
            f"WARNING: ematix-flow web is binding to {host!r}. This server "
            "ships without auth in Phase 4a — anyone who can reach this "
            "port can trigger restart / rerun / pause actions on your "
            "pipelines. Use 127.0.0.1 + SSH tunneling, or put a reverse "
            "proxy with auth in front, before exposing this off-host.",
            file=sys.stderr,
        )

    app = create_app()
    uvicorn.run(app, host=host, port=port, log_level=log_level)
