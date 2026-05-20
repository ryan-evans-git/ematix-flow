"""FastAPI app + uvicorn launcher.

First slice (Phase 4a-1): server skeleton with stub data so the CLI
subcommand and SPA plumbing can be tested in isolation. Real RunLog
integration lands in the next slice once the RunLog Protocol gets
its ``list_runs`` / ``get_run`` query extension.
"""
from __future__ import annotations

import logging
import sys
from importlib import resources
from pathlib import Path
from typing import Any

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
    """
    status = run["status"]
    is_streaming = run["pipeline"] == "events_stream"  # stub
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


# ---- App factory ---------------------------------------------------


def create_app(*, ui_dist_dir: Path | None = None):
    """Build the FastAPI app.

    ``ui_dist_dir`` overrides where the SPA bundle is served from.
    Default behavior is to load the bundle from the package's
    ``ematix_flow.web.ui_dist`` data dir, which is populated by the
    Vite build at wheel-build time. If the bundle is absent (e.g.
    running from a source checkout without ``npm run build``), the
    server still serves a friendly placeholder HTML page.
    """
    try:
        from fastapi import FastAPI, HTTPException
        from fastapi.responses import HTMLResponse
        from fastapi.staticfiles import StaticFiles
    except ImportError as exc:
        raise ImportError(
            "fastapi is required for the ematix-flow web UI; install with "
            "`pip install ematix-flow[web]`"
        ) from exc

    app = FastAPI(
        title="ematix-flow Web UI",
        version="0.3.0",
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
        return {"pipelines": _STUB_PIPELINES}

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
