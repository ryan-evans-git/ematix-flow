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
from datetime import UTC
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
    datasources: dict[str, str] | None = None,
    analytics_store: Any = None,
    rbac: Any = None,
    dlq_ops: Any = None,
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
    - ``dlq_ops`` — DLQ Phase 4: the DLQ/rewind operations layer
      for ``/api/streams/{name}/dlq*`` + ``/rewind``. Defaults to
      :class:`ematix_flow.web.dlq.StreamDlqOps` (pyo3-backed,
      resolved lazily on first use); tests inject a fake with the
      same duck-typed surface.
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

    # Make `Request` resolvable as a route-handler annotation. Under
    # `from __future__ import annotations` all annotations are strings,
    # and FastAPI resolves them against the module globals — where
    # `Request` isn't present because fastapi is imported lazily here.
    # Injecting it into the module namespace lets handlers use
    # `request: Request` (needed to read the trusted identity header).
    globals().setdefault("Request", Request)

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

    # RBAC (reverse-proxy / SSO trust). When configured, every /api/*
    # call (except health + /api/me) requires an authenticated identity
    # from the trusted header, and the caller's role must grant the
    # action's permission. Identity + role are stashed on request.state
    # for the handlers (ownership) and /api/me.
    if rbac is not None:
        from ematix_flow.web import auth as _auth

        @app.middleware("http")
        async def _rbac_gate(request: Request, call_next):
            perm = _auth.required_permission(request.method, request.url.path)
            if perm is None:
                # Still resolve identity for /api/me + ownership.
                ident = _auth.resolve_identity(request.headers, rbac)
                request.state.identity = ident
                request.state.role = (
                    _auth.resolve_role(ident, request.headers, rbac) if ident else None
                )
                return await call_next(request)
            from fastapi.responses import JSONResponse

            identity = _auth.resolve_identity(request.headers, rbac)
            if identity is None:
                return JSONResponse({"detail": "authentication required"}, status_code=401)
            role = _auth.resolve_role(identity, request.headers, rbac)
            if not _auth.role_has(role, perm):
                return JSONResponse(
                    {"detail": f"role {role!r} lacks the {perm!r} permission"},
                    status_code=403,
                )
            request.state.identity = identity
            request.state.role = role
            return await call_next(request)

    from ematix_flow.web.analytics import (
        DatasourceNotFound,
        DatasourceRegistry,
        QueryError,
        QueryTimeout,
        catalog_columns,
        catalog_schemas,
        catalog_tables,
        clear_result_cache,
        evaluate_alert,
        execute_chart_for_dashboard,
        execute_query_request,
    )
    from ematix_flow.web.query_jobs import QueryJobRegistry

    datasource_registry = DatasourceRegistry(datasources)
    query_jobs = QueryJobRegistry()

    def _identity(request) -> str | None:
        """Resolve the caller's identity for ownership. With RBAC, the
        proxy-trusted identity (stashed by the middleware) is used;
        otherwise a valid bearer token maps to the single-tenant
        ``operator``; otherwise no owner is recorded."""
        ident = getattr(getattr(request, "state", None), "identity", None)
        if ident:
            return ident
        if bearer_token is not None:
            auth = request.headers.get("authorization", "")
            return "operator" if auth.startswith("Bearer ") else None
        return None

    @app.get("/api/me")
    def whoami(request: Request) -> dict[str, Any]:  # type: ignore[unused-function]
        """Who the caller is + their role/permissions, for the UI to
        gate controls. Anonymous when no identity is present."""
        from ematix_flow.web.auth import permissions_for

        identity = _identity(request)
        role = getattr(getattr(request, "state", None), "role", None)
        if role is None and identity is not None and rbac is None:
            role = "admin"  # non-RBAC single-tenant operator has full access
        return {
            "authenticated": identity is not None,
            "identity": identity,
            "role": role,
            "permissions": permissions_for(role) if role else [],
            "rbac_enabled": rbac is not None,
        }

    def _catalog(fn, *args):
        """Run a catalog introspection call, mapping its failures to
        the right HTTP status."""
        try:
            return fn(datasource_registry, *args)
        except DatasourceNotFound as exc:
            raise HTTPException(
                status_code=404, detail=f"datasource {exc.args[0]!r} not found"
            ) from exc
        except QueryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/health")
    def health() -> dict[str, str]:  # type: ignore[unused-function]
        return {"status": "ok"}

    @app.get("/api/datasources")
    def list_datasources() -> dict[str, Any]:  # type: ignore[unused-function]
        return {
            "datasources": [d.public_dict() for d in datasource_registry.list()]
        }

    @app.get("/api/datasources/{datasource_id}/schemas")
    def get_schemas(datasource_id: str) -> dict[str, Any]:  # type: ignore[unused-function]
        return {"schemas": _catalog(catalog_schemas, datasource_id)}

    @app.get("/api/datasources/{datasource_id}/schemas/{schema}/tables")
    def get_tables(datasource_id: str, schema: str) -> dict[str, Any]:  # type: ignore[unused-function]
        return {"tables": _catalog(catalog_tables, datasource_id, schema)}

    @app.get(
        "/api/datasources/{datasource_id}/schemas/{schema}/tables/{table}/columns"
    )
    def get_columns(datasource_id: str, schema: str, table: str) -> dict[str, Any]:  # type: ignore[unused-function]
        return {"columns": _catalog(catalog_columns, datasource_id, schema, table)}

    # ---- Saved queries ---------------------------------------------
    # If no store is configured, fall back to an ephemeral in-memory
    # one so the SQL Lab's save button works out of the box (the CLI
    # launcher wires a file-backed store for persistence).
    if analytics_store is None:
        from ematix_flow.web.analytics_store import AnalyticsStore

        analytics_store = AnalyticsStore(":memory:")

    @app.get("/api/saved-queries")
    def list_saved_queries() -> dict[str, Any]:  # type: ignore[unused-function]
        return {"saved_queries": analytics_store.list_saved_queries()}

    @app.post("/api/saved-queries")
    def create_saved_query(request: Request, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:  # type: ignore[unused-function]
        name = (payload.get("name") or "").strip()
        sql = (payload.get("sql") or "").strip()
        datasource_id = (payload.get("datasource_id") or "").strip()
        if not name or not sql or not datasource_id:
            raise HTTPException(
                status_code=400,
                detail="name, datasource_id and sql are required",
            )
        return analytics_store.create_saved_query(
            name=name, datasource_id=datasource_id, sql=sql, owner=_identity(request)
        )

    @app.get("/api/saved-queries/{query_id}")
    def get_saved_query(query_id: str) -> dict[str, Any]:  # type: ignore[unused-function]
        item = analytics_store.get_saved_query(query_id)
        if item is None:
            raise HTTPException(status_code=404, detail="saved query not found")
        return item

    @app.put("/api/saved-queries/{query_id}")
    def update_saved_query(query_id: str, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:  # type: ignore[unused-function]
        name = payload.get("name")
        sql = payload.get("sql")
        if name is not None and not str(name).strip():
            raise HTTPException(status_code=400, detail="name cannot be empty")
        item = analytics_store.update_saved_query(
            query_id,
            name=str(name).strip() if name is not None else None,
            datasource_id=payload.get("datasource_id"),
            sql=sql,
        )
        if item is None:
            raise HTTPException(status_code=404, detail="saved query not found")
        return item

    @app.delete("/api/saved-queries/{query_id}")
    def delete_saved_query(query_id: str) -> dict[str, Any]:  # type: ignore[unused-function]
        if not analytics_store.delete_saved_query(query_id):
            raise HTTPException(status_code=404, detail="saved query not found")
        return {"deleted": True}

    # ---- Charts ----------------------------------------------------

    @app.get("/api/charts")
    def list_charts() -> dict[str, Any]:  # type: ignore[unused-function]
        return {"charts": analytics_store.list_charts()}

    @app.post("/api/charts")
    def create_chart(request: Request, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:  # type: ignore[unused-function]
        name = (payload.get("name") or "").strip()
        sql = (payload.get("sql") or "").strip()
        datasource_id = (payload.get("datasource_id") or "").strip()
        viz_type = (payload.get("viz_type") or "").strip()
        if not name or not sql or not datasource_id or not viz_type:
            raise HTTPException(
                status_code=400,
                detail="name, datasource_id, sql and viz_type are required",
            )
        return analytics_store.create_chart(
            name=name,
            datasource_id=datasource_id,
            sql=sql,
            viz_type=viz_type,
            encoding=payload.get("encoding") or {},
            owner=_identity(request),
        )

    @app.get("/api/charts/{chart_id}")
    def get_chart(chart_id: str) -> dict[str, Any]:  # type: ignore[unused-function]
        item = analytics_store.get_chart(chart_id)
        if item is None:
            raise HTTPException(status_code=404, detail="chart not found")
        return item

    @app.put("/api/charts/{chart_id}")
    def update_chart(chart_id: str, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:  # type: ignore[unused-function]
        name = payload.get("name")
        if name is not None and not str(name).strip():
            raise HTTPException(status_code=400, detail="name cannot be empty")
        item = analytics_store.update_chart(
            chart_id,
            name=str(name).strip() if name is not None else None,
            datasource_id=payload.get("datasource_id"),
            sql=payload.get("sql"),
            viz_type=payload.get("viz_type"),
            encoding=payload.get("encoding"),
        )
        if item is None:
            raise HTTPException(status_code=404, detail="chart not found")
        return item

    @app.delete("/api/charts/{chart_id}")
    def delete_chart(chart_id: str) -> dict[str, Any]:  # type: ignore[unused-function]
        if not analytics_store.delete_chart(chart_id):
            raise HTTPException(status_code=404, detail="chart not found")
        return {"deleted": True}

    # ---- Dashboards ------------------------------------------------

    @app.get("/api/dashboards")
    def list_dashboards() -> dict[str, Any]:  # type: ignore[unused-function]
        return {"dashboards": analytics_store.list_dashboards()}

    @app.post("/api/dashboards")
    def create_dashboard(request: Request, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:  # type: ignore[unused-function]
        name = (payload.get("name") or "").strip()
        if not name:
            raise HTTPException(status_code=400, detail="name is required")
        return analytics_store.create_dashboard(
            name=name,
            layout=payload.get("layout") or {"tiles": []},
            owner=_identity(request),
        )

    @app.get("/api/dashboards/{dashboard_id}")
    def get_dashboard(dashboard_id: str) -> dict[str, Any]:  # type: ignore[unused-function]
        item = analytics_store.get_dashboard(dashboard_id)
        if item is None:
            raise HTTPException(status_code=404, detail="dashboard not found")
        return item

    @app.put("/api/dashboards/{dashboard_id}")
    def update_dashboard(dashboard_id: str, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:  # type: ignore[unused-function]
        name = payload.get("name")
        if name is not None and not str(name).strip():
            raise HTTPException(status_code=400, detail="name cannot be empty")
        item = analytics_store.update_dashboard(
            dashboard_id,
            name=str(name).strip() if name is not None else None,
            layout=payload.get("layout"),
        )
        if item is None:
            raise HTTPException(status_code=404, detail="dashboard not found")
        return item

    @app.delete("/api/dashboards/{dashboard_id}")
    def delete_dashboard(dashboard_id: str) -> dict[str, Any]:  # type: ignore[unused-function]
        if not analytics_store.delete_dashboard(dashboard_id):
            raise HTTPException(status_code=404, detail="dashboard not found")
        return {"deleted": True}

    @app.post("/api/dashboards/{dashboard_id}/query")
    def query_dashboard(  # type: ignore[unused-function]
        dashboard_id: str, payload: dict[str, Any] = Body(default={})
    ) -> dict[str, Any]:
        """Run every tile's chart query and return results keyed by
        chart_id, each self-contained ({name, viz_type, encoding,
        columns, rows, truncated} or {error}).

        An optional ``filters`` list (``[{column, values}]``) restricts
        each chart to matching rows where its output has that column —
        the dashboard-filter / cross-filter path."""
        dashboard = analytics_store.get_dashboard(dashboard_id)
        if dashboard is None:
            raise HTTPException(status_code=404, detail="dashboard not found")
        filters = payload.get("filters") or []
        tiles = (dashboard.get("layout") or {}).get("tiles") or []
        chart_ids = list(dict.fromkeys(t.get("chart_id") for t in tiles if t.get("chart_id")))
        results: dict[str, Any] = {}
        for chart_id in chart_ids:
            chart = analytics_store.get_chart(chart_id)
            if chart is None:
                results[chart_id] = {"error": "chart not found"}
                continue
            try:
                data = execute_chart_for_dashboard(datasource_registry, chart, filters)
                results[chart_id] = {
                    "name": chart["name"],
                    "viz_type": chart["viz_type"],
                    "encoding": chart["encoding"],
                    **data,
                }
            except DatasourceNotFound:
                results[chart_id] = {"error": f"datasource {chart['datasource_id']!r} not found"}
            except QueryError as exc:
                results[chart_id] = {"error": str(exc)}
        return {"results": results}

    # ---- Alerts ----------------------------------------------------

    @app.get("/api/alerts")
    def list_alerts() -> dict[str, Any]:  # type: ignore[unused-function]
        return {"alerts": analytics_store.list_alerts()}

    @app.post("/api/alerts")
    def create_alert(request: Request, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:  # type: ignore[unused-function]
        name = (payload.get("name") or "").strip()
        chart_id = (payload.get("chart_id") or "").strip()
        column = (payload.get("column") or "").strip()
        op = (payload.get("op") or "").strip()
        if not name or not chart_id or not column or not op:
            raise HTTPException(
                status_code=400,
                detail="name, chart_id, column and op are required",
            )
        try:
            threshold = float(payload.get("threshold"))
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="threshold must be a number") from exc
        if analytics_store.get_chart(chart_id) is None:
            raise HTTPException(status_code=404, detail="chart not found")
        return analytics_store.create_alert(
            name=name,
            chart_id=chart_id,
            column=column,
            op=op,
            threshold=threshold,
            owner=_identity(request),
        )

    @app.get("/api/alerts/{alert_id}")
    def get_alert(alert_id: str) -> dict[str, Any]:  # type: ignore[unused-function]
        item = analytics_store.get_alert(alert_id)
        if item is None:
            raise HTTPException(status_code=404, detail="alert not found")
        return item

    @app.delete("/api/alerts/{alert_id}")
    def delete_alert(alert_id: str) -> dict[str, Any]:  # type: ignore[unused-function]
        if not analytics_store.delete_alert(alert_id):
            raise HTTPException(status_code=404, detail="alert not found")
        return {"deleted": True}

    @app.post("/api/alerts/{alert_id}/check")
    def check_alert(alert_id: str) -> dict[str, Any]:  # type: ignore[unused-function]
        """Evaluate an alert now. A scheduled runner would call this on
        a cron (via the existing scheduler) and dispatch notifications
        when ``triggered``."""
        alert = analytics_store.get_alert(alert_id)
        if alert is None:
            raise HTTPException(status_code=404, detail="alert not found")
        chart = analytics_store.get_chart(alert["chart_id"])
        if chart is None:
            raise HTTPException(status_code=404, detail="alert's chart not found")
        try:
            outcome = evaluate_alert(
                datasource_registry, chart, alert["column"], alert["op"], alert["threshold"]
            )
        except DatasourceNotFound as exc:
            raise HTTPException(status_code=404, detail="datasource not found") from exc
        except QueryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"alert_id": alert_id, **outcome}

    @app.post("/api/query")
    def run_query(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:  # type: ignore[unused-function]
        datasource_id = payload.get("datasource_id")
        if not datasource_id:
            raise HTTPException(status_code=400, detail="datasource_id is required")
        try:
            return execute_query_request(
                datasource_registry,
                datasource_id=datasource_id,
                sql=payload.get("sql", ""),
                max_rows=payload.get("max_rows"),
            )
        except DatasourceNotFound as exc:
            raise HTTPException(
                status_code=404,
                detail=f"datasource {datasource_id!r} not found",
            ) from exc
        except QueryTimeout as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc
        except QueryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/api/query/async")
    def run_query_async(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:  # type: ignore[unused-function]
        """Submit a query to run on a background thread; returns a
        job id to poll at ``/api/query/jobs/{id}``. For queries too slow
        to hold the request open. Datasource + SQL are validated up
        front so bad requests fail fast."""
        from ematix_flow.web.analytics import guard_readonly, run_query

        datasource_id = payload.get("datasource_id")
        if not datasource_id:
            raise HTTPException(status_code=400, detail="datasource_id is required")
        try:
            datasource = datasource_registry.get(datasource_id)
            cleaned = guard_readonly(payload.get("sql", ""))
        except DatasourceNotFound as exc:
            raise HTTPException(
                status_code=404, detail=f"datasource {datasource_id!r} not found"
            ) from exc
        except QueryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        max_rows = payload.get("max_rows")
        job_id = query_jobs.submit(lambda: run_query(datasource.url, cleaned, max_rows))
        return {"job_id": job_id, "status": "pending"}

    @app.get("/api/query/jobs/{job_id}")
    def get_query_job(job_id: str) -> dict[str, Any]:  # type: ignore[unused-function]
        job = query_jobs.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="query job not found")
        return job

    @app.post("/api/cache/clear")
    def clear_cache() -> dict[str, Any]:  # type: ignore[unused-function]
        clear_result_cache()
        return {"cleared": True}

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

    # ---- Workflows: named groupings of jobs ---------------------------
    def _last_success_time(name: str):
        """Most recent succeeded run start time for ``name``, or None."""
        if history is None:
            return None
        try:
            runs, _ = history.list_runs(pipeline=name, status="succeeded", limit=1)
        except Exception:
            return None
        if not runs:
            return None
        # list_runs returns RunRecord with started_at as datetime or ISO str.
        st = runs[0].started_at
        if isinstance(st, str):
            try:
                from datetime import datetime as _dt
                # Accept both Z-suffix and +00:00.
                return _dt.fromisoformat(st.replace("Z", "+00:00"))
            except Exception:
                return None
        return st

    def _latest_terminal_run(name: str):
        """Most recent terminal (succeeded/failed/cancelled) run for ``name``."""
        if history is None:
            return None
        try:
            runs, _ = history.list_runs(pipeline=name, limit=10)
        except Exception:
            return None
        for r in runs:
            if r.status in ("succeeded", "failed", "cancelled"):
                return r
        return None

    def _eval_trigger_op(
        op: Any,
        *,
        last_self_success,
    ) -> dict[str, Any]:
        """Recursively evaluate a TriggerOp expression tree against
        the workflow's last successful run.

        Returns a JSON-serialisable dict mirroring the tree structure
        and carrying a ``state`` (ready/pending/failed) at every node.

        Leaf state semantics:
            - ready  : upstream has a successful run since last_self_success
            - failed : upstream's most recent terminal run since
                       last_self_success was a failure
            - pending: otherwise (running, never run, etc.)

        Internal-node state semantics:
            - all : ready if every child ready; failed if any failed and
                    none pending; otherwise pending
            - any : ready if any child ready; failed if every child
                    failed; otherwise pending
        """
        from ematix_flow.pipeline import TriggerOp

        if isinstance(op, str):
            # Leaf — compute event state.
            last_up_success = _last_success_time(op)
            latest = _latest_terminal_run(op)
            state = "pending"
            if last_up_success is not None and (
                last_self_success is None or last_up_success > last_self_success
            ):
                state = "ready"
            if (
                state == "pending"
                and latest is not None
                and latest.status == "failed"
                and (
                    last_up_success is None
                    or (
                        last_self_success is not None
                        and last_up_success <= last_self_success
                    )
                )
            ):
                state = "failed"
            return {"kind": "leaf", "name": op, "state": state}

        if isinstance(op, TriggerOp):
            children = [
                _eval_trigger_op(m, last_self_success=last_self_success)
                for m in op.members
            ]
            child_states = [c["state"] for c in children]
            if op.kind == "all":
                if all(s == "ready" for s in child_states):
                    state = "ready"
                elif any(s == "failed" for s in child_states) and "pending" not in child_states:
                    state = "failed"
                else:
                    state = "pending"
            else:  # "any"
                if any(s == "ready" for s in child_states):
                    state = "ready"
                elif all(s == "failed" for s in child_states):
                    state = "failed"
                else:
                    state = "pending"
            return {"kind": op.kind, "members": children, "state": state}

        # Shouldn't happen if normalisation worked, but be defensive.
        return {"kind": "leaf", "name": repr(op), "state": "pending"}

    def _compute_triggers(
        *,
        triggered_by: Any,
        schedule: str | None,
        timezone_name: str | None,
        on_message_repr: str | None,
        last_self_success,
    ) -> list[dict[str, Any]]:
        """Return a per-condition list of trigger dicts with state +
        display info. ``last_self_success`` is this workflow's own
        most-recent successful run start time, or None if never run.

        Each entry shape:
          {
            "kind": "schedule" | "after" | "on_message",
            "label": "human-readable display",
            "state": "ready" | "pending" | "failed",
            ... kind-specific fields ...
          }
        """
        out: list[dict[str, Any]] = []
        from datetime import datetime as _dt

        now = _dt.now(UTC)

        # Schedule trigger
        if schedule:
            from ematix_flow.pipeline import forecast_next_run
            anchor = last_self_success if last_self_success is not None else None
            try:
                next_tick = forecast_next_run(
                    schedule,
                    now=anchor or now,
                    timezone=timezone_name,
                )
            except Exception:
                next_tick = None
            if next_tick is not None:
                state = "ready" if now >= next_tick else "pending"
                # Format the tick in the workflow's tz for the label.
                label = _format_tick(next_tick, timezone_name, now)
            else:
                state = "pending"
                label = schedule
            out.append({
                "kind": "schedule",
                "cron": schedule,
                "timezone": timezone_name,
                "next_at": (next_tick.isoformat() if next_tick is not None else None),
                "state": state,
                "label": label,
            })

        # Upstream event triggers. ``triggered_by`` is now a TriggerOp
        # expression tree (or None). We append a single "after_expr"
        # entry whose nested ``expr`` field carries the boolean tree
        # with per-node state; the UI renders the tree with AND/OR
        # grouping and parentheses.
        if triggered_by is not None:
            expr_eval = _eval_trigger_op(
                triggered_by, last_self_success=last_self_success
            )
            out.append({
                "kind": "after_expr",
                "state": expr_eval["state"],
                "expr": expr_eval,
            })

        # on_message: pending (firing is per-message; no AND eval at tick time)
        if on_message_repr:
            out.append({
                "kind": "on_message",
                "source": on_message_repr,
                "state": "pending",
                "label": on_message_repr,
            })

        return out

    def _format_tick(tick_utc, timezone_name: str | None, now_utc) -> str:
        """Format a UTC datetime in the given IANA tz for display.

        Returns just ``HH:MM TZ`` when the tick is today in that tz, or
        ``YYYY-MM-DD HH:MM TZ`` otherwise.
        """
        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            ZoneInfo = None  # type: ignore[assignment]
        if timezone_name and ZoneInfo is not None:
            try:
                tz = ZoneInfo(timezone_name)
                local_tick = tick_utc.astimezone(tz)
                local_now = now_utc.astimezone(tz)
                short_tz = local_tick.strftime("%Z") or timezone_name
                if local_tick.date() == local_now.date():
                    return f"{local_tick.strftime('%H:%M')} {short_tz}"
                return f"{local_tick.strftime('%Y-%m-%d %H:%M')} {short_tz}"
            except Exception:
                pass
        # Fallback: UTC.
        if tick_utc.date() == now_utc.date():
            return f"{tick_utc.strftime('%H:%M')} UTC"
        return f"{tick_utc.strftime('%Y-%m-%d %H:%M')} UTC"

    @app.get("/api/workflows")
    def list_workflows() -> dict[str, Any]:  # type: ignore[unused-function]
        """Each workflow is a user-named group of jobs + the trigger
        conditions that fire it. Jobs without an explicit workflow show up
        as a synthetic workflow-of-one keyed by their own name."""
        try:
            from ematix_flow.pipeline import (
                _DEPENDS_ON,
                _JOB_TO_WORKFLOW,
                _REGISTRY,
                _WORKFLOWS,
                _workflow_is_streaming,
                workflow_dag_edges,
            )
        except Exception:
            return {"workflows": []}

        # Streaming pipelines live in a separate registry — surface them
        # alongside batch jobs so the Workflows page shows every kind.
        try:
            from ematix_flow.streaming import _STREAMING_PIPELINES
            streaming_names = sorted(_STREAMING_PIPELINES.keys())
        except Exception:
            streaming_names = []

        all_jobs = sorted(_REGISTRY.keys())
        result: list[dict[str, Any]] = []

        # Declared workflows first.
        for wf_name, wf in sorted(_WORKFLOWS.items()):
            # Edges come from each member job's own depends_on, filtered to
            # references inside this workflow.
            edges = [
                {"from": u, "to": d}
                for (u, d) in workflow_dag_edges(wf_name)
            ]
            # Streaming workflow if any member job is a streaming pipeline.
            is_streaming = _workflow_is_streaming(list(wf.jobs))
            kind = "streaming" if is_streaming else "declared"
            on_msg_repr = (
                repr(wf.on_message) if wf.on_message is not None else None
            )
            last_self = _last_success_time(wf_name)
            triggers = (
                []
                if is_streaming
                else _compute_triggers(
                    triggered_by=wf.triggered_by,
                    schedule=wf.schedule,
                    timezone_name=wf.timezone,
                    on_message_repr=on_msg_repr,
                    last_self_success=last_self,
                )
            )
            # Legacy flat list (leaf names) for any UI/consumer that
            # doesn't yet understand the expression tree.
            leaf_names = list(wf.triggered_by_leaves)
            result.append(
                {
                    "name": wf_name,
                    "kind": kind,
                    "jobs": list(wf.jobs),
                    "edges": edges,
                    "triggered_by": leaf_names,
                    "schedule": wf.schedule,
                    "timezone": wf.timezone,
                    "on_message": on_msg_repr,
                    "triggers": triggers,
                }
            )

        # Synthetic workflow-of-one for every unassigned batch job. Carry
        # the job's own per-job schedule/depends_on through as the
        # standalone trigger. Wrap upstream-name list as an AllOf tree
        # so the UI renders consistently.
        from ematix_flow.pipeline import AllOf as _AllOf
        for job_name in all_jobs:
            if job_name in _JOB_TO_WORKFLOW:
                continue
            ups = _DEPENDS_ON.get(job_name) or []
            edges = [{"from": u, "to": job_name} for u in ups]
            sp = _REGISTRY.get(job_name)
            sched = getattr(sp, "schedule", None)
            tz = getattr(sp, "timezone", None)
            last_self = _last_success_time(job_name)
            trigger_expr = _AllOf(*ups) if ups else None
            triggers = _compute_triggers(
                triggered_by=trigger_expr,
                schedule=sched,
                timezone_name=tz,
                on_message_repr=None,
                last_self_success=last_self,
            )
            result.append(
                {
                    "name": job_name,
                    "kind": "single",
                    "jobs": [job_name],
                    "edges": edges,
                    "triggered_by": list(ups),
                    "schedule": sched,
                    "timezone": tz,
                    "on_message": None,
                    "triggers": triggers,
                }
            )

        # Streaming pipelines that aren't part of a declared workflow.
        for name in streaming_names:
            if name in _JOB_TO_WORKFLOW:
                continue
            result.append(
                {
                    "name": name,
                    "kind": "streaming",
                    "jobs": [name],
                    "edges": [],
                    "triggered_by": [],
                    "schedule": None,
                    "timezone": None,
                    "on_message": None,
                    "triggers": [],
                }
            )
        return {"workflows": result}

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

    def _enqueue_run_now(
        *,
        pipeline_name: str,
        kind: str = "batch",
        extras: dict[str, Any] | None = None,
    ) -> str:
        """Write a fresh ``status="requested"`` record for ``pipeline_name``
        that the scheduler / run-due path picks up on its next tick.

        Used by the Workflows/Jobs tabs' "Run now" buttons. Returns the
        new run_id.
        """
        import uuid
        from datetime import datetime

        from ematix_flow.run_log.history import RunRecord

        new_id = f"run-now-{pipeline_name}-{uuid.uuid4().hex[:12]}"
        base_extras: dict[str, Any] = {"source": "run_now"}
        if extras:
            base_extras.update(extras)
        history.record_run_record(
            RunRecord(
                run_id=new_id,
                pipeline=pipeline_name,
                status="requested",
                started_at=datetime.now(UTC),
                finished_at=None,
                attempt=1,
                kind=kind,
                extras=base_extras,
            )
        )
        return new_id

    @app.post("/api/workflows/{name}/run-now")
    def post_workflow_run_now(  # type: ignore[unused-function]
        name: str,
        body: dict[str, Any] = Body(default_factory=dict),
    ) -> dict[str, Any]:
        """Enqueue an immediate run of the named workflow. Ignores
        trigger gates (cron not yet reached, upstream events not
        satisfied).

        Optional body fields:

        - ``jobs``: list of member job names to include in this run.
          Defaults to all the workflow's member jobs. Selected names
          must be a subset of the workflow's ``jobs=`` list.
        """
        _require_history()
        assert history is not None
        try:
            from ematix_flow.pipeline import _REGISTRY, _WORKFLOWS
        except Exception as e:
            raise HTTPException(
                status_code=500, detail="pipeline registry unavailable"
            ) from e
        wf = _WORKFLOWS.get(name)
        if wf is None:
            # Allow a workflow-of-one (standalone job/streaming) by name too,
            # so the "Run now" button works uniformly on every card.
            if name not in _REGISTRY:
                try:
                    from ematix_flow.streaming import _STREAMING_PIPELINES
                    if name not in _STREAMING_PIPELINES:
                        raise HTTPException(status_code=404, detail=f"unknown workflow {name!r}")
                except ImportError as e:
                    raise HTTPException(
                        status_code=404, detail=f"unknown workflow {name!r}"
                    ) from e
            selected = [name]
            all_jobs = (name,)
        else:
            all_jobs = wf.jobs
            requested = body.get("jobs")
            if requested is None:
                selected = list(all_jobs)
            else:
                if not isinstance(requested, list) or not all(
                    isinstance(j, str) for j in requested
                ):
                    raise HTTPException(
                        status_code=400, detail="jobs= must be a list of strings"
                    )
                bad = [j for j in requested if j not in all_jobs]
                if bad:
                    raise HTTPException(
                        status_code=400,
                        detail=f"jobs= contains names not in workflow {name!r}: {bad}",
                    )
                if not requested:
                    raise HTTPException(status_code=400, detail="jobs= cannot be empty")
                selected = requested
        new_id = _enqueue_run_now(
            pipeline_name=name,
            kind="batch",
            extras={"selected_jobs": list(selected)},
        )
        return {"new_run_id": new_id, "selected_jobs": list(selected)}

    @app.post("/api/jobs/{name}/run-now")
    def post_job_run_now(  # type: ignore[unused-function]
        name: str,
        body: dict[str, Any] = Body(default_factory=dict),
    ) -> dict[str, Any]:
        """Enqueue an immediate run of the named job.

        Optional body fields:

        - ``cascade_downstream``: when ``true``, after this job
          succeeds, any job that depends on it (within the same
          workflow, or via legacy per-job depends_on) is also
          enqueued. Default ``false`` — runs just this job.
        """
        _require_history()
        assert history is not None
        try:
            from ematix_flow.pipeline import _REGISTRY
        except Exception as e:
            raise HTTPException(
                status_code=500, detail="pipeline registry unavailable"
            ) from e
        if name not in _REGISTRY:
            raise HTTPException(status_code=404, detail=f"unknown job {name!r}")
        cascade = bool(body.get("cascade_downstream", False))
        new_id = _enqueue_run_now(
            pipeline_name=name,
            kind="batch",
            extras={"cascade_downstream": cascade},
        )
        return {"new_run_id": new_id, "cascade_downstream": cascade}

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

    # ---- DLQ + rewind (DLQ Phase 4) ---------------------------------
    #
    # Server-side gating mirrors the existing mutating actions:
    # mutations (replay / park / purge / rewind) require a
    # configured history store (replay runs register there), rewind
    # additionally refuses while the stream's latest run is
    # `running`. Bearer-token rules are unchanged — the /api/*
    # middleware already covers these routes.

    _dlq_ops_holder: list[Any] = [dlq_ops]

    def _dlq() -> Any:
        if _dlq_ops_holder[0] is None:
            from ematix_flow.web.dlq import StreamDlqOps

            _dlq_ops_holder[0] = StreamDlqOps()
        return _dlq_ops_holder[0]

    def _now_ms() -> int:
        # The HTTP handler is the emission boundary that reads the
        # clock; the Rust ops layers below take now_ms as a
        # parameter (house convention).
        import time

        return int(time.time() * 1000)

    def _dlq_call(fn, *args):
        """Run one ops call, mapping the protocol's error shapes to
        HTTP statuses: unknown stream → 404, typed operation errors
        (ValueError from the Rust layer) → 400."""
        try:
            return fn(*args)
        except KeyError as exc:
            raise HTTPException(
                status_code=404, detail=f"unknown stream {exc.args[0]!r}"
            ) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    _PAYLOAD_PREVIEW_BYTES = 4096

    def _record_summary(stream: str, r: dict[str, Any]) -> dict[str, Any]:
        """List-JSON shape for one DLQ record: metadata + a bounded
        payload preview + the raw-bytes download link. The full
        payload never rides the list response."""
        import base64

        payload: bytes = r.get("payload") or b""
        offset = r.get("offset_bytes")
        return {
            "id": r["id"],
            "stage": r["stage"],
            "error": r["error"],
            "source_id": r["source_id"],
            "offset_base64": (
                base64.b64encode(bytes(offset)).decode("ascii")
                if offset is not None
                else None
            ),
            "event_ts": r.get("event_ts"),
            "failed_at": r["failed_at"],
            "attempt": r["attempt"],
            "payload_format": r["payload_format"],
            "payload_preview": payload[:_PAYLOAD_PREVIEW_BYTES].decode(
                "utf-8", "replace"
            ),
            "payload_size": len(payload),
            "payload_truncated": len(payload) > _PAYLOAD_PREVIEW_BYTES,
            "download": f"/api/streams/{stream}/dlq/records/{r['id']}/payload",
        }

    @app.get("/api/streams/{name}/dlq")
    def get_stream_dlq(name: str) -> dict[str, Any]:  # type: ignore[unused-function]
        stats = _dlq_call(_dlq().stats, name, _now_ms())
        return {
            "pipeline": name,
            "depth": {"pending": stats["pending"], "parked": stats["parked"]},
            "by_stage": stats["by_stage"],
            "arrivals": stats["arrivals"],
            "scanned": stats["scanned"],
            "truncated": stats["truncated"],
        }

    @app.get("/api/streams/{name}/dlq/records")
    def get_stream_dlq_records(  # type: ignore[unused-function]
        name: str,
        status: str | None = None,
        page: int = 0,
        page_size: int = 50,
    ) -> dict[str, Any]:
        page = max(page, 0)
        page_size = max(1, min(page_size, 500))
        records = _dlq_call(_dlq().records, name, status, page, page_size)
        return {
            "pipeline": name,
            "page": page,
            "page_size": page_size,
            "records": [_record_summary(name, r) for r in records],
        }

    @app.get("/api/streams/{name}/dlq/records/{record_id}/payload")
    def get_stream_dlq_payload(name: str, record_id: str):  # type: ignore[unused-function]
        from fastapi.responses import Response

        record = _dlq_call(_dlq().record_by_id, name, record_id)
        if record is None:
            raise HTTPException(
                status_code=404, detail=f"DLQ record {record_id!r} not found"
            )
        return Response(
            content=bytes(record.get("payload") or b""),
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": (
                    f'attachment; filename="{record_id}.'
                    f'{record.get("payload_format", "bin")}"'
                )
            },
        )

    _VALID_SELECTION_KINDS = {"all", "first_n", "ids"}

    def _parse_selection(
        body: dict[str, Any], *, default_all: bool
    ) -> dict[str, Any]:
        selection = body.get("selection")
        if selection is None:
            if default_all:
                return {"kind": "all"}
            raise HTTPException(
                status_code=400,
                detail=(
                    "selection is required — pass "
                    '{"selection": {"kind": "all"}} explicitly for '
                    "destructive whole-DLQ operations"
                ),
            )
        if (
            not isinstance(selection, dict)
            or selection.get("kind") not in _VALID_SELECTION_KINDS
        ):
            raise HTTPException(
                status_code=400,
                detail=(
                    "selection must be one of "
                    '{"kind":"all"}, {"kind":"first_n","n":N}, '
                    '{"kind":"ids","ids":[…]}'
                ),
            )
        return selection

    @app.post("/api/streams/{name}/dlq/replay")
    def post_stream_dlq_replay(  # type: ignore[unused-function]
        name: str,
        body: dict[str, Any] = Body(default_factory=dict),
    ) -> dict[str, Any]:
        _require_history()
        assert history is not None
        selection = _parse_selection(body, default_all=True)
        max_attempts = body.get("max_attempts")
        if max_attempts is not None and int(max_attempts) < 1:
            raise HTTPException(
                status_code=400, detail="max_attempts must be >= 1"
            )
        import uuid
        from datetime import datetime

        run_id = f"replay-{name}-{uuid.uuid4().hex[:12]}"
        try:
            report = _dlq_call(_dlq().replay, name, selection, max_attempts)
        except HTTPException as exc:
            if exc.status_code == 400:
                # Register the failed replay too — operators should
                # see failed redrive attempts in Runs.
                history.record_run_record(
                    RunRecord(
                        run_id=run_id,
                        pipeline=name,
                        status="failed",
                        started_at=datetime.now(UTC),
                        finished_at=datetime.now(UTC),
                        attempt=1,
                        kind="replay",
                        error_summary=str(exc.detail),
                        extras={"selection": selection},
                    )
                )
            raise
        started = datetime.fromtimestamp(report["started_at_ms"] / 1000.0, UTC)
        finished = datetime.fromtimestamp(report["finished_at_ms"] / 1000.0, UTC)
        history.record_run_record(
            RunRecord(
                run_id=run_id,
                pipeline=name,
                status="succeeded",
                started_at=started,
                finished_at=finished,
                attempt=1,
                kind="replay",
                extras={"replay_report": dict(report), "selection": selection},
            )
        )
        return {"run_id": run_id, "report": dict(report)}

    @app.post("/api/streams/{name}/dlq/park")
    def post_stream_dlq_park(  # type: ignore[unused-function]
        name: str,
        body: dict[str, Any] = Body(default_factory=dict),
    ) -> dict[str, Any]:
        _require_history()
        selection = _parse_selection(body, default_all=False)
        parked = _dlq_call(_dlq().park, name, selection)
        return {"parked": parked}

    @app.post("/api/streams/{name}/dlq/purge")
    def post_stream_dlq_purge(  # type: ignore[unused-function]
        name: str,
        body: dict[str, Any] = Body(default_factory=dict),
    ) -> dict[str, Any]:
        _require_history()
        selection = _parse_selection(body, default_all=False)
        purged = _dlq_call(_dlq().purge, name, selection)
        return {"purged": purged}

    _VALID_REWIND_KINDS = {"timestamp", "offset"}

    @app.post("/api/streams/{name}/rewind")
    def post_stream_rewind(  # type: ignore[unused-function]
        name: str,
        body: dict[str, Any] = Body(default_factory=dict),
    ) -> dict[str, Any]:
        _require_history()
        assert history is not None
        to = body.get("to")
        if (
            not isinstance(to, dict)
            or to.get("kind") not in _VALID_REWIND_KINDS
        ):
            raise HTTPException(
                status_code=400,
                detail=(
                    'to is required: {"kind":"timestamp","ms":N} or '
                    '{"kind":"offset","bytes":[…]}'
                ),
            )
        confirm = bool(body.get("confirm_state_reset", False))
        # Server-side gate: a rewind mutates read positions + state;
        # it must not race a live consume loop. Latest run status
        # is the server's view of liveness.
        try:
            latest, _total = history.list_runs(pipeline=name, limit=1, offset=0)
        except Exception:
            latest = []
        if latest and latest[0].status == "running":
            raise HTTPException(
                status_code=409,
                detail=(
                    f"stream {name!r} is running — stop it before "
                    "rewinding (rewind must not race the consume loop)"
                ),
            )
        result = _dlq_call(_dlq().rewind, name, to, confirm)
        import base64

        return {
            "pipeline": name,
            "state_cleared": bool(result["state_cleared"]),
            "sources": [
                {
                    "source": src,
                    "offset_base64": base64.b64encode(bytes(off)).decode("ascii"),
                }
                for (src, off) in result.get("sources", [])
            ],
        }

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
    datasources: dict[str, str] | None = None,
    analytics_store: Any = None,
    rbac: Any = None,
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

    app = create_app(
        bearer_token=bearer_token,
        history=history,
        datasources=datasources,
        analytics_store=analytics_store,
        rbac=rbac,
    )
    uvicorn.run(app, host=host, port=port, log_level=log_level)
