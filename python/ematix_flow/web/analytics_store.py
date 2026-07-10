"""Persistence for the SQL Lab / dashboards surface.

A small stdlib-``sqlite3`` store, in the same shape as
``run_log/sqlite.py`` (idempotent ``CREATE TABLE IF NOT EXISTS`` + a
lock-wrapped connection for cross-thread access under the FastAPI
threadpool). Phase 1 covers ``saved_queries``; charts and dashboards
tables land in Phases 2–3 under the same ``analytics_schema_version``.

Every row carries a nullable ``owner`` so per-user ownership (Phase 4)
is a fill-in rather than a migration.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from datetime import UTC, datetime
from typing import Any

_SCHEMA_VERSION = 5

_SCHEMA = """
    CREATE TABLE IF NOT EXISTS analytics_schema_version (
        version INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS saved_queries (
        id            TEXT PRIMARY KEY,
        name          TEXT NOT NULL,
        datasource_id TEXT NOT NULL,
        sql           TEXT NOT NULL,
        owner         TEXT,
        created_at    TEXT NOT NULL,
        updated_at    TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_saved_queries_name ON saved_queries(name);
    -- v2: charts. `encoding_json` holds the column->visual mapping
    -- (x / y / series / name / value …) as JSON.
    CREATE TABLE IF NOT EXISTS charts (
        id            TEXT PRIMARY KEY,
        name          TEXT NOT NULL,
        datasource_id TEXT NOT NULL,
        sql           TEXT NOT NULL,
        viz_type      TEXT NOT NULL,
        encoding_json TEXT NOT NULL,
        owner         TEXT,
        created_at    TEXT NOT NULL,
        updated_at    TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_charts_name ON charts(name);
    -- v3: dashboards. `layout_json` holds the tile placement:
    -- {"tiles": [{"chart_id", "x", "y", "w", "h"}, ...]}.
    CREATE TABLE IF NOT EXISTS dashboards (
        id          TEXT PRIMARY KEY,
        name        TEXT NOT NULL,
        layout_json TEXT NOT NULL,
        owner       TEXT,
        created_at  TEXT NOT NULL,
        updated_at  TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_dashboards_name ON dashboards(name);
    -- v4: alerts. Evaluate a chart's `column` against `op` `threshold`
    -- on a schedule (or on demand) and notify when it triggers.
    CREATE TABLE IF NOT EXISTS alerts (
        id         TEXT PRIMARY KEY,
        name       TEXT NOT NULL,
        chart_id   TEXT NOT NULL,
        column     TEXT NOT NULL,
        op         TEXT NOT NULL,
        threshold  REAL NOT NULL,
        owner      TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_alerts_chart ON alerts(chart_id);
    -- v5: data-quality. `quality_runs` records one row per expectations
    -- run (from the pipeline post-write stage); `detail_json` holds the
    -- per-assertion list. `freshness_state` is the current freshness SLO
    -- state per pipeline, upserted by the scheduled evaluator.
    CREATE TABLE IF NOT EXISTS quality_runs (
        id            TEXT PRIMARY KEY,
        pipeline      TEXT NOT NULL,
        run_id        TEXT,
        table_name    TEXT NOT NULL,
        schema_name   TEXT,
        verdict       TEXT NOT NULL,
        checks_total  INTEGER NOT NULL DEFAULT 0,
        checks_failed INTEGER NOT NULL DEFAULT 0,
        detail_json   TEXT NOT NULL DEFAULT '[]',
        started_at    TEXT NOT NULL,
        finished_at   TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_qr_pipeline ON quality_runs(pipeline, finished_at);
    CREATE TABLE IF NOT EXISTS freshness_state (
        pipeline     TEXT PRIMARY KEY,
        sla_seconds  INTEGER NOT NULL,
        lag_seconds  INTEGER,
        state        TEXT NOT NULL,
        last_success TEXT,
        evaluated_at TEXT NOT NULL
    );
"""


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


class AnalyticsStore:
    """SQLite-backed store for saved queries (and, later, charts /
    dashboards). Pass ``":memory:"`` for an ephemeral store."""

    def __init__(self, path: str):
        if path != ":memory:":
            import os

            parent = os.path.dirname(path)
            if parent:
                os.makedirs(parent, exist_ok=True)
        self._path = path
        # FastAPI runs sync endpoints in a threadpool; serialise access.
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            path, isolation_level=None, check_same_thread=False
        )
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode = WAL;")
            self._conn.executescript(_SCHEMA)
            row = self._conn.execute(
                "SELECT version FROM analytics_schema_version LIMIT 1"
            ).fetchone()
            if row is None:
                self._conn.execute(
                    "INSERT INTO analytics_schema_version(version) VALUES (?)",
                    (_SCHEMA_VERSION,),
                )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---- saved queries ---------------------------------------------

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "name": row["name"],
            "datasource_id": row["datasource_id"],
            "sql": row["sql"],
            "owner": row["owner"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def list_saved_queries(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM saved_queries ORDER BY updated_at DESC"
            ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_saved_query(self, query_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM saved_queries WHERE id = ?", (query_id,)
            ).fetchone()
        return self._row_to_dict(row) if row else None

    def create_saved_query(
        self, *, name: str, datasource_id: str, sql: str, owner: str | None = None
    ) -> dict[str, Any]:
        now = _now_iso()
        query_id = uuid.uuid4().hex
        with self._lock:
            self._conn.execute(
                "INSERT INTO saved_queries"
                "(id, name, datasource_id, sql, owner, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (query_id, name, datasource_id, sql, owner, now, now),
            )
        return self.get_saved_query(query_id)  # type: ignore[return-value]

    def update_saved_query(
        self,
        query_id: str,
        *,
        name: str | None = None,
        datasource_id: str | None = None,
        sql: str | None = None,
    ) -> dict[str, Any] | None:
        existing = self.get_saved_query(query_id)
        if existing is None:
            return None
        merged = {
            "name": name if name is not None else existing["name"],
            "datasource_id": datasource_id
            if datasource_id is not None
            else existing["datasource_id"],
            "sql": sql if sql is not None else existing["sql"],
            "updated_at": _now_iso(),
        }
        with self._lock:
            self._conn.execute(
                "UPDATE saved_queries SET name = ?, datasource_id = ?, sql = ?, "
                "updated_at = ? WHERE id = ?",
                (
                    merged["name"],
                    merged["datasource_id"],
                    merged["sql"],
                    merged["updated_at"],
                    query_id,
                ),
            )
        return self.get_saved_query(query_id)

    def delete_saved_query(self, query_id: str) -> bool:
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM saved_queries WHERE id = ?", (query_id,)
            )
            return cur.rowcount > 0

    # ---- charts -----------------------------------------------------

    @staticmethod
    def _chart_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "name": row["name"],
            "datasource_id": row["datasource_id"],
            "sql": row["sql"],
            "viz_type": row["viz_type"],
            "encoding": json.loads(row["encoding_json"]),
            "owner": row["owner"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def list_charts(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM charts ORDER BY updated_at DESC"
            ).fetchall()
        return [self._chart_row_to_dict(r) for r in rows]

    def get_chart(self, chart_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM charts WHERE id = ?", (chart_id,)
            ).fetchone()
        return self._chart_row_to_dict(row) if row else None

    def create_chart(
        self,
        *,
        name: str,
        datasource_id: str,
        sql: str,
        viz_type: str,
        encoding: dict[str, Any] | None = None,
        owner: str | None = None,
    ) -> dict[str, Any]:
        now = _now_iso()
        chart_id = uuid.uuid4().hex
        with self._lock:
            self._conn.execute(
                "INSERT INTO charts"
                "(id, name, datasource_id, sql, viz_type, encoding_json, owner, "
                "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    chart_id,
                    name,
                    datasource_id,
                    sql,
                    viz_type,
                    json.dumps(encoding or {}),
                    owner,
                    now,
                    now,
                ),
            )
        return self.get_chart(chart_id)  # type: ignore[return-value]

    def update_chart(
        self,
        chart_id: str,
        *,
        name: str | None = None,
        datasource_id: str | None = None,
        sql: str | None = None,
        viz_type: str | None = None,
        encoding: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        existing = self.get_chart(chart_id)
        if existing is None:
            return None
        merged = {
            "name": name if name is not None else existing["name"],
            "datasource_id": datasource_id
            if datasource_id is not None
            else existing["datasource_id"],
            "sql": sql if sql is not None else existing["sql"],
            "viz_type": viz_type if viz_type is not None else existing["viz_type"],
            "encoding": encoding if encoding is not None else existing["encoding"],
        }
        with self._lock:
            self._conn.execute(
                "UPDATE charts SET name = ?, datasource_id = ?, sql = ?, "
                "viz_type = ?, encoding_json = ?, updated_at = ? WHERE id = ?",
                (
                    merged["name"],
                    merged["datasource_id"],
                    merged["sql"],
                    merged["viz_type"],
                    json.dumps(merged["encoding"]),
                    _now_iso(),
                    chart_id,
                ),
            )
        return self.get_chart(chart_id)

    def delete_chart(self, chart_id: str) -> bool:
        with self._lock:
            cur = self._conn.execute("DELETE FROM charts WHERE id = ?", (chart_id,))
            return cur.rowcount > 0

    # ---- dashboards -------------------------------------------------

    @staticmethod
    def _dashboard_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "name": row["name"],
            "layout": json.loads(row["layout_json"]),
            "owner": row["owner"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def list_dashboards(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM dashboards ORDER BY updated_at DESC"
            ).fetchall()
        return [self._dashboard_row_to_dict(r) for r in rows]

    def get_dashboard(self, dashboard_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM dashboards WHERE id = ?", (dashboard_id,)
            ).fetchone()
        return self._dashboard_row_to_dict(row) if row else None

    def create_dashboard(
        self,
        *,
        name: str,
        layout: dict[str, Any] | None = None,
        owner: str | None = None,
    ) -> dict[str, Any]:
        now = _now_iso()
        dashboard_id = uuid.uuid4().hex
        with self._lock:
            self._conn.execute(
                "INSERT INTO dashboards"
                "(id, name, layout_json, owner, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    dashboard_id,
                    name,
                    json.dumps(layout or {"tiles": []}),
                    owner,
                    now,
                    now,
                ),
            )
        return self.get_dashboard(dashboard_id)  # type: ignore[return-value]

    def update_dashboard(
        self,
        dashboard_id: str,
        *,
        name: str | None = None,
        layout: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        existing = self.get_dashboard(dashboard_id)
        if existing is None:
            return None
        merged = {
            "name": name if name is not None else existing["name"],
            "layout": layout if layout is not None else existing["layout"],
        }
        with self._lock:
            self._conn.execute(
                "UPDATE dashboards SET name = ?, layout_json = ?, updated_at = ? "
                "WHERE id = ?",
                (merged["name"], json.dumps(merged["layout"]), _now_iso(), dashboard_id),
            )
        return self.get_dashboard(dashboard_id)

    def delete_dashboard(self, dashboard_id: str) -> bool:
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM dashboards WHERE id = ?", (dashboard_id,)
            )
            return cur.rowcount > 0

    # ---- alerts -----------------------------------------------------

    @staticmethod
    def _alert_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "name": row["name"],
            "chart_id": row["chart_id"],
            "column": row["column"],
            "op": row["op"],
            "threshold": row["threshold"],
            "owner": row["owner"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def list_alerts(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM alerts ORDER BY updated_at DESC"
            ).fetchall()
        return [self._alert_row_to_dict(r) for r in rows]

    def get_alert(self, alert_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM alerts WHERE id = ?", (alert_id,)
            ).fetchone()
        return self._alert_row_to_dict(row) if row else None

    def create_alert(
        self,
        *,
        name: str,
        chart_id: str,
        column: str,
        op: str,
        threshold: float,
        owner: str | None = None,
    ) -> dict[str, Any]:
        now = _now_iso()
        alert_id = uuid.uuid4().hex
        with self._lock:
            self._conn.execute(
                "INSERT INTO alerts"
                '(id, name, chart_id, "column", op, threshold, owner, '
                "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (alert_id, name, chart_id, column, op, threshold, owner, now, now),
            )
        return self.get_alert(alert_id)  # type: ignore[return-value]

    def delete_alert(self, alert_id: str) -> bool:
        with self._lock:
            cur = self._conn.execute("DELETE FROM alerts WHERE id = ?", (alert_id,))
            return cur.rowcount > 0

    # ---- data-quality runs (v5) ------------------------------------

    @staticmethod
    def _quality_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "pipeline": row["pipeline"],
            "run_id": row["run_id"],
            "table": row["table_name"],
            "schema": row["schema_name"],
            "verdict": row["verdict"],
            "checks_total": row["checks_total"],
            "checks_failed": row["checks_failed"],
            "assertions": json.loads(row["detail_json"]),
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
        }

    def record_quality_run(self, outcome: Any, *, run_id: str | None = None) -> str:
        """Persist a quality run. ``outcome`` is a
        :class:`ematix_flow.quality.QualityOutcome` (duck-typed here to
        avoid a runtime→web import cycle)."""
        now = _now_iso()
        row_id = uuid.uuid4().hex
        started = outcome.started_at.isoformat() if outcome.started_at else now
        finished = outcome.finished_at.isoformat() if outcome.finished_at else now
        detail = json.dumps([a.to_dict() for a in outcome.assertions])
        with self._lock:
            self._conn.execute(
                "INSERT INTO quality_runs"
                "(id, pipeline, run_id, table_name, schema_name, verdict, "
                "checks_total, checks_failed, detail_json, started_at, finished_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    row_id,
                    outcome.pipeline,
                    run_id,
                    outcome.table,
                    outcome.schema,
                    outcome.verdict,
                    outcome.checks_total,
                    outcome.checks_failed,
                    detail,
                    started,
                    finished,
                ),
            )
        return row_id

    def list_quality_runs(
        self, *, pipeline: str | None = None, limit: int = 50, offset: int = 0
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        offset = max(0, int(offset))
        with self._lock:
            if pipeline:
                rows = self._conn.execute(
                    "SELECT * FROM quality_runs WHERE pipeline = ? "
                    "ORDER BY finished_at DESC LIMIT ? OFFSET ?",
                    (pipeline, limit, offset),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM quality_runs ORDER BY finished_at DESC "
                    "LIMIT ? OFFSET ?",
                    (limit, offset),
                ).fetchall()
        return [self._quality_row_to_dict(r) for r in rows]

    # ---- freshness state (v5) --------------------------------------

    @staticmethod
    def _freshness_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "pipeline": row["pipeline"],
            "sla_seconds": row["sla_seconds"],
            "lag_seconds": row["lag_seconds"],
            "state": row["state"],
            "last_success": row["last_success"],
            "evaluated_at": row["evaluated_at"],
        }

    def upsert_freshness_state(self, state: Any) -> None:
        """Insert/replace a pipeline's current freshness state.
        ``state`` is a :class:`ematix_flow.quality.FreshnessState`."""
        d = state.to_dict()
        with self._lock:
            self._conn.execute(
                "INSERT INTO freshness_state"
                "(pipeline, sla_seconds, lag_seconds, state, last_success, "
                "evaluated_at) VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(pipeline) DO UPDATE SET "
                "sla_seconds=excluded.sla_seconds, lag_seconds=excluded.lag_seconds, "
                "state=excluded.state, last_success=excluded.last_success, "
                "evaluated_at=excluded.evaluated_at",
                (
                    d["pipeline"],
                    d["sla_seconds"],
                    d["lag_seconds"],
                    d["state"],
                    d["last_success"],
                    d["evaluated_at"],
                ),
            )

    def list_freshness(
        self, *, pipeline: str | None = None
    ) -> list[dict[str, Any]]:
        with self._lock:
            if pipeline:
                rows = self._conn.execute(
                    "SELECT * FROM freshness_state WHERE pipeline = ?", (pipeline,)
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM freshness_state ORDER BY pipeline"
                ).fetchall()
        return [self._freshness_row_to_dict(r) for r in rows]
