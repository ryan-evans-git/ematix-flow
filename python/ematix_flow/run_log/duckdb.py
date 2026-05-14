"""DuckDBRunLog — embedded DuckDB backend.

In-process analytical DB. Same niche as SqliteRunLog (single-host,
local file or `:memory:`) but uses DuckDB syntax + types — useful if
your stack is already DuckDB-centric or you want to query the run-log
with DuckDB's SQL dialect (window functions, ARRAY types, etc.).

For multi-host coordination, prefer PostgresRunLog or MySQLRunLog —
DuckDB is a single-writer in-process engine, same constraint as SQLite.

Optional dep: `duckdb`.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from ._iso import iso_utc, parse_iso
from .protocol import ClaimResult, ExpiredClaim


class DuckDBRunLog:
    """DuckDB-backed run history.

    Args:
        path: file path. Pass ":memory:" for a non-persistent store
            (handy for tests). Default is a file at the given path.
        create_tables: when True, create the two tables on connect via
            `CREATE TABLE IF NOT EXISTS`. Idempotent.
    """

    _DDL = (
        "CREATE TABLE IF NOT EXISTS run_log ("
        "  pipeline_name VARCHAR PRIMARY KEY,"
        "  last_run_at   VARCHAR NOT NULL,"
        "  success       BOOLEAN NOT NULL"
        ");"
        "CREATE TABLE IF NOT EXISTS attempt_state ("
        "  pipeline_name   VARCHAR PRIMARY KEY,"
        "  attempt_count   INTEGER NOT NULL,"
        "  last_attempt_at VARCHAR NOT NULL,"
        "  gave_up         BOOLEAN NOT NULL"
        ");"
        "CREATE TABLE IF NOT EXISTS pipeline_claims ("
        "  pipeline_name VARCHAR PRIMARY KEY,"
        "  claim_token   VARCHAR NOT NULL,"
        "  worker_id     VARCHAR NOT NULL,"
        "  claimed_at    VARCHAR NOT NULL,"
        "  expires_at    VARCHAR NOT NULL"
        ");"
    )

    def __init__(self, path: str, *, create_tables: bool = True):
        try:
            import duckdb
        except ImportError as e:
            raise ImportError(
                "DuckDBRunLog requires duckdb. Install with `pip install duckdb`."
            ) from e

        import os
        if path != ":memory:":
            parent = os.path.dirname(path)
            if parent:
                os.makedirs(parent, exist_ok=True)

        self._conn = duckdb.connect(path)
        self._path = path
        if create_tables:
            self._conn.execute(self._DDL)

    @property
    def path(self) -> str:
        return self._path

    def close(self) -> None:
        self._conn.close()

    def record_run(self, name: str, ts: datetime, success: bool) -> None:
        # DuckDB supports Postgres-style ON CONFLICT clauses.
        self._conn.execute(
            "INSERT INTO run_log (pipeline_name, last_run_at, success) "
            "VALUES (?, ?, ?) "
            "ON CONFLICT (pipeline_name) DO UPDATE SET "
            "last_run_at = EXCLUDED.last_run_at, "
            "success     = EXCLUDED.success",
            (name, iso_utc(ts), bool(success)),
        )

    def record_attempt(self, name: str, state) -> None:
        self._conn.execute(
            "INSERT INTO attempt_state "
            "(pipeline_name, attempt_count, last_attempt_at, gave_up) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT (pipeline_name) DO UPDATE SET "
            "attempt_count   = EXCLUDED.attempt_count, "
            "last_attempt_at = EXCLUDED.last_attempt_at, "
            "gave_up         = EXCLUDED.gave_up",
            (
                name,
                state.attempt_count,
                iso_utc(state.last_attempt_at),
                bool(state.gave_up),
            ),
        )

    def clear_attempt_state(self, name: str) -> None:
        self._conn.execute(
            "DELETE FROM attempt_state WHERE pipeline_name = ?",
            (name,),
        )

    def restore_into_process(self) -> None:
        from ematix_flow import pipeline as _p

        for name, ts_s, ok in self._conn.execute(
            "SELECT pipeline_name, last_run_at, success FROM run_log"
        ).fetchall():
            _p._LAST_RUN[name] = (parse_iso(ts_s), bool(ok))
        for name, count, ts_s, gave_up in self._conn.execute(
            "SELECT pipeline_name, attempt_count, last_attempt_at, gave_up "
            "FROM attempt_state"
        ).fetchall():
            _p._ATTEMPT_STATE[name] = _p.AttemptState(
                attempt_count=count,
                last_attempt_at=parse_iso(ts_s),
                gave_up=bool(gave_up),
            )

    # ---- Ω.W.1: lease layer ---------------------------------------

    def claim(self, pipeline: str, worker_id: str, lease_seconds: int) -> ClaimResult:
        # DuckDB is single-writer in-process, like SQLite — a single
        # transaction around the read-then-conditional-write is safe.
        now = datetime.now(UTC).replace(microsecond=0)
        self._conn.execute("BEGIN TRANSACTION")
        try:
            row = self._conn.execute(
                "SELECT worker_id, expires_at FROM pipeline_claims "
                "WHERE pipeline_name = ?",
                (pipeline,),
            ).fetchone()
            if row is not None and parse_iso(row[1]) > now:
                self._conn.execute("COMMIT")
                return ClaimResult.busy(holder=row[0], expires_at=parse_iso(row[1]))
            token = uuid.uuid4().hex
            expires_at = now + timedelta(seconds=lease_seconds)
            self._conn.execute(
                "INSERT INTO pipeline_claims "
                "(pipeline_name, claim_token, worker_id, claimed_at, expires_at) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT (pipeline_name) DO UPDATE SET "
                "claim_token = EXCLUDED.claim_token, "
                "worker_id   = EXCLUDED.worker_id, "
                "claimed_at  = EXCLUDED.claimed_at, "
                "expires_at  = EXCLUDED.expires_at",
                (pipeline, token, worker_id, iso_utc(now), iso_utc(expires_at)),
            )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        return ClaimResult.acquired_by(
            token=token, worker_id=worker_id, expires_at=expires_at
        )

    def heartbeat(self, claim_token: str, lease_seconds: int) -> None:
        new_expires = datetime.now(UTC).replace(microsecond=0) + timedelta(
            seconds=lease_seconds
        )
        self._conn.execute(
            "UPDATE pipeline_claims SET expires_at = ? WHERE claim_token = ?",
            (iso_utc(new_expires), claim_token),
        )

    def release(self, claim_token: str) -> None:
        self._conn.execute(
            "DELETE FROM pipeline_claims WHERE claim_token = ?", (claim_token,)
        )

    def sweep_expired_leases(self, now: datetime) -> list[ExpiredClaim]:
        rows = self._conn.execute(
            "SELECT pipeline_name, worker_id, expires_at FROM pipeline_claims "
            "WHERE expires_at < ?",
            (iso_utc(now),),
        ).fetchall()
        return [
            ExpiredClaim(pipeline=name, worker_id=wid, expires_at=parse_iso(exp))
            for name, wid, exp in rows
        ]
