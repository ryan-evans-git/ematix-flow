"""SqliteRunLog — the default, local-file backend.

Uses stdlib `sqlite3`. Two tables, one row per pipeline name; records
are upserted via REPLACE so the DB always reflects the most recent
state. WAL mode improves concurrent-reader tolerance without changing
single-writer semantics.
"""

from __future__ import annotations

from datetime import datetime

from ._iso import iso_utc, parse_iso


class SqliteRunLog:
    """Local SQLite file. The reference implementation other backends
    are measured against (it's what the Ω.D1a oracle tests pin)."""

    _SCHEMA = """
        CREATE TABLE IF NOT EXISTS run_log (
            pipeline_name TEXT PRIMARY KEY,
            last_run_at   TEXT NOT NULL,
            success       INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS attempt_state (
            pipeline_name   TEXT PRIMARY KEY,
            attempt_count   INTEGER NOT NULL,
            last_attempt_at TEXT NOT NULL,
            gave_up         INTEGER NOT NULL
        );
    """

    def __init__(self, path: str):
        import os
        import sqlite3

        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._path = path
        self._conn = sqlite3.connect(path, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode = WAL;")
        self._conn.executescript(self._SCHEMA)

    @property
    def path(self) -> str:
        return self._path

    def close(self) -> None:
        self._conn.close()

    def record_run(self, name: str, ts: datetime, success: bool) -> None:
        self._conn.execute(
            "REPLACE INTO run_log (pipeline_name, last_run_at, success) VALUES (?, ?, ?)",
            (name, iso_utc(ts), 1 if success else 0),
        )

    def record_attempt(self, name: str, state) -> None:
        self._conn.execute(
            "REPLACE INTO attempt_state "
            "(pipeline_name, attempt_count, last_attempt_at, gave_up) "
            "VALUES (?, ?, ?, ?)",
            (
                name,
                state.attempt_count,
                iso_utc(state.last_attempt_at),
                1 if state.gave_up else 0,
            ),
        )

    def clear_attempt_state(self, name: str) -> None:
        self._conn.execute("DELETE FROM attempt_state WHERE pipeline_name = ?", (name,))

    def restore_into_process(self) -> None:
        from ematix_flow import pipeline as _p

        cur = self._conn.execute(
            "SELECT pipeline_name, last_run_at, success FROM run_log"
        )
        for name, ts_s, ok in cur.fetchall():
            _p._LAST_RUN[name] = (parse_iso(ts_s), bool(ok))
        cur = self._conn.execute(
            "SELECT pipeline_name, attempt_count, last_attempt_at, gave_up "
            "FROM attempt_state"
        )
        for name, count, ts_s, gave_up in cur.fetchall():
            _p._ATTEMPT_STATE[name] = _p.AttemptState(
                attempt_count=count,
                last_attempt_at=parse_iso(ts_s),
                gave_up=bool(gave_up),
            )
