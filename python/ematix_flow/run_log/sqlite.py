"""SqliteRunLog — the default, local-file backend.

Uses stdlib `sqlite3`. Three tables, one row per pipeline name;
records are upserted via REPLACE so the DB always reflects the most
recent state. WAL mode improves concurrent-reader tolerance without
changing single-writer semantics.

Ω.W.1: a `pipeline_claims` table holds the lease layer. SQLite is
single-writer, so a `BEGIN IMMEDIATE` transaction around
read-then-conditional-write is sufficient for safe CAS within one
process. Distributed multi-process CAS lives on Postgres/MySQL
(Ω.W.2).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from ._iso import iso_utc, parse_iso
from .protocol import ClaimResult, ExpiredClaim


class _LockingConn:
    """Thin proxy over sqlite3.Connection that holds an RLock for the
    duration of each call. Used by `SqliteRunLog` so the
    `HeartbeatThread` (a non-main thread) can `execute()` against the
    same connection the scheduler / worker main thread uses, without
    Python's `sqlite3` rejecting it for cross-thread access."""

    __slots__ = ("_lock", "_raw")

    def __init__(self, raw, lock):
        self._raw = raw
        self._lock = lock

    def execute(self, *a, **kw):
        with self._lock:
            return self._raw.execute(*a, **kw)

    def executescript(self, *a, **kw):
        with self._lock:
            return self._raw.executescript(*a, **kw)

    def commit(self):
        with self._lock:
            return self._raw.commit()

    def close(self):
        with self._lock:
            return self._raw.close()


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
        CREATE TABLE IF NOT EXISTS pipeline_claims (
            pipeline_name TEXT PRIMARY KEY,
            claim_token   TEXT NOT NULL,
            worker_id     TEXT NOT NULL,
            claimed_at    TEXT NOT NULL,
            expires_at    TEXT NOT NULL
        );
    """

    def __init__(self, path: str):
        import os
        import sqlite3
        import threading

        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._path = path
        # Cross-thread access (Ω.W.3 HeartbeatThread calls in from a
        # non-main thread). `check_same_thread=False` defuses SQLite's
        # built-in rejection; `_LockingConn` then serialises every
        # `.execute()` / `.executescript()` / `.close()` call via an
        # `RLock`. SQLite is single-writer anyway — this just makes
        # the serialisation explicit + visible to callers.
        raw = sqlite3.connect(
            path, isolation_level=None, check_same_thread=False
        )
        self._conn = _LockingConn(raw, threading.RLock())
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

    # ---- Ω.W.1: lease layer ---------------------------------------

    def claim(self, pipeline: str, worker_id: str, lease_seconds: int) -> ClaimResult:
        # Truncate to second precision so what we hand back in
        # ClaimResult matches what comes back out of the next SELECT.
        now = datetime.now(UTC).replace(microsecond=0)
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self._conn.execute(
                "SELECT worker_id, expires_at FROM pipeline_claims WHERE pipeline_name = ?",
                (pipeline,),
            ).fetchone()
            if row is not None and parse_iso(row[1]) > now:
                self._conn.execute("COMMIT")
                return ClaimResult.busy(holder=row[0], expires_at=parse_iso(row[1]))
            token = uuid.uuid4().hex
            expires_at = now + timedelta(seconds=lease_seconds)
            self._conn.execute(
                "REPLACE INTO pipeline_claims "
                "(pipeline_name, claim_token, worker_id, claimed_at, expires_at) "
                "VALUES (?, ?, ?, ?, ?)",
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
        cur = self._conn.execute(
            "SELECT pipeline_name, worker_id, expires_at "
            "FROM pipeline_claims WHERE expires_at < ?",
            (iso_utc(now),),
        )
        return [
            ExpiredClaim(pipeline=name, worker_id=wid, expires_at=parse_iso(exp))
            for name, wid, exp in cur.fetchall()
        ]
