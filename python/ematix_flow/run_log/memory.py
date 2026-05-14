"""InMemoryRunLog — non-persistent backend, primarily for tests.

Holds the same dicts SqliteRunLog stores rows for. State doesn't
survive process exit — that's the whole point. Use it when you want
the `run_log=` write-through path exercised without touching disk
(e.g. replaying a sequence of fake ticks in a single test).

Ω.W.1 adds the lease layer on top of the same `_claims` dict. Since
this backend is single-process by definition, no inter-process
locking is needed.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from .protocol import ClaimResult, ExpiredClaim


@dataclass
class _ClaimRecord:
    pipeline: str
    token: str
    worker_id: str
    claimed_at: datetime
    expires_at: datetime


class InMemoryRunLog:
    def __init__(self) -> None:
        self._runs: dict[str, tuple[datetime, bool]] = {}
        self._attempts: dict[str, object] = {}
        self._claims: dict[str, _ClaimRecord] = {}

    def close(self) -> None:  # nothing to release
        pass

    def record_run(self, name: str, ts: datetime, success: bool) -> None:
        self._runs[name] = (ts, success)

    def record_attempt(self, name: str, state) -> None:
        self._attempts[name] = state

    def clear_attempt_state(self, name: str) -> None:
        self._attempts.pop(name, None)

    def restore_into_process(self) -> None:
        from ematix_flow import pipeline as _p

        for name, (ts, ok) in self._runs.items():
            _p._LAST_RUN[name] = (ts, ok)
        for name, state in self._attempts.items():
            _p._ATTEMPT_STATE[name] = state

    # ---- Ω.W.1: lease layer ---------------------------------------

    def claim(self, pipeline: str, worker_id: str, lease_seconds: int) -> ClaimResult:
        # Truncate to second precision so the same value survives a
        # round-trip through any SQL backend's iso_utc storage.
        now = datetime.now(UTC).replace(microsecond=0)
        existing = self._claims.get(pipeline)
        if existing is not None and existing.expires_at > now:
            return ClaimResult.busy(
                holder=existing.worker_id, expires_at=existing.expires_at
            )
        token = uuid.uuid4().hex
        expires_at = now + timedelta(seconds=lease_seconds)
        self._claims[pipeline] = _ClaimRecord(
            pipeline=pipeline,
            token=token,
            worker_id=worker_id,
            claimed_at=now,
            expires_at=expires_at,
        )
        return ClaimResult.acquired_by(
            token=token, worker_id=worker_id, expires_at=expires_at
        )

    def heartbeat(self, claim_token: str, lease_seconds: int) -> None:
        for rec in self._claims.values():
            if rec.token == claim_token:
                rec.expires_at = datetime.now(UTC).replace(
                    microsecond=0
                ) + timedelta(seconds=lease_seconds)
                return
        # Stale token — silently no-op so the scheduler doesn't bark
        # when a worker that lost its lease catches up.

    def release(self, claim_token: str) -> None:
        for name, rec in list(self._claims.items()):
            if rec.token == claim_token:
                del self._claims[name]
                return
        # Stale token — silently no-op (same reason as heartbeat).

    def sweep_expired_leases(self, now: datetime) -> list[ExpiredClaim]:
        return [
            ExpiredClaim(
                pipeline=rec.pipeline,
                worker_id=rec.worker_id,
                expires_at=rec.expires_at,
            )
            for rec in self._claims.values()
            if rec.expires_at < now
        ]
