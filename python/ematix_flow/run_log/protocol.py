"""RunLog Protocol — the contract every backend satisfies."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from ematix_flow.pipeline import AttemptState


# ---- Ω.W.1: lease semantics for distributed dispatch ---------------


@dataclass(frozen=True)
class ClaimResult:
    """Outcome of `RunLog.claim()`.

    `acquired=True` means the caller now holds the lease until
    `expires_at`; pass `token` to `heartbeat()` / `release()`.

    `acquired=False` means another worker holds an unexpired lease;
    `holder` and `expires_at` describe whose, and until when.
    """

    acquired: bool
    token: str | None
    holder: str
    expires_at: datetime

    @classmethod
    def acquired_by(cls, token: str, worker_id: str, expires_at: datetime) -> ClaimResult:
        return cls(acquired=True, token=token, holder=worker_id, expires_at=expires_at)

    @classmethod
    def busy(cls, holder: str, expires_at: datetime) -> ClaimResult:
        return cls(acquired=False, token=None, holder=holder, expires_at=expires_at)


@dataclass(frozen=True)
class ExpiredClaim:
    """A claim whose lease ran out. Returned from
    `RunLog.sweep_expired_leases()` so the scheduler can record the
    death + re-mark the pipeline for re-claim."""

    pipeline: str
    worker_id: str
    expires_at: datetime


@runtime_checkable
class RunLog(Protocol):
    """Persistent home for `pipeline._LAST_RUN` + `_ATTEMPT_STATE`,
    plus (Ω.W) the lease/claim layer that lets multiple workers
    coordinate through a shared store.

    The first surface — `record_run` / `record_attempt` /
    `clear_attempt_state` / `restore_into_process` — is the same one
    every backend has shipped since Ω.D1a. Implementations should be
    idempotent on the `record_*` calls (mirror the in-memory
    dict-assignment semantics) and tolerant of restoring from an
    empty store (a fresh deploy has nothing to load).

    The second surface — `claim` / `heartbeat` / `release` /
    `sweep_expired_leases` — is the Ω.W lease layer. Single-process
    backends (SQLite, DuckDB, InMemory) implement these with a local
    transaction; SQL backends (Postgres, MySQL) get real CAS in
    Ω.W.2; blob backends (S3, Azure, GCS) raise `NotImplementedError`
    because object stores can't do CAS without an external lock
    service.
    """

    def record_run(self, name: str, ts: datetime, success: bool) -> None:
        """Persist the last completed run's timestamp + outcome."""

    def record_attempt(self, name: str, state: AttemptState) -> None:
        """Persist the current retry cycle's attempt state."""

    def clear_attempt_state(self, name: str) -> None:
        """Drop the retry cycle (called on a successful run)."""

    def restore_into_process(self) -> None:
        """Hydrate `pipeline._LAST_RUN` and `_ATTEMPT_STATE` from
        the store. Call at process startup; no-op on empty stores."""

    def claim(
        self,
        pipeline: str,
        worker_id: str,
        lease_seconds: int,
    ) -> ClaimResult:
        """Try to lease `pipeline` for `worker_id` for the next
        `lease_seconds`. Returns `ClaimResult.acquired_by(...)` with
        a token the worker should heartbeat with, or
        `ClaimResult.busy(...)` describing the current holder if
        another worker has an unexpired lease."""

    def heartbeat(self, claim_token: str, lease_seconds: int) -> None:
        """Extend the lease keyed by `claim_token` for another
        `lease_seconds`. No-op if the token is stale (lease already
        expired and someone else re-claimed). Workers call this on
        their heartbeat thread."""

    def release(self, claim_token: str) -> None:
        """Voluntarily drop the lease. Called on clean worker exit
        so another scheduler pass can fire the pipeline again
        without waiting for lease expiry."""

    def sweep_expired_leases(self, now: datetime) -> list[ExpiredClaim]:
        """Return claims whose lease ran out before `now`. The
        scheduler walks this each tick to detect worker death and
        re-mark pipelines as available for re-claim."""

    def close(self) -> None:
        """Release any underlying handles. Safe to call multiple times."""
