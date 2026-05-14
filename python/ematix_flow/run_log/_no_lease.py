"""Ω.W.1: shared "lease layer not supported" mixins.

Two flavors:

  - `NoLeaseBlobBackend` for S3 / Azure / GCS: blob stores can't do
    CAS without an external lock service, so the lease methods will
    *never* be implemented for these backends. Use a SQL backend if
    you want distributed scheduler-dispatch.

  - `NoLeaseSQLBackend` for Postgres / MySQL: real CAS impls land in
    Ω.W.2. Until then, calling `claim` etc. raises with a pointer to
    the in-flight phase.
"""

from __future__ import annotations

from datetime import datetime

from .protocol import ClaimResult, ExpiredClaim

_BLOB_MSG = (
    "{cls}.{method} is not supported: object stores can't do CAS without "
    "an external lock service. For distributed scheduler dispatch, use "
    "PostgresRunLog or MySQLRunLog as your RunLog backend."
)

_DEFERRED_MSG = (
    "{cls}.{method} is not yet implemented — landing in Ω.W.2. Track "
    "phase progress in docs/PHASE_OMEGA_W_PLAN.md."
)


def _raise(template: str, cls: type, method: str):
    raise NotImplementedError(
        template.format(cls=cls.__name__, method=method)
    )


class NoLeaseBlobBackend:
    """Mixin marking a backend as lease-incompatible (object stores)."""

    def claim(self, pipeline: str, worker_id: str, lease_seconds: int) -> ClaimResult:
        _raise(_BLOB_MSG, type(self), "claim")

    def heartbeat(self, claim_token: str, lease_seconds: int) -> None:
        _raise(_BLOB_MSG, type(self), "heartbeat")

    def release(self, claim_token: str) -> None:
        _raise(_BLOB_MSG, type(self), "release")

    def sweep_expired_leases(self, now: datetime) -> list[ExpiredClaim]:
        _raise(_BLOB_MSG, type(self), "sweep_expired_leases")


class NoLeaseSQLBackend:
    """Mixin marking a backend's lease layer as Ω.W.2-deferred."""

    def claim(self, pipeline: str, worker_id: str, lease_seconds: int) -> ClaimResult:
        _raise(_DEFERRED_MSG, type(self), "claim")

    def heartbeat(self, claim_token: str, lease_seconds: int) -> None:
        _raise(_DEFERRED_MSG, type(self), "heartbeat")

    def release(self, claim_token: str) -> None:
        _raise(_DEFERRED_MSG, type(self), "release")

    def sweep_expired_leases(self, now: datetime) -> list[ExpiredClaim]:
        _raise(_DEFERRED_MSG, type(self), "sweep_expired_leases")
