"""Ω.W.1: shared "lease layer not supported" mixin for blob backends.

Object stores (S3 / Azure / GCS) can't do CAS without an external
lock service, so the lease methods will *never* be implemented for
these backends. Mixing in `NoLeaseBlobBackend` makes the failure
mode loud and informative: callers learn at the call site that they
should pick a SQL backend for distributed scheduler dispatch.

The Ω.W.2-deferred SQL mixin that lived here was removed when
PostgresRunLog and MySQLRunLog landed real CAS impls.
"""

from __future__ import annotations

from datetime import datetime

from .protocol import ClaimResult, ExpiredClaim

_BLOB_MSG = (
    "{cls}.{method} is not supported: object stores can't do CAS without "
    "an external lock service. For distributed scheduler dispatch, use "
    "PostgresRunLog or MySQLRunLog as your RunLog backend."
)


def _raise(cls: type, method: str):
    raise NotImplementedError(_BLOB_MSG.format(cls=cls.__name__, method=method))


class NoLeaseBlobBackend:
    """Mixin marking a backend as lease-incompatible (object stores)."""

    def claim(self, pipeline: str, worker_id: str, lease_seconds: int) -> ClaimResult:
        _raise(type(self), "claim")

    def heartbeat(self, claim_token: str, lease_seconds: int) -> None:
        _raise(type(self), "heartbeat")

    def release(self, claim_token: str) -> None:
        _raise(type(self), "release")

    def sweep_expired_leases(self, now: datetime) -> list[ExpiredClaim]:
        _raise(type(self), "sweep_expired_leases")
