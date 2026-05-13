"""RunLog Protocol — the contract every backend satisfies."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from ematix_flow.pipeline import AttemptState


@runtime_checkable
class RunLog(Protocol):
    """Persistent home for `pipeline._LAST_RUN` + `_ATTEMPT_STATE`.

    Methods are intentionally narrow — backends differ wildly in how
    they store this state (one SQLite file vs object-store keys vs
    Postgres rows) but the API surface is the same four operations.

    Implementations should be idempotent on `record_*` (mirror the
    in-memory dict-assignment semantics) and tolerant of restoring
    from an empty store (a fresh deploy has nothing to load).
    """

    def record_run(self, name: str, ts: datetime, success: bool) -> None:
        """Persist the last completed run's timestamp + outcome."""

    def record_attempt(self, name: str, state: "AttemptState") -> None:
        """Persist the current retry cycle's attempt state."""

    def clear_attempt_state(self, name: str) -> None:
        """Drop the retry cycle (called on a successful run)."""

    def restore_into_process(self) -> None:
        """Hydrate `pipeline._LAST_RUN` and `_ATTEMPT_STATE` from
        the store. Call at process startup; no-op on empty stores."""

    def close(self) -> None:
        """Release any underlying handles. Safe to call multiple times."""
