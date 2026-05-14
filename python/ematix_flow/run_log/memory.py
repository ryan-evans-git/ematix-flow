"""InMemoryRunLog — non-persistent backend, primarily for tests.

Holds the same two dicts SqliteRunLog stores rows for. State doesn't
survive process exit — that's the whole point. Use it when you want
the `run_log=` write-through path exercised without touching disk
(e.g. replaying a sequence of fake ticks in a single test).
"""

from __future__ import annotations

from datetime import datetime


class InMemoryRunLog:
    def __init__(self) -> None:
        self._runs: dict[str, tuple[datetime, bool]] = {}
        self._attempts: dict[str, object] = {}

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
