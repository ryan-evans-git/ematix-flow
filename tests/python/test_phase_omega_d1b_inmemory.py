"""Phase Ω.D1b — InMemoryRunLog backend.

A non-persistent backend that satisfies the same RunLog Protocol as
SqliteRunLog. Useful for tests that want to exercise the run_log=
write-through path without touching disk, and for synthetic / replay
scenarios.

Tests mirror the SqliteRunLog round-trip suite, minus the cross-tick
on-disk tests (the whole point of in-memory is that state doesn't
survive process exit).
"""

from __future__ import annotations

import datetime as _dt

import pytest

from ematix_flow import pipeline as p
from ematix_flow.run_log import InMemoryRunLog, RunLog

_SIDE_TABLES = (
    "_REGISTRY",
    "_DEPENDS_ON",
    "_UPSTREAM_FRESHNESS",
    "_LAST_RUN",
    "_RETRY_POLICY",
    "_ATTEMPT_STATE",
)


@pytest.fixture(autouse=True)
def _clean_registry():
    for tbl in _SIDE_TABLES:
        d = getattr(p, tbl, None)
        if d is not None:
            d.clear()
    yield
    for tbl in _SIDE_TABLES:
        d = getattr(p, tbl, None)
        if d is not None:
            d.clear()


def test_in_memory_satisfies_runlog_protocol():
    """The Protocol uses @runtime_checkable so isinstance works."""
    log = InMemoryRunLog()
    assert isinstance(log, RunLog)


def test_record_run_then_restore_repopulates_last_run():
    log = InMemoryRunLog()
    ts = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    log.record_run("alpha", ts, success=True)
    p._LAST_RUN.clear()
    log.restore_into_process()
    rec_ts, ok = p._LAST_RUN["alpha"]
    assert ok is True
    assert rec_ts == ts


def test_attempt_state_round_trip():
    log = InMemoryRunLog()
    last_at = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    state = p.AttemptState(attempt_count=2, last_attempt_at=last_at, gave_up=False)
    log.record_attempt("flaky", state)
    p._ATTEMPT_STATE.clear()
    log.restore_into_process()
    rec = p._ATTEMPT_STATE["flaky"]
    assert rec.attempt_count == 2
    assert rec.last_attempt_at == last_at
    assert rec.gave_up is False


def test_clear_attempt_state():
    log = InMemoryRunLog()
    last_at = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    log.record_attempt("flaky", p.AttemptState(1, last_at, False))
    log.clear_attempt_state("flaky")
    p._ATTEMPT_STATE.clear()
    log.restore_into_process()
    assert "flaky" not in p._ATTEMPT_STATE


def test_run_due_with_dag_writes_through_to_in_memory_log():
    @p.register(
        name="fail",
        schedule="@hourly",
        retry={"max_attempts": 3, "backoff": "fixed", "base_secs": 30},
    )
    def _fail():
        raise RuntimeError("boom")

    log = InMemoryRunLog()
    t = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    p.run_due_with_dag(["fail"], now=t, run_log=log)

    p._LAST_RUN.clear()
    p._ATTEMPT_STATE.clear()
    log.restore_into_process()
    assert p._LAST_RUN["fail"][1] is False
    assert p._ATTEMPT_STATE["fail"].attempt_count == 1


def test_backwards_compat_pipeline_runlog_alias_still_works():
    """Existing code that imports `from ematix_flow.pipeline import RunLog`
    expecting the SQLite backend should still work — the rename is
    additive, not breaking."""
    from ematix_flow.pipeline import RunLog as LegacyRunLog
    from ematix_flow.run_log import SqliteRunLog
    assert LegacyRunLog is SqliteRunLog
