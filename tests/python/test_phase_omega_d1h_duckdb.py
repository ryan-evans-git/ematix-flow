"""Phase Ω.D1h — DuckDBRunLog backend.

DuckDB is an in-process embedded DB (think SQLite but analytical).
Tests skip if `duckdb` isn't installed.
"""

from __future__ import annotations

import datetime as _dt
import pytest

from ematix_flow import pipeline as p


pytest.importorskip("duckdb")


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


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "run_log.duckdb")


def test_protocol_check(db_path):
    from ematix_flow.run_log import DuckDBRunLog, RunLog

    log = DuckDBRunLog(db_path)
    try:
        assert isinstance(log, RunLog)
    finally:
        log.close()


def test_round_trip_file_backed(db_path):
    """Same cross-tick scenario the SQLite oracle tests use: write
    state through one connection, restore through a second."""
    from ematix_flow.run_log import DuckDBRunLog

    log = DuckDBRunLog(db_path)
    ts = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.timezone.utc)
    log.record_run("alpha", ts, success=True)
    log.record_attempt(
        "flaky",
        p.AttemptState(attempt_count=2, last_attempt_at=ts, gave_up=False),
    )
    log.close()  # release file handle so the next conn can open it

    p._LAST_RUN.clear()
    p._ATTEMPT_STATE.clear()
    log2 = DuckDBRunLog(db_path)
    try:
        log2.restore_into_process()
        assert p._LAST_RUN["alpha"] == (ts, True)
        assert p._ATTEMPT_STATE["flaky"].attempt_count == 2
        assert p._ATTEMPT_STATE["flaky"].gave_up is False
    finally:
        log2.close()


def test_in_memory_round_trip():
    """`:memory:` works for single-connection use (the same handle
    holds the state). Useful for tests that want DuckDB semantics
    without touching disk."""
    from ematix_flow.run_log import DuckDBRunLog

    log = DuckDBRunLog(":memory:")
    try:
        ts = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.timezone.utc)
        log.record_run("alpha", ts, success=True)
        log.restore_into_process()
        assert p._LAST_RUN["alpha"] == (ts, True)
    finally:
        log.close()


def test_clear_attempt(db_path):
    from ematix_flow.run_log import DuckDBRunLog

    log = DuckDBRunLog(db_path)
    try:
        ts = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.timezone.utc)
        log.record_attempt("flaky", p.AttemptState(1, ts, False))
        log.clear_attempt_state("flaky")
        p._ATTEMPT_STATE.clear()
        log.restore_into_process()
        assert "flaky" not in p._ATTEMPT_STATE
    finally:
        log.close()


def test_run_due_writes_through(db_path):
    from ematix_flow.run_log import DuckDBRunLog

    @p.register(
        name="fail",
        schedule="@hourly",
        retry={"max_attempts": 3, "backoff": "fixed", "base_secs": 30},
    )
    def _fail():
        raise RuntimeError("boom")

    log = DuckDBRunLog(db_path)
    try:
        t = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.timezone.utc)
        p.run_due_with_dag(["fail"], now=t, run_log=log)
    finally:
        log.close()

    p._LAST_RUN.clear()
    p._ATTEMPT_STATE.clear()
    log2 = DuckDBRunLog(db_path)
    try:
        log2.restore_into_process()
        assert p._LAST_RUN["fail"][1] is False
        assert p._ATTEMPT_STATE["fail"].attempt_count == 1
    finally:
        log2.close()
