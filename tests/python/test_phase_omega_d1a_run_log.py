"""Phase Ω.D1a — durable run-history (SQLite-backed RunLog).

In-process state from Ω.1/Ω.2/Ω.3 (last-run timestamp, attempt count,
gave-up flag) currently lives in module-level dicts and is wiped at
process exit. This phase persists those tables to a local SQLite file
so a `flow run-due` cron tick at 12:01 can see what the 12:00 tick did.

Scope: single local file, single-process writes (the read path is
multi-reader-safe via SQLite WAL). Cross-host coordination is out of
scope — that's where a hosted backend (Postgres / object storage)
takes over.

Tests cover:
  - Round-trip: record_run → restore_into_process repopulates _LAST_RUN
  - Attempt state survives: record_attempt → restore repopulates
    _ATTEMPT_STATE including gave_up + last_attempt_at
  - Clear on success: record_run(success=True) wipes the attempt row
  - Two RunLog instances against the same DB see each other's writes
    after a fresh restore (the "next cron tick" scenario)
  - run_due_with_dag with run_log= writes through every mutation
"""

from __future__ import annotations

import datetime as _dt

import pytest

from ematix_flow import pipeline as p

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
    return str(tmp_path / "run_log.db")


# ---- record_run / restore round-trips -----------------------------


def test_empty_db_restores_empty_state(db_path):
    log = p.RunLog(db_path)
    log.restore_into_process()
    assert p._LAST_RUN == {}
    assert p._ATTEMPT_STATE == {}


def test_record_run_then_restore_repopulates_last_run(db_path):
    log = p.RunLog(db_path)
    ts = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    log.record_run("alpha", ts, success=True)
    # Clear in-memory state, then restore from disk.
    p._LAST_RUN.clear()
    p.RunLog(db_path).restore_into_process()
    assert "alpha" in p._LAST_RUN
    rec_ts, ok = p._LAST_RUN["alpha"]
    assert ok is True
    assert rec_ts == ts


def test_failed_run_is_recorded(db_path):
    log = p.RunLog(db_path)
    ts = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    log.record_run("alpha", ts, success=False)
    p._LAST_RUN.clear()
    p.RunLog(db_path).restore_into_process()
    _, ok = p._LAST_RUN["alpha"]
    assert ok is False


def test_record_run_overwrites_prior(db_path):
    log = p.RunLog(db_path)
    t1 = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    t2 = t1 + _dt.timedelta(hours=1)
    log.record_run("alpha", t1, success=False)
    log.record_run("alpha", t2, success=True)
    p._LAST_RUN.clear()
    p.RunLog(db_path).restore_into_process()
    ts, ok = p._LAST_RUN["alpha"]
    assert ts == t2
    assert ok is True


# ---- attempt state ------------------------------------------------


def test_attempt_state_round_trip(db_path):
    log = p.RunLog(db_path)
    last_at = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    state = p.AttemptState(attempt_count=2, last_attempt_at=last_at, gave_up=False)
    log.record_attempt("flaky", state)
    p._ATTEMPT_STATE.clear()
    p.RunLog(db_path).restore_into_process()
    rec = p._ATTEMPT_STATE["flaky"]
    assert rec.attempt_count == 2
    assert rec.last_attempt_at == last_at
    assert rec.gave_up is False


def test_gave_up_flag_persists(db_path):
    log = p.RunLog(db_path)
    last_at = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    state = p.AttemptState(attempt_count=3, last_attempt_at=last_at, gave_up=True)
    log.record_attempt("dies", state)
    p._ATTEMPT_STATE.clear()
    p.RunLog(db_path).restore_into_process()
    assert p._ATTEMPT_STATE["dies"].gave_up is True


def test_clear_attempt_state(db_path):
    log = p.RunLog(db_path)
    last_at = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    log.record_attempt("flaky", p.AttemptState(1, last_at, False))
    log.clear_attempt_state("flaky")
    p._ATTEMPT_STATE.clear()
    p.RunLog(db_path).restore_into_process()
    assert "flaky" not in p._ATTEMPT_STATE


# ---- cross-tick scenario ------------------------------------------


def test_two_processes_share_state_via_disk(db_path):
    """Simulates two `flow run-due` invocations: tick 1 records state,
    tick 2 opens a fresh RunLog and sees it."""
    tick1 = p.RunLog(db_path)
    t1 = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    tick1.record_run("root", t1, success=True)
    tick1.record_attempt("flaky", p.AttemptState(1, t1, False))

    # Tick 2: fresh process, fresh in-memory state, fresh RunLog
    # instance backed by the same file.
    p._LAST_RUN.clear()
    p._ATTEMPT_STATE.clear()
    tick2 = p.RunLog(db_path)
    tick2.restore_into_process()
    assert p._LAST_RUN["root"][1] is True
    assert p._ATTEMPT_STATE["flaky"].attempt_count == 1


# ---- write-through from run_due_with_dag --------------------------


def test_run_due_with_dag_writes_through_on_success(db_path):
    @p.register(name="ok", schedule="@hourly")
    def _ok():
        return {}

    log = p.RunLog(db_path)
    t = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    p.run_due_with_dag(["ok"], now=t, run_log=log)

    # Wipe and restore; should still see the success.
    p._LAST_RUN.clear()
    p.RunLog(db_path).restore_into_process()
    assert p._LAST_RUN["ok"][1] is True


def test_run_due_with_dag_writes_through_on_failure(db_path):
    @p.register(
        name="fail",
        schedule="@hourly",
        retry={"max_attempts": 3, "backoff": "fixed", "base_secs": 30},
    )
    def _fail():
        raise RuntimeError("boom")

    log = p.RunLog(db_path)
    t = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    p.run_due_with_dag(["fail"], now=t, run_log=log)

    p._LAST_RUN.clear()
    p._ATTEMPT_STATE.clear()
    p.RunLog(db_path).restore_into_process()
    assert p._LAST_RUN["fail"][1] is False
    assert p._ATTEMPT_STATE["fail"].attempt_count == 1
    assert p._ATTEMPT_STATE["fail"].gave_up is False


def test_freshness_gate_works_across_ticks(db_path):
    """End-to-end Ω.1 + Ω.D1a: tick 1 fires `root` (success). Tick 2 is
    a fresh process — RunLog restores `root`'s success so `leaf` (which
    depends_on=root) fires on tick 2 even though no upstream is in the
    `due` set."""
    @p.register(name="root", schedule="@hourly")
    def _root():
        return {}

    @p.register(name="leaf", schedule="@hourly", depends_on=["root"])
    def _leaf():
        return {}

    log = p.RunLog(db_path)
    p.run_due_with_dag(["root"], run_log=log)

    # Tick 2: same registry (still in-process), but pretend we lost
    # in-memory state — restore from disk and the freshness check
    # against root should pass.
    p._LAST_RUN.clear()
    p.RunLog(db_path).restore_into_process()
    fired = p.run_due_with_dag(["leaf"], run_log=log)
    assert fired == ["leaf"]


def test_retry_backoff_survives_tick_restart(db_path):
    """The killer scenario for Ω.D1a: a pipeline failed at 12:00. The
    next cron tick at 12:01 must see the attempt state and wait for
    the backoff window even though we're a fresh process."""
    @p.register(
        name="flaky",
        schedule="@hourly",
        retry={"max_attempts": 3, "backoff": "fixed", "base_secs": 120},
    )
    def _flaky():
        raise RuntimeError("boom")

    log = p.RunLog(db_path)
    t1 = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    p.run_due_with_dag(["flaky"], now=t1, run_log=log)
    assert p._ATTEMPT_STATE["flaky"].attempt_count == 1

    # Simulate cron tick 2 minute later, fresh process. Restore from
    # disk, then try to run — should be blocked by the 120s window.
    p._LAST_RUN.clear()
    p._ATTEMPT_STATE.clear()
    p.RunLog(db_path).restore_into_process()
    fired = p.run_due_with_dag(
        ["flaky"],
        now=t1 + _dt.timedelta(seconds=60),
        run_log=log,
    )
    assert fired == []
    # attempt_count didn't increment — the function never ran.
    assert p._ATTEMPT_STATE["flaky"].attempt_count == 1
