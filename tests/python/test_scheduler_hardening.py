"""Scheduler-loop hardening regressions.

Covers three defects in `ematix_flow.scheduler.loop`:

  * timezone: the pipeline's ``timezone=`` must be honored when
    evaluating its cron ``schedule`` (previously always UTC).
  * fire-slot dedup: a pipeline that completes in less than the cron
    match window must NOT be re-dispatched on every subsequent poll
    within that same window.
  * crash resilience: a non-``DispatchError`` raised by the executor
    (or anywhere in a tick) must not kill the long-running daemon.
"""
from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from ematix_flow import pipeline as p
from ematix_flow.executors import DispatchError
from ematix_flow.run_log.protocol import ClaimResult
from ematix_flow.scheduler.loop import _dispatch_one, _walk_and_dispatch


@pytest.fixture(autouse=True)
def _clean_registry():
    for tbl in (
        "_REGISTRY",
        "_DEPENDS_ON",
        "_UPSTREAM_FRESHNESS",
        "_LAST_RUN",
        "_RETRY_POLICY",
        "_ATTEMPT_STATE",
    ):
        d = getattr(p, tbl, None)
        if d is not None:
            d.clear()
    yield
    for tbl in (
        "_REGISTRY",
        "_DEPENDS_ON",
        "_UPSTREAM_FRESHNESS",
        "_LAST_RUN",
        "_RETRY_POLICY",
        "_ATTEMPT_STATE",
    ):
        d = getattr(p, tbl, None)
        if d is not None:
            d.clear()


def _run_log_acquires():
    run_log = MagicMock()
    run_log.claim.return_value = ClaimResult.acquired_by(
        token="tok", worker_id="sched-1",
        expires_at=datetime(2026, 5, 20, 17, tzinfo=UTC),
    )
    run_log.restore_into_process.return_value = None
    return run_log


def _walk(run_log, executor, now, interval_seconds=60):
    _walk_and_dispatch(
        module="m",
        run_log=run_log,
        run_log_url="sqlite:///x.db",
        executor=executor,
        alerter_urls=[],
        metrics_url=None,
        alerters=[],
        metrics=None,
        worker_id="sched-1",
        lease_seconds=300,
        interval_seconds=interval_seconds,
        now=now,
    )


def test_pipeline_timezone_is_honored_in_schedule_match():
    """`schedule="0 12 * * *"` with tz=America/New_York fires at noon
    Eastern (16:00 UTC in May), NOT noon UTC. At 16:00:30 UTC the
    pipeline is due only if the tz is applied."""

    @p.register(name="tz_pipe", schedule="0 12 * * *", timezone="America/New_York")
    def _tz():
        return {}

    run_log = _run_log_acquires()
    executor = MagicMock()
    now = datetime(2026, 5, 20, 16, 0, 30, tzinfo=UTC)  # 12:00:30 EDT
    _walk(run_log, executor, now)
    executor.dispatch.assert_called_once()


def test_pipeline_not_due_in_wrong_utc_slot_without_tz_semantics():
    """Same pipeline at 12:00:30 UTC (08:00 EDT) is NOT due — guards
    against a fix that just always fires."""

    @p.register(name="tz_pipe2", schedule="0 12 * * *", timezone="America/New_York")
    def _tz():
        return {}

    run_log = _run_log_acquires()
    executor = MagicMock()
    now = datetime(2026, 5, 20, 12, 0, 30, tzinfo=UTC)  # 08:00 EDT — not noon
    _walk(run_log, executor, now)
    executor.dispatch.assert_not_called()


def test_no_double_fire_after_completion_within_window():
    """A pipeline that already ran for the current cron fire-slot must
    not be re-dispatched on a later poll still inside the match window."""

    @p.register(name="hourly_pipe", schedule="0 * * * *")
    def _h():
        return {}

    run_log = _run_log_acquires()
    executor = MagicMock()

    # First poll shortly after the top of the hour: due, dispatched.
    t0 = datetime(2026, 5, 20, 14, 0, 5, tzinfo=UTC)
    _walk(run_log, executor, t0)
    executor.dispatch.assert_called_once()

    # Simulate the worker completing the run (as restore_into_process
    # would surface from the RunLog on the next tick).
    p._LAST_RUN["hourly_pipe"] = (datetime(2026, 5, 20, 14, 0, 12, tzinfo=UTC), True)

    # Second poll 10s later — still inside the (now-60s, now] window,
    # so is_due is still True — must NOT re-dispatch.
    executor.reset_mock()
    t1 = datetime(2026, 5, 20, 14, 0, 15, tzinfo=UTC)
    _walk(run_log, executor, t1)
    executor.dispatch.assert_not_called()


def test_next_slot_still_fires_after_dedup():
    """Dedup is per fire-slot, not permanent: the next scheduled slot
    dispatches again."""

    @p.register(name="hourly_pipe2", schedule="0 * * * *")
    def _h():
        return {}

    run_log = _run_log_acquires()
    executor = MagicMock()
    p._LAST_RUN["hourly_pipe2"] = (datetime(2026, 5, 20, 14, 0, 12, tzinfo=UTC), True)

    # Next hour's slot: last run (14:00:12) predates the 15:00 fire.
    t = datetime(2026, 5, 20, 15, 0, 8, tzinfo=UTC)
    _walk(run_log, executor, t)
    executor.dispatch.assert_called_once()


def test_dispatch_non_dispatcherror_releases_claim_and_does_not_raise():
    """A raw OSError (not DispatchError) from executor.dispatch must be
    contained: the claim is released and the exception is swallowed so
    the daemon survives."""
    run_log = MagicMock()
    executor = MagicMock()
    executor.dispatch.side_effect = OSError("too many open files")
    from ematix_flow.executors import DispatchSpec

    spec = DispatchSpec(
        pipeline_name="p",
        module="m",
        claim_token="tok",
        lease_seconds=300,
        run_log_url="sqlite:///x.db",
        alerter_urls=[],
        metrics_url=None,
        env={},
    )
    # Must not raise.
    _dispatch_one(
        executor=executor,
        spec=spec,
        run_log=run_log,
        claim_token="tok",
        alerters=[],
        metrics=None,
    )
    run_log.release.assert_called_once_with("tok")


def test_walk_survives_non_dispatcherror_from_executor():
    """End-to-end: a non-DispatchError during dispatch does not
    propagate out of a scheduling tick."""

    @p.register(name="boom_pipe", schedule="* * * * *")
    def _b():
        return {}

    run_log = _run_log_acquires()
    executor = MagicMock()
    executor.dispatch.side_effect = RuntimeError("k8s api blip")
    now = datetime(2026, 5, 20, 14, 0, 30, tzinfo=UTC)
    # Should not raise.
    _walk(run_log, executor, now)
    run_log.release.assert_called()  # claim released on failure
