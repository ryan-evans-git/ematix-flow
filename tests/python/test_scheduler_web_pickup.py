"""Phase 4b-3 scheduler-loop pickup of Web-UI-enqueued actions.

Verifies `_pickup_pending_actions` reads from a RunHistoryStore,
claims the pipeline via RunLog, and dispatches via the Executor
with the right env vars carrying restart_from_step / rerun_full /
prior_run_id."""
from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from ematix_flow.run_log.history import InMemoryRunHistory, RunRecord
from ematix_flow.run_log.protocol import ClaimResult
from ematix_flow.scheduler.loop import _pickup_pending_actions

UTC = UTC


def _ts(year=2026, month=5, day=20, hour=14) -> datetime:
    return datetime(year, month, day, hour, 0, 0, tzinfo=UTC)


def _make_history_with_restart() -> tuple[InMemoryRunHistory, str]:
    store = InMemoryRunHistory()
    prior = RunRecord(
        run_id="prior-failed",
        pipeline="warehouse_etl",
        status="failed",
        started_at=_ts(),
        finished_at=_ts(hour=15),
        failed_step="merge_payments",
    )
    store.record_run_record(prior)
    new_id = store.enqueue_restart("prior-failed", "merge_payments")
    return store, new_id


def _make_history_with_rerun() -> tuple[InMemoryRunHistory, str]:
    store = InMemoryRunHistory()
    prior = RunRecord(
        run_id="prior-failed",
        pipeline="warehouse_etl",
        status="failed",
        started_at=_ts(),
        finished_at=_ts(hour=15),
    )
    store.record_run_record(prior)
    new_id = store.enqueue_rerun("prior-failed")
    return store, new_id


def _mock_run_log_acquires_claim():
    run_log = MagicMock()
    run_log.claim.return_value = ClaimResult.acquired_by(
        token="token-abc", worker_id="sched-1", expires_at=_ts(hour=16)
    )
    return run_log


def _mock_run_log_busy(holder: str = "other-worker"):
    run_log = MagicMock()
    run_log.claim.return_value = ClaimResult.busy(
        holder=holder, expires_at=_ts(hour=16)
    )
    return run_log


class TestPickup:
    def test_restart_dispatch_carries_step_in_env(self):
        store, new_id = _make_history_with_restart()
        run_log = _mock_run_log_acquires_claim()
        executor = MagicMock()
        _pickup_pending_actions(
            history=store,
            module="my_pipelines",
            run_log=run_log,
            run_log_url="sqlite:///x.db",
            executor=executor,
            alerter_urls=[],
            metrics_url=None,
            alerters=[],
            metrics=None,
            worker_id="sched-1",
            lease_seconds=300,
        )
        # Consumed → status flipped.
        assert store.get_run(new_id).status == "running"  # type: ignore[union-attr]
        # Pipeline was claimed.
        run_log.claim.assert_called_once_with(
            "warehouse_etl", "sched-1", lease_seconds=300
        )
        # Executor dispatched once with the right env.
        executor.dispatch.assert_called_once()
        spec = executor.dispatch.call_args.args[0]
        assert spec.pipeline_name == "warehouse_etl"
        assert spec.env["EMATIX_FLOW_RESTART_FROM_STEP"] == "merge_payments"
        assert spec.env["EMATIX_FLOW_PRIOR_RUN_ID"] == "prior-failed"
        assert "EMATIX_FLOW_RERUN_FULL" not in spec.env

    def test_rerun_dispatch_carries_rerun_full_flag(self):
        store, new_id = _make_history_with_rerun()
        run_log = _mock_run_log_acquires_claim()
        executor = MagicMock()
        _pickup_pending_actions(
            history=store,
            module="my_pipelines",
            run_log=run_log,
            run_log_url="sqlite:///x.db",
            executor=executor,
            alerter_urls=[],
            metrics_url=None,
            alerters=[],
            metrics=None,
            worker_id="sched-1",
            lease_seconds=300,
        )
        executor.dispatch.assert_called_once()
        spec = executor.dispatch.call_args.args[0]
        assert spec.env["EMATIX_FLOW_RERUN_FULL"] == "1"
        assert "EMATIX_FLOW_RESTART_FROM_STEP" not in spec.env

    def test_busy_claim_defers_dispatch(self):
        store, new_id = _make_history_with_restart()
        run_log = _mock_run_log_busy()
        executor = MagicMock()
        _pickup_pending_actions(
            history=store,
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
        )
        # No dispatch because the pipeline is busy.
        executor.dispatch.assert_not_called()
        # But the row was already consumed (transitioned from
        # requested → running). This matches the existing pattern
        # of "consume now, retry on next tick if dispatch fails";
        # the next tick will see status="running" + still pending
        # in the cron-driven _walk_and_dispatch which will skip it
        # too. A future tick after the busy lease expires will
        # eventually pick it up. (Documented limitation.)
        assert store.get_run(new_id).status == "running"  # type: ignore[union-attr]

    def test_already_consumed_skips(self):
        # If another scheduler instance flipped the row to "running"
        # between pending_actions() and consume_requested_run(), the
        # consume returns False; the dispatch is skipped.
        store, new_id = _make_history_with_restart()
        # Pre-flip via direct consume.
        store.consume_requested_run(new_id)
        run_log = _mock_run_log_acquires_claim()
        executor = MagicMock()
        _pickup_pending_actions(
            history=store,
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
        )
        # Because the row was already running, pending_actions() no
        # longer returned it. claim + dispatch should not be invoked.
        run_log.claim.assert_not_called()
        executor.dispatch.assert_not_called()

    def test_pause_resume_rows_in_pending_not_dispatched(self):
        # Worker-side handles pause/resume (Phase 4b-4). The
        # scheduler's pickup loop should leave those rows alone.
        store = InMemoryRunHistory()
        store.record_run_record(
            RunRecord(
                run_id="r1",
                pipeline="p",
                status="running",
                started_at=_ts(),
                extras={"pause_requested": True},
            )
        )
        run_log = MagicMock()
        executor = MagicMock()
        _pickup_pending_actions(
            history=store,
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
        )
        executor.dispatch.assert_not_called()
        run_log.claim.assert_not_called()
        # Row left untouched — its status flips later (worker-side).
        assert store.get_run("r1").status == "running"  # type: ignore[union-attr]
        assert store.get_run("r1").extras["pause_requested"] is True  # type: ignore[union-attr]

    def test_pending_actions_failure_doesnt_crash_loop(self):
        # If the history backend raises (e.g., backend offline mid-
        # tick), the scheduler should log + continue rather than
        # propagating the exception up into run_scheduler's main loop.
        broken_store = MagicMock()
        broken_store.pending_actions.side_effect = RuntimeError("oops")
        run_log = MagicMock()
        executor = MagicMock()
        _pickup_pending_actions(
            history=broken_store,
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
        )
        executor.dispatch.assert_not_called()
