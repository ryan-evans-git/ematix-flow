"""Tests for the worker-side PauseChecker (Phase 4b-4)."""
from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from ematix_flow.run_log.history import InMemoryRunHistory, RunRecord
from ematix_flow.web.pause import PauseChecker

UTC = UTC


def _ts() -> datetime:
    return datetime(2026, 5, 20, 14, 0, 0, tzinfo=UTC)


def _store_with_running_run(run_id: str = "r1") -> InMemoryRunHistory:
    store = InMemoryRunHistory()
    store.record_run_record(
        RunRecord(
            run_id=run_id,
            pipeline="warehouse_etl",
            status="running",
            started_at=_ts(),
        )
    )
    return store


class TestShouldPause:
    def test_returns_false_when_no_pause_flag(self):
        store = _store_with_running_run()
        checker = PauseChecker(store, "r1")
        assert checker.should_pause() is False

    def test_returns_true_when_pause_requested(self):
        store = _store_with_running_run()
        store.set_pause("r1", True)
        checker = PauseChecker(store, "r1")
        assert checker.should_pause() is True

    def test_returns_false_when_pause_cleared(self):
        store = _store_with_running_run()
        store.set_pause("r1", True)
        store.set_pause("r1", False)
        checker = PauseChecker(store, "r1")
        assert checker.should_pause() is False

    def test_returns_false_when_run_missing(self):
        store = _store_with_running_run()
        checker = PauseChecker(store, "unknown-run-id")
        assert checker.should_pause() is False

    def test_store_failure_defaults_to_false(self):
        broken = MagicMock()
        broken.get_run.side_effect = RuntimeError("offline")
        checker = PauseChecker(broken, "r1")
        # A transient backend error must not crash the worker. The
        # safe default is "don't pause" — the worker will get
        # another chance on the next check.
        assert checker.should_pause() is False

    def test_should_pause_after_acknowledge_stays_true(self):
        # Cached after the first ack — even if the row state
        # changes externally, the worker sees the pause it has
        # already accepted.
        store = _store_with_running_run()
        store.set_pause("r1", True)
        checker = PauseChecker(store, "r1")
        assert checker.should_pause() is True
        checker.acknowledge_pause()
        # External party clears the flag (e.g., resume click).
        store.set_pause("r1", False)
        # The worker still sees its own paused state cached.
        assert checker.should_pause() is True

    def test_reset_drops_cached_paused_state(self):
        store = _store_with_running_run()
        store.set_pause("r1", True)
        checker = PauseChecker(store, "r1")
        checker.acknowledge_pause()
        checker.reset()
        # After reset and external resume, should_pause re-reads.
        store.set_pause("r1", False)
        assert checker.should_pause() is False


class TestAcknowledgePause:
    def test_flips_row_status_to_paused(self):
        store = _store_with_running_run()
        store.set_pause("r1", True)
        checker = PauseChecker(store, "r1")
        checker.acknowledge_pause()
        rec = store.get_run("r1")
        assert rec.status == "paused"  # type: ignore[union-attr]

    def test_idempotent_on_already_paused(self):
        store = InMemoryRunHistory()
        store.record_run_record(
            RunRecord(
                run_id="r1",
                pipeline="p",
                status="paused",
                started_at=_ts(),
            )
        )
        checker = PauseChecker(store, "r1")
        # No exception, no state regression.
        checker.acknowledge_pause()
        assert store.get_run("r1").status == "paused"  # type: ignore[union-attr]

    def test_missing_run_is_noop(self):
        store = InMemoryRunHistory()
        checker = PauseChecker(store, "no-such-run")
        # Doesn't raise.
        checker.acknowledge_pause()

    def test_pause_preserves_other_extras(self):
        store = InMemoryRunHistory()
        store.record_run_record(
            RunRecord(
                run_id="r1",
                pipeline="p",
                status="running",
                started_at=_ts(),
                extras={"k8s_job_id": "abc-123", "scheduler_tick": 42},
            )
        )
        store.set_pause("r1", True)
        PauseChecker(store, "r1").acknowledge_pause()
        rec = store.get_run("r1")
        assert rec.extras["k8s_job_id"] == "abc-123"  # type: ignore[union-attr]
        assert rec.extras["scheduler_tick"] == 42  # type: ignore[union-attr]
        assert "paused_at" in rec.extras  # type: ignore[union-attr]

    def test_pause_sets_finished_at_to_none(self):
        # Pause is not termination — finished_at stays None so the
        # UI can show "paused at step X, last check Y" without
        # claiming the run completed.
        store = _store_with_running_run()
        store.set_pause("r1", True)
        PauseChecker(store, "r1").acknowledge_pause()
        rec = store.get_run("r1")
        assert rec.finished_at is None  # type: ignore[union-attr]
