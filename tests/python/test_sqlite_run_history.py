"""SQLite rich-history extension — record + list + get + REPLACE semantics."""
from __future__ import annotations

import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from ematix_flow.run_log.history import RunRecord
from ematix_flow.run_log.sqlite import SqliteRunLog


@pytest.fixture
def store():
    with tempfile.TemporaryDirectory() as d:
        rl = SqliteRunLog(str(Path(d) / "run.db"))
        try:
            yield rl
        finally:
            rl.close()


def _rec(run_id: str, *, started: datetime, **overrides) -> RunRecord:
    base = dict(
        run_id=run_id,
        pipeline="orders",
        status="succeeded",
        started_at=started,
        finished_at=started + timedelta(seconds=30),
        attempt=1,
        kind="batch",
        extras={"foo": 1},
    )
    base.update(overrides)
    return RunRecord(**base)


def test_record_then_get_round_trip(store) -> None:
    ts = datetime(2026, 5, 21, 12, 0, 0, tzinfo=UTC)
    rec = _rec("r1", started=ts, extras={"snapshot_at": "x", "rows": 42})
    store.record_run_record(rec)
    got = store.get_run("r1")
    assert got is not None
    assert got.run_id == "r1"
    assert got.extras["rows"] == 42
    assert got.started_at == ts
    assert got.finished_at == ts + timedelta(seconds=30)


def test_replace_on_same_run_id(store) -> None:
    ts = datetime(2026, 5, 21, 12, 0, 0, tzinfo=UTC)
    store.record_run_record(_rec("r1", started=ts, status="running",
                                  finished_at=None))
    store.record_run_record(_rec("r1", started=ts, status="succeeded"))
    records, total = store.list_runs()
    assert total == 1
    assert records[0].status == "succeeded"


def test_list_runs_orders_started_at_desc(store) -> None:
    base = datetime(2026, 5, 21, 12, 0, 0, tzinfo=UTC)
    for i, mins in enumerate([10, 0, 5]):
        store.record_run_record(_rec(f"r{i}", started=base + timedelta(minutes=mins)))
    records, total = store.list_runs()
    assert total == 3
    # Newest (mins=10 → r0) first.
    assert [r.run_id for r in records] == ["r0", "r2", "r1"]


def test_list_runs_filters_by_pipeline_and_status(store) -> None:
    base = datetime(2026, 5, 21, 12, 0, 0, tzinfo=UTC)
    store.record_run_record(_rec("r1", started=base))
    store.record_run_record(
        _rec("r2", started=base, pipeline="payments", status="failed"),
    )
    by_pipe, _ = store.list_runs(pipeline="payments")
    assert [r.run_id for r in by_pipe] == ["r2"]
    by_status, _ = store.list_runs(status="failed")
    assert [r.run_id for r in by_status] == ["r2"]


def test_pagination(store) -> None:
    base = datetime(2026, 5, 21, 12, 0, 0, tzinfo=UTC)
    for i in range(5):
        store.record_run_record(_rec(f"r{i}", started=base + timedelta(seconds=i)))
    page1, total = store.list_runs(limit=2, offset=0)
    page2, _ = store.list_runs(limit=2, offset=2)
    assert total == 5
    assert len(page1) == 2 and len(page2) == 2
    assert {r.run_id for r in page1}.isdisjoint({r.run_id for r in page2})


def test_streaming_extras_round_trip(store) -> None:
    ts = datetime(2026, 5, 21, 12, 0, 0, tzinfo=UTC)
    extras = {
        "snapshot_at": "2026-05-21T12:00:30Z",
        "rows_consumed_total": 12345,
        "stats_1m": {"rows_consumed_per_sec": 200.5, "avg_batch_cycle_ms": 250},
        "stats_5m": None,
    }
    store.record_run_record(_rec(
        "stream-r1", started=ts, kind="streaming", status="running",
        finished_at=None, extras=extras,
    ))
    got = store.get_run("stream-r1")
    assert got is not None
    assert got.extras == extras
    assert got.kind == "streaming"


def test_get_run_returns_none_when_missing(store) -> None:
    assert store.get_run("nope") is None


def test_record_run_record_doesnt_break_lightweight_path(store) -> None:
    ts = datetime(2026, 5, 21, 12, 0, 0, tzinfo=UTC)
    # Use both protocols against the same store.
    store.record_run(name="orders", ts=ts, success=True)
    store.record_run_record(_rec("r1", started=ts))
    records, _ = store.list_runs()
    assert len(records) == 1
