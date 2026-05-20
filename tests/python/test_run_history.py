"""Tests for the RunHistoryStore protocol + InMemoryRunHistory impl."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from ematix_flow.run_log.history import (
    InMemoryRunHistory,
    RunHistoryStore,
    RunRecord,
)


UTC = timezone.utc


def _ts(year=2026, month=5, day=20, hour=14, minute=0, second=0) -> datetime:
    return datetime(year, month, day, hour, minute, second, tzinfo=UTC)


class TestRunRecord:
    def test_minimal_construction(self):
        r = RunRecord(
            run_id="01HQ",
            pipeline="warehouse_etl",
            status="succeeded",
            started_at=_ts(),
            finished_at=_ts(hour=14, minute=1),
        )
        assert r.status == "succeeded"
        assert r.attempt == 1
        assert r.kind == "batch"
        assert r.duration_ms == 60_000

    def test_running_status_must_have_no_finished_at(self):
        with pytest.raises(ValueError, match="finished_at=None"):
            RunRecord(
                run_id="x",
                pipeline="p",
                status="running",
                started_at=_ts(),
                finished_at=_ts(),
            )

    def test_paused_status_must_have_no_finished_at(self):
        with pytest.raises(ValueError, match="finished_at=None"):
            RunRecord(
                run_id="x",
                pipeline="p",
                status="paused",
                started_at=_ts(),
                finished_at=_ts(),
            )

    def test_invalid_status_rejected(self):
        with pytest.raises(ValueError, match="status"):
            RunRecord(
                run_id="x", pipeline="p", status="bogus", started_at=_ts()
            )

    def test_invalid_kind_rejected(self):
        with pytest.raises(ValueError, match="kind"):
            RunRecord(
                run_id="x",
                pipeline="p",
                status="succeeded",
                started_at=_ts(),
                finished_at=_ts(hour=14, minute=1),
                kind="bogus",
            )

    def test_attempt_must_be_positive(self):
        with pytest.raises(ValueError, match="attempt"):
            RunRecord(
                run_id="x",
                pipeline="p",
                status="succeeded",
                started_at=_ts(),
                finished_at=_ts(hour=14, minute=1),
                attempt=0,
            )

    def test_duration_ms_none_for_running(self):
        r = RunRecord(run_id="x", pipeline="p", status="running", started_at=_ts())
        assert r.duration_ms is None

    def test_to_summary_dict_shape(self):
        r = RunRecord(
            run_id="01HQ",
            pipeline="warehouse_etl",
            status="failed",
            started_at=_ts(),
            finished_at=_ts(hour=14, minute=2),
            attempt=2,
            failed_step="merge_payments",
        )
        d = r.to_summary_dict()
        assert d["run_id"] == "01HQ"
        assert d["status"] == "failed"
        assert d["attempt"] == 2
        assert d["failed_step"] == "merge_payments"
        assert d["duration_ms"] == 120_000
        assert d["kind"] == "batch"
        assert d["started_at"].endswith("Z")

    def test_to_detail_dict_includes_error_summary_and_extras(self):
        r = RunRecord(
            run_id="x",
            pipeline="p",
            status="failed",
            started_at=_ts(),
            finished_at=_ts(hour=14, minute=1),
            error_summary="ValueError: bad input",
            extras={"k8s_job_id": "abc-123"},
        )
        d = r.to_detail_dict()
        assert d["error_summary"] == "ValueError: bad input"
        assert d["extras"] == {"k8s_job_id": "abc-123"}


class TestInMemoryRunHistory:
    def test_satisfies_protocol(self):
        store = InMemoryRunHistory()
        assert isinstance(store, RunHistoryStore)

    def test_record_and_get(self):
        store = InMemoryRunHistory()
        rec = RunRecord(
            run_id="01HQ",
            pipeline="p",
            status="succeeded",
            started_at=_ts(),
            finished_at=_ts(hour=14, minute=1),
        )
        store.record_run_record(rec)
        assert store.get_run("01HQ") == rec

    def test_get_unknown_returns_none(self):
        store = InMemoryRunHistory()
        assert store.get_run("missing") is None

    def test_record_is_idempotent(self):
        store = InMemoryRunHistory()
        rec1 = RunRecord(
            run_id="x",
            pipeline="p",
            status="running",
            started_at=_ts(),
        )
        store.record_run_record(rec1)
        rec2 = RunRecord(
            run_id="x",
            pipeline="p",
            status="succeeded",
            started_at=_ts(),
            finished_at=_ts(hour=14, minute=1),
        )
        store.record_run_record(rec2)
        # Same run_id replaces in place.
        assert len(store) == 1
        assert store.get_run("x").status == "succeeded"  # type: ignore[union-attr]

    def test_list_runs_sorts_descending_by_started_at(self):
        store = InMemoryRunHistory()
        a = RunRecord(
            run_id="a",
            pipeline="p",
            status="succeeded",
            started_at=_ts(hour=10),
            finished_at=_ts(hour=11),
        )
        b = RunRecord(
            run_id="b",
            pipeline="p",
            status="succeeded",
            started_at=_ts(hour=15),
            finished_at=_ts(hour=16),
        )
        store.record_run_record(a)
        store.record_run_record(b)
        rows, total = store.list_runs()
        assert total == 2
        assert [r.run_id for r in rows] == ["b", "a"]

    def test_list_runs_pipeline_filter(self):
        store = InMemoryRunHistory()
        store.record_run_record(
            RunRecord(
                run_id="a",
                pipeline="alpha",
                status="succeeded",
                started_at=_ts(),
                finished_at=_ts(hour=14, minute=1),
            )
        )
        store.record_run_record(
            RunRecord(
                run_id="b",
                pipeline="beta",
                status="succeeded",
                started_at=_ts(),
                finished_at=_ts(hour=14, minute=1),
            )
        )
        rows, total = store.list_runs(pipeline="alpha")
        assert total == 1
        assert rows[0].run_id == "a"

    def test_list_runs_status_filter(self):
        store = InMemoryRunHistory()
        store.record_run_record(
            RunRecord(
                run_id="a",
                pipeline="p",
                status="failed",
                started_at=_ts(),
                finished_at=_ts(hour=14, minute=1),
            )
        )
        store.record_run_record(
            RunRecord(
                run_id="b",
                pipeline="p",
                status="succeeded",
                started_at=_ts(),
                finished_at=_ts(hour=14, minute=1),
            )
        )
        rows, total = store.list_runs(status="failed")
        assert total == 1
        assert rows[0].status == "failed"

    def test_list_runs_pagination(self):
        store = InMemoryRunHistory()
        for i in range(5):
            store.record_run_record(
                RunRecord(
                    run_id=f"r{i}",
                    pipeline="p",
                    status="succeeded",
                    started_at=_ts() + timedelta(minutes=i),
                    finished_at=_ts() + timedelta(minutes=i + 1),
                )
            )
        rows, total = store.list_runs(limit=2, offset=1)
        assert total == 5
        assert len(rows) == 2
        # Sorted desc by started_at: r4, r3, r2, r1, r0. offset=1 → r3, r2.
        assert [r.run_id for r in rows] == ["r3", "r2"]

    def test_clear_drops_all(self):
        store = InMemoryRunHistory()
        store.record_run_record(
            RunRecord(
                run_id="x",
                pipeline="p",
                status="running",
                started_at=_ts(),
            )
        )
        assert len(store) == 1
        store.clear()
        assert len(store) == 0


class TestWebServerWithHistory:
    """End-to-end: hand a populated InMemoryRunHistory to create_app
    and verify the GET endpoints return real records."""

    def _make_client(self):
        from fastapi.testclient import TestClient
        from ematix_flow.web.server import create_app

        store = InMemoryRunHistory()
        store.record_run_record(
            RunRecord(
                run_id="01HQ-real-batch",
                pipeline="real_etl",
                status="failed",
                started_at=_ts(hour=14),
                finished_at=_ts(hour=14, minute=2),
                attempt=2,
                failed_step="merge_payments",
                error_summary="ValueError",
                kind="batch",
            )
        )
        store.record_run_record(
            RunRecord(
                run_id="01HQ-real-streaming",
                pipeline="real_events",
                status="running",
                started_at=_ts(hour=15),
                kind="streaming",
            )
        )
        return TestClient(create_app(history=store)), store

    def test_list_runs_returns_records_from_store(self):
        pytest.importorskip("fastapi")
        client, store = self._make_client()
        body = client.get("/api/runs").json()
        ids = [r["run_id"] for r in body["runs"]]
        assert "01HQ-real-batch" in ids
        assert "01HQ-real-streaming" in ids
        assert body["total"] == len(store)

    def test_get_run_returns_detail_from_store(self):
        pytest.importorskip("fastapi")
        client, _ = self._make_client()
        body = client.get("/api/runs/01HQ-real-batch").json()
        assert body["run_id"] == "01HQ-real-batch"
        assert body["status"] == "failed"
        assert body["failed_step"] == "merge_payments"
        assert body["error_summary"] == "ValueError"
        # actions: failed batch → restart_from_step + rerun_full
        actions = body["actions"]
        assert actions["restart_from_step"] == ["merge_payments"]
        assert actions["rerun_full"] is True
        assert actions["pause"] is False

    def test_streaming_failed_run_offers_resume_from_watermark(self):
        pytest.importorskip("fastapi")
        from fastapi.testclient import TestClient
        from ematix_flow.web.server import create_app

        store = InMemoryRunHistory()
        store.record_run_record(
            RunRecord(
                run_id="stream-failed",
                pipeline="events",
                status="failed",
                started_at=_ts(),
                finished_at=_ts(hour=14, minute=1),
                error_summary="kafka closed",
                kind="streaming",
                failed_watermark="2026-05-20T13:59:00Z",
            )
        )
        client = TestClient(create_app(history=store))
        body = client.get("/api/runs/stream-failed").json()
        actions = body["actions"]
        assert actions["resume_from_watermark"] is True
        assert actions["restart_from_step"] == []
        assert actions["rerun_full"] is True

    def test_pipelines_aggregated_from_history(self):
        pytest.importorskip("fastapi")
        client, _ = self._make_client()
        body = client.get("/api/pipelines").json()
        names = {p["name"] for p in body["pipelines"]}
        assert names == {"real_etl", "real_events"}
        etl = next(p for p in body["pipelines"] if p["name"] == "real_etl")
        assert etl["kind"] == "batch"
        assert etl["latest_run"]["run_id"] == "01HQ-real-batch"
