"""DLQ Phase 4: FastAPI DLQ + rewind endpoints.

Fixture-driven: a ``FakeDlqOps`` stands in for the pyo3-backed ops
layer so the HTTP semantics (paging params, preview truncation,
status codes, action gating, RunHistory ``kind=replay``
registration, bearer-token rules) are pinned without a Rust store.
The live-store path is covered by the CLI crate's ``dlq_ops`` suite
plus ``test_dlq_ops_live.py``'s pyo3 smoke.

TDD note: written FIRST, red, before the endpoints existed.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient

from ematix_flow.run_log.history import InMemoryRunHistory, RunRecord
from ematix_flow.web.server import create_app


def _record(n: int, *, stage: str = "write", payload: bytes | None = None) -> dict[str, Any]:
    return {
        "id": f"rec-{n:04d}",
        "stage": stage,
        "error": f"boom {n}",
        "source_id": "events",
        "offset_bytes": None,
        "event_ts": None,
        "failed_at": 1_700_000_000_000 + n,
        "attempt": 1,
        "payload_format": "json",
        "payload": payload if payload is not None else f'{{"v": {n}}}'.encode(),
    }


class FakeDlqOps:
    """Canned ops layer implementing the server's DLQ protocol."""

    def __init__(self) -> None:
        self.known = {"events_stream"}
        self.records_data = [_record(1), _record(2, payload=b"x" * 10_000)]
        self.calls: list[tuple[str, Any]] = []
        self.replay_report = {
            "taken": 2,
            "succeeded": 1,
            "redeadlettered": 1,
            "parked": 0,
            "started_at_ms": 1_700_000_000_000,
            "finished_at_ms": 1_700_000_001_000,
        }
        self.rewind_result: dict[str, Any] = {
            "sources": [("events", b"{}")],
            "state_cleared": False,
        }
        self.rewind_error: Exception | None = None

    def _check(self, name: str) -> None:
        if name not in self.known:
            raise KeyError(name)

    def stats(self, name: str, now_ms: int) -> dict[str, Any]:
        self._check(name)
        self.calls.append(("stats", (name, now_ms)))
        return {
            "pending": 2,
            "parked": 1,
            "by_stage": {"write": 2, "transform": 1},
            "arrivals": {"last_1m": 1, "last_5m": 2, "last_15m": 2, "last_60m": 3},
            "scanned": 3,
            "truncated": False,
        }

    def records(
        self, name: str, status: str | None, page: int, page_size: int
    ) -> list[dict[str, Any]]:
        self._check(name)
        self.calls.append(("records", (name, status, page, page_size)))
        return self.records_data

    def record_by_id(self, name: str, record_id: str) -> dict[str, Any] | None:
        self._check(name)
        for r in self.records_data:
            if r["id"] == record_id:
                return r
        return None

    def replay(
        self, name: str, selection: dict[str, Any], max_attempts: int | None
    ) -> dict[str, Any]:
        self._check(name)
        self.calls.append(("replay", (name, selection, max_attempts)))
        return dict(self.replay_report)

    def park(self, name: str, selection: dict[str, Any]) -> int:
        self._check(name)
        self.calls.append(("park", (name, selection)))
        return 2

    def purge(self, name: str, selection: dict[str, Any]) -> int:
        self._check(name)
        self.calls.append(("purge", (name, selection)))
        return 3

    def rewind(
        self, name: str, to: dict[str, Any], confirm_state_reset: bool
    ) -> dict[str, Any]:
        self._check(name)
        self.calls.append(("rewind", (name, to, confirm_state_reset)))
        if self.rewind_error is not None:
            raise self.rewind_error
        return dict(self.rewind_result)


@pytest.fixture
def history() -> InMemoryRunHistory:
    return InMemoryRunHistory()


@pytest.fixture
def ops() -> FakeDlqOps:
    return FakeDlqOps()


@pytest.fixture
def client(history: InMemoryRunHistory, ops: FakeDlqOps) -> TestClient:
    return TestClient(create_app(history=history, dlq_ops=ops))


class TestDlqSummary:
    def test_summary_shape(self, client: TestClient):
        r = client.get("/api/streams/events_stream/dlq")
        assert r.status_code == 200
        body = r.json()
        assert body["pipeline"] == "events_stream"
        assert body["depth"] == {"pending": 2, "parked": 1}
        assert body["by_stage"] == {"write": 2, "transform": 1}
        assert body["arrivals"]["last_1m"] == 1
        assert body["truncated"] is False

    def test_unknown_stream_404s(self, client: TestClient):
        r = client.get("/api/streams/nope/dlq")
        assert r.status_code == 404

    def test_now_ms_is_passed_in(self, client: TestClient, ops: FakeDlqOps):
        client.get("/api/streams/events_stream/dlq")
        (call,) = [c for c in ops.calls if c[0] == "stats"]
        _, (_, now_ms) = call
        assert isinstance(now_ms, int) and now_ms > 1_600_000_000_000


class TestDlqRecords:
    def test_paging_params_forwarded(self, client: TestClient, ops: FakeDlqOps):
        r = client.get(
            "/api/streams/events_stream/dlq/records",
            params={"status": "pending", "page": 3, "page_size": 7},
        )
        assert r.status_code == 200
        assert ("records", ("events_stream", "pending", 3, 7)) in ops.calls

    def test_record_shape_preview_and_download_link(self, client: TestClient):
        body = client.get("/api/streams/events_stream/dlq/records").json()
        recs = body["records"]
        assert len(recs) == 2
        small = recs[0]
        assert small["id"] == "rec-0001"
        assert small["stage"] == "write"
        assert small["attempt"] == 1
        assert small["payload_preview"] == '{"v": 1}'
        assert small["payload_size"] == len('{"v": 1}')
        assert small["payload_truncated"] is False
        assert (
            small["download"]
            == "/api/streams/events_stream/dlq/records/rec-0001/payload"
        )
        assert "payload" not in small, "raw payload never rides the list JSON"

    def test_preview_truncates_at_4kb(self, client: TestClient):
        body = client.get("/api/streams/events_stream/dlq/records").json()
        big = body["records"][1]
        assert big["payload_size"] == 10_000
        assert len(big["payload_preview"]) == 4096
        assert big["payload_truncated"] is True

    def test_payload_download_returns_raw_bytes(self, client: TestClient):
        r = client.get("/api/streams/events_stream/dlq/records/rec-0002/payload")
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("application/octet-stream")
        assert r.content == b"x" * 10_000

    def test_payload_download_404s_for_unknown_record(self, client: TestClient):
        r = client.get("/api/streams/events_stream/dlq/records/zzz/payload")
        assert r.status_code == 404


class TestReplay:
    def test_replay_returns_report_and_registers_run(
        self, client: TestClient, history: InMemoryRunHistory, ops: FakeDlqOps
    ):
        r = client.post(
            "/api/streams/events_stream/dlq/replay",
            json={"selection": {"kind": "first_n", "n": 2}},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["report"]["taken"] == 2
        assert body["run_id"]

        # RunHistory registration: kind=replay, visible via /api/runs.
        record = history.get_run(body["run_id"])
        assert record is not None
        assert record.kind == "replay"
        assert record.pipeline == "events_stream"
        assert record.extras["replay_report"]["succeeded"] == 1

        runs = client.get(
            "/api/runs", params={"pipeline": "events_stream"}
        ).json()["runs"]
        assert any(x["run_id"] == body["run_id"] for x in runs)

        assert (
            "replay",
            ("events_stream", {"kind": "first_n", "n": 2}, None),
        ) in ops.calls

    def test_replay_forwards_max_attempts(self, client: TestClient, ops: FakeDlqOps):
        client.post(
            "/api/streams/events_stream/dlq/replay",
            json={"selection": {"kind": "all"}, "max_attempts": 7},
        )
        assert ("replay", ("events_stream", {"kind": "all"}, 7)) in ops.calls

    def test_replay_defaults_selection_to_all(self, client: TestClient, ops: FakeDlqOps):
        client.post("/api/streams/events_stream/dlq/replay", json={})
        assert ("replay", ("events_stream", {"kind": "all"}, None)) in ops.calls

    def test_replay_non_integer_max_attempts_400s(
        self, client: TestClient, ops: FakeDlqOps
    ):
        # Regression: a non-numeric max_attempts used to reach int() bare
        # and 500. It must be a clean 400.
        r = client.post(
            "/api/streams/events_stream/dlq/replay",
            json={"selection": {"kind": "all"}, "max_attempts": "abc"},
        )
        assert r.status_code == 400

    def test_replay_requires_history_store(self, ops: FakeDlqOps):
        client = TestClient(create_app(dlq_ops=ops))
        r = client.post(
            "/api/streams/events_stream/dlq/replay",
            json={"selection": {"kind": "all"}},
        )
        assert r.status_code == 400


class TestParkPurge:
    def test_park(self, client: TestClient, ops: FakeDlqOps):
        r = client.post(
            "/api/streams/events_stream/dlq/park",
            json={"selection": {"kind": "ids", "ids": ["rec-0001"]}},
        )
        assert r.status_code == 200
        assert r.json() == {"parked": 2}
        assert (
            "park",
            ("events_stream", {"kind": "ids", "ids": ["rec-0001"]}),
        ) in ops.calls

    def test_purge(self, client: TestClient, ops: FakeDlqOps):
        r = client.post(
            "/api/streams/events_stream/dlq/purge",
            json={"selection": {"kind": "all"}},
        )
        assert r.status_code == 200
        assert r.json() == {"purged": 3}

    def test_purge_requires_explicit_selection(self, client: TestClient):
        # Purge is destructive — no implicit "all" default.
        r = client.post("/api/streams/events_stream/dlq/purge", json={})
        assert r.status_code == 400


class TestRewind:
    def test_rewind_passes_through(self, client: TestClient, ops: FakeDlqOps):
        r = client.post(
            "/api/streams/events_stream/rewind",
            json={"to": {"kind": "timestamp", "ms": 1_700_000_000_000}},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["state_cleared"] is False
        assert (
            "rewind",
            ("events_stream", {"kind": "timestamp", "ms": 1_700_000_000_000}, False),
        ) in ops.calls

    def test_rewind_requires_to(self, client: TestClient):
        r = client.post("/api/streams/events_stream/rewind", json={})
        assert r.status_code == 400

    def test_rewind_blocked_while_stream_running(
        self, client: TestClient, history: InMemoryRunHistory
    ):
        history.record_run_record(
            RunRecord(
                run_id="live-1",
                pipeline="events_stream",
                status="running",
                started_at=datetime.now(UTC),
                finished_at=None,
                attempt=1,
                kind="streaming",
            )
        )
        r = client.post(
            "/api/streams/events_stream/rewind",
            json={"to": {"kind": "timestamp", "ms": 1}},
        )
        assert r.status_code == 409, r.text
        assert "running" in r.json()["detail"]

    def test_confirm_state_reset_error_maps_to_400(
        self, client: TestClient, ops: FakeDlqOps
    ):
        ops.rewind_error = ValueError(
            "pipeline `events_stream` has a stateful transform — pass "
            "confirm_state_reset = true to proceed."
        )
        r = client.post(
            "/api/streams/events_stream/rewind",
            json={"to": {"kind": "timestamp", "ms": 1}},
        )
        assert r.status_code == 400
        assert "confirm_state_reset" in r.json()["detail"]


class TestBearerTokenUnchanged:
    def test_streams_endpoints_are_token_gated(self, ops: FakeDlqOps):
        client = TestClient(
            create_app(history=InMemoryRunHistory(), dlq_ops=ops, bearer_token="s3cret")
        )
        assert client.get("/api/streams/events_stream/dlq").status_code == 401
        ok = client.get(
            "/api/streams/events_stream/dlq",
            headers={"Authorization": "Bearer s3cret"},
        )
        assert ok.status_code == 200


class TestRunRecordReplayKind:
    def test_replay_kind_is_valid(self):
        RunRecord(
            run_id="r1",
            pipeline="p",
            status="succeeded",
            started_at=datetime.now(UTC),
            finished_at=datetime.now(UTC),
            attempt=1,
            kind="replay",
        )
