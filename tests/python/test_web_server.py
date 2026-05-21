"""Phase 4a-1 smoke tests for the FastAPI server skeleton.

This slice ships with stub data; tests confirm the JSON shape the
SPA will consume. Real RunLog wiring lands in Phase 4a-2.
"""
from __future__ import annotations

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")  # transitive via fastapi.testclient

from fastapi.testclient import TestClient

from ematix_flow.web.server import create_app


@pytest.fixture
def client() -> TestClient:
    return TestClient(create_app())


class TestHealth:
    def test_health_returns_ok(self, client: TestClient):
        r = client.get("/api/health")
        assert r.status_code == 200
        assert r.json() == {"status": "ok"}


class TestListRuns:
    def test_returns_runs_total_and_offset(self, client: TestClient):
        r = client.get("/api/runs")
        assert r.status_code == 200
        body = r.json()
        assert "runs" in body
        assert "total" in body
        assert "next_offset" in body
        assert isinstance(body["runs"], list)

    def test_each_run_has_required_fields(self, client: TestClient):
        body = client.get("/api/runs").json()
        for run in body["runs"]:
            for field in (
                "run_id",
                "pipeline",
                "status",
                "started_at",
                "attempt",
            ):
                assert field in run, f"missing field {field!r} in {run!r}"

    def test_pipeline_filter(self, client: TestClient):
        body = client.get("/api/runs", params={"pipeline": "warehouse_etl"}).json()
        assert all(r["pipeline"] == "warehouse_etl" for r in body["runs"])

    def test_status_filter(self, client: TestClient):
        body = client.get("/api/runs", params={"status": "failed"}).json()
        assert all(r["status"] == "failed" for r in body["runs"])
        # At least one stub failed run.
        assert len(body["runs"]) >= 1


class TestGetRun:
    def test_known_id_returns_detail(self, client: TestClient):
        # Find a known stub id via the list endpoint.
        ids = [r["run_id"] for r in client.get("/api/runs").json()["runs"]]
        assert ids, "no stub runs"
        r = client.get(f"/api/runs/{ids[0]}")
        assert r.status_code == 200
        detail = r.json()
        for field in ("run_id", "attempts", "steps", "actions"):
            assert field in detail

    def test_unknown_id_404s(self, client: TestClient):
        r = client.get("/api/runs/nonexistent-id")
        assert r.status_code == 404

    def test_action_gating_running_run_offers_pause(self, client: TestClient):
        # The streaming stub is "running"; its actions should include
        # `pause` but not `rerun_full`.
        running = next(
            r for r in client.get("/api/runs").json()["runs"]
            if r["status"] == "running"
        )
        detail = client.get(f"/api/runs/{running['run_id']}").json()
        actions = detail["actions"]
        assert actions["pause"] is True
        assert actions["rerun_full"] is False

    def test_action_gating_failed_run_offers_restart_and_rerun(
        self, client: TestClient
    ):
        failed = next(
            r for r in client.get("/api/runs").json()["runs"]
            if r["status"] == "failed"
        )
        detail = client.get(f"/api/runs/{failed['run_id']}").json()
        actions = detail["actions"]
        assert actions["rerun_full"] is True
        # Streaming failed runs surface resume_from_watermark; batch
        # failed runs surface restart_from_step. The streaming stub
        # currently doesn't fail, so we assert at least one applies.
        assert bool(actions["restart_from_step"]) or actions["resume_from_watermark"]

    def test_action_gating_succeeded_run_offers_rerun_only(
        self, client: TestClient
    ):
        succeeded = next(
            r for r in client.get("/api/runs").json()["runs"]
            if r["status"] == "succeeded"
        )
        detail = client.get(f"/api/runs/{succeeded['run_id']}").json()
        actions = detail["actions"]
        assert actions["pause"] is False
        assert actions["resume"] is False
        assert actions["restart_from_step"] == []
        assert actions["rerun_full"] is True


class TestPipelines:
    def test_returns_pipelines_list(self, client: TestClient):
        r = client.get("/api/pipelines")
        assert r.status_code == 200
        body = r.json()
        assert "pipelines" in body
        for p in body["pipelines"]:
            for field in ("name", "kind", "latest_run"):
                assert field in p


class TestRootPlaceholder:
    """Without a built SPA, the root path serves a friendly HTML
    placeholder pointing operators at /api/docs and the build
    command. With a built SPA (the wheel ships ui_dist/), the root
    serves index.html instead."""

    def test_root_serves_html(self, client: TestClient):
        # The default fixture wires `create_app()` which picks up the
        # bundled ui_dist/ — so root serves the SPA shell HTML, not the
        # placeholder. Either way, it must be HTML pointing at /api/docs
        # or mounting the SPA at #app.
        r = client.get("/")
        assert r.status_code == 200
        assert "text/html" in r.headers["content-type"]
        body = r.text
        # Accept either the legacy placeholder text or the Vite-emitted
        # SPA shell that mounts at <div id="app">.
        assert ("ematix-flow Web UI" in body) or ('id="app"' in body)

    def test_placeholder_when_ui_dist_missing(self, tmp_path):
        """Explicit placeholder path — when ui_dist_dir points at a
        non-existent directory, the server falls back to the friendly
        placeholder that names /api/docs."""
        missing = tmp_path / "no-such-dist"
        app = create_app(ui_dist_dir=missing)
        client = TestClient(app)
        r = client.get("/")
        assert r.status_code == 200
        body = r.text
        assert "ematix-flow Web UI" in body
        assert "/api/docs" in body
