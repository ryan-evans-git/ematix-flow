"""Phase 4: result cache, async query jobs, alerts, ownership."""
from __future__ import annotations

import sqlite3
import time

import pytest

from ematix_flow.web import analytics
from ematix_flow.web.analytics import clear_result_cache, run_query

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from ematix_flow.web.analytics_store import AnalyticsStore  # noqa: E402
from ematix_flow.web.server import create_app  # noqa: E402


@pytest.fixture
def sqlite_db(tmp_path):
    path = tmp_path / "p4.db"
    conn = sqlite3.connect(str(path))
    conn.executescript(
        "CREATE TABLE sales (region TEXT, amount REAL);"
        "INSERT INTO sales VALUES ('west', 100), ('east', 20), ('west', 5);"
    )
    conn.commit()
    conn.close()
    return path


@pytest.fixture
def client(sqlite_db):
    return TestClient(
        create_app(
            datasources={"db": f"sqlite:///{sqlite_db}"},
            analytics_store=AnalyticsStore(":memory:"),
        )
    )


class TestResultCache:
    def test_second_call_is_cached(self, sqlite_db, monkeypatch):
        monkeypatch.setenv("EMATIX_FLOW_CACHE_TTL_S", "60")
        clear_result_cache()
        url = f"sqlite:///{sqlite_db}"
        first = run_query(url, "SELECT region, amount FROM sales", use_cache=True)
        assert first["stats"]["cached"] is False
        second = run_query(url, "SELECT region, amount FROM sales", use_cache=True)
        assert second["stats"]["cached"] is True
        assert second["rows"] == first["rows"]

    def test_cache_disabled_by_default(self, sqlite_db):
        clear_result_cache()
        url = f"sqlite:///{sqlite_db}"
        # No TTL env -> never cached even with use_cache=True.
        r = run_query(url, "SELECT 1", use_cache=True)
        assert r["stats"]["cached"] is False

    def test_clear(self, sqlite_db, monkeypatch):
        monkeypatch.setenv("EMATIX_FLOW_CACHE_TTL_S", "60")
        clear_result_cache()
        url = f"sqlite:///{sqlite_db}"
        run_query(url, "SELECT 1", use_cache=True)
        assert run_query(url, "SELECT 1", use_cache=True)["stats"]["cached"] is True
        clear_result_cache()
        assert run_query(url, "SELECT 1", use_cache=True)["stats"]["cached"] is False


class TestAsyncJobs:
    def _await_job(self, client, job_id, timeout=5.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            job = client.get(f"/api/query/jobs/{job_id}").json()
            if job["status"] != "pending":
                return job
            time.sleep(0.05)
        raise AssertionError("job did not finish")

    def test_submit_and_poll(self, client):
        r = client.post(
            "/api/query/async",
            json={"datasource_id": "db", "sql": "SELECT region, SUM(amount) AS t FROM sales GROUP BY region"},
        )
        assert r.status_code == 200
        job_id = r.json()["job_id"]
        job = self._await_job(client, job_id)
        assert job["status"] == "done"
        assert [c["name"] for c in job["result"]["columns"]] == ["region", "t"]

    def test_unknown_job_404s(self, client):
        assert client.get("/api/query/jobs/nope").status_code == 404

    def test_bad_datasource_fails_fast(self, client):
        assert client.post("/api/query/async", json={"datasource_id": "x", "sql": "SELECT 1"}).status_code == 404

    def test_bad_sql_fails_fast(self, client):
        assert client.post("/api/query/async", json={"datasource_id": "db", "sql": "DROP TABLE sales"}).status_code == 400


class TestAlerts:
    def _chart(self, client):
        return client.post(
            "/api/charts",
            json={
                "name": "totals", "datasource_id": "db",
                "sql": "SELECT region, SUM(amount) AS total FROM sales GROUP BY region",
                "viz_type": "bar", "encoding": {"x": "region", "y": ["total"]},
            },
        ).json()["id"]

    def test_create_check_triggered(self, client):
        cid = self._chart(client)
        alert = client.post(
            "/api/alerts",
            json={"name": "big west", "chart_id": cid, "column": "total", "op": ">", "threshold": 100},
        ).json()
        out = client.post(f"/api/alerts/{alert['id']}/check").json()
        # west total = 105 > 100 -> triggered.
        assert out["triggered"] is True
        assert 105.0 in out["matched_values"]

    def test_check_not_triggered(self, client):
        cid = self._chart(client)
        alert = client.post(
            "/api/alerts",
            json={"name": "huge", "chart_id": cid, "column": "total", "op": ">", "threshold": 10000},
        ).json()
        assert client.post(f"/api/alerts/{alert['id']}/check").json()["triggered"] is False

    def test_create_requires_known_chart(self, client):
        r = client.post("/api/alerts", json={"name": "x", "chart_id": "ghost", "column": "total", "op": ">", "threshold": 1})
        assert r.status_code == 404

    def test_bad_column_400s_on_check(self, client):
        cid = self._chart(client)
        alert = client.post(
            "/api/alerts",
            json={"name": "x", "chart_id": cid, "column": "nope", "op": ">", "threshold": 1},
        ).json()
        assert client.post(f"/api/alerts/{alert['id']}/check").status_code == 400

    def test_crud(self, client):
        cid = self._chart(client)
        aid = client.post("/api/alerts", json={"name": "a", "chart_id": cid, "column": "total", "op": ">", "threshold": 1}).json()["id"]
        assert aid in {a["id"] for a in client.get("/api/alerts").json()["alerts"]}
        assert client.delete(f"/api/alerts/{aid}").status_code == 200
        assert client.get(f"/api/alerts/{aid}").status_code == 404


class TestOwnership:
    def test_owner_set_from_bearer(self, sqlite_db):
        app = create_app(
            datasources={"db": f"sqlite:///{sqlite_db}"},
            analytics_store=AnalyticsStore(":memory:"),
            bearer_token="secret",
        )
        c = TestClient(app)
        r = c.post(
            "/api/charts",
            headers={"Authorization": "Bearer secret"},
            json={"name": "n", "datasource_id": "db", "sql": "SELECT 1", "viz_type": "table", "encoding": {}},
        )
        assert r.status_code == 200
        assert r.json()["owner"] == "operator"

    def test_owner_none_without_auth(self, client):
        r = client.post(
            "/api/charts",
            json={"name": "n", "datasource_id": "db", "sql": "SELECT 1", "viz_type": "table", "encoding": {}},
        )
        assert r.json()["owner"] is None
