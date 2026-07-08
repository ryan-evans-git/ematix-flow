"""Phase 3: dashboards CRUD + batch query."""
from __future__ import annotations

import sqlite3

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient

from ematix_flow.web.analytics_store import AnalyticsStore
from ematix_flow.web.server import create_app


@pytest.fixture
def sqlite_db(tmp_path):
    path = tmp_path / "dash.db"
    conn = sqlite3.connect(str(path))
    conn.executescript(
        """
        CREATE TABLE sales (region TEXT, amount REAL);
        INSERT INTO sales VALUES ('west', 10.5), ('east', 20.0), ('west', 5.25);
        """
    )
    conn.commit()
    conn.close()
    return path


@pytest.fixture
def client(sqlite_db) -> TestClient:
    return TestClient(
        create_app(
            datasources={"db": f"sqlite:///{sqlite_db}"},
            analytics_store=AnalyticsStore(":memory:"),
        )
    )


@pytest.fixture
def chart_id(client) -> str:
    r = client.post(
        "/api/charts",
        json={
            "name": "totals",
            "datasource_id": "db",
            "sql": "SELECT region, SUM(amount) AS total FROM sales GROUP BY region ORDER BY region",
            "viz_type": "bar",
            "encoding": {"x": "region", "y": ["total"]},
        },
    )
    return r.json()["id"]


class TestDashboardsCrud:
    def test_create_and_layout_roundtrips(self, client, chart_id):
        layout = {"tiles": [{"chart_id": chart_id, "x": 0, "y": 0, "w": 6, "h": 6}]}
        r = client.post("/api/dashboards", json={"name": "sales", "layout": layout})
        assert r.status_code == 200, r.text
        d = r.json()
        assert d["id"] and d["name"] == "sales"
        assert d["layout"] == layout  # structured, not a string

    def test_create_requires_name(self, client):
        assert client.post("/api/dashboards", json={"name": " "}).status_code == 400

    def test_list_get_update_delete(self, client, chart_id):
        did = client.post("/api/dashboards", json={"name": "d1"}).json()["id"]
        assert did in {d["id"] for d in client.get("/api/dashboards").json()["dashboards"]}
        assert client.get(f"/api/dashboards/{did}").json()["id"] == did
        new_layout = {"tiles": [{"chart_id": chart_id, "x": 1, "y": 2, "w": 4, "h": 5}]}
        up = client.put(f"/api/dashboards/{did}", json={"name": "d1b", "layout": new_layout})
        assert up.json()["name"] == "d1b"
        assert up.json()["layout"] == new_layout
        assert client.delete(f"/api/dashboards/{did}").status_code == 200
        assert client.get(f"/api/dashboards/{did}").status_code == 404

    def test_get_unknown_404s(self, client):
        assert client.get("/api/dashboards/nope").status_code == 404


class TestDashboardBatchQuery:
    def test_runs_all_tile_charts(self, client, chart_id):
        layout = {"tiles": [{"chart_id": chart_id, "x": 0, "y": 0, "w": 6, "h": 6}]}
        did = client.post("/api/dashboards", json={"name": "d", "layout": layout}).json()["id"]
        r = client.post(f"/api/dashboards/{did}/query")
        assert r.status_code == 200, r.text
        results = r.json()["results"]
        assert chart_id in results
        tile = results[chart_id]
        assert tile["viz_type"] == "bar"
        assert tile["encoding"] == {"x": "region", "y": ["total"]}
        assert [c["name"] for c in tile["columns"]] == ["region", "total"]
        totals = {row[0]: row[1] for row in tile["rows"]}
        assert totals["west"] == pytest.approx(15.75)

    def test_missing_chart_reports_error(self, client):
        layout = {"tiles": [{"chart_id": "ghost", "x": 0, "y": 0, "w": 4, "h": 4}]}
        did = client.post("/api/dashboards", json={"name": "d", "layout": layout}).json()["id"]
        results = client.post(f"/api/dashboards/{did}/query").json()["results"]
        assert "error" in results["ghost"]

    def test_query_unknown_dashboard_404s(self, client):
        assert client.post("/api/dashboards/nope/query").status_code == 404
