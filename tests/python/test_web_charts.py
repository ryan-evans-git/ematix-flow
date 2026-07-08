"""Phase 2: charts CRUD + store."""
from __future__ import annotations

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient

from ematix_flow.web.analytics_store import AnalyticsStore
from ematix_flow.web.server import create_app


@pytest.fixture
def client() -> TestClient:
    return TestClient(
        create_app(
            datasources={"db": "sqlite:///:memory:"},
            analytics_store=AnalyticsStore(":memory:"),
        )
    )


def _create(client, **over):
    body = {
        "name": "revenue by region",
        "datasource_id": "db",
        "sql": "SELECT region, SUM(amount) AS total FROM sales GROUP BY region",
        "viz_type": "bar",
        "encoding": {"x": "region", "y": ["total"]},
    }
    body.update(over)
    return client.post("/api/charts", json=body)


class TestChartsCrud:
    def test_create_returns_item(self, client: TestClient):
        r = _create(client)
        assert r.status_code == 200, r.text
        c = r.json()
        assert c["id"]
        assert c["name"] == "revenue by region"
        assert c["viz_type"] == "bar"
        # encoding round-trips as structured JSON, not a string.
        assert c["encoding"] == {"x": "region", "y": ["total"]}
        assert c["created_at"] and c["updated_at"]

    def test_list(self, client: TestClient):
        _create(client, name="a")
        _create(client, name="b", viz_type="pie", encoding={"name": "region", "value": "total"})
        charts = client.get("/api/charts").json()["charts"]
        names = {c["name"] for c in charts}
        assert {"a", "b"} <= names

    def test_get_by_id(self, client: TestClient):
        cid = _create(client).json()["id"]
        r = client.get(f"/api/charts/{cid}")
        assert r.status_code == 200
        assert r.json()["id"] == cid

    def test_get_unknown_404s(self, client: TestClient):
        assert client.get("/api/charts/nope").status_code == 404

    def test_update(self, client: TestClient):
        cid = _create(client).json()["id"]
        r = client.put(
            f"/api/charts/{cid}",
            json={"name": "renamed", "viz_type": "line", "encoding": {"x": "region", "y": ["total"]}},
        )
        assert r.status_code == 200, r.text
        c = r.json()
        assert c["name"] == "renamed"
        assert c["viz_type"] == "line"
        # datasource_id + sql untouched.
        assert c["datasource_id"] == "db"

    def test_update_unknown_404s(self, client: TestClient):
        assert client.put("/api/charts/nope", json={"name": "x"}).status_code == 404

    def test_delete(self, client: TestClient):
        cid = _create(client).json()["id"]
        assert client.delete(f"/api/charts/{cid}").status_code == 200
        assert client.get(f"/api/charts/{cid}").status_code == 404

    def test_create_requires_fields(self, client: TestClient):
        assert _create(client, name="").status_code == 400
        assert _create(client, sql="  ").status_code == 400
        assert _create(client, viz_type="").status_code == 400
