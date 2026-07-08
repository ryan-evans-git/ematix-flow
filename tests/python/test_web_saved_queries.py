"""Phase 1: saved-queries CRUD + store."""
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
    body = {"name": "q1", "datasource_id": "db", "sql": "SELECT 1"}
    body.update(over)
    return client.post("/api/saved-queries", json=body)


class TestSavedQueriesCrud:
    def test_create_returns_item_with_id(self, client: TestClient):
        r = _create(client)
        assert r.status_code == 200, r.text
        item = r.json()
        assert item["id"]
        assert item["name"] == "q1"
        assert item["datasource_id"] == "db"
        assert item["sql"] == "SELECT 1"
        assert item["created_at"] and item["updated_at"]

    def test_list_includes_created(self, client: TestClient):
        _create(client, name="alpha")
        _create(client, name="beta")
        names = {q["name"] for q in client.get("/api/saved-queries").json()["saved_queries"]}
        assert {"alpha", "beta"} <= names

    def test_get_by_id(self, client: TestClient):
        qid = _create(client).json()["id"]
        r = client.get(f"/api/saved-queries/{qid}")
        assert r.status_code == 200
        assert r.json()["id"] == qid

    def test_get_unknown_404s(self, client: TestClient):
        assert client.get("/api/saved-queries/nope").status_code == 404

    def test_update(self, client: TestClient):
        qid = _create(client).json()["id"]
        r = client.put(f"/api/saved-queries/{qid}", json={"name": "renamed", "sql": "SELECT 2"})
        assert r.status_code == 200, r.text
        item = r.json()
        assert item["name"] == "renamed"
        assert item["sql"] == "SELECT 2"
        # datasource_id untouched.
        assert item["datasource_id"] == "db"

    def test_update_unknown_404s(self, client: TestClient):
        assert client.put("/api/saved-queries/nope", json={"name": "x"}).status_code == 404

    def test_delete(self, client: TestClient):
        qid = _create(client).json()["id"]
        assert client.delete(f"/api/saved-queries/{qid}").status_code == 200
        assert client.get(f"/api/saved-queries/{qid}").status_code == 404

    def test_delete_unknown_404s(self, client: TestClient):
        assert client.delete("/api/saved-queries/nope").status_code == 404

    def test_create_requires_name_and_sql(self, client: TestClient):
        assert _create(client, name="").status_code == 400
        assert _create(client, sql="   ").status_code == 400
