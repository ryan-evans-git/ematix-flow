"""Phase 0: ad-hoc SQL query endpoint (SQL Lab spine).

Exercises the analytics query surface end to end against a real
in-process backend: a sqlite file is populated with stdlib ``sqlite3``,
registered as a datasource, and queried through the ematix engine via
``Connection.query()`` -> ``/api/query``. No mocking; no parquet fixtures.
"""
from __future__ import annotations

import sqlite3

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")  # transitive via fastapi.testclient

from fastapi.testclient import TestClient

from ematix_flow.web.server import create_app


@pytest.fixture
def sqlite_db(tmp_path):
    path = tmp_path / "analytics.db"
    conn = sqlite3.connect(str(path))
    conn.executescript(
        """
        CREATE TABLE sales (region TEXT, amount REAL, qty INTEGER);
        INSERT INTO sales VALUES
            ('west', 10.5, 3),
            ('east', 20.0, 5),
            ('west',  5.25, 1);
        """
    )
    conn.commit()
    conn.close()
    return path


@pytest.fixture
def client(sqlite_db) -> TestClient:
    app = create_app(datasources={"testdb": f"sqlite:///{sqlite_db}"})
    return TestClient(app)


class TestDatasources:
    def test_lists_configured_datasource(self, client: TestClient):
        r = client.get("/api/datasources")
        assert r.status_code == 200
        body = r.json()
        assert "datasources" in body
        by_id = {d["id"]: d for d in body["datasources"]}
        assert "testdb" in by_id
        # Never leak the raw connection URL (may hold credentials).
        assert "url" not in by_id["testdb"]
        assert "dialect" in by_id["testdb"]


class TestQuery:
    def test_select_returns_columns_rows_stats(self, client: TestClient):
        r = client.post(
            "/api/query",
            json={
                "datasource_id": "testdb",
                "sql": "SELECT region, amount, qty FROM sales ORDER BY amount",
            },
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert [c["name"] for c in body["columns"]] == ["region", "amount", "qty"]
        # Smallest amount (5.25) sorts first -> region 'west'.
        assert body["rows"][0][0] == "west"
        assert body["stats"]["row_count"] == 3
        assert body["stats"]["truncated"] is False
        assert "elapsed_ms" in body["stats"]

    def test_aggregate_query(self, client: TestClient):
        r = client.post(
            "/api/query",
            json={
                "datasource_id": "testdb",
                "sql": (
                    "SELECT region, SUM(amount) AS total "
                    "FROM sales GROUP BY region ORDER BY region"
                ),
            },
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["columns"][1]["name"] == "total"
        totals = {row[0]: row[1] for row in body["rows"]}
        assert totals["west"] == pytest.approx(15.75)
        assert totals["east"] == pytest.approx(20.0)

    def test_row_cap_truncates(self, client: TestClient):
        r = client.post(
            "/api/query",
            json={
                "datasource_id": "testdb",
                "sql": "SELECT * FROM sales",
                "max_rows": 2,
            },
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert len(body["rows"]) == 2
        assert body["stats"]["truncated"] is True

    def test_unknown_datasource_404s(self, client: TestClient):
        r = client.post(
            "/api/query",
            json={"datasource_id": "nope", "sql": "SELECT 1"},
        )
        assert r.status_code == 404

    @pytest.mark.parametrize(
        "sql",
        [
            "DELETE FROM sales",
            "DROP TABLE sales",
            "INSERT INTO sales VALUES ('x', 1.0, 1)",
            "UPDATE sales SET qty = 0",
            "CREATE TABLE t (x INT)",
        ],
    )
    def test_readonly_guard_rejects_writes(self, client: TestClient, sql: str):
        r = client.post(
            "/api/query", json={"datasource_id": "testdb", "sql": sql}
        )
        assert r.status_code == 400, f"expected 400 for {sql!r}, got {r.status_code}"

    def test_multi_statement_rejected(self, client: TestClient):
        r = client.post(
            "/api/query",
            json={"datasource_id": "testdb", "sql": "SELECT 1; SELECT 2"},
        )
        assert r.status_code == 400

    def test_empty_sql_rejected(self, client: TestClient):
        r = client.post(
            "/api/query", json={"datasource_id": "testdb", "sql": "   "}
        )
        assert r.status_code == 400
