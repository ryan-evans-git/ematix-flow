"""Phase 1: catalog endpoints (schema browser backend).

Introspection runs through the ematix engine against a real sqlite
datasource — no mocking.
"""
from __future__ import annotations

import sqlite3

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient

from ematix_flow.web.server import create_app


@pytest.fixture
def sqlite_db(tmp_path):
    path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(path))
    conn.executescript(
        """
        CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT NOT NULL, amount REAL);
        CREATE TABLE regions (code TEXT, name TEXT);
        CREATE VIEW big_sales AS SELECT * FROM sales WHERE amount > 100;
        """
    )
    conn.commit()
    conn.close()
    return path


@pytest.fixture
def client(sqlite_db) -> TestClient:
    return TestClient(create_app(datasources={"db": f"sqlite:///{sqlite_db}"}))


class TestSchemas:
    def test_sqlite_reports_main(self, client: TestClient):
        r = client.get("/api/datasources/db/schemas")
        assert r.status_code == 200, r.text
        assert r.json()["schemas"] == ["main"]

    def test_unknown_datasource_404s(self, client: TestClient):
        assert client.get("/api/datasources/nope/schemas").status_code == 404


class TestTables:
    def test_lists_tables_and_views(self, client: TestClient):
        r = client.get("/api/datasources/db/schemas/main/tables")
        assert r.status_code == 200, r.text
        tables = {t["name"]: t for t in r.json()["tables"]}
        assert "sales" in tables and "regions" in tables
        assert "big_sales" in tables
        assert tables["sales"]["kind"] == "table"
        assert tables["big_sales"]["kind"] == "view"
        # sqlite internal tables are hidden.
        assert not any(n.startswith("sqlite_") for n in tables)


class TestColumns:
    def test_lists_columns_with_types_and_nullability(self, client: TestClient):
        r = client.get("/api/datasources/db/schemas/main/tables/sales/columns")
        assert r.status_code == 200, r.text
        cols = {c["name"]: c for c in r.json()["columns"]}
        assert [c for c in cols] == ["id", "region", "amount"]
        assert cols["region"]["nullable"] is False  # NOT NULL
        assert cols["amount"]["nullable"] is True
        assert cols["amount"]["type"].upper() == "REAL"
