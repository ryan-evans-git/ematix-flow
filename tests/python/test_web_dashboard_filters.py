"""Phase 4: dashboard filters / cross-filtering in the batch query."""
from __future__ import annotations

import sqlite3

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient

from ematix_flow.web.analytics import build_filtered_sql
from ematix_flow.web.analytics_store import AnalyticsStore
from ematix_flow.web.server import create_app


@pytest.fixture
def sqlite_db(tmp_path):
    path = tmp_path / "f.db"
    conn = sqlite3.connect(str(path))
    conn.executescript(
        """
        CREATE TABLE sales (region TEXT, product TEXT, amount REAL);
        INSERT INTO sales VALUES
            ('west','widget',100), ('west','gadget',20),
            ('east','widget',50),  ('east','gadget',5);
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


def _dashboard_with_chart(client, sql, encoding, viz="bar"):
    cid = client.post(
        "/api/charts",
        json={"name": "c", "datasource_id": "db", "sql": sql, "viz_type": viz, "encoding": encoding},
    ).json()["id"]
    did = client.post(
        "/api/dashboards",
        json={"name": "d", "layout": {"tiles": [{"chart_id": cid, "x": 0, "y": 0, "w": 6, "h": 6}]}},
    ).json()["id"]
    return cid, did


class TestBuildFilteredSql:
    def test_wraps_and_escapes(self):
        sql = build_filtered_sql(
            "SELECT region, x FROM t",
            [{"column": "region", "values": ["west", "o'brien"]}],
            ["region", "x"],
        )
        assert "_emat_sub" in sql
        assert "\"region\" IN ('west', 'o''brien')" in sql

    def test_skips_columns_not_in_output(self):
        assert build_filtered_sql("SELECT a FROM t", [{"column": "region", "values": ["west"]}], ["a"]) is None

    def test_none_when_no_filters(self):
        assert build_filtered_sql("SELECT a FROM t", [], ["a"]) is None


class TestDashboardFilterQuery:
    def test_filter_restricts_rows(self, client):
        cid, did = _dashboard_with_chart(
            client,
            "SELECT region, SUM(amount) AS total FROM sales GROUP BY region ORDER BY region",
            {"x": "region", "y": ["total"]},
        )
        # Unfiltered: both regions.
        unfiltered = client.post(f"/api/dashboards/{did}/query").json()["results"][cid]
        assert {r[0] for r in unfiltered["rows"]} == {"east", "west"}

        # Filter region = west → only west row survives.
        filtered = client.post(
            f"/api/dashboards/{did}/query",
            json={"filters": [{"column": "region", "values": ["west"]}]},
        ).json()["results"][cid]
        assert [r[0] for r in filtered["rows"]] == ["west"]
        assert filtered["rows"][0][1] == pytest.approx(120)  # 100 + 20

    def test_filter_on_absent_column_is_ignored(self, client):
        # Chart output has no 'region' column → filter doesn't apply, all rows stay.
        cid, did = _dashboard_with_chart(
            client,
            "SELECT product, SUM(amount) AS total FROM sales GROUP BY product ORDER BY product",
            {"x": "product", "y": ["total"]},
        )
        filtered = client.post(
            f"/api/dashboards/{did}/query",
            json={"filters": [{"column": "region", "values": ["west"]}]},
        ).json()["results"][cid]
        assert {r[0] for r in filtered["rows"]} == {"gadget", "widget"}
