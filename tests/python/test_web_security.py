"""Security guards for the ad-hoc query surface: file/network function
denylist and query timeout."""
from __future__ import annotations

import sqlite3
import time

import pytest

from ematix_flow.web import analytics
from ematix_flow.web.analytics import (
    QueryTimeout,
    _run_with_timeout,
    guard_readonly,
)

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from ematix_flow.web.server import create_app


class TestDenylist:
    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT * FROM read_csv('/etc/passwd')",
            "SELECT * FROM read_parquet('s3://bucket/x.parquet')",
            "SELECT * FROM read_json('https://evil.example/x.json')",
            "SELECT * FROM read_ndjson_auto('/tmp/x')",
            "SELECT * FROM parquet_scan('/data/x')",
            "SELECT * FROM sqlite_scan('/var/run-log.db', 'run_records')",
            "SELECT * FROM postgres_scan('host=evil', 'public', 't')",
            "SELECT load_extension('evil.so')",
            "SELECT * FROM glob('/etc/*')",
            "WITH x AS (SELECT * FROM read_csv('/etc/passwd')) SELECT * FROM x",
            "EXPLAIN ATTACH DATABASE 'other.db' AS o",
        ],
    )
    def test_blocks_file_and_network_access(self, sql):
        with pytest.raises(analytics.QueryError):
            guard_readonly(sql)

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT read_count FROM t",  # column that merely starts with 'read'
            "SELECT payload, download_url FROM t",
            "SELECT region, SUM(amount) FROM sales GROUP BY region",
            "SELECT * FROM pragma_table_info('sales')",  # catalog-style, allowed
        ],
    )
    def test_allows_benign_queries(self, sql):
        assert guard_readonly(sql)


class TestTimeoutWrapper:
    def test_returns_value_when_fast(self):
        assert _run_with_timeout(lambda: 42, 1.0) == 42

    def test_raises_on_timeout(self):
        with pytest.raises(QueryTimeout):
            _run_with_timeout(lambda: time.sleep(2.0), 0.05)

    def test_propagates_inner_error(self):
        def boom():
            raise ValueError("inner")

        with pytest.raises(ValueError, match="inner"):
            _run_with_timeout(boom, 1.0)

    def test_zero_timeout_runs_inline(self):
        assert _run_with_timeout(lambda: 7, 0) == 7


class TestEndpointGuards:
    @pytest.fixture
    def sqlite_db(self, tmp_path):
        path = tmp_path / "s.db"
        conn = sqlite3.connect(str(path))
        conn.executescript("CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1);")
        conn.commit()
        conn.close()
        return path

    @pytest.fixture
    def client(self, sqlite_db):
        return TestClient(create_app(datasources={"db": f"sqlite:///{sqlite_db}"}))

    def test_query_with_file_function_400s(self, client):
        r = client.post(
            "/api/query",
            json={"datasource_id": "db", "sql": "SELECT * FROM read_csv('/etc/passwd')"},
        )
        assert r.status_code == 400
        assert "security" in r.json()["detail"].lower()

    def test_timeout_maps_to_504(self, client, monkeypatch):
        # Make the engine call appear to time out, deterministically.
        def _timeout(*a, **k):
            raise QueryTimeout("query exceeded the 0.01s time limit")

        monkeypatch.setattr(analytics, "run_query", _timeout)
        r = client.post("/api/query", json={"datasource_id": "db", "sql": "SELECT 1"})
        assert r.status_code == 504
