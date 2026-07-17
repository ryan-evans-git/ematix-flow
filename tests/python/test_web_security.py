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
            # Replacement-scan / bare-path-in-table-position: no function
            # token, so the denylist misses these — the real bypass.
            "SELECT * FROM 'file:///etc/passwd'",
            "SELECT * FROM '/etc/passwd'",
            "SELECT * FROM 'https://evil.example/x.parquet'",
            "SELECT * FROM 's3://bucket/x.parquet'",
            "select * from\n  'data.parquet'",
            "SELECT a FROM t JOIN 'file.parquet' ON t.id = 1",
            "WITH x AS (SELECT * FROM 'sneaky.parquet') SELECT * FROM x",
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
            # Double-quoted identifiers are NOT paths — must stay allowed.
            'SELECT * FROM "weird table name"',
            # A string literal in WHERE/SELECT position is a value, not a
            # table — must not be mistaken for a replacement scan.
            "SELECT * FROM t WHERE url = 'https://example.com/page'",
            "SELECT * FROM t WHERE path = '/var/log/app.log'",
            "SELECT 'file.parquet' AS label FROM t",
        ],
    )
    def test_allows_benign_queries(self, sql):
        assert guard_readonly(sql)


class TestErrorSanitization:
    def test_sanitize_redacts_datasource_url(self):
        from ematix_flow.web.analytics import _sanitize_error

        url = "postgres://alice:s3cr3t@db.internal:5432/prod"
        msg = f"could not connect to {url}: timeout"
        out = _sanitize_error(msg, url)
        assert "s3cr3t" not in out
        assert url not in out

    def test_sanitize_redacts_inline_credentials_generically(self):
        from ematix_flow.web.analytics import _sanitize_error

        # Even a URL the function wasn't told about (e.g. an ATTACH'd
        # secondary DSN echoed by the driver) must have its credentials
        # stripped.
        msg = "auth failed for mysql://bob:hunter2@10.0.0.5/db"
        out = _sanitize_error(msg, "sqlite:///local.db")
        assert "hunter2" not in out


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

    def test_engine_error_does_not_leak_credentials(self, tmp_path, monkeypatch):
        # A datasource URL with an embedded password; force the engine to
        # raise an error that echoes the DSN. The HTTP 400 detail must
        # not contain the password.
        url = "postgres://alice:s3cr3t@db.internal:5432/prod"
        app = create_app(datasources={"pg": url})
        client = TestClient(app)

        def _boom(*a, **k):
            raise RuntimeError(f"connection refused for {url}")

        monkeypatch.setattr(analytics, "_run_with_timeout", _boom)
        r = client.post("/api/query", json={"datasource_id": "pg", "sql": "SELECT 1"})
        assert r.status_code == 400
        assert "s3cr3t" not in r.text
        assert url not in r.text


class TestRunsPaginationClamp:
    def test_runs_limit_is_clamped(self):
        app = create_app(datasources={"db": "sqlite:///:memory:"})
        client = TestClient(app)
        # An absurd limit must be accepted (200) but clamped, not passed
        # through unbounded. The stub path echoes the slice; we just
        # assert it doesn't error and returns the documented shape.
        r = client.get("/api/runs?limit=100000000&offset=-5")
        assert r.status_code == 200
        assert "runs" in r.json() and "total" in r.json()


class TestOpenApiNotExposed:
    def test_openapi_under_api_prefix(self):
        # The schema must live under /api/ so the auth middleware covers
        # it; the default /openapi.json would be unauthenticated.
        app = create_app(datasources={"db": "sqlite:///:memory:"})
        client = TestClient(app)
        assert client.get("/openapi.json").status_code == 404
        assert client.get("/api/openapi.json").status_code == 200
