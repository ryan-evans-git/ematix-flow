"""Unit tests for the analytics query layer's pure logic.

These deliberately avoid the ematix engine (no ``Connection.query``),
so they never touch the tokio runtime / Arrow-FFI teardown path — they
run crash-free on every environment. The engine-integration path is
covered by ``test_web_query.py``.
"""
from __future__ import annotations

import pytest

from ematix_flow.web.analytics import (
    DEFAULT_MAX_ROWS,
    MAX_ROWS_CEILING,
    DatasourceNotFound,
    DatasourceRegistry,
    QueryError,
    _clamp_max_rows,
    dialect_from_url,
    guard_readonly,
)


class TestGuardReadonly:
    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT 1",
            "  select * from t  ",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "EXPLAIN SELECT 1",
            "SELECT 1;",  # trailing semicolon is fine
            "SELECT 1 -- trailing comment",
            "/* lead */ SELECT 1",
        ],
    )
    def test_accepts_readonly(self, sql: str):
        assert guard_readonly(sql)  # returns cleaned, truthy SQL

    @pytest.mark.parametrize(
        "sql",
        [
            "DELETE FROM t",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET x = 1",
            "DROP TABLE t",
            "CREATE TABLE t (x int)",
            "ALTER TABLE t ADD COLUMN y int",
            "TRUNCATE t",
            "ATTACH DATABASE 'x' AS y",
            "GRANT SELECT ON t TO u",
        ],
    )
    def test_rejects_writes_and_ddl(self, sql: str):
        with pytest.raises(QueryError):
            guard_readonly(sql)

    @pytest.mark.parametrize("sql", ["SELECT 1; SELECT 2", "SELECT 1; DROP TABLE t"])
    def test_rejects_multi_statement(self, sql: str):
        with pytest.raises(QueryError):
            guard_readonly(sql)

    @pytest.mark.parametrize("sql", ["", "   ", ";", "-- just a comment", None])
    def test_rejects_empty(self, sql):
        with pytest.raises(QueryError):
            guard_readonly(sql)

    def test_strips_trailing_semicolon_and_comments(self):
        assert guard_readonly("SELECT 1;  -- note") == "SELECT 1"


class TestClampMaxRows:
    def test_default(self):
        assert _clamp_max_rows(None) == DEFAULT_MAX_ROWS

    def test_ceiling(self):
        assert _clamp_max_rows(10**9) == MAX_ROWS_CEILING

    def test_passthrough(self):
        assert _clamp_max_rows(42) == 42

    @pytest.mark.parametrize("bad", [0, -1])
    def test_rejects_nonpositive(self, bad: int):
        with pytest.raises(QueryError):
            _clamp_max_rows(bad)


class TestDialectFromUrl:
    @pytest.mark.parametrize(
        "url,expected",
        [
            ("postgres://u@h/db", "postgres"),
            ("postgresql://u@h/db", "postgres"),
            ("mysql://u@h/db", "mysql"),
            ("mysql+pymysql://u@h/db", "mysql"),
            ("sqlite:///a.db", "sqlite"),
            (":memory:", "sqlite"),
            ("duckdb:///a.db", "duckdb"),
        ],
    )
    def test_dialect(self, url: str, expected: str):
        assert dialect_from_url(url) == expected


class TestDatasourceRegistry:
    def test_list_and_public_dict_omits_url(self):
        reg = DatasourceRegistry({"db": "postgres://user:secret@h/db"})
        listing = [d.public_dict() for d in reg.list()]
        assert listing == [{"id": "db", "dialect": "postgres"}]
        # The raw URL (with credentials) is never in the public dict.
        assert all("url" not in d for d in listing)

    def test_get_known(self):
        reg = DatasourceRegistry({"db": "sqlite:///a.db"})
        assert reg.get("db").url == "sqlite:///a.db"

    def test_get_unknown_raises(self):
        with pytest.raises(DatasourceNotFound):
            DatasourceRegistry({}).get("nope")
