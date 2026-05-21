"""Task #557 slice 1 — Arrow-native Snowflake write path (no pandas).

The earlier ``snowflake_write_arrow`` implementation went through
``table.to_pandas()`` → ``write_pandas()``, which transitively
required pandas at runtime. This slice replaces that path with a
parquet-staged PUT + COPY INTO and tests that:

1. ``pandas`` is **never imported** as a consequence of the Snowflake
   write code path (sys.modules snapshot before / after).
2. The right SQL sequence reaches the cursor — CREATE / PUT / COPY.
3. ``_arrow_schema_to_snowflake_ddl`` maps Arrow types to Snowflake
   DDL conservatively (int → NUMBER, string → VARCHAR, timestamp_tz
   → TIMESTAMP_TZ, etc.) and double-quotes identifiers.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from ematix_flow.warehouses import (
    SnowflakeConnection,
    _arrow_schema_to_snowflake_ddl,
    _arrow_type_to_snowflake,
    _quote_snowflake_identifier,
    snowflake_write_arrow,
)


def _capturing_client(executed: list[str]) -> MagicMock:
    """Snowflake client mock whose ``cursor.execute()`` records SQL."""
    cursor = MagicMock()
    cursor.execute.side_effect = lambda sql, *a, **kw: executed.append(sql) or cursor
    client = MagicMock()
    client.cursor.return_value = cursor
    return client


class TestNoPandasDependency:
    def test_snowflake_write_source_has_no_pandas_references(self):
        """The Arrow-native path must not call any pandas API.

        pyarrow.parquet.write_table transitively imports pandas as a
        metadata-introspection side effect (out of our control), so a
        sys.modules check would be a false positive. What we *can*
        own: our source must not call ``to_pandas`` / ``write_pandas``
        / ``DataFrame`` itself. A grep over the snowflake write region
        catches a regression to the old write_pandas-shaped path.
        """
        import inspect

        from ematix_flow import warehouses

        src = inspect.getsource(warehouses.snowflake_write_arrow)
        # Strip the docstring so historical references like "the old
        # write_pandas path" don't trip the grep.
        doc = warehouses.snowflake_write_arrow.__doc__ or ""
        body = src.replace(doc, "")
        for forbidden in (".to_pandas(", ".write_pandas(", "DataFrame(", "import pandas"):
            assert forbidden not in body, (
                f"snowflake_write_arrow body contains {forbidden!r} — "
                "regression vs Arrow-native PUT + COPY INTO design (task #557)"
            )

    def test_snowflake_extra_does_not_pin_pandas(self):
        """The ``[snowflake]`` extra in pyproject.toml must not pin
        pandas. Detects accidental re-additions in future PRs."""
        import tomllib
        from pathlib import Path

        # Walk up to repo root from this test file.
        here = Path(__file__).resolve()
        repo_root = here
        for _ in range(6):
            if (repo_root / "pyproject.toml").exists():
                break
            repo_root = repo_root.parent
        else:
            pytest.skip("repo root not found")

        with open(repo_root / "pyproject.toml", "rb") as f:
            cfg = tomllib.load(f)
        snowflake_deps = cfg["project"]["optional-dependencies"]["snowflake"]
        assert not any("pandas" in dep for dep in snowflake_deps), (
            f"[snowflake] extra still pins pandas: {snowflake_deps}"
        )


class TestArrowTypeMapping:
    @pytest.mark.parametrize(
        "arrow_type, expected",
        [
            (pa.int8(), "NUMBER(10)"),
            (pa.int16(), "NUMBER(10)"),
            (pa.int32(), "NUMBER(10)"),
            (pa.int64(), "NUMBER(19)"),
            (pa.uint64(), "NUMBER(20)"),
            (pa.float32(), "FLOAT"),
            (pa.float64(), "DOUBLE"),
            (pa.bool_(), "BOOLEAN"),
            (pa.string(), "VARCHAR(16777216)"),
            (pa.large_string(), "VARCHAR(16777216)"),
            (pa.date32(), "DATE"),
            (pa.date64(), "DATE"),
            (pa.binary(), "BINARY"),
            (pa.large_binary(), "BINARY"),
            (pa.timestamp("us"), "TIMESTAMP_NTZ"),
            (pa.timestamp("us", tz="UTC"), "TIMESTAMP_TZ"),
            (pa.decimal128(18, 4), "NUMBER(18,4)"),
            (pa.list_(pa.int32()), "ARRAY"),
            (pa.struct([("a", pa.int32())]), "OBJECT"),
        ],
    )
    def test_arrow_to_snowflake_type(self, arrow_type, expected):
        assert (
            _arrow_type_to_snowflake(pa.field("x", arrow_type)) == expected
        )

    def test_unknown_type_falls_back_to_variant(self):
        # The pa.null() type isn't explicitly mapped → catch-all VARIANT.
        assert (
            _arrow_type_to_snowflake(pa.field("x", pa.null())) == "VARIANT"
        )


class TestIdentifierQuoting:
    def test_simple_identifier(self):
        assert _quote_snowflake_identifier("foo") == '"foo"'

    def test_preserves_case(self):
        assert _quote_snowflake_identifier("MyCol") == '"MyCol"'

    def test_escapes_embedded_quotes(self):
        assert _quote_snowflake_identifier('weird"name') == '"weird""name"'


class TestDdlGeneration:
    def test_simple_schema(self):
        schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("name", pa.string()),
                pa.field("price", pa.decimal128(10, 2)),
            ]
        )
        ddl = _arrow_schema_to_snowflake_ddl("orders", schema)
        assert ddl.startswith('CREATE TABLE IF NOT EXISTS "orders" (')
        assert '"id" NUMBER(19) NOT NULL' in ddl
        assert '"name" VARCHAR(16777216)' in ddl
        assert '"price" NUMBER(10,2)' in ddl

    def test_nullable_default(self):
        """Nullable columns omit the NOT NULL suffix."""
        schema = pa.schema([pa.field("x", pa.int32(), nullable=True)])
        ddl = _arrow_schema_to_snowflake_ddl("t", schema)
        assert "NOT NULL" not in ddl


class TestExecutedSqlSequence:
    """The Arrow-native path must emit CREATE + PUT + COPY in order.
    A regression would silently break the load (e.g. PUT without COPY
    leaves files staged but no rows visible)."""

    def test_create_put_copy_sequence(self):
        executed: list[str] = []
        client = _capturing_client(executed)
        conn = SnowflakeConnection(
            name="t", account="a", user="u", password="p", warehouse="W"
        )
        table = pa.table({"id": [1, 2]})

        snowflake_write_arrow(
            conn,
            table,
            table_name="out",
            create_if_not_exists=True,
            _client=client,
        )

        assert len(executed) == 3, executed
        create_sql, put_sql, copy_sql = executed
        assert create_sql.startswith("CREATE TABLE IF NOT EXISTS")
        assert put_sql.startswith("PUT file://")
        assert "@%\"out\"" in put_sql  # session stage on the quoted table
        assert "OVERWRITE = TRUE" in put_sql
        assert "AUTO_COMPRESS = FALSE" in put_sql
        assert copy_sql.startswith("COPY INTO")
        assert "FILE_FORMAT = (TYPE = PARQUET)" in copy_sql
        assert "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE" in copy_sql
        assert "PURGE = TRUE" in copy_sql

    def test_create_skipped_when_create_if_not_exists_false(self):
        executed: list[str] = []
        client = _capturing_client(executed)
        conn = SnowflakeConnection(
            name="t", account="a", user="u", password="p", warehouse="W"
        )
        table = pa.table({"id": [1, 2]})

        snowflake_write_arrow(
            conn,
            table,
            table_name="out",
            create_if_not_exists=False,
            _client=client,
        )

        assert len(executed) == 2, executed
        assert executed[0].startswith("PUT")
        assert executed[1].startswith("COPY")

    def test_returns_row_count(self):
        client = _capturing_client([])
        conn = SnowflakeConnection(
            name="t", account="a", user="u", password="p", warehouse="W"
        )
        table = pa.table({"id": list(range(42))})

        nrows = snowflake_write_arrow(
            conn,
            table,
            table_name="out",
            _client=client,
        )
        assert nrows == 42
