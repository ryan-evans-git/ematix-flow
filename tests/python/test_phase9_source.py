"""Phase 9: Source abstraction polish.

Adds Source.postgres_table(...) sugar and pipeline.sync(force_path=...)
override so tests can exercise the cross-DB code path against a same-DB
connection.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from ematix_flow import _core, pipeline
from ematix_flow.source import Source
from ematix_flow.table import ManagedTable
from ematix_flow.types import BigInt, Column, String, Text

# --- pure (no DB) ------------------------------------------------------------


def test_postgres_table_builds_select_star_when_no_columns() -> None:
    s = Source.postgres_table(connection=object(), schema="public", table="users")
    assert s.query == "SELECT * FROM public.users"


def test_postgres_table_builds_explicit_select_when_columns_given() -> None:
    s = Source.postgres_table(
        connection=object(),
        schema="public",
        table="users",
        columns=["id", "email"],
    )
    assert s.query == "SELECT id, email FROM public.users"


def test_postgres_table_handles_iterable_columns() -> None:
    s = Source.postgres_table(
        connection=object(),
        schema="public",
        table="users",
        columns=("id", "email"),
    )
    assert s.query == "SELECT id, email FROM public.users"


# --- integration -------------------------------------------------------------


pytestmark_integration = pytest.mark.integration


@pytest.fixture
def schema_name() -> str:
    return f"phase9_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def src_schema() -> str:
    return f"phase9src_{uuid.uuid4().hex[:8]}"


def _seed_source(conn: Any, schema: str) -> None:
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.execute(f"DROP TABLE IF EXISTS {schema}.src_customers")
    conn.execute(
        f"CREATE TABLE {schema}.src_customers ("
        f"  customer_id BIGINT PRIMARY KEY,"
        f"  email VARCHAR(256) NOT NULL,"
        f"  name TEXT NOT NULL"
        f")"
    )
    conn.execute(
        f"INSERT INTO {schema}.src_customers VALUES "
        f"(1, 'a@x.com', 'Alice'), (2, 'b@x.com', 'Bob')"
    )


def _make_target_class(schema: str) -> type[ManagedTable]:
    class CustomerDim(ManagedTable):
        __schema__ = schema
        __tablename__ = "customer_dim"

        customer_id = Column(BigInt(), nullable=False, primary_key=True)
        email = Column(String(256), nullable=False)
        name = Column(Text(), nullable=False)

    return CustomerDim


@pytest.mark.integration
def test_postgres_table_runs_end_to_end_append(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema)
    Cls = _make_target_class(schema_name)
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_table(conn, src_schema, "src_customers"),
        target_connection=conn,
        mode="append",
        pipeline_name="customers_phase9_pgtable",
    )
    assert result["status"] == "success"
    assert result["rows_inserted"] == 2


@pytest.mark.integration
def test_force_path_cross_db_uses_copy_staging_on_same_conn(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    """Same connection, forced cross-DB → COPY staging path runs."""
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema)
    Cls = _make_target_class(schema_name)
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(
            conn,
            f"SELECT customer_id, email, name FROM {src_schema}.src_customers",
        ),
        target_connection=conn,
        mode="append",
        pipeline_name="customers_phase9_force_cross",
        force_path="cross_db",
    )
    assert result["path"] == "cross_db"
    assert result["rows_inserted"] == 2


@pytest.mark.integration
def test_force_path_same_db_explicit(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    """Explicit same-DB override works (and matches auto-detect here)."""
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema)
    Cls = _make_target_class(schema_name)
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(
            conn,
            f"SELECT customer_id, email, name FROM {src_schema}.src_customers",
        ),
        target_connection=conn,
        mode="append",
        pipeline_name="customers_phase9_force_same",
        force_path="same_db",
    )
    assert result["path"] == "same_db"


@pytest.mark.integration
def test_force_path_cross_db_works_with_scd2(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema)
    Cls = _make_target_class(schema_name)
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(
            conn,
            f"SELECT customer_id, email, name FROM {src_schema}.src_customers",
        ),
        target_connection=conn,
        mode="scd2",
        pipeline_name="customers_phase9_scd2_cross",
        force_path="cross_db",
    )
    assert result["path"] == "cross_db"
    assert result["rows_inserted"] == 2


def test_invalid_force_path_raises() -> None:
    """Pure validation — no DB needed."""
    Cls = _make_target_class("anything")

    class _FakeConn:
        def connection_info(self) -> dict:
            return {"host": "h", "port": 5432, "dbname": "d", "user": "u"}

        def ensure_table(self, *a, **kw) -> None:
            raise AssertionError("should not be called")

    with pytest.raises(ValueError, match="force_path"):
        pipeline.sync(
            target=Cls,
            source=Source.postgres_query(_FakeConn(), "SELECT 1"),
            target_connection=_FakeConn(),
            mode="append",
            force_path="bogus",
        )
