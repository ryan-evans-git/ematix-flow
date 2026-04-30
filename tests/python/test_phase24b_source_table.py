"""Phase 24b: `source_table=` + `column_map=` kwargs."""

from __future__ import annotations

import uuid
from typing import Annotated, Any

import pytest

from ematix_flow import _core, ematix, pipeline as p, pk
from ematix_flow.types import BigInt, Date, Numeric, String, Text


@pytest.fixture(autouse=True)
def _clean_registry():
    p._REGISTRY.clear()
    yield
    p._REGISTRY.clear()


# --- decoration-time validation --------------------------------------------


def test_source_table_unqualified_rejected_at_decoration_time() -> None:
    @ematix.table(schema="s")
    class T:
        id: Annotated[BigInt, pk()]

    with pytest.raises(ValueError, match="schema.table"):

        @ematix.pipeline(
            target=T, source_table="users", schedule="0 * * * *", mode="merge"
        )
        def f():
            pass


def test_source_table_too_many_dots_rejected() -> None:
    @ematix.table(schema="s")
    class T:
        id: Annotated[BigInt, pk()]

    with pytest.raises(ValueError, match="schema.table"):

        @ematix.pipeline(
            target=T, source_table="a.b.c", schedule="0 * * * *", mode="merge"
        )
        def f():
            pass


def test_source_table_qualified_accepted() -> None:
    @ematix.table(schema="s")
    class T:
        id: Annotated[BigInt, pk()]

    @ematix.pipeline(
        target=T,
        source_table="public.users",
        schedule="0 * * * *",
        mode="merge",
    )
    def f():
        pass

    sp = p.get_pipeline("f")
    assert sp.name == "f"


# --- integration ------------------------------------------------------------


pytestmark_int = pytest.mark.integration


@pytest.fixture
def schema_name() -> str:
    return f"phase24bst_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def src_schema() -> str:
    return f"phase24bstsrc_{uuid.uuid4().hex[:8]}"


def _seed_source(conn: Any, schema: str) -> None:
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.execute(f"DROP TABLE IF EXISTS {schema}.users")
    conn.execute(
        f"CREATE TABLE {schema}.users ("
        f"  id BIGINT PRIMARY KEY,"
        f"  email VARCHAR(256) NOT NULL,"
        f"  name TEXT NOT NULL,"
        f"  created_at DATE NOT NULL,"
        f"  password TEXT"
        f")"
    )
    conn.execute(
        f"INSERT INTO {schema}.users VALUES "
        f"(1, 'a@x.com', 'Alice', '2026-01-01'::date, 'unused'),"
        f"(2, 'b@x.com', 'Bob', '2026-02-01'::date, 'unused')"
    )


@pytest.mark.integration
def test_source_table_synthesizes_select_for_matching_columns(
    pg_url: str, schema_name: str, src_schema: str, monkeypatch
) -> None:
    """When target columns match source column names exactly."""
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    seed = _core.connect(pg_url)
    _seed_source(seed, src_schema)
    # Rename `id` to match target naming.
    seed.execute(f"ALTER TABLE {src_schema}.users RENAME COLUMN id TO customer_id")

    @ematix.table(schema=schema_name)
    class CustomerDim:
        customer_id: Annotated[BigInt, pk()]
        email: String[256]
        name: Text

    @ematix.pipeline(
        target=CustomerDim,
        source_table=f"{src_schema}.users",
        schedule="0 * * * *",
        mode="merge",
        name="customers_phase24b_st_match",
    )
    def f():
        pass

    result = p.run_pipeline("customers_phase24b_st_match")
    assert result["status"] == "success"
    assert result["rows_inserted"] == 2


@pytest.mark.integration
def test_source_table_with_column_map_renames(
    pg_url: str, schema_name: str, src_schema: str, monkeypatch
) -> None:
    """column_map: target column → source column."""
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    seed = _core.connect(pg_url)
    _seed_source(seed, src_schema)

    @ematix.table(schema=schema_name)
    class CustomerDim:
        customer_id: Annotated[BigInt, pk()]
        email: String[256]
        name: Text
        signup_at: Date

    @ematix.pipeline(
        target=CustomerDim,
        source_table=f"{src_schema}.users",
        column_map={
            "customer_id": "id",
            "signup_at": "created_at",
        },
        schedule="0 * * * *",
        mode="merge",
        name="customers_phase24b_colmap",
    )
    def f():
        pass

    result = p.run_pipeline("customers_phase24b_colmap")
    assert result["status"] == "success"
    assert result["rows_inserted"] == 2


@pytest.mark.integration
def test_function_body_wins_over_source_table(
    pg_url: str, schema_name: str, src_schema: str, monkeypatch
) -> None:
    """When both source_table and a returning function body are present."""
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    seed = _core.connect(pg_url)
    _seed_source(seed, src_schema)

    @ematix.table(schema=schema_name)
    class CustomerDim:
        customer_id: Annotated[BigInt, pk()]
        email: String[256]
        name: Text

    @ematix.pipeline(
        target=CustomerDim,
        source_table=f"{src_schema}.users",  # ignored when body returns
        schedule="0 * * * *",
        mode="merge",
        name="customers_phase24b_body_wins",
    )
    def f(conn):
        # Body returns explicit SQL with WHERE — only id=1 qualifies.
        return (
            f"SELECT id AS customer_id, email, name "
            f"FROM {src_schema}.users WHERE id = 1"
        )

    result = p.run_pipeline("customers_phase24b_body_wins")
    assert result["rows_inserted"] == 1
