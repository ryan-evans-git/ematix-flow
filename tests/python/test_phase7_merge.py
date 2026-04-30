"""Phase 7: MergeUpsert / SCD1 — pipeline.sync(mode='merge'|'scd1')."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from typing import Any

import pytest

from ematix_flow import _core, pipeline
from ematix_flow.source import Source
from ematix_flow.table import ManagedTable
from ematix_flow.types import BigInt, Column, String, Text

pytestmark = pytest.mark.integration


@pytest.fixture
def schema_name() -> str:
    return f"phase7_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def src_schema() -> str:
    return f"phase7src_{uuid.uuid4().hex[:8]}"


def _seed_source(conn: Any, schema: str, rows: list[tuple[int, str, str]]) -> None:
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.execute(f"DROP TABLE IF EXISTS {schema}.src_customers")
    conn.execute(
        f"CREATE TABLE {schema}.src_customers ("
        f"  id BIGINT PRIMARY KEY,"
        f"  email VARCHAR(256) NOT NULL,"
        f"  name TEXT NOT NULL"
        f")"
    )
    if rows:
        values = ", ".join(f"({i}, '{e}', '{n}')" for i, e, n in rows)
        conn.execute(f"INSERT INTO {schema}.src_customers VALUES {values}")


def _make_target_class(schema: str) -> type[ManagedTable]:
    class CustomerDim(ManagedTable):
        __schema__ = schema
        __tablename__ = "customer_dim"

        customer_id = Column(BigInt(), nullable=False, primary_key=True)
        email = Column(String(256), nullable=False)
        name = Column(Text(), nullable=False)

    return CustomerDim


def _src_query(src_schema: str) -> str:
    return (
        f"SELECT id AS customer_id, email, name FROM {src_schema}.src_customers"
    )


def test_merge_inserts_new_rows(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema, [(1, "a@x.com", "Alice"), (2, "b@x.com", "Bob")])
    Cls = _make_target_class(schema_name)
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="merge",
        pipeline_name="customers_phase7_insert",
    )
    assert result["status"] == "success"
    assert result["rows_inserted"] == 2
    assert result["rows_updated"] == 0
    assert result["rows_unchanged"] == 0


def test_merge_is_idempotent(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema, [(1, "a@x.com", "Alice"), (2, "b@x.com", "Bob")])
    Cls = _make_target_class(schema_name)
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="merge",
        pipeline_name="customers_phase7_idem",
    )
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="merge",
        pipeline_name="customers_phase7_idem",
    )
    assert result["rows_inserted"] == 0
    assert result["rows_updated"] == 0
    assert result["rows_unchanged"] == 2


def test_merge_updates_changed_rows(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema, [(1, "a@x.com", "Alice"), (2, "b@x.com", "Bob")])
    Cls = _make_target_class(schema_name)
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="merge",
        pipeline_name="customers_phase7_update",
    )
    # Modify Alice's email; Bob unchanged.
    conn.execute(
        f"UPDATE {src_schema}.src_customers SET email='a2@x.com' WHERE id=1"
    )
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="merge",
        pipeline_name="customers_phase7_update",
    )
    assert result["rows_inserted"] == 0
    assert result["rows_updated"] == 1
    assert result["rows_unchanged"] == 1
    new_email = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_dim "
        f"WHERE customer_id=1 AND email='a2@x.com'"
    )
    assert new_email == 1


def test_merge_mixed_insert_update_unchanged(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(
        conn,
        src_schema,
        [(1, "a@x.com", "Alice"), (2, "b@x.com", "Bob"), (3, "c@x.com", "Carol")],
    )
    Cls = _make_target_class(schema_name)
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="merge",
        pipeline_name="customers_phase7_mixed",
    )
    # Update one, keep one, add two.
    conn.execute(
        f"UPDATE {src_schema}.src_customers SET name='Alice2' WHERE id=1"
    )
    conn.execute(
        f"INSERT INTO {src_schema}.src_customers VALUES "
        f"(4, 'd@x.com', 'Dan'), (5, 'e@x.com', 'Eve')"
    )
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="merge",
        pipeline_name="customers_phase7_mixed",
    )
    assert result["rows_inserted"] == 2
    assert result["rows_updated"] == 1
    assert result["rows_unchanged"] == 2


def test_scd1_alias(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema, [(1, "a@x.com", "Alice")])
    Cls = _make_target_class(schema_name)
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="scd1",
        pipeline_name="customers_phase7_scd1",
    )
    assert result["rows_inserted"] == 1
    found = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM ematix_flow.run_history "
        "WHERE pipeline_name='customers_phase7_scd1' AND mode='scd1'"
    )
    assert found == 1


def test_explicit_update_columns(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    """`update_columns` restricts which columns participate in compare+set."""
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema, [(1, "a@x.com", "Alice")])
    Cls = _make_target_class(schema_name)
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="merge",
        pipeline_name="customers_phase7_explicit_update",
    )
    # Change email AND name in source, but only allow updating email.
    conn.execute(
        f"UPDATE {src_schema}.src_customers SET email='a2@x.com', name='Alice2' WHERE id=1"
    )
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="merge",
        pipeline_name="customers_phase7_explicit_update",
        update_columns=("email",),
    )
    assert result["rows_updated"] == 1
    # email updated, name not.
    matched = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_dim "
        f"WHERE customer_id=1 AND email='a2@x.com' AND name='Alice'"
    )
    assert matched == 1


def test_run_history_records_all_counts(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema, [(1, "a@x.com", "Alice"), (2, "b@x.com", "Bob")])
    Cls = _make_target_class(schema_name)
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="merge",
        pipeline_name="customers_phase7_history",
    )
    found = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM ematix_flow.run_history "
        f"WHERE run_id='{result['run_id']}'::uuid "
        f"AND rows_inserted=2 AND rows_updated=0 AND rows_unchanged=0"
    )
    assert found == 1


# --- cross-DB ----------------------------------------------------------------


@pytest.fixture(scope="module")
def pg_url_secondary() -> Iterator[str]:
    pytest.importorskip("testcontainers.postgres")
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine", driver=None) as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5432)
        user = container.username
        password = container.password
        dbname = container.dbname
        yield f"postgres://{user}:{password}@{host}:{port}/{dbname}"


def test_cross_db_merge(
    pg_url: str, pg_url_secondary: str, schema_name: str, src_schema: str
) -> None:
    src_conn = _core.connect(pg_url_secondary)
    tgt_conn = _core.connect(pg_url)
    _seed_source(src_conn, src_schema, [(1, "a@x.com", "Alice"), (2, "b@x.com", "Bob")])
    Cls = _make_target_class(schema_name)
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(src_conn, _src_query(src_schema)),
        target_connection=tgt_conn,
        mode="merge",
        pipeline_name="customers_phase7_cross_db",
    )
    assert result["status"] == "success"
    assert result["path"] == "cross_db"
    assert result["rows_inserted"] == 2
    # Modify and re-sync.
    src_conn.execute(
        f"UPDATE {src_schema}.src_customers SET email='a2@x.com' WHERE id=1"
    )
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(src_conn, _src_query(src_schema)),
        target_connection=tgt_conn,
        mode="merge",
        pipeline_name="customers_phase7_cross_db",
    )
    assert result["rows_updated"] == 1
    assert result["rows_unchanged"] == 1
