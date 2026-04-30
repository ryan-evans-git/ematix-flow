"""Phase 11: handle_deletes opt-in for merge (hard) and scd2 (soft)."""

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
    return f"phase11_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def src_schema() -> str:
    return f"phase11src_{uuid.uuid4().hex[:8]}"


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
    return f"SELECT id AS customer_id, email, name FROM {src_schema}.src_customers"


# --- merge + hard delete ----------------------------------------------------


def test_merge_hard_delete_removes_missing_keys(
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
        pipeline_name="customers_phase11_hard",
        handle_deletes="hard",
    )
    # Drop one row from source.
    conn.execute(f"DELETE FROM {src_schema}.src_customers WHERE id=2")
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="merge",
        pipeline_name="customers_phase11_hard",
        handle_deletes="hard",
    )
    count = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_dim"
    )
    assert count == 2
    survivors = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_dim WHERE customer_id IN (1, 3)"
    )
    assert survivors == 2


def test_merge_hard_delete_atomic_on_failure(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    """If a later step fails, the upsert AND delete both roll back."""
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema, [(1, "a@x.com", "Alice")])
    Cls = _make_target_class(schema_name)
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="merge",
        pipeline_name="customers_phase11_hard_atomic",
        handle_deletes="hard",
    )
    # Bad source — column mismatch — the upsert step itself fails.
    with pytest.raises(ValueError):
        pipeline.sync(
            target=Cls,
            source=Source.postgres_query(conn, "SELECT 1 AS bogus"),
            target_connection=conn,
            mode="merge",
            pipeline_name="customers_phase11_hard_atomic",
            handle_deletes="hard",
        )
    # Original row survived.
    surv = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_dim WHERE customer_id=1"
    )
    assert surv == 1


# --- scd2 + soft delete -----------------------------------------------------


def test_scd2_soft_delete_closes_out_missing_keys(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(
        conn,
        src_schema,
        [(1, "a@x.com", "Alice"), (2, "b@x.com", "Bob")],
    )
    Cls = _make_target_class(schema_name)
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="scd2",
        pipeline_name="customers_phase11_scd2_soft",
        handle_deletes="soft",
    )
    # Drop Bob from source.
    conn.execute(f"DELETE FROM {src_schema}.src_customers WHERE id=2")
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="scd2",
        pipeline_name="customers_phase11_scd2_soft",
        handle_deletes="soft",
    )
    # Bob's current version is now closed.
    bob_current = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_dim "
        f"WHERE customer_id=2 AND is_current"
    )
    assert bob_current == 0
    bob_closed = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_dim "
        f"WHERE customer_id=2 AND NOT is_current AND valid_to IS NOT NULL"
    )
    assert bob_closed == 1
    # Alice still current.
    alice_current = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_dim "
        f"WHERE customer_id=1 AND is_current"
    )
    assert alice_current == 1


def test_scd2_soft_delete_resurrection_creates_new_version(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    """A key reappearing in source after a tombstone gets a fresh version."""
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema, [(1, "a@x.com", "Alice")])
    Cls = _make_target_class(schema_name)
    name = "customers_phase11_resurrect"
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="scd2",
        pipeline_name=name,
        handle_deletes="soft",
    )
    # Tombstone Alice.
    conn.execute(f"DELETE FROM {src_schema}.src_customers WHERE id=1")
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="scd2",
        pipeline_name=name,
        handle_deletes="soft",
    )
    # Resurrect with the same data — a fresh current version is born.
    conn.execute(
        f"INSERT INTO {src_schema}.src_customers VALUES (1, 'a@x.com', 'Alice')"
    )
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="scd2",
        pipeline_name=name,
        handle_deletes="soft",
    )
    current = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_dim "
        f"WHERE customer_id=1 AND is_current"
    )
    assert current == 1
    total = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_dim WHERE customer_id=1"
    )
    assert total == 2  # original (closed) + resurrected (current)


# --- validation -------------------------------------------------------------


def test_handle_deletes_rejects_with_incremental_column(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema, [(1, "a@x.com", "Alice")])
    Cls = _make_target_class(schema_name)
    with pytest.raises(ValueError, match="incremental_column"):
        pipeline.sync(
            target=Cls,
            source=Source.postgres_query(conn, _src_query(src_schema)),
            target_connection=conn,
            mode="merge",
            pipeline_name="customers_phase11_excl_inc",
            handle_deletes="hard",
            incremental_column="customer_id",
        )


def test_handle_deletes_rejects_with_append_mode(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema, [(1, "a@x.com", "Alice")])
    Cls = _make_target_class(schema_name)
    with pytest.raises(ValueError, match="handle_deletes"):
        pipeline.sync(
            target=Cls,
            source=Source.postgres_query(conn, _src_query(src_schema)),
            target_connection=conn,
            mode="append",
            pipeline_name="customers_phase11_excl_append",
            handle_deletes="hard",
        )


def test_handle_deletes_rejects_with_truncate_mode(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema, [(1, "a@x.com", "Alice")])
    Cls = _make_target_class(schema_name)
    with pytest.raises(ValueError, match="handle_deletes"):
        pipeline.sync(
            target=Cls,
            source=Source.postgres_query(conn, _src_query(src_schema)),
            target_connection=conn,
            mode="truncate",
            pipeline_name="customers_phase11_excl_trunc",
            handle_deletes="hard",
        )


def test_invalid_handle_deletes_value_rejected(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema, [(1, "a@x.com", "Alice")])
    Cls = _make_target_class(schema_name)
    with pytest.raises(ValueError, match="handle_deletes"):
        pipeline.sync(
            target=Cls,
            source=Source.postgres_query(conn, _src_query(src_schema)),
            target_connection=conn,
            mode="merge",
            pipeline_name="customers_phase11_invalid",
            handle_deletes="bogus",
        )


def test_soft_merge_not_yet_implemented(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    """Phase 11.5 will land soft-delete for merge (auto-add _is_deleted)."""
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema, [(1, "a@x.com", "Alice")])
    Cls = _make_target_class(schema_name)
    with pytest.raises(NotImplementedError, match="soft"):
        pipeline.sync(
            target=Cls,
            source=Source.postgres_query(conn, _src_query(src_schema)),
            target_connection=conn,
            mode="merge",
            pipeline_name="customers_phase11_soft_merge",
            handle_deletes="soft",
        )


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


def test_cross_db_merge_hard_delete(
    pg_url: str, pg_url_secondary: str, schema_name: str, src_schema: str
) -> None:
    src_conn = _core.connect(pg_url_secondary)
    tgt_conn = _core.connect(pg_url)
    _seed_source(
        src_conn,
        src_schema,
        [(1, "a@x.com", "Alice"), (2, "b@x.com", "Bob")],
    )
    Cls = _make_target_class(schema_name)
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(src_conn, _src_query(src_schema)),
        target_connection=tgt_conn,
        mode="merge",
        pipeline_name="customers_phase11_cross_hard",
        handle_deletes="hard",
    )
    src_conn.execute(f"DELETE FROM {src_schema}.src_customers WHERE id=2")
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(src_conn, _src_query(src_schema)),
        target_connection=tgt_conn,
        mode="merge",
        pipeline_name="customers_phase11_cross_hard",
        handle_deletes="hard",
    )
    count = tgt_conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_dim"
    )
    assert count == 1
