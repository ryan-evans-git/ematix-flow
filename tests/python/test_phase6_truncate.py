"""Phase 6: TruncateReplace — replace target rows atomically each sync."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from typing import Any

import pytest

from ematix_flow import _core, pipeline
from ematix_flow.source import Source
from ematix_flow.table import ManagedTable
from ematix_flow.types import BigInt, Column, String, TimestampTZ

pytestmark = pytest.mark.integration


@pytest.fixture
def schema_name() -> str:
    return f"phase6_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def src_schema() -> str:
    return f"phase6src_{uuid.uuid4().hex[:8]}"


def _seed_source(conn: Any, schema: str, ids: list[int]) -> None:
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.execute(f"DROP TABLE IF EXISTS {schema}.src_customers")
    conn.execute(
        f"CREATE TABLE {schema}.src_customers ("
        f"  id BIGINT PRIMARY KEY,"
        f"  email VARCHAR(256) NOT NULL,"
        f"  created_at TIMESTAMPTZ NOT NULL"
        f")"
    )
    values = ", ".join(f"({i}, 'u{i}@x.com', now())" for i in ids)
    conn.execute(f"INSERT INTO {schema}.src_customers VALUES {values}")


def _make_target_class(schema: str) -> type[ManagedTable]:
    class CustomerSnapshot(ManagedTable):
        __schema__ = schema
        __tablename__ = "customer_snapshot"

        customer_id = Column(BigInt(), nullable=False, primary_key=True)
        email = Column(String(256), nullable=False)
        created_at = Column(TimestampTZ(), nullable=False)

    return CustomerSnapshot


def test_truncate_replaces_existing_rows(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema, [1, 2, 3])
    Cls = _make_target_class(schema_name)
    src_query = (
        f"SELECT id AS customer_id, email, created_at FROM {src_schema}.src_customers"
    )
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, src_query),
        target_connection=conn,
        mode="truncate",
        pipeline_name="customers_phase6_replace",
    )
    # Replace source contents and re-sync.
    _seed_source(conn, src_schema, [10, 11])
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, src_query),
        target_connection=conn,
        mode="truncate",
        pipeline_name="customers_phase6_replace",
    )
    assert result["status"] == "success"
    assert result["rows_inserted"] == 2
    count = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_snapshot"
    )
    assert count == 2
    ids = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_snapshot "
        f"WHERE customer_id IN (10, 11)"
    )
    assert ids == 2


def test_truncate_is_atomic_on_failure(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    """If the INSERT fails, the TRUNCATE rolls back — old rows survive."""
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema, [1, 2, 3])
    Cls = _make_target_class(schema_name)
    src_query = (
        f"SELECT id AS customer_id, email, created_at FROM {src_schema}.src_customers"
    )
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, src_query),
        target_connection=conn,
        mode="truncate",
        pipeline_name="customers_phase6_atomic",
    )
    pre_count = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_snapshot"
    )
    assert pre_count == 3

    # Force failure: column mismatch in the source.
    with pytest.raises(ValueError):
        pipeline.sync(
            target=Cls,
            source=Source.postgres_query(conn, "SELECT 1 AS bogus"),
            target_connection=conn,
            mode="truncate",
            pipeline_name="customers_phase6_atomic",
        )
    post_count = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_snapshot"
    )
    assert post_count == 3, "TRUNCATE must roll back when INSERT fails"


def test_truncate_records_run_history_with_correct_mode(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema, [1, 2])
    Cls = _make_target_class(schema_name)
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(
            conn,
            f"SELECT id AS customer_id, email, created_at FROM {src_schema}.src_customers",
        ),
        target_connection=conn,
        mode="truncate",
        pipeline_name="customers_phase6_history",
    )
    found = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM ematix_flow.run_history "
        f"WHERE run_id = '{result['run_id']}'::uuid AND mode = 'truncate'"
    )
    assert found == 1


def test_truncate_with_empty_source_clears_target(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema, [1, 2, 3])
    Cls = _make_target_class(schema_name)
    src_query = (
        f"SELECT id AS customer_id, email, created_at FROM {src_schema}.src_customers"
    )
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, src_query),
        target_connection=conn,
        mode="truncate",
        pipeline_name="customers_phase6_empty",
    )
    # Empty source.
    conn.execute(f"DELETE FROM {src_schema}.src_customers")
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, src_query),
        target_connection=conn,
        mode="truncate",
        pipeline_name="customers_phase6_empty",
    )
    assert result["rows_inserted"] == 0
    count = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_snapshot"
    )
    assert count == 0


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


def test_cross_db_truncate_replaces(
    pg_url: str, pg_url_secondary: str, schema_name: str, src_schema: str
) -> None:
    src_conn = _core.connect(pg_url_secondary)
    tgt_conn = _core.connect(pg_url)
    _seed_source(src_conn, src_schema, [1, 2, 3])
    Cls = _make_target_class(schema_name)
    src_query = (
        f"SELECT id AS customer_id, email, created_at FROM {src_schema}.src_customers"
    )
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(src_conn, src_query),
        target_connection=tgt_conn,
        mode="truncate",
        pipeline_name="customers_phase6_cross_db",
    )
    _seed_source(src_conn, src_schema, [99])
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(src_conn, src_query),
        target_connection=tgt_conn,
        mode="truncate",
        pipeline_name="customers_phase6_cross_db",
    )
    assert result["status"] == "success"
    assert result["path"] == "cross_db"
    assert result["rows_inserted"] == 1
    count = tgt_conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_snapshot"
    )
    assert count == 1
