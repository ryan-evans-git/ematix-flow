"""Phase 5: AppendOnly strategy — pipeline.sync end-to-end."""

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
    return f"phase5_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def src_schema() -> str:
    return f"phase5src_{uuid.uuid4().hex[:8]}"


def _seed_source(conn: Any, schema: str, table: str = "src_customers") -> None:
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
    conn.execute(
        f"CREATE TABLE {schema}.{table} ("
        f"  id BIGINT PRIMARY KEY,"
        f"  email VARCHAR(256) NOT NULL,"
        f"  created_at TIMESTAMPTZ NOT NULL"
        f")"
    )
    conn.execute(
        f"INSERT INTO {schema}.{table} VALUES "
        f"(1, 'a@x.com', now()), (2, 'b@x.com', now()), (3, 'c@x.com', now())"
    )


def _make_target_class(schema: str) -> type[ManagedTable]:
    class CustomerDim(ManagedTable):
        __schema__ = schema
        __tablename__ = "customer_dim"

        customer_id = Column(BigInt(), nullable=False, primary_key=True)
        email = Column(String(256), nullable=False)
        created_at = Column(TimestampTZ(), nullable=False)

    return CustomerDim


def test_same_db_append_moves_rows(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema)
    Cls = _make_target_class(schema_name)
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(
            conn,
            f"SELECT id AS customer_id, email, created_at FROM {src_schema}.src_customers",
        ),
        target_connection=conn,
        mode="append",
        pipeline_name="customers_phase5_same_db",
    )
    assert result["status"] == "success"
    assert result["rows_inserted"] == 3
    count = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_dim"
    )
    assert count == 3


def test_same_db_append_populates_metadata_columns(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema)
    Cls = _make_target_class(schema_name)
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(
            conn,
            f"SELECT id AS customer_id, email, created_at FROM {src_schema}.src_customers",
        ),
        target_connection=conn,
        mode="append",
        pipeline_name="customers_phase5_meta",
    )
    null_meta = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_dim "
        f"WHERE _loaded_at IS NULL OR _batch_id IS NULL"
    )
    assert null_meta == 0
    distinct_batches = conn.fetch_scalar_int(
        f"SELECT count(DISTINCT _batch_id)::int FROM {schema_name}.customer_dim"
    )
    assert distinct_batches == 1


def test_second_sync_appends(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    """Two pipeline.sync calls produce two batches; rows accumulate."""
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema)
    Cls = _make_target_class(schema_name)
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(
            conn,
            f"SELECT id AS customer_id, email, created_at "
            f"FROM {src_schema}.src_customers WHERE id <= 3",
        ),
        target_connection=conn,
        mode="append",
        pipeline_name="customers_phase5_double",
    )
    # New rows arrive in the source between syncs.
    conn.execute(
        f"INSERT INTO {src_schema}.src_customers VALUES "
        f"(4, 'd@x.com', now()), (5, 'e@x.com', now()), (6, 'f@x.com', now())"
    )
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(
            conn,
            f"SELECT id AS customer_id, email, created_at "
            f"FROM {src_schema}.src_customers WHERE id > 3",
        ),
        target_connection=conn,
        mode="append",
        pipeline_name="customers_phase5_double",
    )
    count = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_dim"
    )
    assert count == 6
    distinct_batches = conn.fetch_scalar_int(
        f"SELECT count(DISTINCT _batch_id)::int FROM {schema_name}.customer_dim"
    )
    assert distinct_batches == 2


def test_run_history_row_recorded_on_success(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema)
    Cls = _make_target_class(schema_name)
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(
            conn,
            f"SELECT id AS customer_id, email, created_at FROM {src_schema}.src_customers",
        ),
        target_connection=conn,
        mode="append",
        pipeline_name="customers_phase5_history",
    )
    found = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM ematix_flow.run_history "
        f"WHERE run_id = '{result['run_id']}'::uuid AND status = 'success' "
        f"AND pipeline_name = 'customers_phase5_history'"
    )
    assert found == 1


def test_run_history_records_failure(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    """Source schema mismatch fails the load; run_history records 'failed'."""
    conn = _core.connect(pg_url)
    Cls = _make_target_class(schema_name)
    # pipeline.sync handles its own ensure() with the augmented spec; do not
    # pre-ensure here or you'll see drift on the metadata columns.
    with pytest.raises(ValueError):
        pipeline.sync(
            target=Cls,
            source=Source.postgres_query(conn, "SELECT 1 AS bogus"),
            target_connection=conn,
            mode="append",
            pipeline_name="customers_phase5_fail",
        )
    failed = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM ematix_flow.run_history "
        "WHERE pipeline_name = 'customers_phase5_fail' AND status = 'failed'"
    )
    assert failed == 1


# --- cross-DB path: spin up a second container -------------------------------


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


def test_cross_db_append_moves_rows(
    pg_url: str, pg_url_secondary: str, schema_name: str, src_schema: str
) -> None:
    src_conn = _core.connect(pg_url_secondary)
    tgt_conn = _core.connect(pg_url)
    _seed_source(src_conn, src_schema)
    Cls = _make_target_class(schema_name)
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(
            src_conn,
            f"SELECT id AS customer_id, email, created_at FROM {src_schema}.src_customers",
        ),
        target_connection=tgt_conn,
        mode="append",
        pipeline_name="customers_phase5_cross_db",
    )
    assert result["status"] == "success"
    assert result["rows_inserted"] == 3
    assert result["path"] == "cross_db"
    count = tgt_conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_dim"
    )
    assert count == 3
