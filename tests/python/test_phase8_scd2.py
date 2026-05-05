"""Phase 8: SCD2 — pipeline.sync(mode='scd2')."""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from ematix_flow import _core, pipeline
from ematix_flow.source import Source
from ematix_flow.table import ManagedTable
from ematix_flow.types import BigInt, Column, String, Text

pytestmark = pytest.mark.integration


@pytest.fixture
def schema_name() -> str:
    return f"phase8_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def src_schema() -> str:
    return f"phase8src_{uuid.uuid4().hex[:8]}"


def _seed_source(
    conn: Any,
    schema: str,
    rows: list[tuple[int, str, str | None]],
) -> None:
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.execute(f"DROP TABLE IF EXISTS {schema}.src_customers")
    conn.execute(
        f"CREATE TABLE {schema}.src_customers ("
        f"  id BIGINT PRIMARY KEY,"
        f"  email VARCHAR(256) NOT NULL,"
        f"  name TEXT"
        f")"
    )
    if rows:
        def _fmt(i: int, e: str, n: str | None) -> str:
            n_sql = "NULL" if n is None else f"'{n}'"
            return f"({i}, '{e}', {n_sql})"

        values = ", ".join(_fmt(i, e, n) for i, e, n in rows)
        conn.execute(f"INSERT INTO {schema}.src_customers VALUES {values}")


def _make_target_class(schema: str) -> type[ManagedTable]:
    class CustomerDim(ManagedTable):
        __schema__ = schema
        __tablename__ = "customer_dim"

        customer_id = Column(BigInt(), nullable=False, primary_key=True)
        email = Column(String(256), nullable=False)
        name = Column(Text())

    return CustomerDim


def _src_query(src_schema: str) -> str:
    return f"SELECT id AS customer_id, email, name FROM {src_schema}.src_customers"


def _current_count(conn: Any, schema: str, table: str = "customer_dim") -> int:
    return conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema}.{table} WHERE is_current"
    )


def _historical_count(conn: Any, schema: str, table: str = "customer_dim") -> int:
    return conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema}.{table} WHERE NOT is_current"
    )


def test_first_load_creates_one_current_per_key(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(
        conn,
        src_schema,
        [(1, "a@x.com", "Alice"), (2, "b@x.com", "Bob")],
    )
    Cls = _make_target_class(schema_name)
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="scd2",
        pipeline_name="customers_phase8_first",
    )
    assert result["status"] == "success"
    assert result["rows_inserted"] == 2
    assert result["rows_closed"] == 0
    assert _current_count(conn, schema_name) == 2
    assert _historical_count(conn, schema_name) == 0


def test_re_sync_with_no_changes_is_noop(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema, [(1, "a@x.com", "Alice")])
    Cls = _make_target_class(schema_name)
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="scd2",
        pipeline_name="customers_phase8_idem",
    )
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="scd2",
        pipeline_name="customers_phase8_idem",
    )
    assert result["rows_inserted"] == 0
    assert result["rows_closed"] == 0
    assert _current_count(conn, schema_name) == 1
    assert _historical_count(conn, schema_name) == 0


def test_change_compare_column_creates_new_version(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema, [(1, "a@x.com", "Alice")])
    Cls = _make_target_class(schema_name)
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="scd2",
        pipeline_name="customers_phase8_change",
    )
    conn.execute(
        f"UPDATE {src_schema}.src_customers SET email='a2@x.com' WHERE id=1"
    )
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="scd2",
        pipeline_name="customers_phase8_change",
    )
    assert result["rows_inserted"] == 1
    assert result["rows_closed"] == 1
    assert _current_count(conn, schema_name) == 1
    assert _historical_count(conn, schema_name) == 1
    # The current version has the new email; historical row has valid_to set.
    new_email = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_dim "
        f"WHERE customer_id=1 AND is_current AND email='a2@x.com'"
    )
    assert new_email == 1
    closed = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_dim "
        f"WHERE customer_id=1 AND NOT is_current AND email='a@x.com' "
        f"AND valid_to IS NOT NULL"
    )
    assert closed == 1


def test_only_one_current_per_key_after_many_versions(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema, [(1, "a@x.com", "Alice")])
    Cls = _make_target_class(schema_name)
    name = "customers_phase8_invariant"
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="scd2",
        pipeline_name=name,
    )
    for new_email in ("a2@x.com", "a3@x.com", "a4@x.com"):
        conn.execute(
            f"UPDATE {src_schema}.src_customers SET email='{new_email}' WHERE id=1"
        )
        pipeline.sync(
            target=Cls,
            source=Source.postgres_query(conn, _src_query(src_schema)),
            target_connection=conn,
            mode="scd2",
            pipeline_name=name,
        )
    current_for_key = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_dim "
        f"WHERE customer_id=1 AND is_current"
    )
    assert current_for_key == 1
    total_for_key = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_dim WHERE customer_id=1"
    )
    assert total_for_key == 4


def test_null_to_value_detected(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema, [(1, "a@x.com", None)])
    Cls = _make_target_class(schema_name)
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="scd2",
        pipeline_name="customers_phase8_null",
    )
    conn.execute(
        f"UPDATE {src_schema}.src_customers SET name='Alice' WHERE id=1"
    )
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="scd2",
        pipeline_name="customers_phase8_null",
    )
    assert result["rows_inserted"] == 1
    assert result["rows_closed"] == 1


def test_value_to_null_detected(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema, [(1, "a@x.com", "Alice")])
    Cls = _make_target_class(schema_name)
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="scd2",
        pipeline_name="customers_phase8_unnull",
    )
    conn.execute(f"UPDATE {src_schema}.src_customers SET name=NULL WHERE id=1")
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="scd2",
        pipeline_name="customers_phase8_unnull",
    )
    assert result["rows_inserted"] == 1
    assert result["rows_closed"] == 1


def test_run_history_records_scd2_counts(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema, [(1, "a@x.com", "Alice")])
    Cls = _make_target_class(schema_name)
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="scd2",
        pipeline_name="customers_phase8_history",
    )
    found = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM ematix_flow.run_history "
        f"WHERE run_id='{result['run_id']}'::uuid AND mode='scd2' "
        f"AND rows_inserted=1"
    )
    assert found == 1


# --- cross-DB (uses session-scoped pg_url_secondary from conftest) ----------


def test_cross_db_scd2(
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
        mode="scd2",
        pipeline_name="customers_phase8_cross_db",
    )
    assert result["status"] == "success"
    assert result["path"] == "cross_db"
    assert result["rows_inserted"] == 2
    # Update one and re-sync.
    src_conn.execute(
        f"UPDATE {src_schema}.src_customers SET email='a2@x.com' WHERE id=1"
    )
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(src_conn, _src_query(src_schema)),
        target_connection=tgt_conn,
        mode="scd2",
        pipeline_name="customers_phase8_cross_db",
    )
    assert result["rows_inserted"] == 1
    assert result["rows_closed"] == 1
    current = tgt_conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.customer_dim WHERE is_current"
    )
    assert current == 2
