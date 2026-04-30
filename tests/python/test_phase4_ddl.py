"""Phase 4: DDL planner — ensure_table creates, matches, or reports drift."""

from __future__ import annotations

import json
import uuid

import pytest

from ematix_flow import _core
from ematix_flow.table import ManagedTable
from ematix_flow.types import BigInt, Boolean, Column, Numeric, String, Text, TimestampTZ

pytestmark = pytest.mark.integration


@pytest.fixture
def schema_name(pg_url: str) -> str:
    """Generate a unique schema per test for isolation."""
    return f"phase4_{uuid.uuid4().hex[:8]}"


def _make_managed_table(schema: str, table: str = "customer_dim") -> type[ManagedTable]:
    class CustomerDim(ManagedTable):
        __schema__ = schema
        __tablename__ = table

        customer_id = Column(BigInt(), nullable=False, primary_key=True)
        email = Column(String(256), nullable=False)
        balance = Column(Numeric(precision=12, scale=2))
        is_active = Column(Boolean(), nullable=False)
        created_at = Column(TimestampTZ(), nullable=False)

    return CustomerDim


def test_ensure_table_creates_from_scratch(pg_url: str, schema_name: str) -> None:
    conn = _core.connect(pg_url)
    cls = _make_managed_table(schema_name)
    result = cls.ensure(conn)
    assert result["action"] == "created"
    # Reflection round-trip: reading back should match the spec.
    count = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM information_schema.tables "
        f"WHERE table_schema = '{schema_name}' AND table_name = 'customer_dim'"
    )
    assert count == 1


def test_ensure_table_matched_on_rerun(pg_url: str, schema_name: str) -> None:
    conn = _core.connect(pg_url)
    cls = _make_managed_table(schema_name)
    cls.ensure(conn)
    result = cls.ensure(conn)
    assert result["action"] == "matched"
    assert result["differences"] == []


def test_ensure_table_errors_on_drift(pg_url: str, schema_name: str) -> None:
    conn = _core.connect(pg_url)
    cls = _make_managed_table(schema_name)
    cls.ensure(conn)
    # Drift: drop a column behind ensure's back.
    conn.execute(f"ALTER TABLE {schema_name}.customer_dim DROP COLUMN balance")
    with pytest.raises(ValueError) as excinfo:
        cls.ensure(conn, on_drift="error")
    assert "balance" in str(excinfo.value)


def test_ensure_table_warns_on_drift(pg_url: str, schema_name: str) -> None:
    conn = _core.connect(pg_url)
    cls = _make_managed_table(schema_name)
    cls.ensure(conn)
    conn.execute(f"ALTER TABLE {schema_name}.customer_dim DROP COLUMN balance")
    result = cls.ensure(conn, on_drift="warn")
    assert result["action"] == "drift"
    assert any("balance" in d for d in result["differences"])


def test_ensure_table_detects_type_drift(pg_url: str, schema_name: str) -> None:
    conn = _core.connect(pg_url)
    cls = _make_managed_table(schema_name)
    cls.ensure(conn)
    # Drift: change email length.
    conn.execute(
        f"ALTER TABLE {schema_name}.customer_dim ALTER COLUMN email TYPE VARCHAR(128)"
    )
    result = cls.ensure(conn, on_drift="warn")
    assert result["action"] == "drift"
    diffs = " ".join(result["differences"])
    assert "email" in diffs


def test_create_table_sql_is_exposed(schema_name: str) -> None:
    cls = _make_managed_table(schema_name)
    sql = _core.create_table_sql(json.dumps(cls._to_spec()))
    assert f"CREATE TABLE {schema_name}.customer_dim" in sql
    assert "BIGINT NOT NULL" in sql
    assert "VARCHAR(256) NOT NULL" in sql
    assert "PRIMARY KEY (customer_id)" in sql


def test_ensure_table_with_text_column(pg_url: str, schema_name: str) -> None:
    """TEXT round-trips through reflection without spurious drift."""

    class T(ManagedTable):
        __schema__ = schema_name
        __tablename__ = "text_only"
        id = Column(BigInt(), nullable=False, primary_key=True)
        notes = Column(Text())

    conn = _core.connect(pg_url)
    T.ensure(conn)
    result = T.ensure(conn)
    assert result["action"] == "matched"


def test_invalid_on_drift_value_raises(pg_url: str, schema_name: str) -> None:
    conn = _core.connect(pg_url)
    cls = _make_managed_table(schema_name)
    with pytest.raises(ValueError, match="on_drift"):
        cls.ensure(conn, on_drift="bogus")
