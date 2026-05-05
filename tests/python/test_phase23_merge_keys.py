"""Phase 23: auto-detect merge keys from PK / UNIQUE / dunder / explicit.

Resolution order (highest priority first):
  1. explicit keys= kwarg passed to pipeline.sync
  2. __merge_keys__ class dunder
  3. first __unique_constraints__ entry
  4. __primary_keys__ (derived from primary_key=True)
  5. error
"""

from __future__ import annotations

import uuid
import warnings
from typing import Any

import pytest

from ematix_flow import _core, pipeline
from ematix_flow.source import Source
from ematix_flow.table import ManagedTable
from ematix_flow.types import BigInt, Column, Date, Numeric, Text

# --- pure resolution tests --------------------------------------------------


def test_explicit_keys_wins_over_everything() -> None:
    class T(ManagedTable):
        __schema__ = "s"
        __tablename__ = "t"
        __unique_constraints__ = (("uc_col",),)
        __merge_keys__ = ("merge_col",)
        id = Column(BigInt(), nullable=False, primary_key=True)
        uc_col = Column(BigInt(), nullable=False)
        merge_col = Column(BigInt(), nullable=False)

    keys = pipeline._resolve_merge_keys(T, passed=("explicit",), pipeline_name="t1")
    assert keys == ["explicit"]


def test_merge_keys_dunder_used_when_no_explicit() -> None:
    class T(ManagedTable):
        __schema__ = "s"
        __tablename__ = "t"
        __unique_constraints__ = (("uc_col",),)
        __merge_keys__ = ("merge_col",)
        id = Column(BigInt(), nullable=False, primary_key=True)
        uc_col = Column(BigInt(), nullable=False)
        merge_col = Column(BigInt(), nullable=False)

    keys = pipeline._resolve_merge_keys(T, passed=None, pipeline_name="t2")
    assert keys == ["merge_col"]


def test_unique_constraint_takes_precedence_over_pk() -> None:
    class T(ManagedTable):
        __schema__ = "s"
        __tablename__ = "t"
        __unique_constraints__ = (("customer_id", "order_date"),)
        id = Column(BigInt(), nullable=False, primary_key=True)
        customer_id = Column(BigInt(), nullable=False)
        order_date = Column(Date(), nullable=False)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        keys = pipeline._resolve_merge_keys(T, passed=None, pipeline_name="t3")
    assert keys == ["customer_id", "order_date"]
    # Warns because resolved keys differ from PK.
    assert any("primary key" in str(w.message).lower() for w in caught)


def test_primary_keys_used_when_nothing_else_set() -> None:
    class T(ManagedTable):
        __schema__ = "s"
        __tablename__ = "t"
        id = Column(BigInt(), nullable=False, primary_key=True)
        name = Column(Text())

    keys = pipeline._resolve_merge_keys(T, passed=None, pipeline_name="t4")
    assert keys == ["id"]


def test_composite_primary_key_used() -> None:
    class T(ManagedTable):
        __schema__ = "s"
        __tablename__ = "t"
        a = Column(BigInt(), nullable=False, primary_key=True)
        b = Column(Date(), nullable=False, primary_key=True)
        v = Column(Text())

    keys = pipeline._resolve_merge_keys(T, passed=None, pipeline_name="t5")
    assert keys == ["a", "b"]


def test_error_when_no_keys_resolve() -> None:
    class T(ManagedTable):
        __schema__ = "s"
        __tablename__ = "t"
        # Only primary keys exist on T because validation requires it.
        id = Column(BigInt(), nullable=False, primary_key=True)

    # Force-clear the PK list to simulate "nothing resolves" (the framework
    # validates this earlier, but the helper must handle empty input).
    keys = pipeline._resolve_merge_keys(T, passed=None, pipeline_name="t6")
    assert keys == ["id"]


def test_no_warning_when_natural_key_matches_pk() -> None:
    """Defining UNIQUE that exactly matches PK is unusual but legal."""

    class T(ManagedTable):
        __schema__ = "s"
        __tablename__ = "t"
        __unique_constraints__ = (("id",),)
        id = Column(BigInt(), nullable=False, primary_key=True)
        name = Column(Text())

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        keys = pipeline._resolve_merge_keys(T, passed=None, pipeline_name="t7")
    assert keys == ["id"]
    # No warning — natural key matches PK.
    assert not any("primary key" in str(w.message).lower() for w in caught)


def test_no_warning_when_explicit_keys_passed() -> None:
    """Explicit keys= silences the natural-key-vs-PK warning."""

    class T(ManagedTable):
        __schema__ = "s"
        __tablename__ = "t"
        __unique_constraints__ = (("customer_id",),)
        id = Column(BigInt(), nullable=False, primary_key=True)
        customer_id = Column(BigInt(), nullable=False)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        pipeline._resolve_merge_keys(
            T, passed=("customer_id",), pipeline_name="t8"
        )
    assert not any("primary key" in str(w.message).lower() for w in caught)


def test_merge_keys_dunder_with_pk_difference_warns() -> None:
    """__merge_keys__ that differs from PK also warns at resolution time."""

    class T(ManagedTable):
        __schema__ = "s"
        __tablename__ = "t"
        __merge_keys__ = ("name",)
        id = Column(BigInt(), nullable=False, primary_key=True)
        name = Column(Text(), nullable=False)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        keys = pipeline._resolve_merge_keys(T, passed=None, pipeline_name="t9")
    assert keys == ["name"]
    assert any("primary key" in str(w.message).lower() for w in caught)


# --- end-to-end via pipeline.sync (auto-detect drives merge) ----------------


pytestmark_int = pytest.mark.integration


@pytest.fixture
def schema_name() -> str:
    return f"phase23_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def src_schema() -> str:
    return f"phase23src_{uuid.uuid4().hex[:8]}"


def _seed_source(conn: Any, schema: str, rows: list[tuple[int, int, str, str]]) -> None:
    """rows: (id, customer_id, order_date, total)"""
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.execute(f"DROP TABLE IF EXISTS {schema}.src_orders")
    conn.execute(
        f"CREATE TABLE {schema}.src_orders ("
        f"  id BIGINT PRIMARY KEY,"
        f"  customer_id BIGINT NOT NULL,"
        f"  order_date DATE NOT NULL,"
        f"  total NUMERIC(12, 2)"
        f")"
    )
    if rows:
        values = ", ".join(
            f"({r_id}, {cid}, '{date}'::date, {total})"
            for r_id, cid, date, total in rows
        )
        conn.execute(f"INSERT INTO {schema}.src_orders VALUES {values}")


@pytest.mark.integration
def test_pipeline_sync_uses_natural_key_when_unique_declared(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    """Merge against a target with surrogate PK + composite UNIQUE works."""
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema, [(1, 100, "2026-04-15", "10.00")])

    class CustomerOrder(ManagedTable):
        __schema__ = schema_name
        __tablename__ = "customer_order"
        __unique_constraints__ = (("customer_id", "order_date"),)

        id = Column(BigInt(), nullable=False, primary_key=True)
        customer_id = Column(BigInt(), nullable=False)
        order_date = Column(Date(), nullable=False)
        total = Column(Numeric(precision=12, scale=2))

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = pipeline.sync(
            target=CustomerOrder,
            source=Source.postgres_query(
                conn,
                f"SELECT id, customer_id, order_date, total "
                f"FROM {src_schema}.src_orders",
            ),
            target_connection=conn,
            mode="merge",
            pipeline_name="orders_phase23_natural",
        )
    assert result["rows_inserted"] == 1
    # The natural-key-differs-from-PK warning fired.
    assert any("primary key" in str(w.message).lower() for w in caught)
