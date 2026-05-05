"""Phase 22: __unique_constraints__ dunder + DDL + drift round-trip."""

from __future__ import annotations

import uuid

import pytest

from ematix_flow import _core
from ematix_flow.table import ManagedTable
from ematix_flow.types import BigInt, Column, Date, Numeric, Text

# --- pure (no DB) ----------------------------------------------------------


def test_table_with_no_unique_constraints_serializes_with_empty_list() -> None:
    class T(ManagedTable):
        __schema__ = "s"
        __tablename__ = "t"
        id = Column(BigInt(), nullable=False, primary_key=True)
        name = Column(Text())

    spec = T._to_spec()
    assert spec.get("unique_constraints", []) == []


def test_unique_constraints_dunder_passes_through_to_spec() -> None:
    class T(ManagedTable):
        __schema__ = "s"
        __tablename__ = "t"
        __unique_constraints__ = (("customer_id", "order_date"),)
        id = Column(BigInt(), nullable=False, primary_key=True)
        customer_id = Column(BigInt(), nullable=False)
        order_date = Column(Date(), nullable=False)

    spec = T._to_spec()
    assert spec["unique_constraints"] == [["customer_id", "order_date"]]


def test_unique_constraints_round_trip_through_rust() -> None:
    class T(ManagedTable):
        __schema__ = "s"
        __tablename__ = "t"
        __unique_constraints__ = (("customer_id", "order_date"),)
        id = Column(BigInt(), nullable=False, primary_key=True)
        customer_id = Column(BigInt(), nullable=False)
        order_date = Column(Date(), nullable=False)

    normalized = T._to_normalized_spec()
    assert normalized["unique_constraints"] == [["customer_id", "order_date"]]
    assert "fingerprint" in normalized


def test_unique_constraint_fingerprint_changes_when_constraint_added() -> None:
    class A(ManagedTable):
        __schema__ = "s"
        __tablename__ = "t"
        id = Column(BigInt(), nullable=False, primary_key=True)
        x = Column(Text())
        y = Column(Text())

    class B(ManagedTable):
        __schema__ = "s"
        __tablename__ = "t"
        __unique_constraints__ = (("x", "y"),)
        id = Column(BigInt(), nullable=False, primary_key=True)
        x = Column(Text())
        y = Column(Text())

    fa = A._to_normalized_spec()["fingerprint"]
    fb = B._to_normalized_spec()["fingerprint"]
    assert fa != fb


def test_unique_constraint_referencing_unknown_column_rejected() -> None:
    class T(ManagedTable):
        __schema__ = "s"
        __tablename__ = "t"
        __unique_constraints__ = (("nonexistent",),)
        id = Column(BigInt(), nullable=False, primary_key=True)

    with pytest.raises(ValueError, match="nonexistent"):
        T._to_normalized_spec()


# --- integration (live Postgres) ------------------------------------------


pytestmark_int = pytest.mark.integration


@pytest.fixture
def schema_name() -> str:
    return f"phase22_{uuid.uuid4().hex[:8]}"


def _make_target_class(schema: str) -> type[ManagedTable]:
    class CustomerOrder(ManagedTable):
        __schema__ = schema
        __tablename__ = "customer_order"
        __unique_constraints__ = (("customer_id", "order_date"),)

        id = Column(BigInt(), nullable=False, primary_key=True)
        customer_id = Column(BigInt(), nullable=False)
        order_date = Column(Date(), nullable=False)
        total = Column(Numeric(precision=12, scale=2))

    return CustomerOrder


@pytest.mark.integration
def test_ensure_creates_table_with_unique_clause(
    pg_url: str, schema_name: str
) -> None:
    conn = _core.connect(pg_url)
    Cls = _make_target_class(schema_name)
    result = Cls.ensure(conn)
    assert result["action"] == "created"
    # The UNIQUE constraint exists in information_schema.
    found = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM information_schema.table_constraints "
        f"WHERE table_schema = '{schema_name}' "
        f"AND table_name = 'customer_order' "
        f"AND constraint_type = 'UNIQUE'"
    )
    assert found == 1


@pytest.mark.integration
def test_ensure_re_run_matches_with_unique_constraint(
    pg_url: str, schema_name: str
) -> None:
    conn = _core.connect(pg_url)
    Cls = _make_target_class(schema_name)
    Cls.ensure(conn)
    result = Cls.ensure(conn)
    assert result["action"] == "matched"
    assert result["differences"] == []


@pytest.mark.integration
def test_external_alter_drops_unique_detected_as_drift(
    pg_url: str, schema_name: str
) -> None:
    conn = _core.connect(pg_url)
    Cls = _make_target_class(schema_name)
    Cls.ensure(conn)
    # Drop the UNIQUE constraint behind ensure's back.
    constraint_name = conn.execute(
        f"DO $$ "
        f"DECLARE cn text; "
        f"BEGIN "
        f"  SELECT constraint_name INTO cn FROM information_schema.table_constraints "
        f"  WHERE table_schema = '{schema_name}' "
        f"  AND table_name = 'customer_order' "
        f"  AND constraint_type = 'UNIQUE'; "
        f"  EXECUTE format('ALTER TABLE {schema_name}.customer_order DROP CONSTRAINT %I', cn); "
        f"END $$"
    )
    with pytest.raises(ValueError) as excinfo:
        Cls.ensure(conn, on_drift="error")
    msg = str(excinfo.value)
    assert "customer_id" in msg
    assert "order_date" in msg


@pytest.mark.integration
def test_external_alter_adds_unique_detected_as_extra(
    pg_url: str, schema_name: str
) -> None:
    """Drift surfaces when the live DB has a UNIQUE we didn't declare."""
    conn = _core.connect(pg_url)

    class T(ManagedTable):
        __schema__ = schema_name
        __tablename__ = "no_unique"
        id = Column(BigInt(), nullable=False, primary_key=True)
        name = Column(Text())

    T.ensure(conn)
    conn.execute(
        f"ALTER TABLE {schema_name}.no_unique ADD CONSTRAINT extra_unique UNIQUE (name)"
    )
    result = T.ensure(conn, on_drift="warn")
    assert result["action"] == "drift"
    assert any("name" in d for d in result["differences"])
