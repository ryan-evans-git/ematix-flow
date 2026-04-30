"""Phase 2: type catalogue + ManagedTable round-trip across Rust↔Python."""

from __future__ import annotations

import pytest

from ematix_flow.table import ManagedTable
from ematix_flow.types import (
    JSON,
    JSONB,
    UUID,
    BigInt,
    Boolean,
    Bytes,
    Column,
    Date,
    Double,
    Float,
    Integer,
    Numeric,
    SmallInt,
    String,
    Text,
    Timestamp,
    TimestampTZ,
)


class CustomerDim(ManagedTable):
    __schema__ = "warehouse"
    __tablename__ = "customer_dim"

    customer_id = Column(BigInt(), nullable=False, primary_key=True)
    email = Column(String(256), nullable=False)
    balance = Column(Numeric(precision=12, scale=2))
    created_at = Column(TimestampTZ(), nullable=False)


def test_columns_collected_in_declaration_order() -> None:
    names = [name for name, _ in CustomerDim._columns()]
    assert names == ["customer_id", "email", "balance", "created_at"]


def test_primary_keys_inferred() -> None:
    assert CustomerDim._primary_keys() == ["customer_id"]


def test_default_nullable_is_true() -> None:
    cols = dict(CustomerDim._columns())
    assert cols["balance"].nullable is True
    assert cols["customer_id"].nullable is False


def test_to_spec_shape() -> None:
    spec = CustomerDim._to_spec()
    assert spec["schema"] == "warehouse"
    assert spec["name"] == "customer_dim"
    assert spec["columns"][0] == {
        "name": "customer_id",
        "type": {"kind": "big_int"},
        "nullable": False,
        "primary_key": True,
    }
    assert spec["columns"][1]["type"] == {"kind": "string", "length": 256}
    assert spec["columns"][2]["type"] == {
        "kind": "numeric",
        "precision": 12,
        "scale": 2,
    }
    assert spec["columns"][3]["type"] == {"kind": "timestamp_tz"}


def test_round_trip_through_rust_adds_fingerprint() -> None:
    spec = CustomerDim._to_spec()
    normalized = CustomerDim._to_normalized_spec()
    assert normalized["schema"] == spec["schema"]
    assert normalized["name"] == spec["name"]
    assert normalized["columns"] == spec["columns"]
    assert isinstance(normalized["fingerprint"], str)
    assert len(normalized["fingerprint"]) == 32


def test_fingerprint_stable_across_identical_declarations() -> None:
    class A(ManagedTable):
        __schema__ = "s"
        __tablename__ = "t"
        id = Column(BigInt(), nullable=False, primary_key=True)
        name = Column(Text())

    class B(ManagedTable):
        __schema__ = "s"
        __tablename__ = "t"
        id = Column(BigInt(), nullable=False, primary_key=True)
        name = Column(Text())

    assert A._to_normalized_spec()["fingerprint"] == B._to_normalized_spec()["fingerprint"]


def test_fingerprint_changes_when_type_changes() -> None:
    class A(ManagedTable):
        __schema__ = "s"
        __tablename__ = "t"
        id = Column(BigInt(), nullable=False, primary_key=True)

    class B(ManagedTable):
        __schema__ = "s"
        __tablename__ = "t"
        id = Column(Integer(), nullable=False, primary_key=True)

    assert A._to_normalized_spec()["fingerprint"] != B._to_normalized_spec()["fingerprint"]


def test_missing_tablename_is_rejected() -> None:
    with pytest.raises(TypeError, match="__tablename__"):

        class Bad(ManagedTable):
            __schema__ = "s"
            id = Column(BigInt(), primary_key=True)


def test_missing_schema_is_rejected() -> None:
    with pytest.raises(TypeError, match="__schema__"):

        class Bad(ManagedTable):
            __tablename__ = "t"
            id = Column(BigInt(), primary_key=True)


def test_zero_columns_is_rejected() -> None:
    with pytest.raises(TypeError, match="Column"):

        class Bad(ManagedTable):
            __schema__ = "s"
            __tablename__ = "t"


def test_no_primary_key_is_rejected_at_round_trip() -> None:
    class NoPk(ManagedTable):
        __schema__ = "s"
        __tablename__ = "t"
        name = Column(Text())

    with pytest.raises(ValueError, match="primary"):
        NoPk._to_normalized_spec()


@pytest.mark.parametrize(
    ("type_instance", "expected"),
    [
        (SmallInt(), {"kind": "small_int"}),
        (Integer(), {"kind": "integer"}),
        (BigInt(), {"kind": "big_int"}),
        (Float(), {"kind": "float"}),
        (Double(), {"kind": "double"}),
        (Numeric(precision=10, scale=4), {"kind": "numeric", "precision": 10, "scale": 4}),
        (Boolean(), {"kind": "boolean"}),
        (Text(), {"kind": "text"}),
        (String(64), {"kind": "string", "length": 64}),
        (Date(), {"kind": "date"}),
        (Timestamp(), {"kind": "timestamp"}),
        (TimestampTZ(), {"kind": "timestamp_tz"}),
        (JSON(), {"kind": "json"}),
        (JSONB(), {"kind": "jsonb"}),
        (UUID(), {"kind": "uuid"}),
        (Bytes(), {"kind": "bytes"}),
    ],
)
def test_full_type_catalogue_serializes(type_instance, expected) -> None:
    assert type_instance.to_spec() == expected
