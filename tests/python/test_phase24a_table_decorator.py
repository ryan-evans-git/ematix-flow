"""Phase 24a: `@ematix.table` class decorator.

Reads PEP 593 `Annotated[T, marker()]` annotations, `T | None` for
nullability, and `pk()` / `natural_key()` / `nullable()` markers.
Produces a class that behaves identically to the imperative
`class X(ManagedTable)` form.
"""

from __future__ import annotations

from typing import Annotated

import pytest

from ematix_flow import ematix, natural_key, pk
from ematix_flow.table import ManagedTable
from ematix_flow.types import BigInt, Boolean, Column, Date, Numeric, String, Text

# --- basic shape ------------------------------------------------------------


def test_decorator_produces_managed_table_subclass() -> None:
    @ematix.table(schema="warehouse")
    class CustomerDim:
        customer_id: Annotated[BigInt, pk()]
        email: String[256]

    assert issubclass(CustomerDim, ManagedTable)
    assert CustomerDim.__schema__ == "warehouse"
    assert CustomerDim.__tablename__ == "customer_dim"  # snake-cased class name


def test_decorator_explicit_name_overrides_class_name() -> None:
    @ematix.table(schema="s", name="my_table_v2")
    class MyTable:
        id: Annotated[BigInt, pk()]

    assert MyTable.__tablename__ == "my_table_v2"


def test_decorator_collects_columns_in_declaration_order() -> None:
    @ematix.table(schema="s")
    class T:
        a: Annotated[BigInt, pk()]
        b: String[64]
        c: Text | None

    names = [name for name, _ in T._columns()]
    assert names == ["a", "b", "c"]


# --- nullability ------------------------------------------------------------


def test_t_or_none_infers_nullable_true() -> None:
    @ematix.table(schema="s")
    class T:
        id: Annotated[BigInt, pk()]
        name: Text | None
        balance: Numeric[12, 2] | None

    cols = dict(T._columns())
    assert cols["id"].nullable is False
    assert cols["name"].nullable is True
    assert cols["balance"].nullable is True


def test_default_nullability_is_false() -> None:
    @ematix.table(schema="s")
    class T:
        id: Annotated[BigInt, pk()]
        email: String[256]

    cols = dict(T._columns())
    assert cols["email"].nullable is False


# --- pk + natural_key -------------------------------------------------------


def test_pk_marker_collected_into_primary_keys() -> None:
    @ematix.table(schema="s")
    class T:
        id: Annotated[BigInt, pk()]
        name: Text

    assert T._primary_keys() == ["id"]


def test_composite_pk_collected_in_declaration_order() -> None:
    @ematix.table(schema="s")
    class T:
        a: Annotated[BigInt, pk()]
        b: Annotated[Date, pk()]
        v: Text

    assert T._primary_keys() == ["a", "b"]


def test_natural_key_default_group_collected_into_unique_constraints() -> None:
    @ematix.table(schema="s")
    class T:
        id: Annotated[BigInt, pk()]
        customer_id: Annotated[BigInt, natural_key()]
        order_date: Annotated[Date, natural_key()]
        total: Numeric[12, 2]

    constraints = list(T.__unique_constraints__)
    assert constraints == [("customer_id", "order_date")]


def test_natural_key_grouped_into_separate_constraints() -> None:
    @ematix.table(schema="s")
    class T:
        id: Annotated[BigInt, pk()]
        a: Annotated[Text, natural_key()]
        b: Annotated[Text, natural_key()]
        c: Annotated[Text, natural_key("legacy")]
        d: Annotated[Text, natural_key("legacy")]

    constraints = list(T.__unique_constraints__)
    # Two distinct unique constraints; group "" (default) and "legacy".
    assert len(constraints) == 2
    constraint_set = {tuple(c) for c in constraints}
    assert ("a", "b") in constraint_set
    assert ("c", "d") in constraint_set


# --- round trip -------------------------------------------------------------


def test_decorated_class_round_trips_through_rust() -> None:
    @ematix.table(schema="warehouse")
    class CustomerDim:
        customer_id: Annotated[BigInt, pk()]
        email: String[256]
        name: Text | None
        is_active: Boolean

    normalized = CustomerDim._to_normalized_spec()
    assert normalized["schema"] == "warehouse"
    assert normalized["name"] == "customer_dim"
    column_names = [c["name"] for c in normalized["columns"]]
    assert column_names == ["customer_id", "email", "name", "is_active"]


def test_decorated_class_matches_imperative_equivalent() -> None:
    @ematix.table(schema="s", name="t")
    class Decorated:
        customer_id: Annotated[BigInt, pk()]
        email: String[256]
        name: Text | None

    class Imperative(ManagedTable):
        __schema__ = "s"
        __tablename__ = "t"
        customer_id = Column(BigInt(), nullable=False, primary_key=True)
        email = Column(String(256), nullable=False)
        name = Column(Text(), nullable=True)

    # Both produce the same canonical spec (up to ordering / fingerprint).
    a = Decorated._to_normalized_spec()
    b = Imperative._to_normalized_spec()
    assert a["schema"] == b["schema"]
    assert a["name"] == b["name"]
    assert a["columns"] == b["columns"]
    assert a["fingerprint"] == b["fingerprint"]


# --- mutual exclusion -------------------------------------------------------


def test_dataclass_stacking_rejected() -> None:
    from dataclasses import dataclass

    with pytest.raises(TypeError, match="dataclass"):

        @ematix.table(schema="s")
        @dataclass
        class T:
            id: Annotated[BigInt, pk()]


def test_pydantic_basemodel_stacking_rejected() -> None:
    pydantic = pytest.importorskip("pydantic")

    with pytest.raises(TypeError, match="pydantic|BaseModel"):

        @ematix.table(schema="s")
        class T(pydantic.BaseModel):
            id: Annotated[BigInt, pk()]


# --- error paths ------------------------------------------------------------


def test_missing_pk_or_natural_key_raises_at_use() -> None:
    """Tables need at least one PK to satisfy the Rust validator. The
    decorator itself doesn't enforce this — Rust does on round-trip."""

    @ematix.table(schema="s")
    class T:
        name: Text  # no pk(), no natural_key() — round-trip will fail

    with pytest.raises(ValueError):
        T._to_normalized_spec()


def test_schema_kwarg_required() -> None:
    with pytest.raises(TypeError):

        @ematix.table()
        class T:
            id: Annotated[BigInt, pk()]
