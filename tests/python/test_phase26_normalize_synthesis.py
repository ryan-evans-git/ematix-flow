"""Phase 26: per-column normalizers carried into the table decorator,
plus pipeline-level transforms_pre that compile to CTE-stacked SQL.
"""

from __future__ import annotations

from typing import Annotated

import pytest

from ematix_flow import ematix, pk
from ematix_flow.normalize import (
    apply_normalization,
    deduplicate_by,
    default,
    derive,
    empty_to_null,
    filter_where,
    limit,
    lower,
    parse_timestamp,
    sample_pct,
    trim,
)
from ematix_flow.types import BigInt, String, Text, TimestampTZ

# --- table decorator collects normalizers ---------------------------------


@ematix.table(schema="warehouse")
class CustomerDim:
    customer_id: Annotated[BigInt, pk()]
    email: Annotated[String[256], lower(), trim(), empty_to_null()]
    name: Annotated[Text | None, trim(), empty_to_null()]
    signup_at: Annotated[TimestampTZ, parse_timestamp(format="YYYY-MM-DD")]
    region: Annotated[String[8], default("US")]


def test_table_decorator_collects_normalizers_per_column():
    cols = dict(CustomerDim._columns())
    # Phase 26: Column carries a `.normalizers` tuple of marker objects.
    assert cols["customer_id"].normalizers == ()
    assert len(cols["email"].normalizers) == 3  # lower, trim, empty_to_null
    assert len(cols["name"].normalizers) == 2  # trim, empty_to_null
    assert len(cols["signup_at"].normalizers) == 1
    assert len(cols["region"].normalizers) == 1


def test_table_decorator_filters_pk_natural_key_nullable_from_normalizers():
    """pk() / natural_key() / nullable() never appear in normalizers."""
    from ematix_flow.markers import _NaturalKeyMarker, _NullableMarker, _PkMarker

    cols = dict(CustomerDim._columns())
    for _, col in cols.items():
        for n in col.normalizers:
            assert not isinstance(n, (_PkMarker, _NaturalKeyMarker, _NullableMarker))


# --- source SQL synthesis -------------------------------------------------


def test_apply_normalization_no_normalizers_passthrough():
    """If no column has normalizers and no transforms_pre, return base unchanged."""

    @ematix.table(schema="s")
    class Plain:
        id: Annotated[BigInt, pk()]
        name: Text

    sql = apply_normalization(Plain, "SELECT id, name FROM src.t", transforms_pre=[])
    assert sql == "SELECT id, name FROM src.t"


def test_apply_normalization_wraps_source_with_normalize_cte():
    base = "SELECT customer_id, email, name, signup_at, region FROM src.users"
    sql = apply_normalization(CustomerDim, base, transforms_pre=[])

    # Should use a `_src_user` raw CTE and a `_src_normalized` CTE.
    assert "WITH _src_user AS (" in sql
    assert "_src_normalized AS (" in sql
    # email normalizers compose left-to-right: lower → trim → empty_to_null.
    assert "NULLIF(trim(lower(email)), '') AS email" in sql
    # signup_at gets a CASE chain.
    assert "to_timestamp(signup_at," in sql
    # region default is wrapped.
    assert "COALESCE(region, 'US') AS region" in sql
    # final SELECT pulls from the normalized CTE.
    assert sql.rstrip().endswith("SELECT * FROM _src_normalized")


def test_apply_normalization_with_derive_uses_expression_not_column():
    @ematix.table(schema="s")
    class FullName:
        id: Annotated[BigInt, pk()]
        full_name: Annotated[Text, derive("first_name || ' ' || last_name"), trim()]

    base = "SELECT id, first_name, last_name FROM src.people"
    sql = apply_normalization(FullName, base, transforms_pre=[])
    assert "trim(first_name || ' ' || last_name) AS full_name" in sql


def test_apply_normalization_stacks_transforms_pre_as_ctes():
    base = "SELECT customer_id, email, name, signup_at, region FROM src.users"
    sql = apply_normalization(
        CustomerDim,
        base,
        transforms_pre=[
            deduplicate_by("customer_id", order_by="signup_at DESC"),
            filter_where("region IS NOT NULL"),
        ],
    )
    # Each transform_pre adds a numbered CTE.
    assert "_src_pre_1 AS (" in sql
    assert "_src_pre_2 AS (" in sql
    assert "DISTINCT ON (customer_id)" in sql
    assert "ORDER BY customer_id, signup_at DESC" in sql
    assert "WHERE region IS NOT NULL" in sql
    # Final SELECT pulls from the LAST CTE.
    assert sql.rstrip().endswith("SELECT * FROM _src_pre_2")


def test_apply_normalization_no_normalizers_only_transforms_pre():
    """If column normalizers are empty, transforms_pre still wrap a `_src_user` CTE."""

    @ematix.table(schema="s")
    class Plain:
        id: Annotated[BigInt, pk()]
        name: Text

    base = "SELECT id, name FROM src.t"
    sql = apply_normalization(
        Plain, base, transforms_pre=[filter_where("id > 0")]
    )
    assert "WITH _src_user AS (" in sql
    assert "_src_pre_1 AS (" in sql
    assert "WHERE id > 0" in sql
    assert sql.rstrip().endswith("SELECT * FROM _src_pre_1")


# --- transforms_pre helpers ----------------------------------------------


def test_deduplicate_by_compiles():
    sql = deduplicate_by("a", "b", order_by="ts DESC").to_sql_wrap("inner")
    assert sql == (
        "SELECT DISTINCT ON (a, b) * FROM inner "
        "ORDER BY a, b, ts DESC"
    )


def test_deduplicate_by_no_order_by():
    sql = deduplicate_by("a").to_sql_wrap("inner")
    assert sql == "SELECT DISTINCT ON (a) * FROM inner ORDER BY a"


def test_filter_where_compiles():
    sql = filter_where("region IS NOT NULL").to_sql_wrap("inner")
    assert sql == "SELECT * FROM inner WHERE region IS NOT NULL"


def test_limit_compiles():
    sql = limit(100).to_sql_wrap("inner")
    assert sql == "SELECT * FROM inner LIMIT 100"


def test_sample_pct_compiles():
    sql = sample_pct(0.1).to_sql_wrap("inner")
    assert sql == "SELECT * FROM inner WHERE random() < 0.1"


def test_sample_pct_rejects_out_of_range():
    with pytest.raises(ValueError):
        sample_pct(1.5)
    with pytest.raises(ValueError):
        sample_pct(-0.1)


# --- pipeline decorator accepts transforms_pre ---------------------------


def test_pipeline_decorator_accepts_transforms_pre_kwarg():
    """Smoke test: the decorator should accept transforms_pre=[...] without error."""

    @ematix.pipeline(
        target=CustomerDim,
        schedule="0 * * * *",
        mode="merge",
        transforms_pre=[
            deduplicate_by("customer_id", order_by="signup_at DESC"),
            filter_where("email IS NOT NULL"),
        ],
    )
    def sync_customers(conn):
        return "SELECT customer_id, email, name, signup_at, region FROM src.users"

    # The wrapped fn should expose preview which renders normalized SQL.
    preview = sync_customers.preview()
    src_sql = preview.source_sql
    assert "_src_normalized" in src_sql
    assert "_src_pre_1" in src_sql
    assert "_src_pre_2" in src_sql
