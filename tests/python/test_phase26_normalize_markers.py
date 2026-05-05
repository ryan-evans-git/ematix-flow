"""Phase 26: per-column normalizer markers.

Each marker is a small frozen dataclass with a `to_sql(col)` method
that returns the SQL expression for `col` after this normalizer
applies. Tests below pin every marker in the §3.3 catalogue.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

# --- string ---------------------------------------------------------------


def test_trim_to_sql():
    from ematix_flow.normalize import trim

    assert trim().to_sql("email") == "trim(email)"


def test_lower_to_sql():
    from ematix_flow.normalize import lower

    assert lower().to_sql("email") == "lower(email)"


def test_upper_to_sql():
    from ematix_flow.normalize import upper

    assert upper().to_sql("region") == "upper(region)"


def test_empty_to_null_to_sql():
    from ematix_flow.normalize import empty_to_null

    assert empty_to_null().to_sql("note") == "NULLIF(note, '')"


def test_whitespace_to_null_to_sql():
    from ematix_flow.normalize import whitespace_to_null

    assert whitespace_to_null().to_sql("note") == "NULLIF(trim(note), '')"


def test_replace_to_sql():
    from ematix_flow.normalize import replace

    assert replace("@", "+at+").to_sql("email") == "replace(email, '@', '+at+')"


def test_replace_escapes_quotes():
    from ematix_flow.normalize import replace

    # Single quotes in either argument double up.
    assert replace("'", "''").to_sql("col") == "replace(col, '''', '''''')"


def test_regex_replace_to_sql():
    from ematix_flow.normalize import regex_replace

    sql = regex_replace(r"\s+", " ", flags="g").to_sql("name")
    assert sql == "regexp_replace(name, '\\s+', ' ', 'g')"


def test_regex_replace_default_flags_is_global():
    from ematix_flow.normalize import regex_replace

    sql = regex_replace("a", "b").to_sql("col")
    assert sql == "regexp_replace(col, 'a', 'b', 'g')"


def test_truncate_to_sql():
    from ematix_flow.normalize import truncate

    assert truncate(8).to_sql("region") == "left(region, 8)"


# --- canonicalization -----------------------------------------------------


def test_email_normalize_to_sql():
    from ematix_flow.normalize import email_normalize

    assert email_normalize().to_sql("email") == "lower(trim(email))"


def test_phone_normalize_to_sql():
    from ematix_flow.normalize import phone_normalize

    sql = phone_normalize().to_sql("phone")
    assert sql == "regexp_replace(phone, '[^0-9+]', '', 'g')"


# --- date / time ----------------------------------------------------------


def test_parse_timestamp_single_format():
    from ematix_flow.normalize import parse_timestamp

    sql = parse_timestamp(format="YYYY-MM-DD HH24:MI:SS").to_sql("ts")
    # default on_failure="null"; one format → CASE pre-filtered by regex.
    assert "to_timestamp(ts, 'YYYY-MM-DD HH24:MI:SS')" in sql
    assert "CASE" in sql
    assert "ELSE NULL" in sql


def test_parse_timestamp_multi_format():
    from ematix_flow.normalize import parse_timestamp

    sql = parse_timestamp(formats=["YYYY-MM-DD", "MM/DD/YYYY"]).to_sql("ts")
    # CASE chain tries each format in order.
    assert sql.count("WHEN") == 2
    assert "to_timestamp(ts, 'YYYY-MM-DD')" in sql
    assert "to_timestamp(ts, 'MM/DD/YYYY')" in sql
    assert "ELSE NULL" in sql


def test_parse_timestamp_on_failure_error_uses_raw_to_timestamp():
    from ematix_flow.normalize import parse_timestamp

    sql = parse_timestamp(format="YYYY-MM-DD", on_failure="error").to_sql("ts")
    # error mode skips the regex guard so Postgres raises on a bad value.
    assert sql == "to_timestamp(ts, 'YYYY-MM-DD')"


def test_parse_timestamp_on_failure_default_uses_coalesce():
    from ematix_flow.normalize import parse_timestamp

    sql = parse_timestamp(
        format="YYYY-MM-DD",
        on_failure="default",
        default=datetime(2026, 1, 1, tzinfo=UTC),
    ).to_sql("ts")
    assert sql.startswith("COALESCE(")
    assert "2026-01-01T00:00:00+00:00'::timestamptz" in sql


def test_parse_timestamp_default_without_on_failure_default_raises():
    from ematix_flow.normalize import parse_timestamp

    with pytest.raises(ValueError, match='on_failure="default"'):
        parse_timestamp(format="YYYY-MM-DD", default=datetime(2026, 1, 1))


def test_parse_timestamp_format_and_formats_conflict_raises():
    from ematix_flow.normalize import parse_timestamp

    with pytest.raises(ValueError, match="format"):
        parse_timestamp(format="YYYY-MM-DD", formats=["MM/DD/YYYY"])


def test_parse_timestamp_empty_formats_raises():
    from ematix_flow.normalize import parse_timestamp

    # Empty `formats=[]` is still rejected — that's distinct from
    # passing nothing (which now picks the auto-detect catalogue per
    # Phase 26 follow-up Q3.5 γ).
    with pytest.raises(ValueError, match="format"):
        parse_timestamp(formats=[])


def test_parse_date_single_format():
    from ematix_flow.normalize import parse_date

    sql = parse_date(format="YYYY-MM-DD").to_sql("d")
    assert "to_date(d, 'YYYY-MM-DD')" in sql
    assert "CASE" in sql


def test_to_timezone_to_sql():
    from ematix_flow.normalize import to_timezone

    assert to_timezone("UTC").to_sql("ts") == "ts AT TIME ZONE 'UTC'"


def test_date_trunc_to_sql():
    from ematix_flow.normalize import date_trunc

    assert date_trunc("day").to_sql("ts") == "date_trunc('day', ts)"


# --- numeric --------------------------------------------------------------


def test_parse_int_default_null():
    from ematix_flow.normalize import parse_int

    sql = parse_int().to_sql("col")
    # Regex pre-filter then cast to bigint, NULL otherwise.
    assert "::bigint" in sql
    assert "CASE" in sql
    assert "ELSE NULL" in sql


def test_parse_int_on_failure_error():
    from ematix_flow.normalize import parse_int

    sql = parse_int(on_failure="error").to_sql("col")
    assert sql == "col::bigint"


def test_parse_int_on_failure_default():
    from ematix_flow.normalize import parse_int

    sql = parse_int(on_failure="default", default=0).to_sql("col")
    assert sql.startswith("COALESCE(")
    assert ", 0)" in sql


def test_parse_numeric_default_null():
    from ematix_flow.normalize import parse_numeric

    sql = parse_numeric(12, 2).to_sql("col")
    assert "::numeric(12, 2)" in sql
    assert "CASE" in sql


def test_round_to_sql():
    from ematix_flow.normalize import round as _round

    assert _round(2).to_sql("col") == "round(col, 2)"


def test_clamp_to_sql():
    from ematix_flow.normalize import clamp

    assert clamp(0, 100).to_sql("col") == "least(greatest(col, 0), 100)"


# --- boolean --------------------------------------------------------------


def test_parse_bool_default_lists():
    from ematix_flow.normalize import parse_bool

    sql = parse_bool().to_sql("col")
    assert "lower(col) IN" in sql
    assert "true" in sql.lower()
    assert "false" in sql.lower()


def test_parse_bool_custom_lists():
    from ematix_flow.normalize import parse_bool

    sql = parse_bool(truthy=("y",), falsy=("n",)).to_sql("col")
    # 'y' and 'n' both appear; defaults are NOT used.
    assert "'y'" in sql
    assert "'n'" in sql
    assert "'yes'" not in sql


# --- NULL handling --------------------------------------------------------


def test_default_string_quotes():
    from ematix_flow.normalize import default

    assert default("US").to_sql("region") == "COALESCE(region, 'US')"


def test_default_string_escapes_single_quotes():
    from ematix_flow.normalize import default

    assert default("O'Brien").to_sql("name") == "COALESCE(name, 'O''Brien')"


def test_default_int_inlined():
    from ematix_flow.normalize import default

    assert default(42).to_sql("col") == "COALESCE(col, 42)"


def test_default_float_inlined():
    from ematix_flow.normalize import default

    assert default(3.14).to_sql("col") == "COALESCE(col, 3.14)"


def test_default_bool_lowercase():
    from ematix_flow.normalize import default

    assert default(True).to_sql("col") == "COALESCE(col, true)"
    assert default(False).to_sql("col") == "COALESCE(col, false)"


def test_default_date_with_cast():
    from ematix_flow.normalize import default

    assert default(date(2026, 1, 1)).to_sql("d") == "COALESCE(d, '2026-01-01'::date)"


def test_default_datetime_with_cast():
    from ematix_flow.normalize import default

    sql = default(datetime(2026, 1, 1, tzinfo=UTC)).to_sql("ts")
    assert sql == "COALESCE(ts, '2026-01-01T00:00:00+00:00'::timestamptz)"


def test_default_none_raises():
    from ematix_flow.normalize import default

    with pytest.raises(ValueError, match="T \\| None"):
        default(None)


def test_default_unsupported_type_raises():
    from ematix_flow.normalize import default

    with pytest.raises(ValueError, match="sql"):
        default([1, 2, 3])


def test_nullif_uses_same_quoting():
    from ematix_flow.normalize import nullif

    assert nullif("N/A").to_sql("col") == "NULLIF(col, 'N/A')"


def test_not_null_or_alias():
    from ematix_flow.normalize import default, not_null_or

    # `not_null_or` is documented as a `default` alias.
    assert not_null_or("X").to_sql("col") == default("X").to_sql("col")


# --- escape hatch ---------------------------------------------------------


def test_sql_substitutes_col_placeholder():
    from ematix_flow.normalize import sql

    expr = "CASE WHEN col LIKE '%@x.com' THEN 'x' ELSE col END"
    assert sql(expr).to_sql("email") == expr.replace("col", "email")


# --- derive ---------------------------------------------------------------


def test_derive_marker_holds_expression():
    from ematix_flow.normalize import derive

    d = derive("first_name || ' ' || last_name")
    assert d.expression == "first_name || ' ' || last_name"


# --- chain composition ----------------------------------------------------


def test_compose_chain_left_to_right():
    """Markers compose left-to-right: [lower(), trim()] → trim(lower(col))."""
    from ematix_flow.normalize import compose_chain, lower, trim

    sql = compose_chain([lower(), trim()], "email")
    assert sql == "trim(lower(email))"


def test_compose_chain_skips_pk_natural_key_nullable():
    """Non-normalizer markers are filtered out of the chain."""
    from ematix_flow.markers import natural_key, nullable, pk
    from ematix_flow.normalize import compose_chain, lower, trim

    sql = compose_chain([pk(), lower(), nullable(), trim(), natural_key()], "email")
    assert sql == "trim(lower(email))"


def test_compose_chain_with_derive_at_start():
    from ematix_flow.normalize import compose_chain, derive, trim

    sql = compose_chain(
        [derive("first_name || ' ' || last_name"), trim()], "full_name"
    )
    assert sql == "trim(first_name || ' ' || last_name)"


def test_compose_chain_derive_not_first_raises():
    from ematix_flow.normalize import compose_chain, derive, trim

    with pytest.raises(ValueError, match="derive"):
        compose_chain([trim(), derive("a || b")], "x")


def test_compose_chain_empty_returns_bare_col():
    from ematix_flow.normalize import compose_chain

    assert compose_chain([], "col") == "col"
