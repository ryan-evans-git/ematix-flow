"""Phase 26 follow-up: parse_timestamp / parse_date auto-detect (Q3.5 γ).

Calling `parse_timestamp()` / `parse_date()` with neither `format=` nor
`formats=` falls back to a curated default catalogue. The catalogue is
tried in declared order — first-match-wins when a value is ambiguous
across formats. Documented behavior; explicit format wins when users
care about disambiguation.

`on_failure="null"` (default) preserves rows that match none of the
catalogue formats.
"""

from datetime import datetime, timezone
from typing import Annotated

import pytest

from ematix_flow import _core, ematix, pipeline as p, pk
from ematix_flow.normalize import (
    DEFAULT_DATE_FORMATS,
    DEFAULT_TIMESTAMP_FORMATS,
    parse_date,
    parse_timestamp,
)
from ematix_flow.types import BigInt, TimestampTZ


# --- unit shape ----------------------------------------------------------


def test_parse_timestamp_no_args_uses_default_catalogue():
    pt = parse_timestamp()
    assert pt.formats == tuple(DEFAULT_TIMESTAMP_FORMATS)


def test_parse_date_no_args_uses_default_catalogue():
    pd = parse_date()
    assert pd.formats == tuple(DEFAULT_DATE_FORMATS)


def test_parse_timestamp_default_catalogue_includes_iso_and_common_us():
    fmts = set(DEFAULT_TIMESTAMP_FORMATS)
    assert "YYYY-MM-DD HH24:MI:SS" in fmts
    assert "YYYY-MM-DDTHH24:MI:SS" in fmts
    assert "YYYY-MM-DD" in fmts


def test_parse_timestamp_no_args_compiles_multi_format_case():
    sql = parse_timestamp().to_sql("ts")
    assert sql.count("WHEN") >= 3
    assert "ELSE NULL" in sql


def test_parse_timestamp_no_args_with_on_failure_default():
    pt = parse_timestamp(
        on_failure="default", default=datetime(2026, 1, 1, tzinfo=timezone.utc)
    )
    sql = pt.to_sql("ts")
    assert sql.startswith("COALESCE(")
    assert "2026-01-01T00:00:00+00:00'::timestamptz" in sql


def test_parse_timestamp_no_args_rejects_on_failure_error():
    with pytest.raises(ValueError, match="single format"):
        parse_timestamp(on_failure="error").to_sql("ts")


# --- end-to-end against Postgres -----------------------------------------


pytestmark_int = pytest.mark.integration


@pytest.mark.integration
def test_parse_timestamp_autodetect_handles_mixed_formats(monkeypatch, pg_url):
    conn = _core.connect(pg_url)
    conn.execute("DROP SCHEMA IF EXISTS warehouse CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS src CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS ematix_flow CASCADE")
    conn.execute("CREATE SCHEMA src")
    conn.execute("CREATE TABLE src.uploads (id BIGINT PRIMARY KEY, ts TEXT)")
    conn.execute(
        "INSERT INTO src.uploads VALUES "
        "(1, '2026-01-15 12:00:00'), "
        "(2, '2026-02-20T08:30:00'), "
        "(3, '2026-03-25'), "
        "(4, 'not a timestamp')"
    )

    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._FEATURE_VIEWS_REGISTRY.clear()

    @ematix.table(schema="warehouse")
    class UploadDim:
        id: Annotated[BigInt, pk()]
        ts: Annotated[TimestampTZ | None, parse_timestamp()]  # no args → auto-detect

    @ematix.pipeline(target=UploadDim, schedule="0 * * * *", mode="merge")
    def sync_uploads_autodetect(c):
        return "SELECT id, ts FROM src.uploads"

    result = sync_uploads_autodetect()
    assert result["rows_inserted"] == 4
    parsed = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM warehouse.upload_dim WHERE ts IS NOT NULL"
    )
    null_row = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM warehouse.upload_dim WHERE id = 4 AND ts IS NULL"
    )
    assert parsed == 3
    assert null_row == 1
