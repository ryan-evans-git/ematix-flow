"""Phase 26: end-to-end normalization against a real Postgres container.

Validates that per-column normalizers and pipeline-level transforms_pre
compile to SQL Postgres actually executes.
"""

from __future__ import annotations

from typing import Annotated

import pytest

from ematix_flow import _core, ematix, pk
from ematix_flow.normalize import (
    deduplicate_by,
    default,
    empty_to_null,
    filter_where,
    lower,
    parse_int,
    parse_timestamp,
    trim,
)
from ematix_flow.types import BigInt, String, Text, TimestampTZ

pytestmark = pytest.mark.integration


def _setup_pg(pg_url: str):
    conn = _core.connect(pg_url)
    conn.execute("DROP SCHEMA IF EXISTS warehouse CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS src CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS ematix_flow CASCADE")
    conn.execute("CREATE SCHEMA src")
    return conn


def test_per_column_normalizers_apply_at_load_time(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    conn.execute(
        """
        CREATE TABLE src.users (
            user_id BIGINT PRIMARY KEY,
            email TEXT,
            name TEXT,
            region TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO src.users VALUES
          (1, '  Alice@Example.COM ', '  Alice  ', 'us'),
          (2, '', NULL, NULL),
          (3, ' bob@x.io', 'Bob', 'CA')
        """
    )

    @ematix.table(schema="warehouse")
    class UserDim:
        user_id: Annotated[BigInt, pk()]
        email: Annotated[String[256] | None, lower(), trim(), empty_to_null()]
        name: Annotated[Text | None, trim(), empty_to_null()]
        region: Annotated[String[8], default("US")]

    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)

    @ematix.pipeline(
        target=UserDim,
        schedule="0 * * * *",
        mode="merge",
    )
    def sync_users(c):
        return "SELECT user_id, email, name, region FROM src.users"

    result = sync_users()
    assert result["rows_inserted"] == 3

    # row 1: trimmed + lowered email; trimmed name; lowercase region preserved
    assert (
        conn.fetch_scalar_int(
            "SELECT count(*)::int FROM warehouse.user_dim "
            "WHERE user_id = 1 AND email = 'alice@example.com' AND name = 'Alice' AND region = 'us'"
        )
        == 1
    )
    # row 2: empty email → NULL; name stays NULL; NULL region defaults to 'US'.
    assert (
        conn.fetch_scalar_int(
            "SELECT count(*)::int FROM warehouse.user_dim "
            "WHERE user_id = 2 AND email IS NULL AND name IS NULL AND region = 'US'"
        )
        == 1
    )
    # row 3
    assert (
        conn.fetch_scalar_int(
            "SELECT count(*)::int FROM warehouse.user_dim "
            "WHERE user_id = 3 AND email = 'bob@x.io' AND name = 'Bob' AND region = 'CA'"
        )
        == 1
    )


def test_transforms_pre_dedup_then_filter(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    conn.execute(
        """
        CREATE TABLE src.events (
            user_id BIGINT,
            email TEXT,
            ts TIMESTAMPTZ
        )
        """
    )
    conn.execute(
        """
        INSERT INTO src.events VALUES
          (1, 'a@x.io',       '2026-01-01T00:00:00Z'),
          (1, 'a-newer@x.io', '2026-02-01T00:00:00Z'),
          (2, NULL,           '2026-01-15T00:00:00Z'),
          (3, 'c@x.io',       '2026-01-10T00:00:00Z')
        """
    )

    @ematix.table(schema="warehouse")
    class EventDim:
        user_id: Annotated[BigInt, pk()]
        email: Annotated[String[256], lower(), trim()]
        ts: TimestampTZ

    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)

    @ematix.pipeline(
        target=EventDim,
        schedule="0 * * * *",
        mode="merge",
        transforms_pre=[
            deduplicate_by("user_id", order_by="ts DESC"),
            filter_where("email IS NOT NULL"),
        ],
    )
    def sync_events(c):
        return "SELECT user_id, email, ts FROM src.events"

    result = sync_events()
    # 1 (latest of two), 3 → 2 rows. user 2 filtered (NULL email).
    assert result["rows_inserted"] == 2
    assert (
        conn.fetch_scalar_int("SELECT count(*)::int FROM warehouse.event_dim") == 2
    )
    assert (
        conn.fetch_scalar_int(
            "SELECT count(*)::int FROM warehouse.event_dim "
            "WHERE user_id = 1 AND email = 'a-newer@x.io'"
        )
        == 1
    )
    assert (
        conn.fetch_scalar_int(
            "SELECT count(*)::int FROM warehouse.event_dim WHERE user_id = 2"
        )
        == 0
    )


def test_parse_timestamp_null_on_failure_preserves_other_rows(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    conn.execute(
        """
        CREATE TABLE src.uploads (
            id BIGINT PRIMARY KEY,
            ts TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO src.uploads VALUES
          (1, '2026-01-15 12:00:00'),
          (2, 'not a timestamp'),
          (3, '2026-02-20 00:30:00')
        """
    )

    @ematix.table(schema="warehouse")
    class UploadDim:
        id: Annotated[BigInt, pk()]
        ts: Annotated[
            TimestampTZ | None,
            parse_timestamp(formats=["YYYY-MM-DD HH24:MI:SS"]),
        ]

    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)

    @ematix.pipeline(
        target=UploadDim,
        schedule="0 * * * *",
        mode="merge",
    )
    def sync_uploads(c):
        return "SELECT id, ts FROM src.uploads"

    result = sync_uploads()
    assert result["rows_inserted"] == 3
    # Bad row 2 lands with NULL ts; rows 1/3 parsed.
    assert (
        conn.fetch_scalar_int(
            "SELECT count(*)::int FROM warehouse.upload_dim WHERE id = 2 AND ts IS NULL"
        )
        == 1
    )
    assert (
        conn.fetch_scalar_int(
            "SELECT count(*)::int FROM warehouse.upload_dim WHERE id IN (1, 3) AND ts IS NOT NULL"
        )
        == 2
    )


def test_parse_int_with_default_on_failure(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    conn.execute(
        """
        CREATE TABLE src.scores (
            id BIGINT PRIMARY KEY,
            score TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO src.scores VALUES
          (1, '42'),
          (2, 'oops'),
          (3, '7')
        """
    )

    @ematix.table(schema="warehouse")
    class ScoreDim:
        id: Annotated[BigInt, pk()]
        score: Annotated[BigInt, parse_int(on_failure="default", default=0)]

    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)

    @ematix.pipeline(
        target=ScoreDim,
        schedule="0 * * * *",
        mode="merge",
    )
    def sync_scores(c):
        return "SELECT id, score FROM src.scores"

    result = sync_scores()
    assert result["rows_inserted"] == 3
    assert (
        conn.fetch_scalar_int(
            "SELECT count(*)::int FROM warehouse.score_dim WHERE id = 2 AND score = 0"
        )
        == 1
    )
    assert (
        conn.fetch_scalar_int(
            "SELECT count(*)::int FROM warehouse.score_dim WHERE id = 1 AND score = 42"
        )
        == 1
    )
