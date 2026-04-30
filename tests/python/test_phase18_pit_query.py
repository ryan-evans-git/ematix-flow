"""Phase 18: point-in-time query API for FeatureView classes.

`Cls.point_in_time(conn, entity_keys, as_of)` returns the row valid at
`as_of` for the given entity_keys, or None if no version covers that
point. `Cls.historical_features(conn, spine)` does the same query for
each (entity_keys, as_of) in `spine`, returning a list of dicts —
the standard "asof join" idiom for ML feature backfills.

Both walk the SCD2 valid_from/valid_to history; works against any
@ematix.feature_view-decorated class loaded with mode='scd2'.

Requires `pip install ematix-flow[df]` for psycopg2.
"""

from datetime import datetime, timedelta, timezone
from typing import Annotated

import pytest

from ematix_flow import _core, ematix, pipeline as p, pk
from ematix_flow.types import BigInt, Numeric, TimestampTZ

pytestmark = pytest.mark.integration


def _setup_pg(pg_url: str):
    conn = _core.connect(pg_url)
    conn.execute("DROP SCHEMA IF EXISTS features CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS src CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS ematix_flow CASCADE")
    conn.execute("CREATE SCHEMA src")
    conn.execute(
        "CREATE TABLE src.events (user_id BIGINT, score NUMERIC(10,2), ts TIMESTAMPTZ)"
    )
    return conn


def _seed_three_versions(conn, monkeypatch, pg_url):
    """Seed three event-time SCD2 versions at T+0d, T+5d, T+15d."""
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._FEATURE_VIEWS_REGISTRY.clear()

    @ematix.feature_view(
        schema="features",
        feature_version="v1",
        event_timestamp_column="ts",
    )
    class UserFeatures:
        user_id: Annotated[BigInt, pk()]
        score: Numeric[10, 2]
        ts: TimestampTZ

    # Version 1 at 2026-01-01.
    conn.execute("DELETE FROM src.events")
    conn.execute(
        "INSERT INTO src.events VALUES (1, 100, '2026-01-01T00:00:00Z')"
    )

    @ematix.pipeline(target=UserFeatures, schedule="0 * * * *", name="seed_v1")
    def seed_v1(c):
        return "SELECT user_id, score, ts FROM src.events"

    seed_v1()
    p._REGISTRY.pop("seed_v1", None)

    # Version 2 at 2026-01-06 (5d later).
    conn.execute("DELETE FROM src.events")
    conn.execute(
        "INSERT INTO src.events VALUES (1, 200, '2026-01-06T00:00:00Z')"
    )

    @ematix.pipeline(target=UserFeatures, schedule="0 * * * *", name="seed_v2")
    def seed_v2(c):
        return "SELECT user_id, score, ts FROM src.events"

    seed_v2()
    p._REGISTRY.pop("seed_v2", None)

    # Version 3 at 2026-01-16 (15d later).
    conn.execute("DELETE FROM src.events")
    conn.execute(
        "INSERT INTO src.events VALUES (1, 300, '2026-01-16T00:00:00Z')"
    )

    @ematix.pipeline(target=UserFeatures, schedule="0 * * * *", name="seed_v3")
    def seed_v3(c):
        return "SELECT user_id, score, ts FROM src.events"

    seed_v3()
    return UserFeatures


# --- point_in_time --------------------------------------------------------


def test_point_in_time_returns_version_at_as_of(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    UserFeatures = _seed_three_versions(conn, monkeypatch, pg_url)

    # Query between v2 and v3 → should return v2 (score=200).
    as_of = datetime(2026, 1, 10, tzinfo=timezone.utc)
    row = UserFeatures.point_in_time(conn, entity_keys={"user_id": 1}, as_of=as_of)
    assert row is not None
    assert int(row["user_id"]) == 1
    assert float(row["score"]) == 200.0


def test_point_in_time_returns_first_version_at_its_valid_from(
    monkeypatch, pg_url
):
    conn = _setup_pg(pg_url)
    UserFeatures = _seed_three_versions(conn, monkeypatch, pg_url)

    # At v1's valid_from exactly → returns v1.
    row = UserFeatures.point_in_time(
        conn,
        entity_keys={"user_id": 1},
        as_of=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    assert float(row["score"]) == 100.0


def test_point_in_time_returns_none_before_first_version(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    UserFeatures = _seed_three_versions(conn, monkeypatch, pg_url)

    row = UserFeatures.point_in_time(
        conn,
        entity_keys={"user_id": 1},
        as_of=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )
    assert row is None


def test_point_in_time_returns_latest_when_as_of_after_all(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    UserFeatures = _seed_three_versions(conn, monkeypatch, pg_url)

    row = UserFeatures.point_in_time(
        conn,
        entity_keys={"user_id": 1},
        as_of=datetime(2027, 1, 1, tzinfo=timezone.utc),
    )
    assert float(row["score"]) == 300.0


def test_point_in_time_unknown_entity_returns_none(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    UserFeatures = _seed_three_versions(conn, monkeypatch, pg_url)

    row = UserFeatures.point_in_time(
        conn,
        entity_keys={"user_id": 999},
        as_of=datetime(2026, 1, 10, tzinfo=timezone.utc),
    )
    assert row is None


def test_point_in_time_columns_subset(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    UserFeatures = _seed_three_versions(conn, monkeypatch, pg_url)

    row = UserFeatures.point_in_time(
        conn,
        entity_keys={"user_id": 1},
        as_of=datetime(2026, 1, 10, tzinfo=timezone.utc),
        columns=["score"],
    )
    assert "score" in row
    # Other columns shouldn't be in the result.
    assert "ts" not in row


def test_point_in_time_rejects_missing_entity_key(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    UserFeatures = _seed_three_versions(conn, monkeypatch, pg_url)

    with pytest.raises(ValueError, match="user_id"):
        UserFeatures.point_in_time(
            conn,
            entity_keys={},
            as_of=datetime(2026, 1, 10, tzinfo=timezone.utc),
        )


def test_point_in_time_rejects_unknown_key(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    UserFeatures = _seed_three_versions(conn, monkeypatch, pg_url)

    with pytest.raises(ValueError, match="not.*key"):
        UserFeatures.point_in_time(
            conn,
            entity_keys={"user_id": 1, "device_id": "abc"},
            as_of=datetime(2026, 1, 10, tzinfo=timezone.utc),
        )


# --- historical_features --------------------------------------------------


def test_historical_features_lateral_asof_join(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    UserFeatures = _seed_three_versions(conn, monkeypatch, pg_url)

    spine = [
        {"user_id": 1, "as_of": datetime(2026, 1, 3, tzinfo=timezone.utc)},  # v1
        {"user_id": 1, "as_of": datetime(2026, 1, 10, tzinfo=timezone.utc)},  # v2
        {"user_id": 1, "as_of": datetime(2026, 1, 20, tzinfo=timezone.utc)},  # v3
        {"user_id": 999, "as_of": datetime(2026, 1, 10, tzinfo=timezone.utc)},  # none
    ]
    rows = UserFeatures.historical_features(conn, spine=spine, columns=["score"])
    assert len(rows) == 4
    assert float(rows[0]["score"]) == 100.0
    assert float(rows[1]["score"]) == 200.0
    assert float(rows[2]["score"]) == 300.0
    assert rows[3]["score"] is None  # missing entity → NULL


def test_historical_features_empty_spine_returns_empty_list(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    UserFeatures = _seed_three_versions(conn, monkeypatch, pg_url)
    rows = UserFeatures.historical_features(conn, spine=[])
    assert rows == []
