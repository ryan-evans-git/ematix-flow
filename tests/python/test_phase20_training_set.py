"""Phase 20: training-dataset builder.

`ematix.training_set(conn, spine=[...], feature_views=[Cls1, Cls2, ...])`
joins multiple FeatureViews against a shared spine via per-FV LATERAL
asof-joins, returns a polars or pandas DataFrame with spine columns +
feature columns from each FV.

Builds on Phase 18 (per-FV historical_features) but with multi-FV
support and DataFrame output. Requires the `[df]` extra (psycopg2 +
polars or pandas).
"""

from datetime import datetime, timezone
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
        "CREATE TABLE src.user_events (user_id BIGINT, score NUMERIC(10,2), "
        "ts TIMESTAMPTZ)"
    )
    conn.execute(
        "CREATE TABLE src.item_events (item_id BIGINT, popularity NUMERIC(10,2), "
        "ts TIMESTAMPTZ)"
    )
    return conn


def _seed_two_feature_views(conn, monkeypatch, pg_url):
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

    @ematix.feature_view(
        schema="features",
        feature_version="v1",
        event_timestamp_column="ts",
    )
    class ItemFeatures:
        item_id: Annotated[BigInt, pk()]
        popularity: Numeric[10, 2]
        ts: TimestampTZ

    # Seed UserFeatures: v1 at Jan 1 score=100, v2 at Jan 10 score=200.
    conn.execute("INSERT INTO src.user_events VALUES (1, 100, '2026-01-01T00:00:00Z')")

    @ematix.pipeline(target=UserFeatures, schedule="0 * * * *", name="seed_uf_1")
    def seed_uf_1(c):
        return "SELECT user_id, score, ts FROM src.user_events"

    seed_uf_1()
    p._REGISTRY.pop("seed_uf_1", None)

    conn.execute("DELETE FROM src.user_events")
    conn.execute("INSERT INTO src.user_events VALUES (1, 200, '2026-01-10T00:00:00Z')")

    @ematix.pipeline(target=UserFeatures, schedule="0 * * * *", name="seed_uf_2")
    def seed_uf_2(c):
        return "SELECT user_id, score, ts FROM src.user_events"

    seed_uf_2()
    p._REGISTRY.pop("seed_uf_2", None)

    # Seed ItemFeatures: v1 at Jan 5 popularity=10, v2 at Jan 15 popularity=20.
    conn.execute(
        "INSERT INTO src.item_events VALUES (100, 10, '2026-01-05T00:00:00Z')"
    )

    @ematix.pipeline(target=ItemFeatures, schedule="0 * * * *", name="seed_if_1")
    def seed_if_1(c):
        return "SELECT item_id, popularity, ts FROM src.item_events"

    seed_if_1()
    p._REGISTRY.pop("seed_if_1", None)

    conn.execute("DELETE FROM src.item_events")
    conn.execute(
        "INSERT INTO src.item_events VALUES (100, 20, '2026-01-15T00:00:00Z')"
    )

    @ematix.pipeline(target=ItemFeatures, schedule="0 * * * *", name="seed_if_2")
    def seed_if_2(c):
        return "SELECT item_id, popularity, ts FROM src.item_events"

    seed_if_2()
    return UserFeatures, ItemFeatures


# --- training_set: polars / pandas / dict ---------------------------------


def test_training_set_polars_default(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    UserFeatures, ItemFeatures = _seed_two_feature_views(conn, monkeypatch, pg_url)

    spine = [
        {"user_id": 1, "item_id": 100, "as_of": datetime(2026, 1, 7, tzinfo=timezone.utc)},  # uf=100, if=10
        {"user_id": 1, "item_id": 100, "as_of": datetime(2026, 1, 12, tzinfo=timezone.utc)},  # uf=200, if=10
        {"user_id": 1, "item_id": 100, "as_of": datetime(2026, 1, 20, tzinfo=timezone.utc)},  # uf=200, if=20
    ]
    df = ematix.training_set(
        conn,
        spine=spine,
        feature_views=[UserFeatures, ItemFeatures],
        columns={UserFeatures: ["score"], ItemFeatures: ["popularity"]},
    )
    import polars as pl

    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] == 3
    # Score / popularity columns present.
    assert "score" in df.columns
    assert "popularity" in df.columns
    scores = df["score"].to_list()
    pops = df["popularity"].to_list()
    assert [float(s) for s in scores] == [100.0, 200.0, 200.0]
    assert [float(p) for p in pops] == [10.0, 10.0, 20.0]


def test_training_set_pandas(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    UserFeatures, ItemFeatures = _seed_two_feature_views(conn, monkeypatch, pg_url)

    spine = [
        {"user_id": 1, "item_id": 100, "as_of": datetime(2026, 1, 7, tzinfo=timezone.utc)},
    ]
    df = ematix.training_set(
        conn,
        spine=spine,
        feature_views=[UserFeatures, ItemFeatures],
        columns={UserFeatures: ["score"], ItemFeatures: ["popularity"]},
        prefer="pandas",
    )
    import pandas as pd

    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 5)  # user_id, item_id, as_of, score, popularity


def test_training_set_disambiguates_collisions(monkeypatch, pg_url):
    """When two FVs both expose a 'ts' column, the framework prefixes."""
    conn = _setup_pg(pg_url)
    UserFeatures, ItemFeatures = _seed_two_feature_views(conn, monkeypatch, pg_url)

    spine = [
        {"user_id": 1, "item_id": 100, "as_of": datetime(2026, 1, 12, tzinfo=timezone.utc)},
    ]
    df = ematix.training_set(
        conn,
        spine=spine,
        feature_views=[UserFeatures, ItemFeatures],
        # Both views expose ts → must prefix.
        columns={UserFeatures: ["ts"], ItemFeatures: ["ts"]},
        prefer="polars",
    )
    cols = df.columns
    # The two `ts` columns become `<table>__ts`.
    assert "user_features__ts" in cols
    assert "item_features__ts" in cols


def test_training_set_empty_spine(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    UserFeatures, _ = _seed_two_feature_views(conn, monkeypatch, pg_url)

    df = ematix.training_set(
        conn, spine=[], feature_views=[UserFeatures]
    )
    import polars as pl

    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] == 0


def test_training_set_single_fv_uses_all_non_meta_columns_by_default(
    monkeypatch, pg_url
):
    conn = _setup_pg(pg_url)
    UserFeatures, _ = _seed_two_feature_views(conn, monkeypatch, pg_url)

    spine = [
        {"user_id": 1, "as_of": datetime(2026, 1, 12, tzinfo=timezone.utc)},
    ]
    df = ematix.training_set(
        conn, spine=spine, feature_views=[UserFeatures]
    )
    # Default columns include user_id (entity key drops out — already in spine),
    # score, ts. Metadata cols should be excluded.
    assert "score" in df.columns
    assert "ts" in df.columns
    assert "valid_from" not in df.columns
    assert "_loaded_at" not in df.columns


def test_training_set_rejects_fv_without_matching_entity_keys_in_spine(
    monkeypatch, pg_url
):
    conn = _setup_pg(pg_url)
    UserFeatures, ItemFeatures = _seed_two_feature_views(conn, monkeypatch, pg_url)

    # Spine has user_id but not item_id → ItemFeatures can't join.
    spine = [
        {"user_id": 1, "as_of": datetime(2026, 1, 12, tzinfo=timezone.utc)},
    ]
    with pytest.raises(ValueError, match="item_id"):
        ematix.training_set(
            conn, spine=spine, feature_views=[ItemFeatures]
        )


def test_training_set_no_feature_views_raises(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    _seed_two_feature_views(conn, monkeypatch, pg_url)

    with pytest.raises(ValueError, match="feature_views"):
        ematix.training_set(
            conn, spine=[{"x": 1, "as_of": datetime(2026, 1, 1, tzinfo=timezone.utc)}],
            feature_views=[],
        )


def test_training_set_rejects_non_feature_view(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    _seed_two_feature_views(conn, monkeypatch, pg_url)

    @ematix.table(schema="features")
    class PlainTable:
        id: Annotated[BigInt, pk()]

    with pytest.raises(TypeError, match="FeatureView"):
        ematix.training_set(
            conn,
            spine=[{"id": 1, "as_of": datetime(2026, 1, 1, tzinfo=timezone.utc)}],
            feature_views=[PlainTable],
        )
