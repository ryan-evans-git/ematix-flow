"""Phase 17: FeatureView declarative class.

`@ematix.feature_view(...)` decorates a class declaring entity keys +
feature columns + ML-specific dunders (feature_version, ttl,
event_timestamp_column, freshness_sla, online, description, owner).
The result is a `ManagedTable` subclass with `is_feature_view=True` so
`pipeline.sync` defaults `mode='scd2'` and inherits the class-level
ttl / event_timestamp_column when the user doesn't override them.

Each FeatureView is registered in a process-local registry. On first
sync against a live database, an UPSERT to `ematix_flow.feature_views`
records the metadata.
"""

from datetime import timedelta
from typing import Annotated

import pytest

from ematix_flow import _core, ematix, pk
from ematix_flow import pipeline as p
from ematix_flow.types import BigInt, Numeric, TimestampTZ

# --- unit shape ------------------------------------------------------------


def test_feature_view_decorator_attaches_dunders():
    @ematix.feature_view(
        schema="features",
        feature_version="v1",
        event_timestamp_column="last_seen",
        ttl=timedelta(days=30),
        freshness_sla=timedelta(hours=4),
        online=True,
        description="Per-user signals",
        owner="ml@example.com",
    )
    class UserFeatures:
        user_id: Annotated[BigInt, pk()]
        score: Numeric[10, 2]
        last_seen: TimestampTZ

    assert UserFeatures.__feature_version__ == "v1"
    assert UserFeatures.__event_timestamp_column__ == "last_seen"
    assert UserFeatures.__ttl__ == timedelta(days=30)
    assert UserFeatures.__freshness_sla__ == timedelta(hours=4)
    assert UserFeatures.__online__ is True
    assert UserFeatures.__description__ == "Per-user signals"
    assert UserFeatures.__owner__ == "ml@example.com"
    assert UserFeatures.__is_feature_view__ is True


def test_feature_view_minimal_decoration_just_schema_and_version():
    @ematix.feature_view(schema="features", feature_version="v1")
    class Minimal:
        user_id: Annotated[BigInt, pk()]
        signal: BigInt

    assert Minimal.__feature_version__ == "v1"
    assert Minimal.__event_timestamp_column__ is None
    assert Minimal.__ttl__ is None
    assert Minimal.__online__ is False


def test_feature_view_requires_at_least_one_pk():
    with pytest.raises(TypeError, match="primary key"):

        @ematix.feature_view(schema="features", feature_version="v1")
        class NoPK:
            x: BigInt


def test_feature_view_registers_in_registry():
    p._FEATURE_VIEWS_REGISTRY.clear()

    @ematix.feature_view(schema="features", feature_version="v1", name="reg_test_fv")
    class RegTest:
        id: Annotated[BigInt, pk()]
        score: BigInt

    assert "features.reg_test_fv.v1" in p._FEATURE_VIEWS_REGISTRY
    fv = p._FEATURE_VIEWS_REGISTRY["features.reg_test_fv.v1"]
    assert fv.entity_keys == ["id"]
    assert fv.schema == "features"
    assert fv.tablename == "reg_test_fv"
    assert fv.feature_version == "v1"


def test_feature_view_event_timestamp_column_must_exist():
    with pytest.raises(ValueError, match="not_a_column"):

        @ematix.feature_view(
            schema="features",
            feature_version="v1",
            event_timestamp_column="not_a_column",
        )
        class Bad:
            id: Annotated[BigInt, pk()]
            real_col: BigInt


def test_feature_view_ttl_requires_event_timestamp_column_optional():
    """ttl is allowed without event_timestamp_column — applies to load-time valid_from."""

    @ematix.feature_view(
        schema="features",
        feature_version="v1",
        ttl=timedelta(days=7),
    )
    class TTLOnly:
        id: Annotated[BigInt, pk()]
        score: BigInt

    assert TTLOnly.__ttl__ == timedelta(days=7)


# --- pipeline.sync inheritance --------------------------------------------


pytestmark_int = pytest.mark.integration


def _setup_pg(pg_url: str):
    conn = _core.connect(pg_url)
    conn.execute("DROP SCHEMA IF EXISTS features CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS src CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS ematix_flow CASCADE")
    conn.execute("CREATE SCHEMA src")
    conn.execute("CREATE TABLE src.users (user_id BIGINT PRIMARY KEY, score INT)")
    conn.execute("INSERT INTO src.users VALUES (1, 10), (2, 20)")
    return conn


@pytest.mark.integration
def test_feature_view_pipeline_defaults_to_scd2(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._FEATURE_VIEWS_REGISTRY.clear()

    @ematix.feature_view(schema="features", feature_version="v1")
    class UserFeatures:
        user_id: Annotated[BigInt, pk()]
        score: BigInt

    # No mode= given. FeatureView default is scd2.
    @ematix.pipeline(
        target=UserFeatures,
        schedule="0 * * * *",
    )
    def sync_users(c):
        return "SELECT user_id, score FROM src.users"

    result = sync_users()
    assert result["rows_inserted"] == 2
    # The target landed with scd2 metadata columns.
    has_valid_from = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM information_schema.columns "
        "WHERE table_schema = 'features' AND table_name = 'user_features' "
        "AND column_name = 'valid_from'"
    )
    assert has_valid_from == 1


@pytest.mark.integration
def test_feature_view_inherits_ttl_and_event_timestamp(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    conn.execute(
        "CREATE TABLE src.events (user_id BIGINT, score INT, ts TIMESTAMPTZ)"
    )
    conn.execute(
        "INSERT INTO src.events VALUES "
        "(1, 10, '2026-01-01T00:00:00Z'), "
        "(2, 20, '2026-02-01T00:00:00Z')"
    )
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._FEATURE_VIEWS_REGISTRY.clear()

    @ematix.feature_view(
        schema="features",
        feature_version="v1",
        event_timestamp_column="ts",
        ttl=timedelta(days=365),  # long enough not to fire
    )
    class UserFeatures:
        user_id: Annotated[BigInt, pk()]
        score: BigInt
        ts: TimestampTZ

    @ematix.pipeline(target=UserFeatures, schedule="0 * * * *")
    def sync_users(c):
        return "SELECT user_id, score, ts FROM src.events"

    result = sync_users()
    assert result["rows_inserted"] == 2
    # valid_from should equal the ts column, not now() — the FV inherited
    # event_timestamp_column='ts'.
    matched = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM features.user_features "
        "WHERE valid_from = ts"
    )
    assert matched == 2


@pytest.mark.integration
def test_feature_view_metadata_table_populated_on_sync(monkeypatch, pg_url):
    _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._FEATURE_VIEWS_REGISTRY.clear()

    @ematix.feature_view(
        schema="features",
        feature_version="v1",
        event_timestamp_column=None,
        ttl=timedelta(days=7),
        description="signals",
        owner="ml@example.com",
    )
    class UserFeatures:
        user_id: Annotated[BigInt, pk()]
        score: BigInt

    @ematix.pipeline(target=UserFeatures, schedule="0 * * * *")
    def sync_users(c):
        return "SELECT user_id, score FROM src.users"

    sync_users()
    conn = _core.connect(pg_url)
    rows = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM ematix_flow.feature_views "
        "WHERE name = 'features.user_features.v1'"
    )
    assert rows == 1
    ttl_seconds = conn.fetch_scalar_int(
        "SELECT ttl_seconds::int FROM ematix_flow.feature_views "
        "WHERE name = 'features.user_features.v1'"
    )
    assert ttl_seconds == 7 * 24 * 3600


@pytest.mark.integration
def test_feature_view_explicit_pipeline_mode_overrides_default(monkeypatch, pg_url):
    """User can still force mode= even on a FeatureView target."""
    _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._FEATURE_VIEWS_REGISTRY.clear()

    @ematix.feature_view(schema="features", feature_version="v1")
    class UserFeatures:
        user_id: Annotated[BigInt, pk()]
        score: BigInt

    @ematix.pipeline(target=UserFeatures, schedule="0 * * * *", mode="merge")
    def sync_users(c):
        return "SELECT user_id, score FROM src.users"

    result = sync_users()
    assert result["rows_inserted"] == 2
    # merge mode → no scd2 metadata columns on the target.
    conn = _core.connect(pg_url)
    has_valid_from = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM information_schema.columns "
        "WHERE table_schema = 'features' AND table_name = 'user_features' "
        "AND column_name = 'valid_from'"
    )
    assert has_valid_from == 0
