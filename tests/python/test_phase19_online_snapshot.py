"""Phase 19: online (is_current) snapshot for FeatureView.

When a `@ematix.feature_view(online=True)` is synced, the framework:
  - Ensures a partial index on `(entity_keys) WHERE is_current` so
    inference lookups are fast on the main table too.
  - Ensures a MATERIALIZED VIEW `<schema>.<table>__online` containing
    only the current versions, plus a UNIQUE index on entity keys so
    CONCURRENTLY refresh works.
  - REFRESHes the MV CONCURRENTLY after each successful sync.

`Cls.online_features(conn, entity_keys)` queries the MV directly for
sub-millisecond inference reads.
"""

from typing import Annotated

import pytest

from ematix_flow import _core, ematix, pipeline as p, pk
from ematix_flow.types import BigInt, Numeric

pytestmark = pytest.mark.integration


def _setup_pg(pg_url: str):
    conn = _core.connect(pg_url)
    conn.execute("DROP SCHEMA IF EXISTS features CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS src CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS ematix_flow CASCADE")
    conn.execute("CREATE SCHEMA src")
    conn.execute(
        "CREATE TABLE src.users (user_id BIGINT PRIMARY KEY, score NUMERIC(10,2))"
    )
    return conn


def test_online_creates_materialized_view_after_sync(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    conn.execute("INSERT INTO src.users VALUES (1, 100), (2, 200)")
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._FEATURE_VIEWS_REGISTRY.clear()

    @ematix.feature_view(
        schema="features",
        feature_version="v1",
        online=True,
    )
    class UserFeatures:
        user_id: Annotated[BigInt, pk()]
        score: Numeric[10, 2]

    @ematix.pipeline(target=UserFeatures, schedule="0 * * * *")
    def sync_users(c):
        return "SELECT user_id, score FROM src.users"

    sync_users()

    # MV exists with the expected name.
    mv_count = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM pg_matviews "
        "WHERE schemaname = 'features' AND matviewname = 'user_features__online'"
    )
    assert mv_count == 1

    # MV holds the current versions.
    online_rows = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM features.user_features__online"
    )
    assert online_rows == 2


def test_online_partial_index_on_main_table(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    conn.execute("INSERT INTO src.users VALUES (1, 100)")
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._FEATURE_VIEWS_REGISTRY.clear()

    @ematix.feature_view(schema="features", feature_version="v1", online=True)
    class UserFeatures:
        user_id: Annotated[BigInt, pk()]
        score: Numeric[10, 2]

    @ematix.pipeline(target=UserFeatures, schedule="0 * * * *")
    def sync_users(c):
        return "SELECT user_id, score FROM src.users"

    sync_users()
    # A partial index covering (user_id) WHERE is_current should exist.
    has_partial = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM pg_indexes "
        "WHERE schemaname = 'features' "
        "  AND tablename = 'user_features' "
        "  AND indexdef ILIKE '%WHERE%is_current%'"
    )
    assert has_partial >= 1


def test_online_mv_refreshes_after_each_sync(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    conn.execute("INSERT INTO src.users VALUES (1, 100)")
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._FEATURE_VIEWS_REGISTRY.clear()

    @ematix.feature_view(schema="features", feature_version="v1", online=True)
    class UserFeatures:
        user_id: Annotated[BigInt, pk()]
        score: Numeric[10, 2]

    @ematix.pipeline(target=UserFeatures, schedule="0 * * * *")
    def sync_users(c):
        return "SELECT user_id, score FROM src.users"

    sync_users()
    # After the next sync the MV reflects the new score.
    conn.execute("UPDATE src.users SET score = 999 WHERE user_id = 1")
    sync_users()
    new_score = conn.fetch_scalar_int(
        "SELECT score::int FROM features.user_features__online WHERE user_id = 1"
    )
    assert new_score == 999


def test_online_features_classmethod_returns_current(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    conn.execute("INSERT INTO src.users VALUES (1, 100), (2, 200)")
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._FEATURE_VIEWS_REGISTRY.clear()

    @ematix.feature_view(schema="features", feature_version="v1", online=True)
    class UserFeatures:
        user_id: Annotated[BigInt, pk()]
        score: Numeric[10, 2]

    @ematix.pipeline(target=UserFeatures, schedule="0 * * * *")
    def sync_users(c):
        return "SELECT user_id, score FROM src.users"

    sync_users()

    row = UserFeatures.online_features(conn, entity_keys={"user_id": 1})
    assert row is not None
    assert int(row["user_id"]) == 1
    assert float(row["score"]) == 100.0


def test_online_features_unknown_entity_returns_none(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    conn.execute("INSERT INTO src.users VALUES (1, 100)")
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._FEATURE_VIEWS_REGISTRY.clear()

    @ematix.feature_view(schema="features", feature_version="v1", online=True)
    class UserFeatures:
        user_id: Annotated[BigInt, pk()]
        score: Numeric[10, 2]

    @ematix.pipeline(target=UserFeatures, schedule="0 * * * *")
    def sync_users(c):
        return "SELECT user_id, score FROM src.users"

    sync_users()
    row = UserFeatures.online_features(conn, entity_keys={"user_id": 999})
    assert row is None


def test_online_false_does_not_create_mv(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    conn.execute("INSERT INTO src.users VALUES (1, 100)")
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._FEATURE_VIEWS_REGISTRY.clear()

    @ematix.feature_view(schema="features", feature_version="v1", online=False)
    class UserFeatures:
        user_id: Annotated[BigInt, pk()]
        score: Numeric[10, 2]

    @ematix.pipeline(target=UserFeatures, schedule="0 * * * *")
    def sync_users(c):
        return "SELECT user_id, score FROM src.users"

    sync_users()
    mv_count = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM pg_matviews "
        "WHERE schemaname = 'features' AND matviewname = 'user_features__online'"
    )
    assert mv_count == 0


def test_online_features_requires_online_true(monkeypatch, pg_url):
    """Calling online_features on a non-online FV raises a clear error."""
    conn = _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._FEATURE_VIEWS_REGISTRY.clear()

    @ematix.feature_view(schema="features", feature_version="v1", online=False)
    class UserFeatures:
        user_id: Annotated[BigInt, pk()]
        score: Numeric[10, 2]

    with pytest.raises(RuntimeError, match="online"):
        UserFeatures.online_features(conn, entity_keys={"user_id": 1})
