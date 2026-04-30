"""Phase 16: TTL / expiry for SCD2 loads.

`ttl=timedelta(...)` on `pipeline.sync` (and @ematix.pipeline) adds a
post-step UPDATE that closes out current versions whose `valid_from`
is older than `now() - ttl`. Runs in the same transaction as the load
— same atomicity as `handle_deletes='soft'` (Phase 11).

Only valid for `mode='scd2'`. Other modes raise.
"""

from datetime import timedelta
from typing import Annotated

import pytest

from ematix_flow import _core, ematix, pipeline as p, pk
from ematix_flow.types import BigInt, Text

pytestmark = pytest.mark.integration


def _setup_pg(pg_url: str):
    conn = _core.connect(pg_url)
    conn.execute("DROP SCHEMA IF EXISTS warehouse CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS src CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS ematix_flow CASCADE")
    conn.execute("CREATE SCHEMA src")
    conn.execute("CREATE SCHEMA warehouse")
    conn.execute("CREATE TABLE src.users (id BIGINT PRIMARY KEY, score INT)")
    return conn


def test_ttl_closes_stale_current_versions(monkeypatch, pg_url):
    """Insert a row at T-10d, then sync at T+0 with ttl=7d → row closed."""
    conn = _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()

    @ematix.table(schema="warehouse")
    class UserDim:
        id: Annotated[BigInt, pk()]
        score: BigInt

    @ematix.pipeline(
        target=UserDim,
        schedule="0 * * * *",
        mode="scd2",
        ttl=timedelta(days=7),
    )
    def sync_users(c):
        return "SELECT id, score FROM src.users"

    # Seed: one stale row, one fresh row.
    conn.execute("INSERT INTO src.users VALUES (1, 100)")
    sync_users()
    # Pretend the sync happened 10 days ago by backdating valid_from.
    conn.execute(
        "UPDATE warehouse.user_dim SET valid_from = now() - interval '10 days' "
        "WHERE id = 1"
    )

    # Now add a second row that's fresh.
    conn.execute("UPDATE src.users SET score = 200 WHERE id = 1")
    conn.execute("INSERT INTO src.users VALUES (2, 50)")
    sync_users()

    # Stale id=1: every version older than 7d should be closed. The new
    # version inserted by this sync is fresh and stays current.
    closed_old = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM warehouse.user_dim "
        "WHERE id = 1 AND score = 100 AND is_current = false"
    )
    assert closed_old == 1
    # Fresh id=2: stays current.
    fresh = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM warehouse.user_dim "
        "WHERE id = 2 AND is_current = true"
    )
    assert fresh == 1


def test_ttl_does_not_close_fresh_rows(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()

    @ematix.table(schema="warehouse")
    class UserDim:
        id: Annotated[BigInt, pk()]
        score: BigInt

    @ematix.pipeline(
        target=UserDim,
        schedule="0 * * * *",
        mode="scd2",
        ttl=timedelta(days=30),
    )
    def sync_users(c):
        return "SELECT id, score FROM src.users"

    conn.execute("INSERT INTO src.users VALUES (1, 100)")
    sync_users()
    sync_users()  # idempotent re-sync

    # All versions of id=1 stay current — nothing is older than 30d.
    closed = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM warehouse.user_dim "
        "WHERE id = 1 AND is_current = false"
    )
    assert closed == 0


def test_ttl_rejected_for_non_scd2_modes(monkeypatch, pg_url):
    _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()

    @ematix.table(schema="warehouse")
    class UserDim:
        id: Annotated[BigInt, pk()]

    @ematix.pipeline(
        target=UserDim,
        schedule="0 * * * *",
        mode="merge",
        ttl=timedelta(days=7),
    )
    def sync_users(c):
        return "SELECT id FROM src.users"

    with pytest.raises(ValueError, match="ttl.*scd2"):
        sync_users()


def test_ttl_atomic_with_load(monkeypatch, pg_url):
    """If the load fails, the TTL UPDATE doesn't fire (same tx)."""
    conn = _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()

    @ematix.table(schema="warehouse")
    class UserDim:
        id: Annotated[BigInt, pk()]
        score: BigInt

    @ematix.pipeline(
        target=UserDim,
        schedule="0 * * * *",
        mode="scd2",
        ttl=timedelta(days=7),
    )
    def sync_users(c):
        # Intentionally bad SQL: missing column.
        return "SELECT id, NOT_A_COLUMN AS score FROM src.users"

    conn.execute("INSERT INTO src.users VALUES (1, 100)")
    # Pre-seed an existing stale row that TTL would close out if it
    # ran independently of the load.
    conn.execute("CREATE SCHEMA IF NOT EXISTS ematix_flow")
    # First, do a successful load to seed the target.
    @ematix.pipeline(
        target=UserDim,
        schedule="0 * * * *",
        mode="scd2",
        name="seed_pipe",
    )
    def seed_pipe(c):
        return "SELECT id, score FROM src.users"

    seed_pipe()
    conn.execute(
        "UPDATE warehouse.user_dim SET valid_from = now() - interval '30 days'"
    )

    # The bad load raises; TTL update must not have run (still current).
    with pytest.raises(Exception):
        sync_users()
    still_current = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM warehouse.user_dim WHERE is_current = true"
    )
    assert still_current >= 1
