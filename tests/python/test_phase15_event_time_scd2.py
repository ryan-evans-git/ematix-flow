"""Phase 15: event-time SCD2 — valid_from from a source column instead of now()."""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from ematix_flow import _core, pipeline
from ematix_flow.source import Source
from ematix_flow.table import ManagedTable
from ematix_flow.types import BigInt, Column, String, Text

pytestmark = pytest.mark.integration


@pytest.fixture
def schema_name() -> str:
    return f"phase15_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def src_schema() -> str:
    return f"phase15src_{uuid.uuid4().hex[:8]}"


def _seed_source(conn: Any, schema: str) -> None:
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.execute(f"DROP TABLE IF EXISTS {schema}.user_events")
    conn.execute(
        f"CREATE TABLE {schema}.user_events ("
        f"  user_id BIGINT,"
        f"  email VARCHAR(256) NOT NULL,"
        f"  name TEXT NOT NULL,"
        f"  event_ts TIMESTAMPTZ NOT NULL"
        f")"
    )


def _insert(conn: Any, schema: str, rows: list[tuple[int, str, str, str]]) -> None:
    if not rows:
        return
    values = ", ".join(
        f"({uid}, '{e}', '{n}', '{ts}'::timestamptz)" for uid, e, n, ts in rows
    )
    conn.execute(f"INSERT INTO {schema}.user_events VALUES {values}")


def _make_target_class(schema: str) -> type[ManagedTable]:
    class UserDim(ManagedTable):
        __schema__ = schema
        __tablename__ = "user_dim"

        user_id = Column(BigInt(), nullable=False, primary_key=True)
        email = Column(String(256), nullable=False)
        name = Column(Text(), nullable=False)

    return UserDim


def _src_query(src_schema: str) -> str:
    return f"SELECT user_id, email, name, event_ts FROM {src_schema}.user_events"


def test_valid_from_uses_event_ts_not_now(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema)
    _insert(
        conn,
        src_schema,
        [(1, "a@x.com", "Alice", "2026-04-15T10:00:00+00:00")],
    )
    Cls = _make_target_class(schema_name)
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="scd2",
        pipeline_name="users_phase15_basic",
        event_timestamp_column="event_ts",
    )
    matched = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.user_dim "
        f"WHERE user_id = 1 AND is_current "
        f"AND valid_from = '2026-04-15T10:00:00+00:00'::timestamptz"
    )
    assert matched == 1


def test_close_out_valid_to_is_new_event_ts(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema)
    _insert(conn, src_schema, [(1, "a@x.com", "Alice", "2026-04-15T10:00:00+00:00")])
    Cls = _make_target_class(schema_name)
    name = "users_phase15_chain"
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="scd2",
        pipeline_name=name,
        event_timestamp_column="event_ts",
    )
    # Newer event: name change.
    conn.execute(f"DELETE FROM {src_schema}.user_events")
    _insert(conn, src_schema, [(1, "a@x.com", "Alice2", "2026-04-15T11:00:00+00:00")])
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="scd2",
        pipeline_name=name,
        event_timestamp_column="event_ts",
    )
    # Closed version: valid_to == new version's event_ts.
    closed_to_match = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.user_dim "
        f"WHERE user_id = 1 AND NOT is_current "
        f"AND valid_to = '2026-04-15T11:00:00+00:00'::timestamptz"
    )
    assert closed_to_match == 1
    # New current version's valid_from.
    current_match = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.user_dim "
        f"WHERE user_id = 1 AND is_current "
        f"AND valid_from = '2026-04-15T11:00:00+00:00'::timestamptz"
    )
    assert current_match == 1


def test_idempotent_under_same_source(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema)
    _insert(conn, src_schema, [(1, "a@x.com", "Alice", "2026-04-15T10:00:00+00:00")])
    Cls = _make_target_class(schema_name)
    name = "users_phase15_idem"
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="scd2",
        pipeline_name=name,
        event_timestamp_column="event_ts",
    )
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="scd2",
        pipeline_name=name,
        event_timestamp_column="event_ts",
    )
    assert result["rows_inserted"] == 0
    assert result["rows_closed"] == 0
    rows = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.user_dim WHERE user_id = 1"
    )
    assert rows == 1


def test_multiple_events_per_key_picks_latest(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    """When source has multiple events for the same key, the highest event_ts wins."""
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema)
    _insert(
        conn,
        src_schema,
        [
            (1, "a@x.com", "Alice", "2026-04-15T10:00:00+00:00"),
            (1, "a@x.com", "Alice2", "2026-04-15T12:00:00+00:00"),
            (1, "a@x.com", "AliceMid", "2026-04-15T11:00:00+00:00"),
        ],
    )
    Cls = _make_target_class(schema_name)
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="scd2",
        pipeline_name="users_phase15_dedup",
        event_timestamp_column="event_ts",
    )
    current = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.user_dim "
        f"WHERE user_id = 1 AND is_current AND name = 'Alice2' "
        f"AND valid_from = '2026-04-15T12:00:00+00:00'::timestamptz"
    )
    assert current == 1
    total_versions = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.user_dim WHERE user_id = 1"
    )
    assert total_versions == 1


def test_out_of_order_arrival_raises(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema)
    _insert(conn, src_schema, [(1, "a@x.com", "Alice", "2026-04-15T12:00:00+00:00")])
    Cls = _make_target_class(schema_name)
    name = "users_phase15_oo"
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="scd2",
        pipeline_name=name,
        event_timestamp_column="event_ts",
    )
    # Now arrive a row with an OLDER event_ts and changed values.
    conn.execute(f"DELETE FROM {src_schema}.user_events")
    _insert(conn, src_schema, [(1, "a@x.com", "AliceOld", "2026-04-15T08:00:00+00:00")])
    with pytest.raises(ValueError, match="event_ts"):
        pipeline.sync(
            target=Cls,
            source=Source.postgres_query(conn, _src_query(src_schema)),
            target_connection=conn,
            mode="scd2",
            pipeline_name=name,
            event_timestamp_column="event_ts",
        )


def test_event_timestamp_column_rejected_for_non_scd2_modes(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema)
    Cls = _make_target_class(schema_name)
    with pytest.raises(ValueError, match="event_timestamp_column"):
        pipeline.sync(
            target=Cls,
            source=Source.postgres_query(conn, _src_query(src_schema)),
            target_connection=conn,
            mode="merge",
            pipeline_name="users_phase15_reject",
            event_timestamp_column="event_ts",
        )


def test_cross_db_event_time_scd2(
    pg_url: str, pg_url_secondary: str, schema_name: str, src_schema: str
) -> None:
    src_conn = _core.connect(pg_url_secondary)
    tgt_conn = _core.connect(pg_url)
    _seed_source(src_conn, src_schema)
    _insert(src_conn, src_schema, [(1, "a@x.com", "Alice", "2026-04-15T10:00:00+00:00")])
    Cls = _make_target_class(schema_name)
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(src_conn, _src_query(src_schema)),
        target_connection=tgt_conn,
        mode="scd2",
        pipeline_name="users_phase15_cross",
        event_timestamp_column="event_ts",
    )
    assert result["status"] == "success"
    assert result["path"] == "cross_db"
    matched = tgt_conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.user_dim "
        f"WHERE user_id = 1 AND is_current "
        f"AND valid_from = '2026-04-15T10:00:00+00:00'::timestamptz"
    )
    assert matched == 1
