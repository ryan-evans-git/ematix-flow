"""Phase 10: incremental loads via `incremental_column` + watermarks."""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from ematix_flow import _core, pipeline
from ematix_flow.source import Source
from ematix_flow.table import ManagedTable
from ematix_flow.types import BigInt, Column, String, TimestampTZ

pytestmark = pytest.mark.integration


@pytest.fixture
def schema_name() -> str:
    return f"phase10_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def src_schema() -> str:
    return f"phase10src_{uuid.uuid4().hex[:8]}"


def _seed_source(conn: Any, schema: str) -> None:
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.execute(f"DROP TABLE IF EXISTS {schema}.events")
    conn.execute(
        f"CREATE TABLE {schema}.events ("
        f"  event_id BIGINT PRIMARY KEY,"
        f"  payload VARCHAR(256) NOT NULL,"
        f"  occurred_at TIMESTAMPTZ NOT NULL"
        f")"
    )


def _insert_rows(conn: Any, schema: str, rows: list[tuple[int, str, str]]) -> None:
    if not rows:
        return
    values = ", ".join(f"({i}, '{p}', '{t}'::timestamptz)" for i, p, t in rows)
    conn.execute(f"INSERT INTO {schema}.events VALUES {values}")


def _make_target_class(schema: str) -> type[ManagedTable]:
    class EventLog(ManagedTable):
        __schema__ = schema
        __tablename__ = "event_log"

        event_id = Column(BigInt(), nullable=False, primary_key=True)
        payload = Column(String(256), nullable=False)
        occurred_at = Column(TimestampTZ(), nullable=False)

    return EventLog


def _src_query(src_schema: str) -> str:
    return f"SELECT event_id, payload, occurred_at FROM {src_schema}.events"


def test_first_run_with_no_watermark_loads_all_rows(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema)
    _insert_rows(
        conn,
        src_schema,
        [
            (1, "a", "2026-04-29T10:00:00+00:00"),
            (2, "b", "2026-04-29T11:00:00+00:00"),
        ],
    )
    Cls = _make_target_class(schema_name)
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="append",
        pipeline_name="events_phase10_first",
        incremental_column="occurred_at",
    )
    assert result["rows_inserted"] == 2


def test_watermark_persisted_after_success(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema)
    _insert_rows(
        conn,
        src_schema,
        [(1, "a", "2026-04-29T10:00:00+00:00")],
    )
    Cls = _make_target_class(schema_name)
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="append",
        pipeline_name="events_phase10_persist",
        incremental_column="occurred_at",
    )
    found = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM ematix_flow.watermarks "
        "WHERE pipeline_name='events_phase10_persist' AND column_name='occurred_at'"
    )
    assert found == 1


def test_second_run_with_no_new_data_inserts_zero(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema)
    _insert_rows(
        conn,
        src_schema,
        [
            (1, "a", "2026-04-29T10:00:00+00:00"),
            (2, "b", "2026-04-29T11:00:00+00:00"),
        ],
    )
    Cls = _make_target_class(schema_name)
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="append",
        pipeline_name="events_phase10_repeat",
        incremental_column="occurred_at",
    )
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="append",
        pipeline_name="events_phase10_repeat",
        incremental_column="occurred_at",
    )
    assert result["rows_inserted"] == 0


def test_only_new_rows_are_loaded_after_watermark(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema)
    _insert_rows(
        conn,
        src_schema,
        [
            (1, "a", "2026-04-29T10:00:00+00:00"),
            (2, "b", "2026-04-29T11:00:00+00:00"),
        ],
    )
    Cls = _make_target_class(schema_name)
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="append",
        pipeline_name="events_phase10_grow",
        incremental_column="occurred_at",
    )
    _insert_rows(
        conn,
        src_schema,
        [
            (3, "c", "2026-04-29T12:00:00+00:00"),
            (4, "d", "2026-04-29T13:00:00+00:00"),
        ],
    )
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="append",
        pipeline_name="events_phase10_grow",
        incremental_column="occurred_at",
    )
    assert result["rows_inserted"] == 2
    target_count = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.event_log"
    )
    assert target_count == 4


def test_failed_run_leaves_watermark_unchanged(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema)
    _insert_rows(
        conn,
        src_schema,
        [(1, "a", "2026-04-29T10:00:00+00:00")],
    )
    Cls = _make_target_class(schema_name)
    pipeline.sync(
        target=Cls,
        source=Source.postgres_query(conn, _src_query(src_schema)),
        target_connection=conn,
        mode="append",
        pipeline_name="events_phase10_fail",
        incremental_column="occurred_at",
    )
    pre = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM ematix_flow.watermarks "
        "WHERE pipeline_name='events_phase10_fail'"
    )
    assert pre == 1
    pre_value = conn.execute(
        "SELECT 1"  # noop, just to confirm the connection's healthy
    )
    # Force a failure: source projects unknown column.
    with pytest.raises(ValueError):
        pipeline.sync(
            target=Cls,
            source=Source.postgres_query(conn, "SELECT 1 AS bogus"),
            target_connection=conn,
            mode="append",
            pipeline_name="events_phase10_fail",
            incremental_column="occurred_at",
        )
    post = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM ematix_flow.watermarks "
        "WHERE pipeline_name='events_phase10_fail'"
    )
    assert post == 1


def test_incremental_column_rejected_for_non_append_modes(
    pg_url: str, schema_name: str, src_schema: str
) -> None:
    conn = _core.connect(pg_url)
    _seed_source(conn, src_schema)
    _insert_rows(conn, src_schema, [(1, "a", "2026-04-29T10:00:00+00:00")])
    Cls = _make_target_class(schema_name)
    with pytest.raises(ValueError, match="incremental_column"):
        pipeline.sync(
            target=Cls,
            source=Source.postgres_query(conn, _src_query(src_schema)),
            target_connection=conn,
            mode="truncate",
            pipeline_name="events_phase10_reject_truncate",
            incremental_column="occurred_at",
        )


# --- cross-DB (uses session-scoped pg_url_secondary from conftest) ----------


def test_cross_db_incremental_append(
    pg_url: str, pg_url_secondary: str, schema_name: str, src_schema: str
) -> None:
    src_conn = _core.connect(pg_url_secondary)
    tgt_conn = _core.connect(pg_url)
    _seed_source(src_conn, src_schema)
    _insert_rows(
        src_conn,
        src_schema,
        [
            (1, "a", "2026-04-29T10:00:00+00:00"),
            (2, "b", "2026-04-29T11:00:00+00:00"),
        ],
    )
    Cls = _make_target_class(schema_name)
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(src_conn, _src_query(src_schema)),
        target_connection=tgt_conn,
        mode="append",
        pipeline_name="events_phase10_cross",
        incremental_column="occurred_at",
    )
    assert result["path"] == "cross_db"
    assert result["rows_inserted"] == 2
    _insert_rows(
        src_conn, src_schema, [(3, "c", "2026-04-29T12:00:00+00:00")]
    )
    result = pipeline.sync(
        target=Cls,
        source=Source.postgres_query(src_conn, _src_query(src_schema)),
        target_connection=tgt_conn,
        mode="append",
        pipeline_name="events_phase10_cross",
        incremental_column="occurred_at",
    )
    assert result["rows_inserted"] == 1
