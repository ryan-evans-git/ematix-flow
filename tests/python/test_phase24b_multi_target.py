"""Phase 24b: multi-target pipelines via `targets=[ematix.target(...)]`."""

from __future__ import annotations

import uuid
from typing import Annotated, Any

import pytest

from ematix_flow import _core, ematix, pk
from ematix_flow import pipeline as p
from ematix_flow.types import BigInt, Text


@pytest.fixture(autouse=True)
def _clean_registry():
    p._REGISTRY.clear()
    yield
    p._REGISTRY.clear()


# --- ematix.target shape ---------------------------------------------------


def test_target_constructor_accepts_class_and_mode() -> None:
    @ematix.table(schema="s")
    class T:
        id: Annotated[BigInt, pk()]

    target = ematix.target(T, mode="merge")
    assert target.target_class is T
    assert target.mode == "merge"


def test_target_accepts_per_target_connection() -> None:
    @ematix.table(schema="s")
    class T:
        id: Annotated[BigInt, pk()]

    target = ematix.target(T, mode="merge", target_connection="alt")
    assert target.target_connection == "alt"


def test_target_accepts_strategy_options() -> None:
    @ematix.table(schema="s")
    class T:
        id: Annotated[BigInt, pk()]
        ts: Text

    target = ematix.target(
        T,
        mode="scd2",
        event_timestamp_column="ts",
        compare_columns=("ts",),
    )
    assert target.event_timestamp_column == "ts"
    assert target.compare_columns == ("ts",)


def test_target_requires_mode() -> None:
    @ematix.table(schema="s")
    class T:
        id: Annotated[BigInt, pk()]

    with pytest.raises(TypeError):
        ematix.target(T)  # missing mode=


# --- decorator multi-target validation -------------------------------------


def test_pipeline_rejects_target_and_targets_together() -> None:
    @ematix.table(schema="s")
    class A:
        id: Annotated[BigInt, pk()]

    with pytest.raises(TypeError, match="target.*targets"):

        @ematix.pipeline(
            target=A,
            targets=[ematix.target(A, mode="merge")],
            schedule="0 * * * *",
        )
        def f(conn):
            return "SELECT 1"


def test_pipeline_rejects_empty_targets_list() -> None:
    with pytest.raises(TypeError, match="non-empty"):

        @ematix.pipeline(targets=[], schedule="0 * * * *")
        def f(conn):
            return "SELECT 1"


# --- integration: multi-target end-to-end -----------------------------------


pytestmark_int = pytest.mark.integration


@pytest.fixture
def schema_name() -> str:
    return f"phase24bmt_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def src_schema() -> str:
    return f"phase24bmtsrc_{uuid.uuid4().hex[:8]}"


def _seed_source(conn: Any, schema: str) -> None:
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.execute(f"DROP TABLE IF EXISTS {schema}.src_events")
    conn.execute(
        f"CREATE TABLE {schema}.src_events ("
        f"  event_id BIGINT PRIMARY KEY,"
        f"  payload TEXT NOT NULL"
        f")"
    )
    conn.execute(
        f"INSERT INTO {schema}.src_events VALUES "
        f"(1, 'a'), (2, 'b'), (3, 'c')"
    )


@pytest.mark.integration
def test_multi_target_runs_each_target(
    pg_url: str, schema_name: str, src_schema: str, monkeypatch
) -> None:
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    seed = _core.connect(pg_url)
    _seed_source(seed, src_schema)

    @ematix.table(schema=schema_name, name="event_log_a")
    class EventLogA:
        event_id: Annotated[BigInt, pk()]
        payload: Text

    @ematix.table(schema=schema_name, name="event_log_b")
    class EventLogB:
        event_id: Annotated[BigInt, pk()]
        payload: Text

    @ematix.pipeline(
        targets=[
            ematix.target(EventLogA, mode="append"),
            ematix.target(EventLogB, mode="append"),
        ],
        schedule="0 * * * *",
        name="events_phase24b_multi",
    )
    def sync_events(conn):
        return f"SELECT event_id, payload FROM {src_schema}.src_events"

    result = p.run_pipeline("events_phase24b_multi")
    # Multi-target returns a list/dict of per-target results.
    assert isinstance(result, dict) or isinstance(result, list)

    # Both targets populated.
    a_count = seed.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.event_log_a"
    )
    b_count = seed.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.event_log_b"
    )
    assert a_count == 3
    assert b_count == 3


@pytest.mark.integration
def test_multi_target_halts_on_first_failure_by_default(
    pg_url: str, schema_name: str, src_schema: str, monkeypatch
) -> None:
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    seed = _core.connect(pg_url)
    _seed_source(seed, src_schema)

    @ematix.table(schema=schema_name, name="bad_event_log")
    class BadEventLog:
        # Target schema requires `non_existent_field` from source.
        event_id: Annotated[BigInt, pk()]
        non_existent_field: Text  # source doesn't have this

    @ematix.table(schema=schema_name, name="good_event_log")
    class GoodEventLog:
        event_id: Annotated[BigInt, pk()]
        payload: Text

    @ematix.pipeline(
        targets=[
            ematix.target(BadEventLog, mode="append"),
            ematix.target(GoodEventLog, mode="append"),
        ],
        schedule="0 * * * *",
        name="events_phase24b_halt",
    )
    def sync_events(conn):
        return f"SELECT event_id, payload FROM {src_schema}.src_events"

    with pytest.raises(Exception):
        p.run_pipeline("events_phase24b_halt")
    # Second target was skipped due to halt-on-first.
    good_exists = seed.fetch_scalar_int(
        f"SELECT count(*)::int FROM information_schema.tables "
        f"WHERE table_schema = '{schema_name}' AND table_name = 'good_event_log'"
    )
    # The good target's table may or may not exist depending on order; we
    # just assert no rows were inserted into it.
    if good_exists:
        good_count = seed.fetch_scalar_int(
            f"SELECT count(*)::int FROM {schema_name}.good_event_log"
        )
        assert good_count == 0


@pytest.mark.integration
def test_multi_target_continue_on_failure(
    pg_url: str, schema_name: str, src_schema: str, monkeypatch
) -> None:
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    seed = _core.connect(pg_url)
    _seed_source(seed, src_schema)

    @ematix.table(schema=schema_name, name="bad_event_log")
    class BadEventLog:
        event_id: Annotated[BigInt, pk()]
        non_existent_field: Text

    @ematix.table(schema=schema_name, name="good_event_log")
    class GoodEventLog:
        event_id: Annotated[BigInt, pk()]
        payload: Text

    @ematix.pipeline(
        targets=[
            ematix.target(BadEventLog, mode="append"),
            ematix.target(GoodEventLog, mode="append"),
        ],
        schedule="0 * * * *",
        continue_on_failure=True,
        name="events_phase24b_continue",
    )
    def sync_events(conn):
        return f"SELECT event_id, payload FROM {src_schema}.src_events"

    # No exception raised; result records both per-target outcomes.
    result = p.run_pipeline("events_phase24b_continue")
    assert isinstance(result, dict)
    # Good target succeeded.
    good_count = seed.fetch_scalar_int(
        f"SELECT count(*)::int FROM {schema_name}.good_event_log"
    )
    assert good_count == 3
