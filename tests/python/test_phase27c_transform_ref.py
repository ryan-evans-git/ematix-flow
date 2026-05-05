"""Phase 27c: ematix.transform_ref("name") for name-based lookup +
merged flow list showing pipelines and transforms with a type column.
"""

from typing import Annotated

import pytest

from ematix_flow import _core, ematix, pk
from ematix_flow import pipeline as p
from ematix_flow.types import BigInt, Text


def _setup_pg_skip():
    """For unit tests that don't need a real database."""
    pass


# --- transform_ref unit shape --------------------------------------------


def test_transform_ref_returns_sentinel_with_name():
    ref = ematix.transform_ref("recompute_segments")
    assert ref.name == "recompute_segments"


def test_transform_ref_rejects_non_string():
    with pytest.raises(TypeError):
        ematix.transform_ref(42)


# --- merged listing ------------------------------------------------------


def test_list_entries_returns_both_with_type():
    """Phase 27c Q4.2 β: list_entries returns pipelines + transforms with type."""
    p._REGISTRY.clear()
    p._TRANSFORMS_REGISTRY.clear()

    @ematix.transform(name="my_xform")
    def my_xform(c):
        pass

    @p.register(name="my_pipe", schedule="0 * * * *")
    def my_pipe():
        return {}

    entries = p.list_entries()
    by_name = {e.name: e for e in entries}
    assert by_name["my_xform"].kind == "transform"
    assert by_name["my_pipe"].kind == "pipeline"


def test_list_entries_transform_carries_schedule():
    p._REGISTRY.clear()
    p._TRANSFORMS_REGISTRY.clear()

    @ematix.transform(name="hourly_xform", schedule="0 * * * *")
    def hourly_xform(c):
        pass

    @ematix.transform(name="manual_xform")
    def manual_xform(c):
        pass

    entries = {e.name: e for e in p.list_entries()}
    assert entries["hourly_xform"].schedule == "0 * * * *"
    assert entries["manual_xform"].schedule is None


# --- transform_ref resolution against transforms registry ----------------


pytestmark_integration = pytest.mark.integration


def _setup_pg(pg_url: str):
    conn = _core.connect(pg_url)
    conn.execute("DROP SCHEMA IF EXISTS warehouse CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS src CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS ematix_flow CASCADE")
    conn.execute("CREATE SCHEMA src")
    conn.execute("CREATE SCHEMA warehouse")
    conn.execute("CREATE TABLE src.events (id BIGINT PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO src.events VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    conn.execute("CREATE TABLE warehouse.scratch (n INT, label TEXT)")
    return conn


@pytest.mark.integration
def test_transform_ref_resolves_to_registered_transform(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._TRANSFORMS_REGISTRY.clear()

    @ematix.transform(name="my_post_xform")
    def my_post_xform(c, parent):
        c.execute(
            f"INSERT INTO warehouse.scratch (n, label) VALUES "
            f"({parent.rows_inserted}, 'ref-{parent.pipeline_name}')"
        )

    @ematix.table(schema="warehouse")
    class EventDim:
        id: Annotated[BigInt, pk()]
        name: Text

    @ematix.pipeline(
        target=EventDim,
        schedule="0 * * * *",
        mode="merge",
        transforms_post=[ematix.transform_ref("my_post_xform")],
    )
    def sync_events(c):
        return "SELECT id, name FROM src.events"

    sync_events()
    assert (
        conn.fetch_scalar_int(
            "SELECT count(*)::int FROM warehouse.scratch "
            "WHERE n = 3 AND label = 'ref-sync_events'"
        )
        == 1
    )


@pytest.mark.integration
def test_transform_ref_resolves_to_chained_pipeline(monkeypatch, pg_url):
    """Q5 A: transform_ref pointing at another @ematix.pipeline runs
    that pipeline's full sync independently."""
    conn = _setup_pg(pg_url)
    conn.execute("CREATE TABLE src.users (id BIGINT PRIMARY KEY, email TEXT)")
    conn.execute("INSERT INTO src.users VALUES (10, 'a@x.io'), (11, 'b@x.io')")
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._TRANSFORMS_REGISTRY.clear()

    @ematix.table(schema="warehouse")
    class EventDim:
        id: Annotated[BigInt, pk()]
        name: Text

    @ematix.table(schema="warehouse")
    class UserDim:
        id: Annotated[BigInt, pk()]
        email: Text

    @ematix.pipeline(
        target=UserDim,
        schedule=None,  # not scheduled — only triggered
        mode="merge",
        name="recompute_users",
    )
    def recompute_users(c):
        return "SELECT id, email FROM src.users"

    @ematix.pipeline(
        target=EventDim,
        schedule="0 * * * *",
        mode="merge",
        transforms_post=[ematix.transform_ref("recompute_users")],
    )
    def sync_events(c):
        return "SELECT id, name FROM src.events"

    sync_events()
    # The chained pipeline ran its full sync.
    assert conn.fetch_scalar_int("SELECT count(*)::int FROM warehouse.user_dim") == 2


@pytest.mark.integration
def test_transform_ref_unknown_name_raises(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._TRANSFORMS_REGISTRY.clear()

    @ematix.table(schema="warehouse")
    class EventDim:
        id: Annotated[BigInt, pk()]
        name: Text

    @ematix.pipeline(
        target=EventDim,
        schedule="0 * * * *",
        mode="merge",
        transforms_post=[ematix.transform_ref("does_not_exist")],
    )
    def sync_events(c):
        return "SELECT id, name FROM src.events"

    with pytest.raises(KeyError, match="does_not_exist"):
        sync_events()


@pytest.mark.integration
def test_transform_ref_chained_pipeline_records_independent_run_history(
    monkeypatch, pg_url
):
    conn = _setup_pg(pg_url)
    conn.execute("CREATE TABLE src.users (id BIGINT PRIMARY KEY, email TEXT)")
    conn.execute("INSERT INTO src.users VALUES (10, 'a@x.io')")
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._TRANSFORMS_REGISTRY.clear()

    @ematix.table(schema="warehouse")
    class EventDim:
        id: Annotated[BigInt, pk()]
        name: Text

    @ematix.table(schema="warehouse")
    class UserDim:
        id: Annotated[BigInt, pk()]
        email: Text

    @ematix.pipeline(
        target=UserDim,
        schedule=None,
        mode="merge",
        name="recompute_users",
    )
    def recompute_users(c):
        return "SELECT id, email FROM src.users"

    @ematix.pipeline(
        target=EventDim,
        schedule="0 * * * *",
        mode="merge",
        transforms_post=[ematix.transform_ref("recompute_users")],
    )
    def sync_events(c):
        return "SELECT id, name FROM src.events"

    sync_events()
    # Each pipeline got its own run_history row (status='success').
    main_count = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM ematix_flow.run_history "
        "WHERE pipeline_name = 'sync_events' AND status = 'success'"
    )
    chained_count = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM ematix_flow.run_history "
        "WHERE pipeline_name = 'recompute_users' AND status = 'success'"
    )
    assert main_count == 1
    assert chained_count == 1
