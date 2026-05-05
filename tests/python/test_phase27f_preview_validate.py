"""Phase 27f: preview / dry_run / validate interaction with transforms_post.

Q10 B:
- preview() lists transforms_post entries (text-only, no execution).
- dry_run() skips transforms entirely (load only, in-tx + rollback).
- validate() EXPLAINs SQL-string transforms; callables and
  transform_ref entries are skipped with a note.
"""

from typing import Annotated

import pytest

from ematix_flow import _core, ematix, pk
from ematix_flow import pipeline as p
from ematix_flow.types import BigInt, Text


def _setup_pg(pg_url: str):
    conn = _core.connect(pg_url)
    conn.execute("DROP SCHEMA IF EXISTS warehouse CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS src CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS ematix_flow CASCADE")
    conn.execute("CREATE SCHEMA src")
    conn.execute("CREATE SCHEMA warehouse")
    conn.execute("CREATE TABLE src.events (id BIGINT PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO src.events VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    return conn


# --- preview lists transforms_post -----------------------------------------


@pytest.mark.integration
def test_preview_lists_sql_string_transforms(monkeypatch, pg_url):
    _setup_pg(pg_url)
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
        transforms_post=[
            "REFRESH MATERIALIZED VIEW warehouse.summary",
            "ANALYZE warehouse.event_dim",
        ],
    )
    def sync_events(c):
        return "SELECT id, name FROM src.events"

    result = p.preview("sync_events")
    assert result.transforms_post is not None
    assert len(result.transforms_post) == 2
    # Each entry has kind + summary.
    assert result.transforms_post[0].kind == "sql"
    assert "REFRESH" in result.transforms_post[0].summary


@pytest.mark.integration
def test_preview_lists_callable_and_transform_ref(monkeypatch, pg_url):
    _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._TRANSFORMS_REGISTRY.clear()

    @ematix.transform(name="my_xform")
    def my_xform(c):
        pass

    def inline_post(c):
        pass

    @ematix.table(schema="warehouse")
    class EventDim:
        id: Annotated[BigInt, pk()]
        name: Text

    @ematix.pipeline(
        target=EventDim,
        schedule="0 * * * *",
        mode="merge",
        transforms_post=[
            inline_post,
            ematix.transform_ref("my_xform"),
        ],
    )
    def sync_events(c):
        return "SELECT id, name FROM src.events"

    result = p.preview("sync_events")
    assert len(result.transforms_post) == 2
    kinds = {t.kind for t in result.transforms_post}
    assert kinds == {"callable", "transform_ref"}


@pytest.mark.integration
def test_preview_no_transforms_post_renders_empty(monkeypatch, pg_url):
    _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._TRANSFORMS_REGISTRY.clear()

    @ematix.table(schema="warehouse")
    class EventDim:
        id: Annotated[BigInt, pk()]
        name: Text

    @ematix.pipeline(target=EventDim, schedule="0 * * * *", mode="merge")
    def sync_events(c):
        return "SELECT id, name FROM src.events"

    result = p.preview("sync_events")
    assert result.transforms_post == []


# --- dry_run skips transforms ---------------------------------------------


@pytest.mark.integration
def test_dry_run_skips_transforms_post_entirely(monkeypatch, pg_url):
    """The transforms list MUST NOT execute in dry_run mode."""
    conn = _setup_pg(pg_url)
    conn.execute("CREATE TABLE warehouse.scratch (n INT)")
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._TRANSFORMS_REGISTRY.clear()

    @ematix.table(schema="warehouse")
    class EventDim:
        id: Annotated[BigInt, pk()]
        name: Text

    fired = {"flag": False}

    def must_not_fire(c):
        fired["flag"] = True
        c.execute("INSERT INTO warehouse.scratch VALUES (1)")

    @ematix.pipeline(
        target=EventDim,
        schedule="0 * * * *",
        mode="merge",
        transforms_post=[
            "INSERT INTO warehouse.scratch VALUES (99)",
            must_not_fire,
        ],
    )
    def sync_events(c):
        return "SELECT id, name FROM src.events"

    result = p.dry_run("sync_events")
    assert result.is_dry_run is True
    assert fired["flag"] is False
    # The SQL transform also did NOT execute.
    assert conn.fetch_scalar_int("SELECT count(*)::int FROM warehouse.scratch") == 0


# --- validate EXPLAINs SQL strings, notes others --------------------------


@pytest.mark.integration
def test_validate_explains_sql_string_transforms(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    conn.execute("CREATE TABLE warehouse.audit_log (n INT)")
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
        transforms_post=[
            "INSERT INTO warehouse.audit_log VALUES (1)",
        ],
    )
    def sync_events(c):
        return "SELECT id, name FROM src.events"

    result = p.validate("sync_events")
    assert result.ok is True


@pytest.mark.integration
def test_validate_fails_on_bad_sql_transform(monkeypatch, pg_url):
    _setup_pg(pg_url)
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
        transforms_post=[
            "INSERT INTO warehouse.does_not_exist VALUES (1)",
        ],
    )
    def sync_events(c):
        return "SELECT id, name FROM src.events"

    result = p.validate("sync_events")
    assert result.ok is False
    joined = " ".join(result.errors).lower()
    assert "does_not_exist" in joined or "does not exist" in joined


@pytest.mark.integration
def test_validate_skips_callables_with_note(monkeypatch, pg_url):
    _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._TRANSFORMS_REGISTRY.clear()

    def some_callable(c):
        pass

    @ematix.table(schema="warehouse")
    class EventDim:
        id: Annotated[BigInt, pk()]
        name: Text

    @ematix.pipeline(
        target=EventDim,
        schedule="0 * * * *",
        mode="merge",
        transforms_post=[some_callable],
    )
    def sync_events(c):
        return "SELECT id, name FROM src.events"

    result = p.validate("sync_events")
    # Callable transforms can't be EXPLAINed; validate succeeds for the
    # load and the notes mention the skip.
    assert result.ok is True
    assert any(
        "callable" in n.lower() or "skipped" in n.lower() for n in result.notes
    )
