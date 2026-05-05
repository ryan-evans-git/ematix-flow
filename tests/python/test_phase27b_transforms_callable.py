"""Phase 27b: callable entries in transforms_post + @ematix.transform.

A transform callable receives the target connection (1-arg) or
(connection, parent_context) (2-arg, auto-detected via inspect).
Optional return-value dict is merged into the run_history row's
metrics_json.

@ematix.transform decorates a standalone-runnable callable that can
also be referenced in transforms_post.
"""

from typing import Annotated

import pytest

from ematix_flow import _core, ematix, pk
from ematix_flow import pipeline as p
from ematix_flow.types import BigInt, Text

pytestmark = pytest.mark.integration


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


# --- raw callable in transforms_post --------------------------------------


def test_callable_1arg_in_transforms_post(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._TRANSFORMS_REGISTRY.clear()

    def my_post(c):
        c.execute("INSERT INTO warehouse.scratch (n, label) VALUES (1, 'one-arg')")

    @ematix.table(schema="warehouse")
    class EventDim:
        id: Annotated[BigInt, pk()]
        name: Text

    @ematix.pipeline(
        target=EventDim,
        schedule="0 * * * *",
        mode="merge",
        transforms_post=[my_post],
    )
    def sync_events(c):
        return "SELECT id, name FROM src.events"

    result = sync_events()
    assert result["rows_inserted"] == 3
    assert (
        conn.fetch_scalar_int(
            "SELECT count(*)::int FROM warehouse.scratch WHERE label = 'one-arg'"
        )
        == 1
    )


def test_callable_2arg_receives_parent_context(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._TRANSFORMS_REGISTRY.clear()

    captured: dict = {}

    def with_parent(c, parent):
        captured["pipeline_name"] = parent.pipeline_name
        captured["run_id"] = parent.run_id
        captured["rows_inserted"] = parent.rows_inserted
        c.execute(
            f"INSERT INTO warehouse.scratch (n, label) VALUES "
            f"(0, 'parent={parent.pipeline_name}')"
        )

    @ematix.table(schema="warehouse")
    class EventDim:
        id: Annotated[BigInt, pk()]
        name: Text

    @ematix.pipeline(
        target=EventDim,
        schedule="0 * * * *",
        mode="merge",
        transforms_post=[with_parent],
    )
    def sync_events(c):
        return "SELECT id, name FROM src.events"

    result = sync_events()
    assert captured["pipeline_name"] == "sync_events"
    assert captured["run_id"] == result["run_id"]
    assert captured["rows_inserted"] == 3


def test_callable_returning_metrics_dict_is_recorded(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._TRANSFORMS_REGISTRY.clear()

    def with_metrics(c):
        c.execute("INSERT INTO warehouse.scratch (n, label) VALUES (42, 'metrics')")
        return {"rows_affected": 42, "step": "metric-bearing"}

    @ematix.table(schema="warehouse")
    class EventDim:
        id: Annotated[BigInt, pk()]
        name: Text

    @ematix.pipeline(
        target=EventDim,
        schedule="0 * * * *",
        mode="merge",
        transforms_post=[with_metrics],
    )
    def sync_events(c):
        return "SELECT id, name FROM src.events"

    result = sync_events()
    parent_run_id = result["run_id"]

    # metrics_json column has the dict serialized as JSON.
    rows = conn.execute  # placeholder; we use scalar checks
    raw = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM ematix_flow.run_history "
        f"WHERE parent_run_id = '{parent_run_id}' AND status = 'transform_success'"
    )
    assert raw == 1
    # Validate metrics_json content via a separate query that pulls the
    # JSON text. We don't have a fetch_text helper, so use jsonb's
    # ->>'rows_affected' and pull as int.
    metric_val = conn.fetch_scalar_int(
        f"SELECT (metrics_json::jsonb ->> 'rows_affected')::int "
        f"FROM ematix_flow.run_history "
        f"WHERE parent_run_id = '{parent_run_id}' AND status = 'transform_success'"
    )
    assert metric_val == 42


def test_callable_returning_none_records_success_without_metrics(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._TRANSFORMS_REGISTRY.clear()

    def silent(c):
        c.execute("INSERT INTO warehouse.scratch (n, label) VALUES (1, 'silent')")
        # implicit None

    @ematix.table(schema="warehouse")
    class EventDim:
        id: Annotated[BigInt, pk()]
        name: Text

    @ematix.pipeline(
        target=EventDim,
        schedule="0 * * * *",
        mode="merge",
        transforms_post=[silent],
    )
    def sync_events(c):
        return "SELECT id, name FROM src.events"

    result = sync_events()
    parent_run_id = result["run_id"]
    null_metrics = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM ematix_flow.run_history "
        f"WHERE parent_run_id = '{parent_run_id}' AND metrics_json IS NULL"
    )
    assert null_metrics == 1


def test_callable_raising_records_failure_and_halts(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._TRANSFORMS_REGISTRY.clear()

    def bad(c):
        raise RuntimeError("boom")

    def good(c):
        c.execute("INSERT INTO warehouse.scratch (n, label) VALUES (99, 'should-not-run')")

    @ematix.table(schema="warehouse")
    class EventDim:
        id: Annotated[BigInt, pk()]
        name: Text

    @ematix.pipeline(
        target=EventDim,
        schedule="0 * * * *",
        mode="merge",
        transforms_post=[bad, good],
    )
    def sync_events(c):
        return "SELECT id, name FROM src.events"

    with pytest.raises(RuntimeError, match="boom"):
        sync_events()
    # good callable did NOT run.
    assert (
        conn.fetch_scalar_int(
            "SELECT count(*)::int FROM warehouse.scratch WHERE n = 99"
        )
        == 0
    )
    # bad callable recorded as transform_failed.
    assert (
        conn.fetch_scalar_int(
            "SELECT count(*)::int FROM ematix_flow.run_history "
            "WHERE status = 'transform_failed'"
        )
        == 1
    )


# --- @ematix.transform standalone -----------------------------------------


def test_ematix_transform_decorator_registers_in_registry(monkeypatch, pg_url):
    _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._TRANSFORMS_REGISTRY.clear()

    @ematix.transform(name="my_transform")
    def my_transform(c):
        c.execute("INSERT INTO warehouse.scratch (n, label) VALUES (1, 'standalone')")

    assert "my_transform" in p._TRANSFORMS_REGISTRY


def test_ematix_transform_runs_standalone(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._TRANSFORMS_REGISTRY.clear()

    @ematix.transform(name="standalone_one")
    def standalone_one(c):
        c.execute("INSERT INTO warehouse.scratch (n, label) VALUES (7, 'solo')")
        return {"rows_affected": 1}

    result = p.run_transform("standalone_one")
    assert result["status"] == "transform_success"
    assert result["metrics"] == {"rows_affected": 1}
    assert (
        conn.fetch_scalar_int(
            "SELECT count(*)::int FROM warehouse.scratch WHERE label = 'solo'"
        )
        == 1
    )


def test_ematix_transform_default_name_from_function(monkeypatch, pg_url):
    _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._TRANSFORMS_REGISTRY.clear()

    @ematix.transform()
    def computed_default_name(c):
        pass

    assert "computed_default_name" in p._TRANSFORMS_REGISTRY


def test_ematix_transform_in_transforms_post_runs_with_parent_context(
    monkeypatch, pg_url
):
    conn = _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._TRANSFORMS_REGISTRY.clear()

    @ematix.transform(name="recompute_seg")
    def recompute_seg(c, parent):
        c.execute(
            f"INSERT INTO warehouse.scratch (n, label) VALUES "
            f"({parent.rows_inserted}, 'seg-from-{parent.pipeline_name}')"
        )

    @ematix.table(schema="warehouse")
    class EventDim:
        id: Annotated[BigInt, pk()]
        name: Text

    @ematix.pipeline(
        target=EventDim,
        schedule="0 * * * *",
        mode="merge",
        transforms_post=[recompute_seg],
    )
    def sync_events(c):
        return "SELECT id, name FROM src.events"

    sync_events()
    assert (
        conn.fetch_scalar_int(
            "SELECT count(*)::int FROM warehouse.scratch "
            "WHERE n = 3 AND label = 'seg-from-sync_events'"
        )
        == 1
    )


def test_ematix_transform_standalone_parent_is_none(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()
    p._TRANSFORMS_REGISTRY.clear()

    captured = {}

    @ematix.transform(name="solo_2arg")
    def solo_2arg(c, parent):
        captured["parent"] = parent
        c.execute("INSERT INTO warehouse.scratch (n, label) VALUES (1, 'solo-2arg')")

    p.run_transform("solo_2arg")
    assert captured["parent"] is None
