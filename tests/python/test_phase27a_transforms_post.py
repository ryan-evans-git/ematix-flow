"""Phase 27a: transforms_post=[...] for SQL strings.

Runs each entry in its own transaction on the pipeline's
target_connection after the main load commits. Halts on first failure
by default; `continue_on_failure_post=True` runs every step.

run_history is extended with parent_run_id (UUID, NULL for the main
load row, set for transform rows) and step_name (NULL for the main
load, set for transforms). status broadens to include
"transform_started" / "transform_success" / "transform_failed".
"""

import subprocess
import sys
import textwrap
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
    conn.execute(
        "CREATE TABLE src.events (id BIGINT PRIMARY KEY, name TEXT)"
    )
    conn.execute(
        "INSERT INTO src.events VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    )
    return conn


def test_transforms_post_runs_sql_string_after_load(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    conn.execute("CREATE SCHEMA warehouse")
    conn.execute("CREATE TABLE warehouse.audit_log (run_id UUID, ts TIMESTAMPTZ DEFAULT now())")

    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()

    @ematix.table(schema="warehouse")
    class EventDim:
        id: Annotated[BigInt, pk()]
        name: Text

    @ematix.pipeline(
        target=EventDim,
        schedule="0 * * * *",
        mode="merge",
        transforms_post=[
            "INSERT INTO warehouse.audit_log (run_id) VALUES (gen_random_uuid())",
        ],
    )
    def sync_events(c):
        return "SELECT id, name FROM src.events"

    result = sync_events()
    assert result["rows_inserted"] == 3
    # The post-transform inserted exactly one row.
    assert conn.fetch_scalar_int("SELECT count(*)::int FROM warehouse.audit_log") == 1


def test_transforms_post_records_in_run_history_with_parent_run_id(monkeypatch, pg_url):
    conn = _setup_pg(pg_url)
    conn.execute("CREATE SCHEMA warehouse")
    conn.execute("CREATE TABLE warehouse.scratch (n INT)")

    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()

    @ematix.table(schema="warehouse")
    class EventDim:
        id: Annotated[BigInt, pk()]
        name: Text

    @ematix.pipeline(
        target=EventDim,
        schedule="0 * * * *",
        mode="merge",
        transforms_post=[
            "INSERT INTO warehouse.scratch VALUES (1)",
            "INSERT INTO warehouse.scratch VALUES (2)",
        ],
    )
    def sync_events(c):
        return "SELECT id, name FROM src.events"

    result = sync_events()
    parent_run_id = result["run_id"]

    # One main-load row + two transform rows linked back to the parent.
    main_count = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM ematix_flow.run_history "
        f"WHERE run_id = '{parent_run_id}' AND parent_run_id IS NULL"
    )
    assert main_count == 1

    transform_count = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM ematix_flow.run_history "
        f"WHERE parent_run_id = '{parent_run_id}' AND status = 'transform_success'"
    )
    assert transform_count == 2

    # step_name is set on transform rows, NULL on the main load row.
    null_step = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM ematix_flow.run_history "
        f"WHERE run_id = '{parent_run_id}' AND step_name IS NULL"
    )
    assert null_step == 1
    set_step = conn.fetch_scalar_int(
        f"SELECT count(*)::int FROM ematix_flow.run_history "
        f"WHERE parent_run_id = '{parent_run_id}' AND step_name IS NOT NULL"
    )
    assert set_step == 2


def test_transforms_post_halt_on_first_failure_default(monkeypatch, pg_url):
    """First transform fails; the second must NOT run."""
    conn = _setup_pg(pg_url)
    conn.execute("CREATE SCHEMA warehouse")
    conn.execute("CREATE TABLE warehouse.scratch (n INT)")

    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()

    @ematix.table(schema="warehouse")
    class EventDim:
        id: Annotated[BigInt, pk()]
        name: Text

    @ematix.pipeline(
        target=EventDim,
        schedule="0 * * * *",
        mode="merge",
        transforms_post=[
            "INSERT INTO warehouse.does_not_exist VALUES (1)",  # FAILS
            "INSERT INTO warehouse.scratch VALUES (99)",        # should NOT run
        ],
    )
    def sync_events(c):
        return "SELECT id, name FROM src.events"

    with pytest.raises(Exception):
        sync_events()

    # Main load committed even though post-transform failed.
    assert conn.fetch_scalar_int("SELECT count(*)::int FROM warehouse.event_dim") == 3
    # Second transform never ran.
    assert conn.fetch_scalar_int("SELECT count(*)::int FROM warehouse.scratch") == 0
    # First transform recorded as failure in run_history.
    failed_count = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM ematix_flow.run_history "
        "WHERE status = 'transform_failed'"
    )
    assert failed_count == 1


def test_transforms_post_continue_on_failure(monkeypatch, pg_url):
    """First transform fails; the second still runs when continue_on_failure_post=True."""
    conn = _setup_pg(pg_url)
    conn.execute("CREATE SCHEMA warehouse")
    conn.execute("CREATE TABLE warehouse.scratch (n INT)")

    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()

    @ematix.table(schema="warehouse")
    class EventDim:
        id: Annotated[BigInt, pk()]
        name: Text

    @ematix.pipeline(
        target=EventDim,
        schedule="0 * * * *",
        mode="merge",
        transforms_post=[
            "INSERT INTO warehouse.does_not_exist VALUES (1)",  # FAILS
            "INSERT INTO warehouse.scratch VALUES (99)",        # SHOULD run
        ],
        continue_on_failure_post=True,
    )
    def sync_events(c):
        return "SELECT id, name FROM src.events"

    # Pipeline returns successfully because continue_on_failure_post swallows.
    result = sync_events()
    assert result["rows_inserted"] == 3
    # Second transform did run.
    assert conn.fetch_scalar_int("SELECT count(*)::int FROM warehouse.scratch WHERE n = 99") == 1
    # Both transform rows recorded — one failure, one success.
    failed = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM ematix_flow.run_history WHERE status = 'transform_failed'"
    )
    success = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM ematix_flow.run_history WHERE status = 'transform_success'"
    )
    assert failed == 1
    assert success == 1


def test_transforms_post_each_runs_in_own_transaction(monkeypatch, pg_url):
    """If transform 2 fails, transform 1's effects stay committed."""
    conn = _setup_pg(pg_url)
    conn.execute("CREATE SCHEMA warehouse")
    conn.execute("CREATE TABLE warehouse.scratch (n INT)")

    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()

    @ematix.table(schema="warehouse")
    class EventDim:
        id: Annotated[BigInt, pk()]
        name: Text

    @ematix.pipeline(
        target=EventDim,
        schedule="0 * * * *",
        mode="merge",
        transforms_post=[
            "INSERT INTO warehouse.scratch VALUES (1)",
            "INSERT INTO warehouse.does_not_exist VALUES (2)",  # FAILS
        ],
    )
    def sync_events(c):
        return "SELECT id, name FROM src.events"

    with pytest.raises(Exception):
        sync_events()

    # Transform 1 committed independently of transform 2's failure.
    assert conn.fetch_scalar_int("SELECT count(*)::int FROM warehouse.scratch WHERE n = 1") == 1
