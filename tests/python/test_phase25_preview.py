"""Phase 25: pipeline.preview(name) + .preview() decorator method + CLI."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from typing import Annotated

import pytest

from ematix_flow import _core, ematix, pipeline as p, pk
from ematix_flow.preview import PreviewResult
from ematix_flow.types import BigInt, String, Text


@pytest.fixture(autouse=True)
def _clean_registry():
    p._REGISTRY.clear()
    yield
    p._REGISTRY.clear()


@pytest.fixture
def clean_env(monkeypatch) -> Iterator[None]:
    for key in list(os.environ):
        if key.startswith("EMATIX_FLOW_"):
            monkeypatch.delenv(key, raising=False)
    yield


# --- pyo3 plan_*_sql wrappers (no DB) --------------------------------------


def test_plan_append_sql_returns_single_insert_statement() -> None:
    spec = {
        "schema": "warehouse",
        "name": "customer_dim",
        "columns": [
            {"name": "customer_id", "type": {"kind": "big_int"}, "nullable": False, "primary_key": True},
            {"name": "email", "type": {"kind": "string", "length": 256}, "nullable": False, "primary_key": False},
            {"name": "_loaded_at", "type": {"kind": "timestamp_tz"}, "nullable": False, "primary_key": False},
            {"name": "_batch_id", "type": {"kind": "uuid"}, "nullable": False, "primary_key": False},
        ],
    }
    sql = _core.plan_append_sql(json.dumps(spec), "SELECT * FROM src")
    assert "INSERT INTO warehouse.customer_dim" in sql
    assert "(customer_id, email, _loaded_at, _batch_id)" in sql
    assert "$1::uuid" in sql


def test_plan_truncate_sql_returns_two_statements() -> None:
    spec = {
        "schema": "s",
        "name": "t",
        "columns": [
            {"name": "id", "type": {"kind": "big_int"}, "nullable": False, "primary_key": True},
        ],
    }
    statements = _core.plan_truncate_sql(json.dumps(spec), "SELECT * FROM src")
    assert isinstance(statements, list)
    assert len(statements) == 2
    assert statements[0].startswith("TRUNCATE TABLE s.t")
    assert "INSERT INTO s.t" in statements[1]


def test_plan_merge_sql_returns_cte_query() -> None:
    spec = {
        "schema": "s",
        "name": "t",
        "columns": [
            {"name": "id", "type": {"kind": "big_int"}, "nullable": False, "primary_key": True},
            {"name": "v", "type": {"kind": "text"}, "nullable": True, "primary_key": False},
        ],
    }
    sql = _core.plan_merge_sql(
        json.dumps(spec), "SELECT * FROM src", ["id"], ["v"]
    )
    assert "ON CONFLICT (id) DO UPDATE" in sql
    assert "v = EXCLUDED.v" in sql
    assert "RETURNING (xmax = 0)" in sql


def test_plan_scd2_sql_returns_three_statements() -> None:
    spec = {
        "schema": "s",
        "name": "t",
        "columns": [
            {"name": "id", "type": {"kind": "big_int"}, "nullable": False, "primary_key": True},
            {"name": "v", "type": {"kind": "text"}, "nullable": True, "primary_key": False},
            {"name": "valid_from", "type": {"kind": "timestamp_tz"}, "nullable": False, "primary_key": True},
            {"name": "valid_to", "type": {"kind": "timestamp_tz"}, "nullable": True, "primary_key": False},
            {"name": "is_current", "type": {"kind": "boolean"}, "nullable": False, "primary_key": False},
            {"name": "row_hash", "type": {"kind": "bytes"}, "nullable": False, "primary_key": False},
        ],
    }
    statements = _core.plan_scd2_sql(
        json.dumps(spec),
        "SELECT * FROM src",
        ["id"],
        ["v"],
        "preview_token",
        None,  # no event_ts
    )
    assert len(statements) == 3
    assert "CREATE TEMP TABLE" in statements[0]
    assert "_scd2_changed_preview_token" in statements[0]
    assert "UPDATE" in statements[1]
    assert "INSERT INTO" in statements[2]


# --- pipeline.preview / .preview() ------------------------------------------


def test_pipeline_preview_returns_preview_result(clean_env, monkeypatch) -> None:
    monkeypatch.setenv("EMATIX_FLOW_DSN", "postgres://noop@localhost/db")

    @ematix.table(schema="warehouse")
    class Customer:
        id: Annotated[BigInt, pk()]
        email: String[256]

    @ematix.pipeline(
        target=Customer, schedule="0 * * * *", mode="merge", name="customer_sync"
    )
    def f(conn):
        return "SELECT id, email FROM source.users"

    result = p.preview("customer_sync")
    assert isinstance(result, PreviewResult)
    assert result.pipeline_name == "customer_sync"
    assert result.schedule == "0 * * * *"
    assert result.mode == "merge"
    # The plan contains at least one statement.
    assert len(result.targets) == 1
    assert len(result.targets[0].plan_sql) >= 1


def test_decorator_preview_method(clean_env, monkeypatch) -> None:
    monkeypatch.setenv("EMATIX_FLOW_DSN", "postgres://noop@localhost/db")

    @ematix.table(schema="warehouse")
    class Customer:
        id: Annotated[BigInt, pk()]
        email: String[256]

    @ematix.pipeline(
        target=Customer, schedule="0 * * * *", mode="merge", name="cust"
    )
    def sync_customers(conn):
        return "SELECT id, email FROM source.users"

    # The decorator-returned wrapper exposes .preview() as a method.
    result = sync_customers.preview()
    assert isinstance(result, PreviewResult)
    assert result.pipeline_name == "cust"


def test_preview_unknown_pipeline_raises() -> None:
    with pytest.raises(KeyError):
        p.preview("does_not_exist")


def test_preview_resolves_keys_with_reason(clean_env, monkeypatch) -> None:
    """Resolved keys should report the source (PK / natural_key / explicit)."""
    monkeypatch.setenv("EMATIX_FLOW_DSN", "postgres://noop@localhost/db")

    @ematix.table(schema="s")
    class CustomerOrder:
        id: Annotated[BigInt, pk()]
        customer_id: BigInt
        order_date: BigInt
        # Composite UNIQUE via the dunder.
        __unique_constraints__ = (("customer_id", "order_date"),)

    @ematix.pipeline(
        target=CustomerOrder,
        schedule="0 * * * *",
        mode="merge",
        name="orders_pipe",
    )
    def f(conn):
        return "SELECT id, customer_id, order_date FROM src"

    result = p.preview("orders_pipe")
    target = result.targets[0]
    # Natural key wins over PK.
    assert target.merge_keys == ["customer_id", "order_date"]
    # Reason mentions the source.
    assert "natural" in target.merge_keys_reason.lower() or "unique" in target.merge_keys_reason.lower()


# --- multi-target preview --------------------------------------------------


def test_multi_target_preview_renders_each_target(clean_env, monkeypatch) -> None:
    monkeypatch.setenv("EMATIX_FLOW_DSN", "postgres://noop@localhost/db")

    @ematix.table(schema="s", name="a")
    class A:
        id: Annotated[BigInt, pk()]
        v: Text

    @ematix.table(schema="s", name="b")
    class B:
        id: Annotated[BigInt, pk()]
        v: Text

    @ematix.pipeline(
        targets=[
            ematix.target(A, mode="append"),
            ematix.target(B, mode="append"),
        ],
        schedule="0 * * * *",
        name="multi_pipe",
    )
    def f(conn):
        return "SELECT id, v FROM src"

    result = p.preview("multi_pipe")
    assert len(result.targets) == 2
    assert result.targets[0].schema_qualified_name == "s.a"
    assert result.targets[1].schema_qualified_name == "s.b"


# --- JSON output -----------------------------------------------------------


def test_preview_serializes_to_json(clean_env, monkeypatch) -> None:
    monkeypatch.setenv("EMATIX_FLOW_DSN", "postgres://noop@localhost/db")

    @ematix.table(schema="s")
    class T:
        id: Annotated[BigInt, pk()]
        v: Text

    @ematix.pipeline(
        target=T, schedule="0 * * * *", mode="merge", name="json_pipe"
    )
    def f(conn):
        return "SELECT id, v FROM src"

    result = p.preview("json_pipe")
    payload = result.to_json()
    parsed = json.loads(payload)
    assert parsed["pipeline_name"] == "json_pipe"
    assert "targets" in parsed
    assert isinstance(parsed["targets"], list)


# --- CLI -------------------------------------------------------------------


def test_flow_preview_cli_text_output(tmp_path, capsys, monkeypatch, clean_env) -> None:
    """`flow preview <name> --module foo` prints the rendered preview."""
    monkeypatch.setenv("EMATIX_FLOW_DSN", "postgres://noop@localhost/db")
    mod = "phase25_preview_mod"
    (tmp_path / f"{mod}.py").write_text(
        "from typing import Annotated\n"
        "from ematix_flow import ematix, pk\n"
        "from ematix_flow.types import BigInt, Text\n"
        "@ematix.table(schema='s')\n"
        "class T:\n"
        "    id: Annotated[BigInt, pk()]\n"
        "    v: Text\n"
        "@ematix.pipeline(target=T, schedule='0 * * * *', mode='merge', name='cli_pipe')\n"
        "def sync(conn):\n"
        "    return 'SELECT id, v FROM src'\n"
    )
    import sys

    sys.path.insert(0, str(tmp_path))
    try:
        from ematix_flow.cli import main

        rc = main(["preview", "cli_pipe", "--module", mod])
        assert rc == 0
        out = capsys.readouterr().out
        assert "cli_pipe" in out
        assert "merge" in out
    finally:
        sys.path.pop(0)
        sys.modules.pop(mod, None)


def test_flow_preview_cli_json_format(tmp_path, capsys, monkeypatch, clean_env) -> None:
    monkeypatch.setenv("EMATIX_FLOW_DSN", "postgres://noop@localhost/db")
    mod = "phase25_preview_mod_json"
    (tmp_path / f"{mod}.py").write_text(
        "from typing import Annotated\n"
        "from ematix_flow import ematix, pk\n"
        "from ematix_flow.types import BigInt, Text\n"
        "@ematix.table(schema='s')\n"
        "class T:\n"
        "    id: Annotated[BigInt, pk()]\n"
        "    v: Text\n"
        "@ematix.pipeline(target=T, schedule='0 * * * *', mode='merge', name='json_cli_pipe')\n"
        "def sync(conn):\n"
        "    return 'SELECT id, v FROM src'\n"
    )
    import sys

    sys.path.insert(0, str(tmp_path))
    try:
        from ematix_flow.cli import main

        rc = main(["preview", "json_cli_pipe", "--module", mod, "--format", "json"])
        assert rc == 0
        out = capsys.readouterr().out
        # Output should be parseable JSON.
        parsed = json.loads(out)
        assert parsed["pipeline_name"] == "json_cli_pipe"
    finally:
        sys.path.pop(0)
        sys.modules.pop(mod, None)
