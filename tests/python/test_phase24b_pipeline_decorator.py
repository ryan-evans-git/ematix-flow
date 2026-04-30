"""Phase 24b: `@ematix.pipeline` function decorator.

Wraps `pipeline.sync(...)` and registers via `pipeline.register(...)`.
Inspects the function signature to decide same-DB vs cross-DB. Resolves
connection names via Phase 21's `connect()`.
"""

from __future__ import annotations

import inspect
import os
import uuid
from collections.abc import Iterator
from typing import Annotated, Any

import pytest

from ematix_flow import _core, ematix, pipeline as p, pk
from ematix_flow.source import Source
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


# --- registration -----------------------------------------------------------


def test_decorator_registers_with_function_name() -> None:
    @ematix.table(schema="s")
    class Customer:
        id: Annotated[BigInt, pk()]
        email: String[256]

    @ematix.pipeline(target=Customer, schedule="0 * * * *", mode="merge")
    def sync_customers(conn):
        return "SELECT id, email FROM source.users"

    assert "sync_customers" in {sp.name for sp in p.list_pipelines()}


def test_decorator_explicit_name_overrides_function_name() -> None:
    @ematix.table(schema="s")
    class Customer:
        id: Annotated[BigInt, pk()]

    @ematix.pipeline(
        target=Customer,
        schedule="0 * * * *",
        mode="merge",
        name="customers_v2",
    )
    def sync_customers(conn):
        return "SELECT id FROM users"

    assert "customers_v2" in {sp.name for sp in p.list_pipelines()}


def test_decorator_preserves_callable() -> None:
    @ematix.table(schema="s")
    class Customer:
        id: Annotated[BigInt, pk()]

    @ematix.pipeline(target=Customer, schedule="0 * * * *", mode="merge")
    def sync_customers(conn):
        return "SELECT id FROM users"

    # Must still be callable as a function for tests / introspection.
    assert callable(sync_customers)
    # And carry the original name for nicer reprs.
    assert sync_customers.__name__ == "sync_customers"


# --- signature inspection ---------------------------------------------------


def test_zero_arg_signature_requires_source_table() -> None:
    @ematix.table(schema="s")
    class Customer:
        id: Annotated[BigInt, pk()]

    with pytest.raises(TypeError, match="source_table"):

        @ematix.pipeline(target=Customer, schedule="0 * * * *", mode="merge")
        def f():
            pass


def test_one_arg_signature_uses_target_connection_only() -> None:
    @ematix.table(schema="s")
    class Customer:
        id: Annotated[BigInt, pk()]

    @ematix.pipeline(target=Customer, schedule="0 * * * *", mode="merge")
    def f(conn):
        return "SELECT id FROM users"

    sig = inspect.signature(f.__wrapped__)
    assert list(sig.parameters) == ["conn"]


def test_two_arg_signature_implies_cross_db() -> None:
    @ematix.table(schema="s")
    class Customer:
        id: Annotated[BigInt, pk()]

    @ematix.pipeline(target=Customer, schedule="0 * * * *", mode="merge")
    def f(src, tgt):
        return "SELECT id FROM users"

    sig = inspect.signature(f.__wrapped__)
    assert list(sig.parameters) == ["src", "tgt"]


def test_three_or_more_args_rejected() -> None:
    @ematix.table(schema="s")
    class Customer:
        id: Annotated[BigInt, pk()]

    with pytest.raises(TypeError, match="0, 1, or 2"):

        @ematix.pipeline(target=Customer, schedule="0 * * * *", mode="merge")
        def f(a, b, c):
            return "SELECT 1"


# --- async support ----------------------------------------------------------


def test_async_def_pipeline_accepted() -> None:
    @ematix.table(schema="s")
    class Customer:
        id: Annotated[BigInt, pk()]

    @ematix.pipeline(target=Customer, schedule="0 * * * *", mode="merge")
    async def f(conn):
        return "SELECT id FROM users"

    sp = p.get_pipeline("f")
    assert sp.name == "f"


# --- integration ------------------------------------------------------------


pytestmark_int = pytest.mark.integration


@pytest.fixture
def schema_name() -> str:
    return f"phase24b_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def src_schema() -> str:
    return f"phase24bsrc_{uuid.uuid4().hex[:8]}"


def _seed_source(conn: Any, schema: str, rows: list[tuple[int, str, str]]) -> None:
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.execute(f"DROP TABLE IF EXISTS {schema}.src_customers")
    conn.execute(
        f"CREATE TABLE {schema}.src_customers ("
        f"  id BIGINT PRIMARY KEY,"
        f"  email VARCHAR(256) NOT NULL,"
        f"  name TEXT NOT NULL"
        f")"
    )
    if rows:
        values = ", ".join(f"({i}, '{e}', '{n}')" for i, e, n in rows)
        conn.execute(f"INSERT INTO {schema}.src_customers VALUES {values}")


@pytest.mark.integration
def test_decorated_pipeline_runs_against_default_connection(
    pg_url: str, schema_name: str, src_schema: str, monkeypatch
) -> None:
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    # Seed data using the same connection.
    seed = _core.connect(pg_url)
    _seed_source(seed, src_schema, [(1, "a@x.com", "Alice")])

    @ematix.table(schema=schema_name)
    class CustomerDim:
        customer_id: Annotated[BigInt, pk()]
        email: String[256]
        name: Text

    @ematix.pipeline(
        target=CustomerDim,
        schedule="0 * * * *",
        mode="merge",
        name="customers_phase24b_default",
    )
    def sync_customers(conn):
        return (
            f"SELECT id AS customer_id, email, name "
            f"FROM {src_schema}.src_customers"
        )

    result = p.run_pipeline("customers_phase24b_default")
    assert result["status"] == "success"
    assert result["rows_inserted"] == 1


@pytest.mark.integration
def test_decorated_pipeline_with_named_connection(
    pg_url: str, schema_name: str, src_schema: str, monkeypatch
) -> None:
    monkeypatch.setenv("EMATIX_FLOW_DSN_WAREHOUSE", pg_url)
    seed = _core.connect(pg_url)
    _seed_source(seed, src_schema, [(1, "a@x.com", "Alice")])

    @ematix.table(schema=schema_name)
    class CustomerDim:
        customer_id: Annotated[BigInt, pk()]
        email: String[256]
        name: Text

    @ematix.pipeline(
        target=CustomerDim,
        schedule="0 * * * *",
        mode="merge",
        target_connection="warehouse",
        name="customers_phase24b_named",
    )
    def sync_customers(conn):
        return (
            f"SELECT id AS customer_id, email, name "
            f"FROM {src_schema}.src_customers"
        )

    result = p.run_pipeline("customers_phase24b_named")
    assert result["status"] == "success"


@pytest.mark.integration
def test_decorated_pipeline_cross_db(
    pg_url: str, pg_url_secondary: str, schema_name: str, src_schema: str,
    monkeypatch,
) -> None:
    monkeypatch.setenv("EMATIX_FLOW_DSN_OLTP", pg_url_secondary)
    monkeypatch.setenv("EMATIX_FLOW_DSN_WAREHOUSE", pg_url)

    src_seed = _core.connect(pg_url_secondary)
    _seed_source(src_seed, src_schema, [(1, "a@x.com", "Alice")])

    @ematix.table(schema=schema_name)
    class CustomerDim:
        customer_id: Annotated[BigInt, pk()]
        email: String[256]
        name: Text

    @ematix.pipeline(
        target=CustomerDim,
        schedule="0 * * * *",
        mode="merge",
        source_connection="oltp",
        target_connection="warehouse",
        name="customers_phase24b_cross_db",
    )
    def sync_customers(src, tgt):
        return (
            f"SELECT id AS customer_id, email, name "
            f"FROM {src_schema}.src_customers"
        )

    result = p.run_pipeline("customers_phase24b_cross_db")
    assert result["status"] == "success"
    assert result["path"] == "cross_db"
