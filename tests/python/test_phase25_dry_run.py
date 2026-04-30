"""Phase 25b: dry-run executes the load inside a tx and ROLLBACKs.

The user sees row counts that *would* have been affected, no rows
actually persist, no run_history rows are written.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from typing import Annotated, Any

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


@pytest.fixture
def schema_name() -> str:
    return f"phase25dr_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def src_schema() -> str:
    return f"phase25drsrc_{uuid.uuid4().hex[:8]}"


def _seed_source(conn: Any, schema: str) -> None:
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.execute(f"DROP TABLE IF EXISTS {schema}.users")
    conn.execute(
        f"CREATE TABLE {schema}.users ("
        f"  id BIGINT PRIMARY KEY,"
        f"  email VARCHAR(256) NOT NULL,"
        f"  name TEXT NOT NULL"
        f")"
    )
    conn.execute(
        f"INSERT INTO {schema}.users VALUES "
        f"(1, 'a@x.com', 'Alice'),"
        f"(2, 'b@x.com', 'Bob'),"
        f"(3, 'c@x.com', 'Carol')"
    )


@pytest.mark.integration
def test_dry_run_returns_preview_result(
    pg_url: str, schema_name: str, src_schema: str, clean_env, monkeypatch
) -> None:
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    seed = _core.connect(pg_url)
    _seed_source(seed, src_schema)

    @ematix.table(schema=schema_name)
    class CustomerDim:
        customer_id: Annotated[BigInt, pk()]
        email: String[256]
        name: Text

    @ematix.pipeline(
        target=CustomerDim,
        schedule="0 * * * *",
        mode="merge",
        name="dry_basic",
    )
    def sync_customers(conn):
        return f"SELECT id AS customer_id, email, name FROM {src_schema}.users"

    result = p.dry_run("dry_basic")
    assert isinstance(result, PreviewResult)
    assert result.is_dry_run is True


@pytest.mark.integration
def test_dry_run_does_not_persist_rows(
    pg_url: str, schema_name: str, src_schema: str, clean_env, monkeypatch
) -> None:
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    seed = _core.connect(pg_url)
    _seed_source(seed, src_schema)

    @ematix.table(schema=schema_name)
    class CustomerDim:
        customer_id: Annotated[BigInt, pk()]
        email: String[256]
        name: Text

    @ematix.pipeline(
        target=CustomerDim,
        schedule="0 * * * *",
        mode="merge",
        name="dry_no_persist",
    )
    def sync_customers(conn):
        return f"SELECT id AS customer_id, email, name FROM {src_schema}.users"

    p.dry_run("dry_no_persist")

    # Target table either doesn't exist, or exists with 0 rows.
    exists = seed.fetch_scalar_int(
        f"SELECT count(*)::int FROM information_schema.tables "
        f"WHERE table_schema = '{schema_name}' AND table_name = 'customer_dim'"
    )
    if exists:
        rows = seed.fetch_scalar_int(
            f"SELECT count(*)::int FROM {schema_name}.customer_dim"
        )
        assert rows == 0


@pytest.mark.integration
def test_dry_run_skips_run_history(
    pg_url: str, schema_name: str, src_schema: str, clean_env, monkeypatch
) -> None:
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    seed = _core.connect(pg_url)
    _seed_source(seed, src_schema)

    @ematix.table(schema=schema_name)
    class CustomerDim:
        customer_id: Annotated[BigInt, pk()]
        email: String[256]
        name: Text

    @ematix.pipeline(
        target=CustomerDim,
        schedule="0 * * * *",
        mode="merge",
        name="dry_no_history",
    )
    def sync_customers(conn):
        return f"SELECT id AS customer_id, email, name FROM {src_schema}.users"

    p.dry_run("dry_no_history")

    # No run_history row for this pipeline.
    found = seed.fetch_scalar_int(
        "SELECT count(*)::int FROM information_schema.tables "
        "WHERE table_schema='ematix_flow' AND table_name='run_history'"
    )
    if found:
        rows = seed.fetch_scalar_int(
            "SELECT count(*)::int FROM ematix_flow.run_history "
            "WHERE pipeline_name = 'dry_no_history'"
        )
        assert rows == 0


@pytest.mark.integration
def test_dry_run_reports_row_counts_that_would_have_been_affected(
    pg_url: str, schema_name: str, src_schema: str, clean_env, monkeypatch
) -> None:
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    seed = _core.connect(pg_url)
    _seed_source(seed, src_schema)

    @ematix.table(schema=schema_name)
    class CustomerDim:
        customer_id: Annotated[BigInt, pk()]
        email: String[256]
        name: Text

    @ematix.pipeline(
        target=CustomerDim,
        schedule="0 * * * *",
        mode="merge",
        name="dry_counts",
    )
    def sync_customers(conn):
        return f"SELECT id AS customer_id, email, name FROM {src_schema}.users"

    result = p.dry_run("dry_counts")
    target = result.targets[0]
    # Row counts captured (would have inserted 3, updated 0).
    assert target.dry_run_rows_affected.get("rows_inserted") == 3


@pytest.mark.integration
def test_dry_run_decorator_method(
    pg_url: str, schema_name: str, src_schema: str, clean_env, monkeypatch
) -> None:
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    seed = _core.connect(pg_url)
    _seed_source(seed, src_schema)

    @ematix.table(schema=schema_name)
    class CustomerDim:
        customer_id: Annotated[BigInt, pk()]
        email: String[256]
        name: Text

    @ematix.pipeline(
        target=CustomerDim,
        schedule="0 * * * *",
        mode="merge",
        name="dry_method",
    )
    def sync_customers(conn):
        return f"SELECT id AS customer_id, email, name FROM {src_schema}.users"

    result = sync_customers.dry_run()
    assert result.is_dry_run is True


@pytest.mark.integration
def test_flow_dry_run_cli(
    pg_url: str, schema_name: str, src_schema: str, tmp_path, capsys, monkeypatch, clean_env
) -> None:
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    seed = _core.connect(pg_url)
    _seed_source(seed, src_schema)

    mod = "phase25_dry_run_cli_mod"
    (tmp_path / f"{mod}.py").write_text(
        "from typing import Annotated\n"
        "from ematix_flow import ematix, pk\n"
        "from ematix_flow.types import BigInt, String, Text\n"
        f"@ematix.table(schema='{schema_name}')\n"
        "class CustomerDim:\n"
        "    customer_id: Annotated[BigInt, pk()]\n"
        "    email: String[256]\n"
        "    name: Text\n"
        "@ematix.pipeline(target=CustomerDim, schedule='0 * * * *', mode='merge', name='dry_cli')\n"
        "def sync(conn):\n"
        f"    return 'SELECT id AS customer_id, email, name FROM {src_schema}.users'\n"
    )
    import sys

    sys.path.insert(0, str(tmp_path))
    try:
        from ematix_flow.cli import main

        rc = main(["dry-run", "dry_cli", "--module", mod])
        assert rc == 0
        out = capsys.readouterr().out
        assert "DRY RUN" in out
        assert "dry_cli" in out
    finally:
        sys.path.pop(0)
        sys.modules.pop(mod, None)
