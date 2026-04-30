"""Phase 26: `flow validate <pipeline>` — runs EXPLAIN against the target
connection on the synthesized source SQL. Surfaces type/syntax errors at
user-controlled times (CI, pre-deploy) without taxing decoration.
"""

import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Annotated

import pytest

from ematix_flow import _core, ematix, pipeline as p, pk
from ematix_flow.normalize import empty_to_null, lower, trim
from ematix_flow.types import BigInt, String, Text

pytestmark = pytest.mark.integration


def _write_user_module(tmp_path: Path, body: str) -> Path:
    mod_dir = tmp_path / "userpkg"
    mod_dir.mkdir()
    (mod_dir / "__init__.py").write_text("")
    (mod_dir / "pipelines.py").write_text(textwrap.dedent(body))
    return mod_dir


def _run_cli(args: list[str], cwd: Path, env_extra: dict[str, str]) -> subprocess.CompletedProcess:
    import os

    env = os.environ.copy()
    env["PYTHONPATH"] = f"{cwd}:{env.get('PYTHONPATH', '')}"
    env.update(env_extra)
    return subprocess.run(
        [sys.executable, "-m", "ematix_flow.cli", *args],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
    )


def _setup_pg(pg_url: str):
    conn = _core.connect(pg_url)
    conn.execute("DROP SCHEMA IF EXISTS warehouse CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS src CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS ematix_flow CASCADE")
    conn.execute("CREATE SCHEMA src")
    conn.execute(
        "CREATE TABLE src.users (user_id BIGINT PRIMARY KEY, email TEXT, name TEXT)"
    )
    return conn


def test_validate_succeeds_for_correct_pipeline(monkeypatch, pg_url, tmp_path):
    _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()

    @ematix.table(schema="warehouse")
    class UserDim:
        user_id: Annotated[BigInt, pk()]
        email: Annotated[String[256] | None, lower(), trim(), empty_to_null()]
        name: Text | None

    @ematix.pipeline(target=UserDim, schedule="0 * * * *", mode="merge")
    def sync_users(c):
        return "SELECT user_id, email, name FROM src.users"

    result = p.validate("sync_users")
    assert result.ok is True
    assert result.errors == []


def test_validate_fails_when_source_column_missing(monkeypatch, pg_url, tmp_path):
    _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()

    @ematix.table(schema="warehouse")
    class UserDim:
        user_id: Annotated[BigInt, pk()]
        # Note: source query selects `nonexistent_column`, but the table
        # decorator declares a different shape — Postgres rejects.
        email: Text | None

    @ematix.pipeline(target=UserDim, schedule="0 * * * *", mode="merge")
    def sync_users(c):
        return "SELECT user_id, nonexistent_column AS email FROM src.users"

    result = p.validate("sync_users")
    assert result.ok is False
    assert len(result.errors) == 1
    assert "nonexistent_column" in result.errors[0].lower() or "does not exist" in result.errors[0].lower()


def test_validate_fails_when_table_missing(monkeypatch, pg_url):
    _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()

    @ematix.table(schema="warehouse")
    class UserDim:
        user_id: Annotated[BigInt, pk()]

    @ematix.pipeline(target=UserDim, schedule="0 * * * *", mode="merge")
    def sync_users(c):
        return "SELECT user_id FROM src.does_not_exist"

    result = p.validate("sync_users")
    assert result.ok is False
    assert "does_not_exist" in " ".join(result.errors).lower() or any(
        "does not exist" in e.lower() for e in result.errors
    )


def test_validate_returns_synthesized_source_sql(monkeypatch, pg_url):
    _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()

    @ematix.table(schema="warehouse")
    class UserDim:
        user_id: Annotated[BigInt, pk()]
        email: Annotated[String[256], lower(), trim()]

    @ematix.pipeline(target=UserDim, schedule="0 * * * *", mode="merge")
    def sync_users(c):
        return "SELECT user_id, email FROM src.users"

    result = p.validate("sync_users")
    assert result.ok is True
    # The synthesized SQL should include the normalized CTE.
    assert "_src_normalized" in result.source_sql
    assert "trim(lower(email))" in result.source_sql


def test_validate_unknown_pipeline_raises_keyerror(monkeypatch, pg_url):
    _setup_pg(pg_url)
    monkeypatch.setenv("EMATIX_FLOW_DSN", pg_url)
    p._REGISTRY.clear()

    with pytest.raises(KeyError):
        p.validate("nope")


def test_flow_validate_cli_exit0_on_success(monkeypatch, pg_url, tmp_path):
    _setup_pg(pg_url)

    body = """
        from typing import Annotated
        from ematix_flow import ematix, pk
        from ematix_flow.types import BigInt, Text

        @ematix.table(schema="warehouse")
        class UserDim:
            user_id: Annotated[BigInt, pk()]
            email: Text | None

        @ematix.pipeline(target=UserDim, schedule="0 * * * *", mode="merge")
        def sync_users(c):
            return "SELECT user_id, email FROM src.users"
    """
    cwd = _write_user_module(tmp_path, body)
    proc = _run_cli(
        ["validate", "--module", "userpkg.pipelines", "sync_users"],
        cwd=tmp_path,
        env_extra={"EMATIX_FLOW_DSN": pg_url},
    )
    assert proc.returncode == 0, proc.stderr
    assert "ok" in proc.stdout.lower()


def test_flow_validate_cli_exit_nonzero_on_failure(monkeypatch, pg_url, tmp_path):
    _setup_pg(pg_url)

    body = """
        from typing import Annotated
        from ematix_flow import ematix, pk
        from ematix_flow.types import BigInt, Text

        @ematix.table(schema="warehouse")
        class UserDim:
            user_id: Annotated[BigInt, pk()]
            email: Text | None

        @ematix.pipeline(target=UserDim, schedule="0 * * * *", mode="merge")
        def sync_users(c):
            return "SELECT user_id, definitely_not_a_column AS email FROM src.users"
    """
    cwd = _write_user_module(tmp_path, body)
    proc = _run_cli(
        ["validate", "--module", "userpkg.pipelines", "sync_users"],
        cwd=tmp_path,
        env_extra={"EMATIX_FLOW_DSN": pg_url},
    )
    assert proc.returncode != 0
    combined = proc.stdout + proc.stderr
    assert "definitely_not_a_column" in combined.lower() or "does not exist" in combined.lower()
