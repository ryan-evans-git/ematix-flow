"""Phase 27d: flow list (merged), flow transform list, flow transform run."""

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest


def _run_cli(args, cwd, env_extra=None):
    env = os.environ.copy()
    env["PYTHONPATH"] = f"{cwd}:{env.get('PYTHONPATH', '')}"
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        [sys.executable, "-m", "ematix_flow.cli", *args],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
    )


def _write_user_module(tmp_path: Path, body: str) -> Path:
    pkg = tmp_path / "userpkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    (pkg / "pipelines.py").write_text(textwrap.dedent(body))
    return pkg


# --- merged `flow list` --------------------------------------------------


def test_flow_list_shows_pipelines_and_transforms_with_kind(tmp_path):
    body = """
        from typing import Annotated
        from ematix_flow import ematix, pk
        from ematix_flow.types import BigInt

        @ematix.transform(name="my_xform")
        def my_xform(c):
            pass

        @ematix.table(schema="s")
        class T:
            id: Annotated[BigInt, pk()]

        @ematix.pipeline(target=T, schedule="0 * * * *", mode="merge")
        def my_pipe(c):
            return "SELECT 1 AS id"
    """
    _write_user_module(tmp_path, body)
    proc = _run_cli(["list", "--module", "userpkg.pipelines"], cwd=tmp_path)
    assert proc.returncode == 0, proc.stderr
    out = proc.stdout
    assert "my_pipe" in out
    assert "my_xform" in out
    # The merged listing shows the kind column.
    assert "pipeline" in out
    assert "transform" in out


def test_flow_list_format_json(tmp_path):
    body = """
        from typing import Annotated
        from ematix_flow import ematix, pk
        from ematix_flow.types import BigInt

        @ematix.transform(name="x1", schedule="*/5 * * * *")
        def x1(c):
            pass

        @ematix.table(schema="s")
        class T:
            id: Annotated[BigInt, pk()]

        @ematix.pipeline(target=T, schedule=None, mode="merge", name="manual_pipe")
        def my_pipe(c):
            return "SELECT 1 AS id"
    """
    _write_user_module(tmp_path, body)
    proc = _run_cli(
        ["list", "--module", "userpkg.pipelines", "--format", "json"],
        cwd=tmp_path,
    )
    assert proc.returncode == 0, proc.stderr
    import json

    rows = json.loads(proc.stdout)
    by_name = {r["name"]: r for r in rows}
    assert by_name["x1"]["kind"] == "transform"
    assert by_name["x1"]["schedule"] == "*/5 * * * *"
    assert by_name["manual_pipe"]["kind"] == "pipeline"
    assert by_name["manual_pipe"]["schedule"] is None


# --- flow transform list -------------------------------------------------


def test_flow_transform_list_shows_only_transforms(tmp_path):
    body = """
        from typing import Annotated
        from ematix_flow import ematix, pk
        from ematix_flow.types import BigInt

        @ematix.transform(name="x1")
        def x1(c):
            pass

        @ematix.transform(name="x2", schedule="0 0 * * *")
        def x2(c):
            pass

        @ematix.table(schema="s")
        class T:
            id: Annotated[BigInt, pk()]

        @ematix.pipeline(target=T, schedule="0 * * * *", mode="merge")
        def my_pipe(c):
            return "SELECT 1 AS id"
    """
    _write_user_module(tmp_path, body)
    proc = _run_cli(
        ["transform", "list", "--module", "userpkg.pipelines"], cwd=tmp_path
    )
    assert proc.returncode == 0, proc.stderr
    out = proc.stdout
    assert "x1" in out
    assert "x2" in out
    # The pipeline should NOT appear in the filtered list.
    assert "my_pipe" not in out


# --- flow transform run --------------------------------------------------


pytestmark_int = pytest.mark.integration


@pytest.mark.integration
def test_flow_transform_run_executes_standalone(monkeypatch, pg_url, tmp_path):
    from ematix_flow import _core

    conn = _core.connect(pg_url)
    conn.execute("DROP SCHEMA IF EXISTS warehouse CASCADE")
    conn.execute("CREATE SCHEMA warehouse")
    conn.execute("CREATE TABLE warehouse.scratch (n INT)")

    body = """
        from ematix_flow import ematix

        @ematix.transform(name="solo")
        def solo(c):
            c.execute("INSERT INTO warehouse.scratch VALUES (42)")
            return {"rows_affected": 1}
    """
    _write_user_module(tmp_path, body)
    proc = _run_cli(
        ["transform", "run", "--module", "userpkg.pipelines", "solo"],
        cwd=tmp_path,
        env_extra={"EMATIX_FLOW_DSN": pg_url},
    )
    assert proc.returncode == 0, proc.stderr
    assert (
        conn.fetch_scalar_int("SELECT count(*)::int FROM warehouse.scratch WHERE n = 42")
        == 1
    )


def test_flow_transform_run_unknown_name(tmp_path):
    body = """
        from ematix_flow import ematix

        @ematix.transform(name="solo")
        def solo(c):
            pass
    """
    _write_user_module(tmp_path, body)
    proc = _run_cli(
        ["transform", "run", "--module", "userpkg.pipelines", "ghost"],
        cwd=tmp_path,
    )
    assert proc.returncode != 0
    assert "ghost" in (proc.stdout + proc.stderr).lower()
