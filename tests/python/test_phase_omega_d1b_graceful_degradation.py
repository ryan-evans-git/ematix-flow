"""Graceful run-log degradation.

When the CLI can't open its configured run-log (read-only FS,
permission denied, missing dir we can't create, locked SQLite,
etc.) it must keep working — print a warning and continue without
persistence rather than crashing the whole `flow run-due` invocation.
"""

from __future__ import annotations

import argparse
import os

import pytest

from ematix_flow import cli


@pytest.fixture(autouse=True)
def _stub_args():
    """Most tests fabricate an argparse.Namespace; the env var is the
    one cross-cutting concern. Clear it so the test's `--run-log-path`
    overrides actually take effect."""
    saved = os.environ.pop("EMATIX_FLOW_RUN_LOG_PATH", None)
    yield
    if saved is not None:
        os.environ["EMATIX_FLOW_RUN_LOG_PATH"] = saved


def test_open_returns_real_runlog_on_writable_path(tmp_path):
    ns = argparse.Namespace(
        no_run_log=False,
        run_log_path=str(tmp_path / "run.db"),
    )
    log = cli._open_run_log_or_none(ns)
    assert log is not None
    log.close()


def test_open_returns_none_when_path_is_in_unwritable_dir(tmp_path, capsys):
    """If the parent directory is read-only, we can't create the DB.
    Should warn + return None, not raise."""
    parent = tmp_path / "ro"
    parent.mkdir(mode=0o555)
    db_path = parent / "subdir" / "run.db"
    try:
        ns = argparse.Namespace(no_run_log=False, run_log_path=str(db_path))
        log = cli._open_run_log_or_none(ns)
        assert log is None
        err = capsys.readouterr().err
        assert "warning" in err.lower()
        assert "run-log" in err.lower()
    finally:
        # Restore writability so tmp_path teardown can clean up.
        parent.chmod(0o755)


def test_no_run_log_short_circuits_to_none(tmp_path):
    """`--no-run-log` must not even attempt to open anything."""
    # Point at a path that WOULD work — proves --no-run-log wins.
    ns = argparse.Namespace(
        no_run_log=True,
        run_log_path=str(tmp_path / "run.db"),
    )
    assert cli._open_run_log_or_none(ns) is None
    # File should not have been created.
    assert not (tmp_path / "run.db").exists()


def test_cmd_run_due_keeps_running_when_run_log_fails(tmp_path, capsys, monkeypatch):
    """End-to-end: when the configured run-log path can't be opened,
    `flow run-due` should fire scheduled pipelines anyway with a warning."""
    from ematix_flow import pipeline as p

    # Register one always-due pipeline.
    fired: list[str] = []

    @p.register(name="alpha", schedule="@hourly")
    def _alpha():
        fired.append("alpha")
        return {}

    # Path with an unwritable parent → run-log open will fail.
    parent = tmp_path / "ro"
    parent.mkdir(mode=0o555)
    bad_path = str(parent / "run.db")

    # Stub `_import_user_module` so the CLI doesn't try to import an
    # arbitrary user module.
    monkeypatch.setattr(cli, "_import_user_module", lambda _m: None)

    ns = argparse.Namespace(
        module="ignored",
        now=None,
        interval=3600,
        run_log_path=bad_path,
        no_run_log=False,
    )
    try:
        rc = cli._cmd_run_due(ns)
        err = capsys.readouterr().err
        # The CLI emitted a warning about the run-log, then fired the pipeline.
        assert "warning" in err.lower()
        # Either the pipeline ran (preferred) or the function returned a
        # non-error exit code (CLI didn't crash).
        assert rc == 0
        assert fired == ["alpha"]
    finally:
        parent.chmod(0o755)
        p._REGISTRY.clear()
