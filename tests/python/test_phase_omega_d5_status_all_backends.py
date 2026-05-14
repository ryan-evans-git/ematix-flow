"""Phase Ω.D5 — `flow status` reads from any RunLog backend.

The plumbing has been there since Ω.D3 (status uses the shared
`_open_run_log_or_none`); this test pins the contract so a future
refactor can't silently break it.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json

import pytest

from ematix_flow import cli
from ematix_flow import pipeline as p

_SIDE_TABLES = (
    "_REGISTRY", "_DEPENDS_ON", "_UPSTREAM_FRESHNESS",
    "_LAST_RUN", "_RETRY_POLICY", "_ATTEMPT_STATE",
)


@pytest.fixture(autouse=True)
def _clean_registry(monkeypatch):
    for tbl in _SIDE_TABLES:
        d = getattr(p, tbl, None)
        if d is not None:
            d.clear()
    monkeypatch.delenv("EMATIX_FLOW_RUN_LOG_URL", raising=False)
    monkeypatch.delenv("EMATIX_FLOW_RUN_LOG_PATH", raising=False)
    yield
    for tbl in _SIDE_TABLES:
        d = getattr(p, tbl, None)
        if d is not None:
            d.clear()


def test_status_reads_from_sqlite_run_log(tmp_path, monkeypatch, capsys):
    """End-to-end: write state via SqliteRunLog, then `flow status
    --run-log-url sqlite:///...` reads it back."""
    from ematix_flow.run_log import SqliteRunLog

    db_path = tmp_path / "state.db"
    log = SqliteRunLog(str(db_path))
    ts = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    log.record_run("alpha", ts, success=True)
    log.close()

    @p.register(name="alpha", schedule="@hourly")
    def _alpha():
        return {}

    monkeypatch.setattr(cli, "_import_user_module", lambda _m: None)
    ns = argparse.Namespace(
        module="ignored",
        format="json",
        no_run_log=False,
        run_log_path=None,
        run_log_url=f"sqlite:///{db_path}",
    )
    rc = cli._cmd_status(ns)
    assert rc == 0
    out = capsys.readouterr().out
    rows = json.loads(out)
    by_name = {r["name"]: r for r in rows}
    assert "alpha" in by_name
    # last_run is serialised as [ts_string, bool]; the success flag should round-trip.
    assert by_name["alpha"]["last_run"][1] is True


def test_status_reads_from_duckdb_run_log(tmp_path, monkeypatch, capsys):
    """Same contract via duckdb:///path."""
    pytest.importorskip("duckdb")
    from ematix_flow.run_log import DuckDBRunLog

    db_path = tmp_path / "state.duckdb"
    log = DuckDBRunLog(str(db_path))
    ts = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    log.record_run("beta", ts, success=False)
    log.close()

    @p.register(name="beta", schedule="@hourly")
    def _beta():
        return {}

    monkeypatch.setattr(cli, "_import_user_module", lambda _m: None)
    ns = argparse.Namespace(
        module="ignored",
        format="json",
        no_run_log=False,
        run_log_path=None,
        run_log_url=f"duckdb:///{db_path}",
    )
    cli._cmd_status(ns)
    rows = json.loads(capsys.readouterr().out)
    by_name = {r["name"]: r for r in rows}
    assert by_name["beta"]["last_run"][1] is False


def test_status_with_memory_url_shows_no_persisted_state(monkeypatch, capsys):
    """memory:// gives a fresh InMemoryRunLog every invocation; the
    snapshot should reflect that (no last_run on any pipeline)."""
    @p.register(name="alpha", schedule="@hourly")
    def _alpha():
        return {}

    monkeypatch.setattr(cli, "_import_user_module", lambda _m: None)
    ns = argparse.Namespace(
        module="ignored",
        format="json",
        no_run_log=False,
        run_log_path=None,
        run_log_url="memory://",
    )
    cli._cmd_status(ns)
    rows = json.loads(capsys.readouterr().out)
    [row] = rows
    assert row["name"] == "alpha"
    assert row["last_run"] is None


def test_status_no_run_log_flag_shows_in_memory_only(monkeypatch, capsys):
    """--no-run-log explicitly: status reflects only what's in this
    process's memory."""
    @p.register(name="alpha", schedule="@hourly")
    def _alpha():
        return {}

    # Pretend a previous in-process run set state. Without restoring
    # from disk, this state is what status sees.
    ts = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    p._LAST_RUN["alpha"] = (ts, True)

    monkeypatch.setattr(cli, "_import_user_module", lambda _m: None)
    ns = argparse.Namespace(
        module="ignored",
        format="json",
        no_run_log=True,
        run_log_path=None,
        run_log_url=None,
    )
    cli._cmd_status(ns)
    rows = json.loads(capsys.readouterr().out)
    [row] = rows
    assert row["last_run"][1] is True


def test_status_text_format_renders_table(tmp_path, monkeypatch, capsys):
    """The default text format produces a readable table — including
    the data we restored from disk."""
    from ematix_flow.run_log import SqliteRunLog

    db_path = tmp_path / "state.db"
    log = SqliteRunLog(str(db_path))
    ts = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    log.record_run("gamma", ts, success=True)
    log.close()

    @p.register(name="gamma", schedule="@hourly")
    def _gamma():
        return {}

    monkeypatch.setattr(cli, "_import_user_module", lambda _m: None)
    ns = argparse.Namespace(
        module="ignored",
        format="text",
        no_run_log=False,
        run_log_path=None,
        run_log_url=f"sqlite:///{db_path}",
    )
    cli._cmd_status(ns)
    out = capsys.readouterr().out
    assert "gamma" in out
    assert "@hourly" in out
    # The timestamp from disk should appear in the rendered row.
    assert "2026-05-13" in out
