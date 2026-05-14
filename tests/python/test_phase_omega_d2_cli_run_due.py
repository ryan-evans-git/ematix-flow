"""Phase Ω.D2 — `flow run-due` CLI honors Ω.2 retry backoff.

The pre-Ω.2 `_cmd_run_due` loop manually stamps `_LAST_RUN` but
doesn't update `_ATTEMPT_STATE` — a failed pipeline with a configured
backoff would re-fire on every cron tick instead of waiting for the
window. This phase migrates the CLI to use `run_due_with_dag` so
retry semantics actually take effect from the command line.

It also covers the `RunDueResult` shape: per-pipeline outcomes carry
enough info to render the structured `ran` / `failed` / `skipped`
JSON the CLI returns.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json

import pytest

from ematix_flow import cli
from ematix_flow import pipeline as p

_SIDE_TABLES = (
    "_REGISTRY",
    "_DEPENDS_ON",
    "_UPSTREAM_FRESHNESS",
    "_LAST_RUN",
    "_RETRY_POLICY",
    "_ATTEMPT_STATE",
)


@pytest.fixture(autouse=True)
def _clean_registry():
    for tbl in _SIDE_TABLES:
        d = getattr(p, tbl, None)
        if d is not None:
            d.clear()
    yield
    for tbl in _SIDE_TABLES:
        d = getattr(p, tbl, None)
        if d is not None:
            d.clear()


# ---- run_due_with_dag_detailed -----------------------------------------


def test_detailed_returns_fired_failed_skipped():
    """The structured-return variant gives per-pipeline outcomes
    instead of just the list of successful fires."""
    @p.register(name="ok", schedule="@hourly")
    def _ok():
        return {"status": "ok"}

    @p.register(name="boom", schedule="@hourly")
    def _boom():
        raise RuntimeError("kaboom")

    result = p.run_due_with_dag_detailed(["ok", "boom"])
    assert [e.name for e in result.fired] == ["ok"]
    assert [e.name for e in result.failed] == ["boom"]
    # First fail with default policy (max_attempts=1) → gave_up=True.
    assert result.failed[0].error_message == "kaboom"
    assert result.failed[0].gave_up is True


def test_detailed_records_skip_reason_for_stale_upstream():
    @p.register(name="root", schedule="@hourly")
    def _root():
        raise RuntimeError("boom")

    @p.register(name="leaf", schedule="@hourly", depends_on=["root"])
    def _leaf():
        return {}

    result = p.run_due_with_dag_detailed(["root", "leaf"])
    skipped_names = [e.name for e in result.skipped]
    assert "leaf" in skipped_names
    leaf_event = next(e for e in result.skipped if e.name == "leaf")
    assert "upstream" in leaf_event.reason.lower()
    assert "root" in leaf_event.reason


def test_detailed_records_skip_reason_for_retry_backoff():
    """A pipeline mid-retry-backoff should appear in `skipped` with
    a backoff reason — this is what the CLI needs to surface so an
    operator can see why nothing fired."""
    @p.register(
        name="flaky",
        schedule="@hourly",
        retry={"max_attempts": 3, "backoff": "fixed", "base_secs": 30},
    )
    def _flaky():
        raise RuntimeError("boom")

    t0 = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    # First tick: fire + fail. attempt_count=1, window opens.
    p.run_due_with_dag_detailed(["flaky"], now=t0)
    # Second tick within the window: skipped, not refired.
    result = p.run_due_with_dag_detailed(
        ["flaky"], now=t0 + _dt.timedelta(seconds=5)
    )
    assert result.fired == []
    assert result.failed == []
    skip_names = [e.name for e in result.skipped]
    assert "flaky" in skip_names
    backoff_event = next(e for e in result.skipped if e.name == "flaky")
    assert "backoff" in backoff_event.reason.lower() or "retry" in backoff_event.reason.lower()


# ---- CLI integration ---------------------------------------------------


def test_cli_run_due_skips_mid_backoff_pipeline(tmp_path, capsys, monkeypatch):
    """End-to-end: a failing pipeline with retry config should NOT
    re-fire on a back-to-back invocation within the backoff window.
    Pre-Ω.D2 the CLI's bespoke loop refired every time."""

    fired_count = {"n": 0}

    @p.register(
        name="flaky",
        schedule="@hourly",
        retry={"max_attempts": 3, "backoff": "fixed", "base_secs": 120},
    )
    def _flaky():
        fired_count["n"] += 1
        raise RuntimeError("boom")

    monkeypatch.setattr(cli, "_import_user_module", lambda _m: None)

    # Use --no-run-log so we don't touch ~/.ematix-flow/run_log.db during
    # the test; persistence isn't what we're checking here.
    ns = argparse.Namespace(
        module="ignored",
        now=None,
        interval=3600,
        no_run_log=True,
        run_log_path=None,
    )

    # First invocation — pipeline is due, fires, fails.
    cli._cmd_run_due(ns)
    assert fired_count["n"] == 1

    # Second invocation seconds later — backoff window (120s) is wide
    # open. Pipeline must NOT refire.
    cli._cmd_run_due(ns)
    assert fired_count["n"] == 1, (
        "CLI re-fired a pipeline that should be in retry backoff "
        f"(invocations={fired_count['n']})"
    )

    # And the JSON output should report it as skipped.
    out = capsys.readouterr().out.strip().splitlines()
    last_json = json.loads(out[-1])
    skipped_names = [s["pipeline"] for s in last_json.get("skipped", [])]
    assert "flaky" in skipped_names


def test_cli_run_due_reports_attempt_count_in_output(tmp_path, monkeypatch, capsys):
    """The JSON output's `failed` entries carry attempt_count so the
    operator can tell whether they're done retrying or still cycling."""
    @p.register(
        name="dies",
        schedule="@hourly",
        retry={"max_attempts": 1, "backoff": "fixed", "base_secs": 0},
    )
    def _dies():
        raise RuntimeError("boom")

    monkeypatch.setattr(cli, "_import_user_module", lambda _m: None)
    ns = argparse.Namespace(
        module="ignored", now=None, interval=3600,
        no_run_log=True, run_log_path=None,
    )
    cli._cmd_run_due(ns)
    out = capsys.readouterr().out.strip().splitlines()
    last_json = json.loads(out[-1])
    failed = last_json["failed"]
    assert len(failed) == 1
    assert failed[0]["pipeline"] == "dies"
    # max_attempts=1 → gave_up=True after first failure.
    assert failed[0].get("gave_up") is True
