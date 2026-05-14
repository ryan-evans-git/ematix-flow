"""Phase Ω.D4 — `--alerter` and `--metrics` URL flags on the CLI.

After Ω.Q3 (alerters) and Ω.M1 (metrics) shipped as Python APIs, this
phase wires them into the CLI so users can attach observability without
writing wrapper code.

  flow run-due \\
      --alerter stdout:// \\
      --alerter slack://hooks.slack.com/services/X/Y/Z \\
      --metrics prometheus://:9090

Env vars:
  $EMATIX_FLOW_ALERTERS   comma-separated URLs
  $EMATIX_FLOW_METRICS    one URL

Resolution: CLI flags win over env; env wins over implicit defaults.
The defaults are: no alerters, no metrics (NullSink). The CLI never
silently invents an alerter or sink — observability is opt-in.
"""

from __future__ import annotations

import argparse
import io
import json
import os

import pytest

from ematix_flow import cli, pipeline as p


_SIDE_TABLES = (
    "_REGISTRY", "_DEPENDS_ON", "_UPSTREAM_FRESHNESS",
    "_LAST_RUN", "_RETRY_POLICY", "_ATTEMPT_STATE",
)


@pytest.fixture(autouse=True)
def _clean_registry_and_env(monkeypatch):
    for tbl in _SIDE_TABLES:
        d = getattr(p, tbl, None)
        if d is not None:
            d.clear()
    monkeypatch.delenv("EMATIX_FLOW_ALERTERS", raising=False)
    monkeypatch.delenv("EMATIX_FLOW_METRICS", raising=False)
    yield
    for tbl in _SIDE_TABLES:
        d = getattr(p, tbl, None)
        if d is not None:
            d.clear()


# ---- _open_alerters / _open_metrics resolution -------------------


def test_no_flags_means_no_alerters_and_null_metrics():
    """The default observability footprint is zero. Operators must
    explicitly opt into Slack / Prometheus / etc."""
    from ematix_flow.metrics import NullSink

    ns = argparse.Namespace(alerter=None, metrics=None)
    assert cli._open_alerters(ns) == []
    sink = cli._open_metrics(ns)
    assert isinstance(sink, NullSink)


def test_single_alerter_flag_constructs_one_alerter():
    from ematix_flow.alerters import StdoutAlerter

    ns = argparse.Namespace(alerter=["stdout://"], metrics=None)
    alerters = cli._open_alerters(ns)
    assert len(alerters) == 1
    assert isinstance(alerters[0], StdoutAlerter)


def test_multiple_alerter_flags_construct_multiple():
    """`--alerter stdout:// --alerter slack://...` → two alerters."""
    from ematix_flow.alerters import SlackAlerter, StdoutAlerter

    ns = argparse.Namespace(
        alerter=[
            "stdout://",
            "slack://hooks.slack.com/services/X/Y/Z",
        ],
        metrics=None,
    )
    alerters = cli._open_alerters(ns)
    assert len(alerters) == 2
    assert any(isinstance(a, StdoutAlerter) for a in alerters)
    assert any(isinstance(a, SlackAlerter) for a in alerters)


def test_alerter_env_var_used_when_no_flag(monkeypatch):
    """$EMATIX_FLOW_ALERTERS is a comma-separated URL list."""
    from ematix_flow.alerters import StdoutAlerter

    monkeypatch.setenv("EMATIX_FLOW_ALERTERS", "stdout://")
    ns = argparse.Namespace(alerter=None, metrics=None)
    alerters = cli._open_alerters(ns)
    assert len(alerters) == 1
    assert isinstance(alerters[0], StdoutAlerter)


def test_alerter_env_var_supports_comma_separated(monkeypatch):
    monkeypatch.setenv(
        "EMATIX_FLOW_ALERTERS",
        "stdout://,slack://hooks.slack.com/services/X/Y/Z",
    )
    ns = argparse.Namespace(alerter=None, metrics=None)
    alerters = cli._open_alerters(ns)
    assert len(alerters) == 2


def test_alerter_flags_beat_env(monkeypatch):
    """Explicit flag wins over $EMATIX_FLOW_ALERTERS."""
    from ematix_flow.alerters import StdoutAlerter

    monkeypatch.setenv("EMATIX_FLOW_ALERTERS", "slack://hooks.slack.com/X/Y/Z")
    ns = argparse.Namespace(alerter=["stdout://"], metrics=None)
    alerters = cli._open_alerters(ns)
    assert len(alerters) == 1
    assert isinstance(alerters[0], StdoutAlerter)


def test_bad_alerter_url_warns_and_skips(monkeypatch, capsys):
    """An invalid URL emits a stderr warning but doesn't crash; the
    rest of the alerter list is still constructed."""
    from ematix_flow.alerters import StdoutAlerter

    ns = argparse.Namespace(
        alerter=["nopechart://broken", "stdout://"],
        metrics=None,
    )
    alerters = cli._open_alerters(ns)
    err = capsys.readouterr().err
    assert "warning" in err.lower()
    assert "nopechart" in err
    # The good alerter still loaded.
    assert any(isinstance(a, StdoutAlerter) for a in alerters)


# ---- metrics resolution ------------------------------------------


def test_metrics_flag_constructs_sink():
    from ematix_flow.metrics import InMemorySink

    ns = argparse.Namespace(alerter=None, metrics="memory://")
    sink = cli._open_metrics(ns)
    assert isinstance(sink, InMemorySink)


def test_metrics_env_var_used_when_no_flag(monkeypatch):
    from ematix_flow.metrics import StdoutSink

    monkeypatch.setenv("EMATIX_FLOW_METRICS", "stdout://")
    ns = argparse.Namespace(alerter=None, metrics=None)
    sink = cli._open_metrics(ns)
    assert isinstance(sink, StdoutSink)


def test_metrics_flag_beats_env(monkeypatch):
    from ematix_flow.metrics import InMemorySink

    monkeypatch.setenv("EMATIX_FLOW_METRICS", "stdout://")
    ns = argparse.Namespace(alerter=None, metrics="memory://")
    sink = cli._open_metrics(ns)
    assert isinstance(sink, InMemorySink)


def test_bad_metrics_url_warns_and_falls_back_to_null(capsys):
    from ematix_flow.metrics import NullSink

    ns = argparse.Namespace(alerter=None, metrics="snmp://unsupported")
    sink = cli._open_metrics(ns)
    err = capsys.readouterr().err
    assert "warning" in err.lower()
    assert isinstance(sink, NullSink)


# ---- end-to-end: run-due fires alerters + metrics ----------------


def test_cli_run_due_writes_through_to_alerters_and_metrics(monkeypatch, capsys):
    """The full chain — flags → constructed alerters/sink → passed
    to run_due_with_dag_detailed → events + metrics fired."""

    @p.register(
        name="fail",
        schedule="@hourly",
        retry={"max_attempts": 1, "backoff": "fixed", "base_secs": 0},
    )
    def _fail():
        raise RuntimeError("boom")

    monkeypatch.setattr(cli, "_import_user_module", lambda _m: None)

    ns = argparse.Namespace(
        module="ignored",
        now=None,
        interval=3600,
        no_run_log=True,
        run_log_path=None,
        run_log_url=None,
        alerter=["stdout://"],
        metrics="stdout://",
    )
    rc = cli._cmd_run_due(ns)
    assert rc == 1  # failure

    err = capsys.readouterr().err
    # StdoutAlerter line on the failed event.
    assert "[ALERT]" in err
    assert "fail" in err
    # StdoutSink line on the counter increment.
    assert "[METRIC]" in err
    assert "pipeline_runs_total" in err
    # gave_up event since max_attempts=1.
    assert "gave_up" in err.lower() or "gave up" in err.lower()
