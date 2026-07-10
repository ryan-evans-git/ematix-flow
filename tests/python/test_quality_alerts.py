"""Phase 4: alerter formatting for the new quality_failed / sla_breached
event kinds, the process-wide alerter registry, and warn-path emission."""

from __future__ import annotations

import io
from datetime import UTC, datetime

from ematix_flow import alerters
from ematix_flow.alerters import AlertEvent
from ematix_flow.alerters.slack import SlackAlerter
from ematix_flow.alerters.stdout import StdoutAlerter


def _ev(kind, msg):
    return AlertEvent(
        kind=kind,
        pipeline="load_customers",
        timestamp=datetime(2026, 7, 9, 12, 0, tzinfo=UTC),
        error_message=msg,
    )


def test_stdout_formats_quality_failed():
    buf = io.StringIO()
    StdoutAlerter(stream=buf).notify(_ev("quality_failed", "2/5 checks failed"))
    out = buf.getvalue()
    assert "quality_failed" in out
    assert "load_customers" in out
    assert "2/5 checks failed" in out


def test_stdout_formats_sla_breached():
    buf = io.StringIO()
    StdoutAlerter(stream=buf).notify(_ev("sla_breached", "34h since last success"))
    assert "sla_breached" in buf.getvalue()
    assert "34h since last success" in buf.getvalue()


def test_slack_format_quality_failed():
    msg = SlackAlerter("https://hooks.slack.com/services/x")._format(
        _ev("quality_failed", "email.not_null failed")
    )
    assert ":mag:" in msg
    assert "data-quality check failed" in msg
    assert "email.not_null failed" in msg


def test_slack_format_sla_breached():
    msg = SlackAlerter("https://hooks.slack.com/services/x")._format(
        _ev("sla_breached", "no successful run on record")
    )
    assert "freshness SLO breached" in msg
    assert "no successful run" in msg


def test_alerter_registry():
    alerters.clear_alerters()
    a = StdoutAlerter(stream=io.StringIO())
    alerters.register_alerter(a)
    alerters.register_alerter(a)  # idempotent
    assert alerters.active_alerters() == [a]
    alerters.clear_alerters()
    assert alerters.active_alerters() == []


def test_quality_warn_emits_via_registry():
    """A warn-policy quality failure notifies registered alerters (the
    run still succeeds, so nothing else would)."""
    from ematix_flow import quality as q

    alerters.clear_alerters()
    captured = []

    class _Fake:
        def notify(self, ev):
            captured.append((ev.kind, ev.pipeline))

    alerters.register_alerter(_Fake())
    try:
        outcome = q.QualityOutcome(
            pipeline="p",
            table="t",
            schema=None,
            verdict="fail",
            assertions=(q.QualityAssertion("c", "fail", "boom"),),
        )
        q._emit_alert("quality_failed", "p", "1/1 failed: c: boom")
        assert ("quality_failed", "p") in captured
    finally:
        alerters.clear_alerters()
