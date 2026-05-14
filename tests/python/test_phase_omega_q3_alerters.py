"""Phase Ω.Q3 — Alerter Protocol + Stdout + Slack webhook concrete.

Alerter is the third leg of the orchestrator observability triangle:
RunLog persists state (Ω.D1), status snapshot reads it (Ω.3), and
alerters push notifications when something goes wrong.

The Protocol is intentionally narrow: one `notify(event)` method per
alerter. Concrete dispatch on `event.kind` is the alerter's choice —
some skip recovery events, some only fire on gave-up, etc.

Tests cover:
  1. Protocol satisfaction (runtime isinstance)
  2. StdoutAlerter writes failed / gave_up / recovered events
  3. SlackAlerter POSTs JSON to the webhook URL on each event
  4. run_due_with_dag_detailed fans events out to all attached alerters
  5. URL factory: stdout://, slack://...
"""

from __future__ import annotations

import datetime as _dt
import io
import json
from unittest.mock import patch

import pytest

from ematix_flow import pipeline as p
from ematix_flow.alerters import (
    Alerter,
    AlertEvent,
    SlackAlerter,
    StdoutAlerter,
)
from ematix_flow.alerters import (
    from_url as alerter_from_url,
)

_SIDE_TABLES = (
    "_REGISTRY", "_DEPENDS_ON", "_UPSTREAM_FRESHNESS",
    "_LAST_RUN", "_RETRY_POLICY", "_ATTEMPT_STATE",
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


# ---- Protocol -----------------------------------------------------


def test_stdout_alerter_satisfies_protocol():
    alerter = StdoutAlerter()
    assert isinstance(alerter, Alerter)


def test_slack_alerter_satisfies_protocol():
    alerter = SlackAlerter("https://hooks.slack.com/services/X/Y/Z")
    assert isinstance(alerter, Alerter)


def test_alert_event_has_expected_fields():
    """The AlertEvent shape is what every alerter consumes."""
    ev = AlertEvent(
        kind="failed",
        pipeline="alpha",
        error_message="boom",
        error_type="RuntimeError",
        attempt_count=1,
        max_attempts=3,
        gave_up=False,
        timestamp=_dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC),
    )
    assert ev.kind == "failed"
    assert ev.pipeline == "alpha"


# ---- StdoutAlerter --------------------------------------------------


def test_stdout_alerter_writes_failed_event():
    out = io.StringIO()
    alerter = StdoutAlerter(stream=out)
    alerter.notify(AlertEvent(
        kind="failed",
        pipeline="alpha",
        error_message="boom",
        error_type="RuntimeError",
        attempt_count=1,
        max_attempts=3,
        gave_up=False,
        timestamp=_dt.datetime.now(_dt.UTC),
    ))
    text = out.getvalue()
    assert "alpha" in text
    assert "failed" in text.lower()
    assert "boom" in text


def test_stdout_alerter_marks_gave_up():
    out = io.StringIO()
    alerter = StdoutAlerter(stream=out)
    alerter.notify(AlertEvent(
        kind="gave_up",
        pipeline="dies",
        error_message="boom",
        error_type="RuntimeError",
        attempt_count=3,
        max_attempts=3,
        gave_up=True,
        timestamp=_dt.datetime.now(_dt.UTC),
    ))
    text = out.getvalue()
    assert "dies" in text
    assert "gave_up" in text.lower() or "gave up" in text.lower()


# ---- SlackAlerter ---------------------------------------------------


def test_slack_alerter_posts_to_webhook():
    """SlackAlerter POSTs a JSON payload to the webhook URL on notify."""
    alerter = SlackAlerter("https://hooks.slack.com/services/X/Y/Z")
    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_urlopen.return_value.__enter__.return_value.read.return_value = b"ok"
        alerter.notify(AlertEvent(
            kind="failed",
            pipeline="alpha",
            error_message="boom",
            error_type="RuntimeError",
            attempt_count=1,
            max_attempts=3,
            gave_up=False,
            timestamp=_dt.datetime.now(_dt.UTC),
        ))
        assert mock_urlopen.called
        req = mock_urlopen.call_args[0][0]
        # The first positional arg is the urllib Request object.
        assert req.full_url == "https://hooks.slack.com/services/X/Y/Z"
        body = json.loads(req.data.decode("utf-8"))
        assert "text" in body
        assert "alpha" in body["text"]


def test_slack_alerter_includes_attempt_info_in_message():
    alerter = SlackAlerter("https://hooks.slack.com/services/X/Y/Z")
    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_urlopen.return_value.__enter__.return_value.read.return_value = b"ok"
        alerter.notify(AlertEvent(
            kind="gave_up",
            pipeline="dies",
            error_message="db connection failed",
            error_type="ConnectionError",
            attempt_count=3,
            max_attempts=3,
            gave_up=True,
            timestamp=_dt.datetime.now(_dt.UTC),
        ))
        body = json.loads(mock_urlopen.call_args[0][0].data.decode("utf-8"))
        assert "3/3" in body["text"] or "3 / 3" in body["text"]
        assert "dies" in body["text"]


def test_slack_alerter_swallows_network_errors():
    """An alerter that crashes on network failure shouldn't take down
    the orchestrator. Swallow + log to stderr is the right semantic."""
    alerter = SlackAlerter("https://hooks.slack.com/services/X/Y/Z")
    with patch("urllib.request.urlopen", side_effect=ConnectionError("nope")):
        # Should not raise.
        alerter.notify(AlertEvent(
            kind="failed", pipeline="alpha", error_message="boom",
            error_type="RuntimeError", attempt_count=1, max_attempts=3,
            gave_up=False, timestamp=_dt.datetime.now(_dt.UTC),
        ))


# ---- run_due_with_dag_detailed integration --------------------------


def test_run_due_fires_alerters_on_failure():
    """Alerters attached to run_due_with_dag_detailed get called for
    every failed event."""
    @p.register(
        name="fail",
        schedule="@hourly",
        retry={"max_attempts": 2, "backoff": "fixed", "base_secs": 30},
    )
    def _fail():
        raise RuntimeError("boom")

    events: list[AlertEvent] = []

    class _Capture:
        def notify(self, ev):
            events.append(ev)

    p.run_due_with_dag_detailed(["fail"], alerters=[_Capture()])

    assert len(events) == 1
    assert events[0].kind == "failed"
    assert events[0].pipeline == "fail"
    assert events[0].attempt_count == 1
    assert events[0].gave_up is False
    assert events[0].max_attempts == 2


def test_run_due_fires_gave_up_event_separately():
    """When attempt_count hits max_attempts, the alerter receives BOTH
    a `failed` and a `gave_up` event. Separating them lets alerters
    silence the noisy retry cycle while still surfacing the final
    give-up — common operator preference."""
    @p.register(
        name="dies",
        schedule="@hourly",
        retry={"max_attempts": 1, "backoff": "fixed", "base_secs": 0},
    )
    def _dies():
        raise RuntimeError("boom")

    events: list[AlertEvent] = []

    class _Capture:
        def notify(self, ev):
            events.append(ev)

    p.run_due_with_dag_detailed(["dies"], alerters=[_Capture()])

    kinds = [e.kind for e in events]
    assert "failed" in kinds
    assert "gave_up" in kinds


def test_run_due_fires_recovered_event():
    """When a pipeline that was failing finally succeeds, alerters
    receive a `recovered` event so operators know to stop watching."""
    calls = []

    @p.register(
        name="flaky",
        schedule="@hourly",
        retry={"max_attempts": 5, "backoff": "fixed", "base_secs": 0},
    )
    def _flaky():
        calls.append("attempt")
        if len(calls) < 3:
            raise RuntimeError("not yet")
        return {}

    events: list[AlertEvent] = []

    class _Capture:
        def notify(self, ev):
            events.append(ev)

    t = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    for i in range(3):
        p.run_due_with_dag_detailed(
            ["flaky"], now=t + _dt.timedelta(seconds=i),
            alerters=[_Capture()],
        )

    kinds = [e.kind for e in events]
    # 2 failures + 1 recovered
    assert kinds.count("failed") == 2
    assert kinds.count("recovered") == 1
    recovered = next(e for e in events if e.kind == "recovered")
    assert recovered.attempt_count == 3  # the successful attempt


def test_alerter_exception_does_not_crash_run_due():
    """A buggy alerter must not poison the orchestrator loop."""
    @p.register(name="ok", schedule="@hourly")
    def _ok():
        return {}

    class _Broken:
        def notify(self, ev):
            raise RuntimeError("alerter broken")

    @p.register(
        name="fail",
        schedule="@hourly",
        retry={"max_attempts": 1, "backoff": "fixed", "base_secs": 0},
    )
    def _fail():
        raise RuntimeError("boom")

    # Must not raise. The fail event triggers the broken alerter;
    # the loop should keep going.
    result = p.run_due_with_dag_detailed(
        ["fail", "ok"], alerters=[_Broken()],
    )
    assert [e.name for e in result.fired] == ["ok"]
    assert [e.name for e in result.failed] == ["fail"]


# ---- URL factory ---------------------------------------------------


def test_from_url_stdout():
    alerter = alerter_from_url("stdout://")
    assert isinstance(alerter, StdoutAlerter)


def test_from_url_slack_keeps_full_https_url():
    """slack://https://... and bare https://hooks.slack.com/... both work."""
    a = alerter_from_url("slack://hooks.slack.com/services/X/Y/Z")
    assert isinstance(a, SlackAlerter)
    # The webhook should be reconstructable as https://
    assert "hooks.slack.com" in a.webhook_url


def test_from_url_unknown_scheme():
    with pytest.raises(ValueError) as ei:
        alerter_from_url("pagerduty://...")
    assert "pagerduty" in str(ei.value).lower() or "unknown" in str(ei.value).lower()
