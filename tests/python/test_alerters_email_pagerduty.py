"""Tests for the EmailAlerter + PagerDutyAlerter."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from ematix_flow.alerters import AlertEvent, from_url
from ematix_flow.alerters.email import EmailAlerter
from ematix_flow.alerters.pagerduty import PagerDutyAlerter


def _event(kind: str = "failed", **overrides) -> AlertEvent:
    defaults = dict(
        kind=kind,
        pipeline="orders_sync",
        timestamp=datetime(2026, 5, 21, 14, 30, 0, tzinfo=UTC),
        error_message="connection refused",
        error_type="ConnectionError",
        attempt_count=2,
        max_attempts=3,
        gave_up=False,
    )
    defaults.update(overrides)
    return AlertEvent(**defaults)


# ---------------------------------------------------------------------------
# EmailAlerter
# ---------------------------------------------------------------------------


class TestEmailAlerterStarttls:
    def test_send_uses_starttls_login_send(self) -> None:
        alerter = EmailAlerter(
            host="smtp.example.com", port=587,
            username="bot@example.com", password="pw",
            to=["oncall@example.com"],
            use_starttls=True,
        )
        with patch("smtplib.SMTP") as mock_smtp:
            instance = mock_smtp.return_value.__enter__.return_value
            alerter.notify(_event())
        instance.starttls.assert_called_once()
        instance.login.assert_called_with("bot@example.com", "pw")
        instance.send_message.assert_called_once()
        # Verify the message has expected headers.
        msg = instance.send_message.call_args.args[0]
        assert "orders_sync failed" in msg["Subject"]
        assert msg["To"] == "oncall@example.com"

    def test_send_failure_logged_not_raised(self, capsys) -> None:
        alerter = EmailAlerter(
            host="smtp.example.com", port=587, to=["a@example.com"],
        )
        import smtplib

        with patch(
            "smtplib.SMTP", side_effect=smtplib.SMTPException("boom"),
        ):
            alerter.notify(_event())  # must not raise
        assert "EmailAlerter failed" in capsys.readouterr().err

    def test_gave_up_subject(self) -> None:
        alerter = EmailAlerter(
            host="smtp.example.com", port=587, to=["a@example.com"],
        )
        with patch("smtplib.SMTP") as mock_smtp:
            instance = mock_smtp.return_value.__enter__.return_value
            alerter.notify(_event(kind="gave_up"))
        msg = instance.send_message.call_args.args[0]
        assert "gave up" in msg["Subject"]

    def test_recovered_subject(self) -> None:
        alerter = EmailAlerter(
            host="smtp.example.com", port=587, to=["a@example.com"],
        )
        with patch("smtplib.SMTP") as mock_smtp:
            instance = mock_smtp.return_value.__enter__.return_value
            alerter.notify(_event(kind="recovered"))
        msg = instance.send_message.call_args.args[0]
        assert "recovered" in msg["Subject"]


class TestEmailAlerterSSL:
    def test_implicit_ssl_path(self) -> None:
        alerter = EmailAlerter(
            host="smtp.example.com", port=465,
            username="bot", password="pw",
            to=["a@example.com"],
            use_starttls=False,
        )
        with patch("smtplib.SMTP_SSL") as mock_ssl:
            instance = mock_ssl.return_value.__enter__.return_value
            alerter.notify(_event())
        instance.login.assert_called_with("bot", "pw")
        instance.send_message.assert_called_once()


class TestEmailAlerterValidation:
    def test_empty_host_rejected(self) -> None:
        with pytest.raises(ValueError, match="host"):
            EmailAlerter(host="", port=587, to=["a@example.com"])

    def test_empty_to_rejected(self) -> None:
        with pytest.raises(ValueError, match="recipient"):
            EmailAlerter(host="smtp.example.com", port=587, to=[])


class TestEmailUrlFactory:
    def test_basic_starttls_url(self) -> None:
        alerter = from_url(
            "email://bot:pw@smtp.example.com:587"
            "?from=bot@example.com&to=a@example.com"
        )
        assert isinstance(alerter, EmailAlerter)
        assert alerter.host == "smtp.example.com"
        assert alerter.port == 587
        assert alerter.username == "bot"
        assert alerter.password == "pw"
        assert alerter.from_addr == "bot@example.com"
        assert alerter.to == ["a@example.com"]
        assert alerter.use_starttls is True

    def test_multiple_recipients(self) -> None:
        alerter = from_url(
            "email://smtp.example.com?to=a@example.com,b@example.com"
        )
        assert isinstance(alerter, EmailAlerter)
        assert alerter.to == ["a@example.com", "b@example.com"]

    def test_implicit_ssl_default_port(self) -> None:
        alerter = from_url(
            "email://smtp.example.com?to=a@example.com&starttls=0"
        )
        assert isinstance(alerter, EmailAlerter)
        assert alerter.port == 465
        assert alerter.use_starttls is False

    def test_missing_to_rejected(self) -> None:
        with pytest.raises(ValueError, match="to=address"):
            from_url("email://smtp.example.com")


# ---------------------------------------------------------------------------
# PagerDutyAlerter
# ---------------------------------------------------------------------------


class TestPagerDutyAlerter:
    def test_failed_emits_trigger(self) -> None:
        alerter = PagerDutyAlerter(routing_key="abc123")
        with patch("urllib.request.urlopen") as mock_open:
            mock_resp = MagicMock()
            mock_resp.__enter__.return_value = mock_resp
            mock_open.return_value = mock_resp
            alerter.notify(_event(kind="failed"))
        req = mock_open.call_args.args[0]
        body = json.loads(req.data.decode("utf-8"))
        assert body["routing_key"] == "abc123"
        assert body["event_action"] == "trigger"
        assert body["dedup_key"] == "ematix-flow:orders_sync"
        assert body["payload"]["severity"] == "error"
        assert "orders_sync" in body["payload"]["summary"]

    def test_gave_up_uses_critical_severity(self) -> None:
        alerter = PagerDutyAlerter(routing_key="abc123")
        with patch("urllib.request.urlopen") as mock_open:
            mock_resp = MagicMock()
            mock_resp.__enter__.return_value = mock_resp
            mock_open.return_value = mock_resp
            alerter.notify(_event(kind="gave_up", gave_up=True))
        body = json.loads(mock_open.call_args.args[0].data.decode("utf-8"))
        assert body["payload"]["severity"] == "critical"

    def test_recovered_emits_resolve(self) -> None:
        alerter = PagerDutyAlerter(routing_key="abc123")
        with patch("urllib.request.urlopen") as mock_open:
            mock_resp = MagicMock()
            mock_resp.__enter__.return_value = mock_resp
            mock_open.return_value = mock_resp
            alerter.notify(_event(kind="recovered"))
        body = json.loads(mock_open.call_args.args[0].data.decode("utf-8"))
        assert body["event_action"] == "resolve"
        assert body["dedup_key"] == "ematix-flow:orders_sync"
        # Resolve payload doesn't carry the full payload object.
        assert "payload" not in body

    def test_service_label_threads_into_dedup_key(self) -> None:
        alerter = PagerDutyAlerter(
            routing_key="abc123", service_label="prod-warehouse",
        )
        with patch("urllib.request.urlopen") as mock_open:
            mock_resp = MagicMock()
            mock_resp.__enter__.return_value = mock_resp
            mock_open.return_value = mock_resp
            alerter.notify(_event(kind="failed"))
        body = json.loads(mock_open.call_args.args[0].data.decode("utf-8"))
        assert body["dedup_key"] == "prod-warehouse:orders_sync"

    def test_network_failure_logged_not_raised(self, capsys) -> None:
        alerter = PagerDutyAlerter(routing_key="abc123")
        with patch("urllib.request.urlopen", side_effect=OSError("nope")):
            alerter.notify(_event())  # must not raise
        assert "PagerDutyAlerter failed" in capsys.readouterr().err


class TestPagerDutyUrlFactory:
    def test_basic_url(self) -> None:
        alerter = from_url("pagerduty://abc123key")
        assert isinstance(alerter, PagerDutyAlerter)
        assert alerter.routing_key == "abc123key"
        assert alerter.service_label == "ematix-flow"
        assert alerter.default_severity == "error"

    def test_service_label_query_param(self) -> None:
        alerter = from_url("pagerduty://abc123?service=prod-warehouse")
        assert isinstance(alerter, PagerDutyAlerter)
        assert alerter.service_label == "prod-warehouse"

    def test_severity_query_param(self) -> None:
        alerter = from_url("pagerduty://abc123?severity=warning")
        assert isinstance(alerter, PagerDutyAlerter)
        assert alerter.default_severity == "warning"

    def test_empty_routing_key_rejected(self) -> None:
        with pytest.raises(ValueError, match="routing key"):
            from_url("pagerduty://")
