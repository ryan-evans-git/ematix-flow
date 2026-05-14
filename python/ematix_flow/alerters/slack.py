"""SlackAlerter — POSTs JSON to a Slack incoming-webhook URL.

Uses stdlib `urllib.request` only — no extra deps. Network errors are
swallowed (logged to stderr) so an unreachable Slack endpoint can't
take down the orchestrator. If you need richer formatting (blocks,
attachments, threads), the implementation is short enough to fork.
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request

from . import AlertEvent


class SlackAlerter:
    def __init__(self, webhook_url: str, *, timeout: float = 5.0):
        self.webhook_url = webhook_url
        self._timeout = timeout

    def notify(self, event: AlertEvent) -> None:
        text = self._format(event)
        payload = {"text": text}
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self.webhook_url,
            data=body,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                resp.read()  # drain
        except (urllib.error.URLError, ConnectionError, OSError) as e:
            # Don't crash the orchestrator on a Slack outage.
            print(
                f"warning: SlackAlerter failed to POST: "
                f"{type(e).__name__}: {e}",
                file=sys.stderr,
            )

    def _format(self, event: AlertEvent) -> str:
        ts = event.timestamp.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        emoji = {
            "failed": ":warning:",
            "gave_up": ":x:",
            "recovered": ":white_check_mark:",
        }.get(event.kind, ":bell:")

        if event.kind == "gave_up":
            return (
                f"{emoji} *{event.pipeline}* gave up "
                f"({event.attempt_count}/{event.max_attempts} attempts): "
                f"`{event.error_type}: {event.error_message}` "
                f"_{ts}_"
            )
        if event.kind == "failed":
            return (
                f"{emoji} *{event.pipeline}* failed "
                f"(attempt {event.attempt_count}/{event.max_attempts}): "
                f"`{event.error_type}: {event.error_message}` "
                f"_{ts}_"
            )
        if event.kind == "recovered":
            return (
                f"{emoji} *{event.pipeline}* recovered "
                f"(after {event.attempt_count} attempts) _{ts}_"
            )
        return f"{emoji} *{event.pipeline}* {event.kind} _{ts}_"
