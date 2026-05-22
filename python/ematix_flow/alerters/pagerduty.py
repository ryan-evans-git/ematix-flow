"""PagerDutyAlerter — posts to PagerDuty's v2 Events API.

Stdlib-only (urllib + json). The Events API v2 takes a single
``routing_key`` (per-service integration key) and accepts events
of three types:

* ``trigger`` — open or update an incident
* ``acknowledge`` — mark an existing incident acknowledged
* ``resolve``  — close the incident

This alerter maps ematix-flow's :class:`AlertEvent` kinds to the
above:

* ``failed`` / ``gave_up`` → ``trigger`` (gave_up sets severity=critical;
  failed sets severity=error so the incident dedupes against the same
  ``dedup_key``).
* ``recovered`` → ``resolve`` (the same ``dedup_key`` closes the
  incident from the failure side).

URL form:

  pagerduty://<integration_routing_key>[?severity=...&service=...]

The routing key is the per-service ``Events API v2`` integration
key (NOT the user API token). Default ``severity`` is ``error``;
``service`` lets multiple ematix-flow deployments share one
routing key with distinguishable dedup paths.
"""
from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request

from . import AlertEvent

_EVENTS_API_URL = "https://events.pagerduty.com/v2/enqueue"


class PagerDutyAlerter:
    def __init__(
        self,
        routing_key: str,
        *,
        service_label: str = "ematix-flow",
        default_severity: str = "error",
        timeout: float = 5.0,
    ):
        if not routing_key:
            raise ValueError("PagerDutyAlerter: routing_key is required")
        self.routing_key = routing_key
        self.service_label = service_label
        self.default_severity = default_severity
        self._timeout = timeout

    def notify(self, event: AlertEvent) -> None:
        payload = self._build_payload(event)
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            _EVENTS_API_URL,
            data=body,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            # _EVENTS_API_URL is a hardcoded https:// constant — no
            # scheme injection surface here.
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:  # nosec B310
                resp.read()
        except (urllib.error.URLError, ConnectionError, OSError) as e:
            print(
                f"warning: PagerDutyAlerter failed to POST: "
                f"{type(e).__name__}: {e}",
                file=sys.stderr,
            )

    def _build_payload(self, event: AlertEvent) -> dict:
        """Build the v2 Events API JSON. The ``dedup_key`` is what
        ties trigger ↔ resolve together — keying on
        ``<service>:<pipeline>`` means a single incident is opened
        per failing pipeline, regardless of how many ``failed`` events
        fire while it's down, and the next ``recovered`` resolves it."""
        dedup_key = f"{self.service_label}:{event.pipeline}"
        ts = event.timestamp.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        if event.kind == "recovered":
            return {
                "routing_key": self.routing_key,
                "event_action": "resolve",
                "dedup_key": dedup_key,
            }
        severity = (
            "critical" if event.kind == "gave_up" else self.default_severity
        )
        summary = (
            f"{event.pipeline} {event.kind} "
            f"({event.attempt_count}/{event.max_attempts}): "
            f"{event.error_type}: {event.error_message}"
        )
        return {
            "routing_key": self.routing_key,
            "event_action": "trigger",
            "dedup_key": dedup_key,
            "payload": {
                "summary": summary[:1024],  # PD caps at 1024 chars
                "source": self.service_label,
                "severity": severity,
                "timestamp": ts,
                "component": event.pipeline,
                "custom_details": {
                    "kind": event.kind,
                    "pipeline": event.pipeline,
                    "error_type": event.error_type,
                    "error_message": event.error_message,
                    "attempt_count": event.attempt_count,
                    "max_attempts": event.max_attempts,
                },
            },
        }


def from_url(url: str) -> PagerDutyAlerter:
    """Build a :class:`PagerDutyAlerter` from a ``pagerduty://`` URL."""
    from urllib.parse import parse_qs, urlparse

    parsed = urlparse(url)
    if parsed.scheme != "pagerduty":
        raise ValueError(
            f"PagerDutyAlerter.from_url: expected scheme 'pagerduty', "
            f"got {parsed.scheme!r}"
        )
    # Routing key is in the netloc position; netloc can be the bare key
    # or user@host form — we take whichever non-empty value parses.
    routing_key = parsed.netloc or parsed.path.lstrip("/")
    if not routing_key:
        raise ValueError(
            f"PagerDutyAlerter.from_url: routing key missing in {url!r}"
        )
    qs = {k: v[0] for k, v in parse_qs(parsed.query).items()}
    return PagerDutyAlerter(
        routing_key=routing_key,
        service_label=qs.get("service", "ematix-flow"),
        default_severity=qs.get("severity", "error"),
    )
