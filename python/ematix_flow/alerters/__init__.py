"""Alerter — push notifications for pipeline failure / gave-up / recovered events.

Third leg of the observability triangle:
  - RunLog persists outcome state (Ω.D1)
  - flow status reads it (Ω.3)
  - Alerters push events the moment they happen (this module)

Every alerter satisfies a narrow Protocol:

    class Alerter(Protocol):
        def notify(self, event: AlertEvent) -> None: ...

Concrete:
  - StdoutAlerter  — prints to stderr; default for dev, zero config.
  - SlackAlerter   — POSTs JSON to a webhook URL.

Add new alerters by satisfying the Protocol; nothing else needs to
change. URL factory dispatches schemes:

    stdout://            -> StdoutAlerter
    slack://<webhook>    -> SlackAlerter("https://<webhook>")
    https://hooks.slack.com/... -> SlackAlerter(url) (passthrough)

Alerters are attached to `run_due_with_dag_detailed(alerters=[...])`;
each event fans out to every attached alerter. A buggy alerter that
raises is logged-and-swallowed so it can't crash the orchestrator loop.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable


# ---- AlertEvent ---------------------------------------------------------


@dataclass(frozen=True)
class AlertEvent:
    """Single unit of observable trouble (or recovery) the orchestrator
    pushes to alerters.

    `kind` is one of:
      - "failed"      — pipeline raised; retry cycle still has attempts
                        left
      - "gave_up"     — attempt_count == max_attempts; orchestrator is
                        done retrying until the next scheduled tick
                        clears the state
      - "recovered"   — a pipeline that was previously failing has now
                        succeeded; safe to stop watching

    Fields are flat (no nested dicts) so alerter implementations can
    template them into JSON / plaintext / a markdown table without
    further unpacking.
    """

    kind: str
    pipeline: str
    timestamp: datetime
    # Optional fields — populated for failed/gave_up, may be empty for
    # recovered.
    error_message: str = ""
    error_type: str = ""
    attempt_count: int = 0
    max_attempts: int = 0
    gave_up: bool = False


@runtime_checkable
class Alerter(Protocol):
    """Anything with a `notify(event)` method satisfies this."""

    def notify(self, event: AlertEvent) -> None: ...


# ---- URL factory --------------------------------------------------------


def from_url(url: str) -> "Alerter":
    """Pick the right alerter for a URL. See module docstring for schemes."""
    from urllib.parse import urlparse

    if not url:
        raise ValueError("Alerter URL must not be empty")

    parsed = urlparse(url)
    scheme = parsed.scheme.lower()

    if scheme == "stdout":
        from .stdout import StdoutAlerter
        return StdoutAlerter()

    if scheme == "slack":
        from .slack import SlackAlerter
        # slack://hooks.slack.com/services/X/Y/Z → https://hooks.slack.com/...
        # Drop the slack:// prefix, glue the rest onto https://.
        rest = url[len("slack://"):]
        webhook = "https://" + rest
        return SlackAlerter(webhook)

    if scheme in ("https", "http"):
        # Bare webhook URL — assume Slack-shaped (or compatible service
        # that accepts {"text": "..."} POSTs).
        from .slack import SlackAlerter
        return SlackAlerter(url)

    raise ValueError(
        f"unknown alerter URL scheme {scheme!r} in {url!r}. "
        f"Supported: stdout, slack, https, http."
    )


__all__ = [
    "Alerter",
    "AlertEvent",
    "StdoutAlerter",
    "SlackAlerter",
    "from_url",
]


# Lazy attribute access for concrete classes — keeps the import surface
# discoverable while not requiring optional deps for users that only
# want the Protocol or StdoutAlerter.
def __getattr__(name: str):
    if name == "StdoutAlerter":
        from .stdout import StdoutAlerter
        return StdoutAlerter
    if name == "SlackAlerter":
        from .slack import SlackAlerter
        return SlackAlerter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
