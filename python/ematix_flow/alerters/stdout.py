"""StdoutAlerter — writes each event to a stream (stderr by default).

Default for dev / local-run use: no config, no network. Output is one
human-readable line per event, prefixed with `[ALERT]` so it's grep-able.
"""

from __future__ import annotations

import sys
from typing import TextIO

from . import AlertEvent


class StdoutAlerter:
    def __init__(self, stream: TextIO | None = None):
        # Default to stderr so structured JSON output on stdout (from
        # `flow run-due`) stays parseable.
        self._stream = stream if stream is not None else sys.stderr

    def notify(self, event: AlertEvent) -> None:
        ts = event.timestamp.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        if event.kind == "failed":
            msg = (
                f"[ALERT] {ts} failed: {event.pipeline} "
                f"(attempt {event.attempt_count}/{event.max_attempts}): "
                f"{event.error_type}: {event.error_message}"
            )
        elif event.kind == "gave_up":
            msg = (
                f"[ALERT] {ts} gave_up: {event.pipeline} "
                f"after {event.attempt_count} attempts: "
                f"{event.error_type}: {event.error_message}"
            )
        elif event.kind == "recovered":
            msg = (
                f"[ALERT] {ts} recovered: {event.pipeline} "
                f"(after {event.attempt_count} attempts)"
            )
        else:
            msg = f"[ALERT] {ts} {event.kind}: {event.pipeline}"
        print(msg, file=self._stream, flush=True)
