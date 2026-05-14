"""Worker-side heartbeat thread.

Started by `flow run` when invoked with `--claim-token`. Calls
`RunLog.heartbeat(token, lease_seconds)` every `interval_seconds`
until the worker either finishes or signals shutdown.

The thread is daemon so it dies with the main process if the
worker is SIGKILL'd; in that case the lease eventually expires and
the scheduler's `sweep_expired_leases` re-marks the pipeline. The
clean-shutdown path calls `stop()` which sets the event and joins.
"""

from __future__ import annotations

import logging
import threading

log = logging.getLogger(__name__)


class HeartbeatThread:
    """Periodic-heartbeat helper for a single claim.

    Usage:
        hb = HeartbeatThread(run_log, claim_token, lease_seconds=300,
                             interval_seconds=100)
        hb.start()
        try:
            run_the_pipeline()
        finally:
            hb.stop()
    """

    def __init__(
        self,
        run_log,
        claim_token: str,
        *,
        lease_seconds: int,
        interval_seconds: int,
    ):
        self._run_log = run_log
        self._claim_token = claim_token
        self._lease_seconds = lease_seconds
        self._interval_seconds = max(1, int(interval_seconds))
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._loop, name="ematix-flow-heartbeat", daemon=True
        )
        self._thread.start()

    def stop(self, *, timeout: float = 2.0) -> None:
        if self._thread is None:
            return
        self._stop_event.set()
        self._thread.join(timeout=timeout)
        self._thread = None

    def _loop(self) -> None:
        while not self._stop_event.wait(self._interval_seconds):
            try:
                self._run_log.heartbeat(
                    self._claim_token, self._lease_seconds
                )
            except Exception as e:
                # Don't crash the worker on a transient RunLog blip.
                # The lease eventually expires; the scheduler's sweep
                # will reclaim — that's the recovery path.
                log.warning(
                    "heartbeat failed for token=%s: %s: %s",
                    self._claim_token,
                    type(e).__name__,
                    e,
                )
