"""Worker-side pause-checking for Phase 4b-4.

The scheduler enqueues restart / rerun / pause actions into the
:class:`ematix_flow.run_log.history.RunHistoryStore` via the Web
UI (Phase 4b-1) and surfaces them on each tick (Phase 4b-3).
Restart / rerun become new dispatched runs the scheduler hands to
the worker via the executor — straightforward.

Pause / resume are different: they target a **currently running**
run. The worker has to notice the request and transition itself
to ``"paused"`` (or back to ``"running"``) at a clean
step / batch / watermark boundary. This module is the helper
workers use to do that check.

Usage in a worker:

.. code-block:: python

    from ematix_flow.web.pause import PauseChecker

    checker = PauseChecker(history=my_store, run_id=this_run_id)
    for batch in fetch_batches():
        if checker.should_pause():
            # Transition + bail out of the loop.
            checker.acknowledge_pause()
            break
        process(batch)

``should_pause()`` is cheap (one ``get_run`` call) so workers can
call it on every batch or every step. ``acknowledge_pause()`` is
the explicit transition — it writes ``status = "paused"`` so the
Web UI shows the right state and the scheduler doesn't re-dispatch
a still-claimed run.

Resume happens server-side (the scheduler re-claims the paused
pipeline once the user clicks Resume + a new "requested" row is
enqueued). Workers don't poll for resume — they exit cleanly on
pause and the next dispatch picks up from the last completed
boundary.
"""
from __future__ import annotations

from dataclasses import replace as _replace
from datetime import UTC, datetime

from ematix_flow.run_log.history import RunHistoryStore

__all__ = ["PauseChecker"]


class PauseChecker:
    """Encapsulates the "should I pause?" + "I'm pausing now"
    handshake between a running worker and the Web UI.

    Stateless across calls — each ``should_pause()`` re-reads the
    history row. Workers can construct one at the top of their
    run and re-use it for the duration.

    Args:
        history: the :class:`RunHistoryStore` the Web UI writes
            into. Workers running in a subprocess typically open
            this from the same URL the scheduler uses (passed via
            an env var).
        run_id: the worker's own run id (the one the scheduler
            handed it on dispatch). Identifies which row to
            check / mutate.
    """

    def __init__(self, history: RunHistoryStore, run_id: str) -> None:
        self._history = history
        self._run_id = run_id
        self._paused = False  # cached so workers can re-call cheaply

    def should_pause(self) -> bool:
        """True iff the Web UI has set ``extras["pause_requested"]
        = True`` on this run and the worker hasn't acknowledged
        yet. Idempotent — calling multiple times before
        ``acknowledge_pause()`` returns True consistently."""
        if self._paused:
            return True
        try:
            rec = self._history.get_run(self._run_id)
        except Exception:
            # Best-effort: a transient history backend error must
            # not crash the worker. Default to "don't pause."
            return False
        if rec is None:
            return False
        return bool(rec.extras.get("pause_requested"))

    def acknowledge_pause(self) -> None:
        """Transition this run's row to ``status = "paused"``.

        Called by the worker immediately before it returns from
        its run loop. The Web UI shows the pause status; the
        scheduler treats the row as terminated (not dispatched)
        until the user clicks Resume and a fresh "requested"
        row is enqueued.

        Idempotent — calling on an already-paused row is a no-op.
        Tolerant of missing rows (logs nothing, returns).
        """
        rec = self._history.get_run(self._run_id)
        if rec is None or rec.status == "paused":
            self._paused = True
            return
        # Use `record_run_record` (the idempotent write API) so
        # backends without dedicated "update status" methods still
        # work uniformly.

        new_extras = dict(rec.extras)
        new_extras["paused_at"] = _iso_now()
        # `paused` status doesn't get a finished_at — the run isn't
        # actually finished, it's just sitting.
        paused = _replace(
            rec,
            status="paused",
            finished_at=None,
            extras=new_extras,
        )
        self._history.record_run_record(paused)
        self._paused = True

    def reset(self) -> None:
        """Drop the cached "paused" flag. Used by tests + workers
        that resume mid-process (rare; usually the worker exits
        and a fresh run picks up later)."""
        self._paused = False


def _iso_now() -> str:
    ts = datetime.now(UTC)
    return ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
