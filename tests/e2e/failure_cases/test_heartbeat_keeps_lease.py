"""A worker that runs longer than `--lease-seconds` must have its
heartbeat thread extending the lease. Otherwise the lease expires
and the scheduler dispatches a second worker, double-running the
pipeline.

This test sets `--lease-seconds 5` and runs a pipeline that sleeps
8 seconds. If the heartbeat is broken, we'd see TWO `slept_secs`
records or evidence of a re-dispatch.
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

from ..conftest import FLOW

FIXTURE_DIR = Path(__file__).parent
RUNLOG = Path("/tmp/ematix-fc-heartbeat-runs.db")


def test_heartbeat_keeps_lease_alive(docker_stack):
    RUNLOG.unlink(missing_ok=True)

    env = {**os.environ, "PYTHONPATH": "."}
    # poll every 2s, lease 5s (shorter than the 8s pipeline body),
    # heartbeat every 1s. 3 iterations = 6s scheduler wall clock — less
    # than the 8s pipeline run. The single dispatch must hold the claim
    # for the whole 6s without expiring; if the heartbeat is broken,
    # the lease would die at t=5s and tick #3 (at t=4s) would skip
    # but tick at t=6 — never mind, we exit at t=6. To force a real
    # heartbeat exercise, we need at least one tick AFTER the 5s
    # lease window where the original claim is still legitimately
    # alive — that's tick #3 at t=4s and tick #4 at t=6s.
    #
    # So: 4 iterations × 2s = 8s. If the heartbeat works, we see 1
    # dispatch at t=0, then ticks 2/3/4 (at t=2/4/6) all see the claim
    # still held (heartbeat extended past 5s) → skip → no second
    # dispatch. If the heartbeat is broken, the claim expires at t=5s,
    # tick 4 at t=6s dispatches a SECOND worker (catching the bug).
    t0 = time.monotonic()
    proc = subprocess.run(
        [
            str(FLOW), "scheduler",
            "--module", "slow_pipeline",
            "--executor", "subprocess+python://",
            "--run-log-url", f"sqlite://{RUNLOG}",
            "--poll-interval", "2",
            "--interval", "60",
            "--lease-seconds", "5",
            "--max-iterations", "4",
        ],
        cwd=str(FIXTURE_DIR),
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    elapsed = time.monotonic() - t0
    assert proc.returncode == 0, f"scheduler died: {proc.stderr[-500:]}"

    # Exactly one dispatch — heartbeat extended the lease past 5s so
    # tick 4 at t=6s still saw the claim as held and skipped.
    dispatches = proc.stderr.count("dispatched pipeline=slow claim=")
    assert dispatches == 1, (
        f"heartbeat broken: scheduler dispatched 'slow' {dispatches} "
        f"times in {elapsed:.1f}s with a 5s lease — heartbeat thread "
        f"isn't extending it across thread boundaries.\n"
        f"stderr tail:\n{proc.stderr[-800:]}"
    )

    # Side benefit: confirm no thread-safety errors in stderr (the
    # earlier sqlite cross-thread bug).
    assert "SQLite objects created in a thread" not in proc.stderr, (
        f"SqliteRunLog cross-thread guard regressed:\n{proc.stderr}"
    )
