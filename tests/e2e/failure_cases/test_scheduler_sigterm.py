"""SIGTERM the scheduler mid-flight; verify the next replica can acquire
the leader lease (either via clean release or lease expiry).

The leader-election machinery lives on the `_scheduler_singleton` row
in the RunLog. If the first scheduler doesn't release on exit, the
second one MUST still be able to acquire on the next tick — either
because the lease expired, or because the lease-expiry CAS branch
fires inside `claim()`.
"""

from __future__ import annotations

import os
import signal
import sqlite3
import subprocess
import time
from pathlib import Path

from ..conftest import EXAMPLES, FLOW

DEMO = EXAMPLES / "10_workflow_dag"
WAREHOUSE = Path("/tmp/ematix-demo-10.db")
RUNLOG = Path("/tmp/ematix-fc-sigterm-runs.db")


def _spawn_scheduler(lease_secs: int = 10) -> subprocess.Popen:
    env = {**os.environ, "PYTHONPATH": "."}
    return subprocess.Popen(
        [
            str(FLOW), "scheduler",
            "--module", "pipelines",
            "--executor", "subprocess+python://",
            "--run-log-url", f"sqlite://{RUNLOG}",
            "--poll-interval", "1",
            "--interval", "60",
            "--lease-seconds", str(lease_secs),
        ],
        cwd=str(DEMO),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def test_second_scheduler_acquires_leader_after_sigterm(docker_stack):
    # Fresh state — leader-election should be unambiguous.
    for f in (WAREHOUSE, RUNLOG):
        f.unlink(missing_ok=True)

    # 1. First scheduler runs for ~3 ticks then gets SIGTERM'd.
    s1 = _spawn_scheduler(lease_secs=10)
    time.sleep(3)
    s1.send_signal(signal.SIGTERM)
    try:
        s1.wait(timeout=5)
    except subprocess.TimeoutExpired:
        s1.kill()
        s1.wait(timeout=3)

    # The leader claim row will either be gone (clean release) or
    # present with an expires_at in the future (lease still alive).
    # Either is fine — what matters is the next scheduler can acquire.

    # 2. Start a second scheduler with a TIGHTER lease so the first
    # one's claim definitely shows as expired. Lease is 10s, slept 3s.
    # The second scheduler is configured with `--lease-seconds 60` so
    # ITS own claim is long; but `claim()` uses `expires_at <= now`
    # against the existing row, so it should reclaim immediately once
    # the prior lease wall-clock has rolled past expires_at.
    #
    # To make this fast, we just wait 8 more seconds (3+8 = 11 > 10s
    # original lease) before starting s2 so the s1 lease is provably
    # stale.
    time.sleep(8)

    s2 = _spawn_scheduler(lease_secs=60)
    try:
        time.sleep(4)  # let s2 do at least 2 ticks
        # Verify s2 acquired leadership + walked the DAG (i.e. logged
        # at least one dispatch). If s1's leader lease had been
        # un-reclaimable, s2 would be backed off and log nothing.
        s2.send_signal(signal.SIGTERM)
        try:
            out, _ = s2.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            s2.kill()
            out, _ = s2.communicate(timeout=3)
        text = out.decode(errors="replace")
        assert "dispatched pipeline=" in text, (
            f"s2 never dispatched anything — leader-lease takeover failed.\n"
            f"s2 output:\n{text[-800:]}"
        )
    finally:
        if s2.poll() is None:
            s2.kill()
            s2.wait(timeout=3)

    # Sanity: at least one pipeline ran (warehouse has rows).
    with sqlite3.connect(WAREHOUSE) as c:
        raw_n = c.execute("SELECT COUNT(*) FROM raw_orders").fetchone()[0]
    assert raw_n > 0, "warehouse empty — no worker actually ran"
