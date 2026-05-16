"""Stale claim → next tick reclaims it.

Simulates a worker that died mid-pipeline by directly writing a
`pipeline_claims` row with an expired `expires_at`. The next
scheduler tick MUST be able to reclaim it (lease-expiry CAS branch
inside `claim()`) and dispatch the pipeline.

This is the death-recovery path the scheduler relies on: workers
don't get to gracefully release on crash, the only signal is that
their heartbeat stops and the row's `expires_at` rolls into the past.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ..conftest import EXAMPLES, FLOW

DEMO = EXAMPLES / "10_workflow_dag"
WAREHOUSE = Path("/tmp/ematix-demo-10.db")
RUNLOG = Path("/tmp/ematix-fc-stale-claim-runs.db")


def _init_run_log_with_stale_claim() -> None:
    """Bootstrap the RunLog schema and inject a stale claim on
    `raw_orders`. We create the tables by opening the SqliteRunLog
    once (it runs CREATE TABLE IF NOT EXISTS), then overwrite the
    claim row with `expires_at` 60s in the past."""
    from ematix_flow.run_log import SqliteRunLog

    log = SqliteRunLog(str(RUNLOG))
    try:
        pass  # CREATE TABLE IF NOT EXISTS ran in __init__
    finally:
        log.close()

    stale_at = (datetime.now(UTC) - timedelta(seconds=60)).isoformat()
    with sqlite3.connect(RUNLOG) as c:
        c.execute(
            "INSERT OR REPLACE INTO pipeline_claims "
            "(pipeline_name, claim_token, worker_id, claimed_at, expires_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("raw_orders", "stale-token-from-dead-worker",
             "dead-worker-1234", stale_at, stale_at),
        )
        c.commit()


def test_scheduler_reclaims_stale_claim(docker_stack):
    for f in (WAREHOUSE, RUNLOG):
        f.unlink(missing_ok=True)

    _init_run_log_with_stale_claim()

    # Sanity-check the stale row is in fact stale.
    with sqlite3.connect(RUNLOG) as c:
        row = c.execute(
            "SELECT worker_id, expires_at FROM pipeline_claims "
            "WHERE pipeline_name='raw_orders'"
        ).fetchone()
    assert row is not None and row[0] == "dead-worker-1234"
    assert datetime.fromisoformat(row[1]) < datetime.now(UTC), (
        "test setup wrong: stale claim isn't actually expired"
    )

    # Run scheduler for 3 ticks — should reclaim raw_orders on tick 1.
    env = {**os.environ, "PYTHONPATH": "."}
    proc = subprocess.run(
        [
            str(FLOW), "scheduler",
            "--module", "pipelines",
            "--executor", "subprocess+python://",
            "--run-log-url", f"sqlite://{RUNLOG}",
            "--poll-interval", "1",
            "--interval", "60",
            "--max-iterations", "3",
        ],
        cwd=str(DEMO),
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 0, f"scheduler died: {proc.stderr[-500:]}"

    # Verify: stale worker_id is GONE from the claim row.
    with sqlite3.connect(RUNLOG) as c:
        rows = c.execute(
            "SELECT worker_id FROM pipeline_claims "
            "WHERE pipeline_name='raw_orders'"
        ).fetchall()
    if rows:
        assert rows[0][0] != "dead-worker-1234", (
            "scheduler didn't reclaim the stale row — leader-lease "
            "expiry CAS broken"
        )

    # Verify: raw_orders has a successful run record.
    with sqlite3.connect(RUNLOG) as c:
        run_log_rows = c.execute(
            "SELECT pipeline_name, success FROM run_log "
            "WHERE pipeline_name='raw_orders'"
        ).fetchall()
    assert run_log_rows, "raw_orders never recorded a run after reclaim"

    # Verify: scheduler logged the dispatch.
    assert "dispatched pipeline=raw_orders" in proc.stderr, proc.stderr[-500:]
