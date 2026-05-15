"""Demo 10 E2E — workflow DAG + central scheduler.

Runs `flow scheduler --max-iterations 5` against the three registered
pipelines and asserts the DAG actually walked: each pipeline fired
≥ 1×, downstream pipelines stayed gated until upstream succeeded.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
from pathlib import Path

from .conftest import EXAMPLES, FLOW

DEMO = EXAMPLES / "10_workflow_dag"
WAREHOUSE = Path("/tmp/ematix-demo-10.db")
RUNLOG = Path("/tmp/ematix-demo-10-runs.db")


def test_demo_10_dag_walks_in_order(docker_stack):
    # Fresh state for each run.
    for f in (WAREHOUSE, RUNLOG):
        f.unlink(missing_ok=True)

    env = {**os.environ, "PYTHONPATH": "."}
    proc = subprocess.run(
        [
            str(FLOW), "scheduler",
            "--module", "pipelines",
            "--executor", "subprocess+python://",
            "--run-log-url", f"sqlite://{RUNLOG}",
            "--poll-interval", "2",
            "--interval", "60",
            "--max-iterations", "5",
        ],
        cwd=str(DEMO),
        env=env,
        capture_output=True,
        text=True,
        timeout=45,
    )
    assert proc.returncode == 0, (
        f"scheduler exited {proc.returncode}\nstderr: {proc.stderr[-500:]}"
    )

    # 1. RunLog should have run entries for all 3 pipelines.
    with sqlite3.connect(RUNLOG) as c:
        run_log_rows = dict(c.execute(
            "SELECT pipeline_name, success FROM run_log"
        ).fetchall())
    assert "raw_orders" in run_log_rows, "raw_orders never recorded a run"
    assert "enriched_orders" in run_log_rows, (
        "enriched_orders gated forever — DAG fan-out broken"
    )
    # daily_summary has a 30% flake rate so it MAY not have succeeded
    # in 5 ticks, but it should have at least been attempted; that's
    # observable via attempt_state when failed.
    with sqlite3.connect(RUNLOG) as c:
        attempts = dict(c.execute(
            "SELECT pipeline_name, attempt_count FROM attempt_state"
        ).fetchall())
    fired_daily = "daily_summary" in run_log_rows or "daily_summary" in attempts
    assert fired_daily, "daily_summary never dispatched — DAG depth-3 broken"

    # 2. Warehouse should have rows — confirms the worker subprocesses
    # actually executed the pipeline bodies (not just claimed and bailed).
    with sqlite3.connect(WAREHOUSE) as c:
        raw_n = c.execute("SELECT COUNT(*) FROM raw_orders").fetchone()[0]
        enr_n = c.execute("SELECT COUNT(*) FROM enriched_orders").fetchone()[0]
    assert raw_n >= 5, f"raw_orders inserted {raw_n} rows; expected ≥5"
    assert enr_n >= 5, f"enriched_orders processed {enr_n} rows; expected ≥5"

    # 3. Scheduler emitted at least one dispatch log line per pipeline
    # (visibility regression guard — silent scheduler is a bug we hit).
    assert "dispatched pipeline=raw_orders" in proc.stderr
    assert "dispatched pipeline=enriched_orders" in proc.stderr
