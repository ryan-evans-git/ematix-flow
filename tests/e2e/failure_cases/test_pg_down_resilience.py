"""When a pipeline's target Postgres is unreachable, the worker must
fail gracefully (no scheduler crash). Retries fire per the pipeline's
`retry={...}` policy. After `max_attempts` the pipeline is marked
`gave_up` and the scheduler stops re-dispatching it — but the
scheduler process itself stays alive and continues running.

Validates the scheduler's resilience to a downstream-DB outage.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
from pathlib import Path

from ..conftest import FLOW

FIXTURE_DIR = Path(__file__).parent
RUNLOG = Path("/tmp/ematix-fc-pg-down-runs.db")


def test_pg_down_pipeline_gives_up_scheduler_stays_alive(docker_stack):
    RUNLOG.unlink(missing_ok=True)

    env = {**os.environ, "PYTHONPATH": "."}
    # 6 iterations × 2s poll = ~12s wallclock. Each worker attempt
    # fails fast (connect_timeout=2 → ~2s). max_attempts=2 with 1s
    # fixed backoff comfortably fits the budget; the extra ticks
    # verify the scheduler doesn't re-dispatch after gave_up.
    proc = subprocess.run(
        [
            str(FLOW), "scheduler",
            "--module", "pg_down_pipeline",
            "--executor", "subprocess+python://",
            "--run-log-url", f"sqlite://{RUNLOG}",
            "--poll-interval", "2",
            "--interval", "60",
            "--max-iterations", "6",
        ],
        cwd=str(FIXTURE_DIR),
        env=env,
        capture_output=True,
        text=True,
        timeout=50,
    )

    # Scheduler must exit cleanly — worker failures don't kill it.
    assert proc.returncode == 0, (
        f"scheduler died (exit={proc.returncode}): "
        f"stderr={proc.stderr[-800:]}"
    )

    with sqlite3.connect(RUNLOG) as c:
        row = c.execute(
            "SELECT attempt_count, gave_up FROM attempt_state "
            "WHERE pipeline_name='pg_writer_against_down_db'"
        ).fetchone()
    assert row is not None, (
        "no attempt_state recorded — worker never wrote outcome "
        "(did the pipeline import fail?)"
    )
    attempt_count, gave_up = row
    assert attempt_count >= 2, (
        f"attempt_count={attempt_count}, expected ≥2 "
        "(retry policy didn't fire)"
    )
    assert gave_up == 1, (
        f"gave_up={gave_up}, expected 1 after max_attempts — "
        "scheduler kept re-dispatching past the give-up boundary"
    )

    # Every run_log row should be a failure — no run can have
    # succeeded against a dead Postgres.
    with sqlite3.connect(RUNLOG) as c:
        runs = c.execute(
            "SELECT success FROM run_log "
            "WHERE pipeline_name='pg_writer_against_down_db'"
        ).fetchall()
    assert runs, "no run_log row for pg_writer_against_down_db"
    assert all(s == 0 for (s,) in runs), (
        f"expected all failures against dead PG, got successes: {runs}"
    )

    # And critically: the scheduler shouldn't keep dispatching after
    # gave_up. Total run count is bounded by max_attempts (2) — any
    # more would mean the give-up gate is not holding.
    assert len(runs) <= 2, (
        f"scheduler re-dispatched after gave_up: {len(runs)} runs "
        "(expected ≤ 2 = max_attempts)"
    )
