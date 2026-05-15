"""A pipeline that always fails should hit max_attempts → gave_up,
then the scheduler should stop re-dispatching it.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
from pathlib import Path

from ..conftest import FLOW

FIXTURE_DIR = Path(__file__).parent
RUNLOG = Path("/tmp/ematix-fc-give-up-runs.db")


def test_always_failing_pipeline_gives_up(docker_stack):
    RUNLOG.unlink(missing_ok=True)

    env = {**os.environ, "PYTHONPATH": "."}
    # Tight loop: poll every 2s, lease 30s, 5 iterations. Plenty of
    # ticks for max_attempts=2 with base_secs=1 backoff to give up.
    proc = subprocess.run(
        [
            str(FLOW), "scheduler",
            "--module", "always_fails_pipelines",
            "--executor", "subprocess+python://",
            "--run-log-url", f"sqlite://{RUNLOG}",
            "--poll-interval", "2",
            "--interval", "60",
            "--max-iterations", "5",
        ],
        cwd=str(FIXTURE_DIR),
        env=env,
        capture_output=True,
        text=True,
        timeout=45,
    )
    assert proc.returncode == 0, f"scheduler died: {proc.stderr[-500:]}"

    # 1. attempt_state should show attempt_count >= max_attempts and
    # gave_up = 1.
    with sqlite3.connect(RUNLOG) as c:
        row = c.execute(
            "SELECT attempt_count, gave_up FROM attempt_state "
            "WHERE pipeline_name='always_fails'"
        ).fetchone()
    assert row is not None, "no attempt_state recorded — worker never wrote outcome"
    attempt_count, gave_up = row
    assert attempt_count >= 2, f"attempt_count={attempt_count}, expected ≥2"
    assert gave_up == 1, f"gave_up={gave_up}, expected 1 after max_attempts"

    # 2. The run_log should show a failure record.
    with sqlite3.connect(RUNLOG) as c:
        runs = c.execute(
            "SELECT success FROM run_log WHERE pipeline_name='always_fails'"
        ).fetchall()
    assert runs, "no run_log row for always_fails"
    assert all(s == 0 for (s,) in runs), f"expected all failures, got {runs}"
