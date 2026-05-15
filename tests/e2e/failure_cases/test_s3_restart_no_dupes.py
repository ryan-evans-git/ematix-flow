"""S3 streaming source must not re-process files on restart — the
high-water mark over object keys is supposed to persist via the
StateStore and skip already-processed files.

Demo 11's `pipeline.py` doesn't currently configure a StateStore,
so this test surfaces whether the in-memory default high-water mark
survives at all. If 600 rows become 1200 on restart, the demo is
under-configured for production and we should fix the demo (or
document the gap).
"""

from __future__ import annotations

import os
import signal
import subprocess
import time

import pytest

from ..conftest import (
    EXAMPLES, PYTHON, psql_count, truncate, wait_for_rows,
)

DEMO = EXAMPLES / "11_s3_parquet_to_postgres"
TARGET = "analytics.events"


def _empty_bucket() -> None:
    subprocess.run(
        ["docker", "exec", "ematix-flow-minio", "sh", "-c",
         "mc alias set local http://localhost:9000 minioadmin minioadmin "
         ">/dev/null && mc rm -r --force local/ematix-demo/events/ "
         "2>/dev/null || true"],
        capture_output=True, check=True,
    )


def _start_pipeline() -> subprocess.Popen:
    return subprocess.Popen(
        [str(PYTHON), str(DEMO / "pipeline.py")],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )


def _stop_pipeline(p: subprocess.Popen) -> None:
    p.send_signal(signal.SIGINT)
    try:
        p.wait(timeout=5)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=3)


def test_s3_pipeline_restart_does_not_reprocess(docker_stack):
    subprocess.run(
        ["docker", "exec", "-i", "ematix-flow-pg",
         "psql", "-U", "postgres", "-f", "-"],
        input=(DEMO / "init.sql").read_text(),
        text=True, check=True, capture_output=True,
    )
    truncate(TARGET)
    _empty_bucket()

    # Seed once.
    subprocess.run(
        [str(PYTHON), str(DEMO / "seed.py")],
        check=True, capture_output=True, timeout=30,
    )

    # First run — drains the 3 files.
    p1 = _start_pipeline()
    try:
        n1 = wait_for_rows(TARGET, target=600, timeout=20)
    finally:
        _stop_pipeline(p1)
    assert n1 == 600, f"first run got {n1} rows, expected 600"

    # Second run against the SAME bucket contents. If the high-water
    # mark persists, this should add 0 new rows. If it doesn't, this
    # will add another 600 (re-process all files).
    p2 = _start_pipeline()
    try:
        # Give the pipeline 10s of wallclock to potentially re-process.
        time.sleep(10)
        n2 = psql_count(TARGET)
    finally:
        _stop_pipeline(p2)

    if n2 > 600:
        pytest.fail(
            f"S3 streaming source RE-PROCESSED already-seen files on "
            f"restart: {n1} → {n2} rows. The high-water mark didn't "
            f"persist between runs. Either the demo needs a "
            f"StateStore config, or the source isn't honoring the "
            f"mark across restarts."
        )

    assert n2 == 600
