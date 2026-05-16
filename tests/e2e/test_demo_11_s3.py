"""Demo 11 E2E — MinIO (S3 API) parquet → Postgres.

Seeds 3 parquet files (600 rows total) into MinIO, runs the pipeline
for ~15 seconds, asserts 600 rows landed in `analytics.events`.
"""

from __future__ import annotations

import signal
import subprocess

from .conftest import (
    EXAMPLES,
    PYTHON,
    psql,
    truncate,
    wait_for_rows,
)

DEMO = EXAMPLES / "11_s3_parquet_to_postgres"
TARGET = "analytics.events"


def test_demo_11_s3_to_postgres(docker_stack):
    # 1. Init schema + empty target + empty bucket prefix.
    subprocess.run(
        ["docker", "exec", "-i", "ematix-flow-pg",
         "psql", "-U", "postgres", "-f", "-"],
        input=(DEMO / "init.sql").read_text(),
        text=True, check=True, capture_output=True,
    )
    truncate(TARGET)
    # `mc rm` needs an alias set per-shell — the container's mc CLI
    # doesn't persist `mc alias set` between `docker exec` invocations
    # for security reasons. Set alias + rm in one shell session.
    subprocess.run(
        ["docker", "exec", "ematix-flow-minio", "sh", "-c",
         "mc alias set local http://localhost:9000 minioadmin minioadmin "
         ">/dev/null && mc rm -r --force local/ematix-demo/events/ "
         "2>/dev/null || true"],
        capture_output=True, check=True,
    )

    # 2. Seed 3 parquet files (600 rows).
    seed = subprocess.run(
        [str(PYTHON), str(DEMO / "seed.py")],
        capture_output=True, text=True, timeout=30,
    )
    assert seed.returncode == 0, f"seed failed: {seed.stderr}"
    assert "done" in seed.stdout, f"seed didn't print 'done': {seed.stdout}"

    # 3. Start the streaming pipeline.
    pipeline = subprocess.Popen(
        [str(PYTHON), str(DEMO / "pipeline.py")],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    try:
        # 4. Wait for at least 600 rows in Postgres (give it 20s).
        n = wait_for_rows(TARGET, target=600, timeout=20)
        assert n == 600, f"expected 600 rows from 3×200-row files, got {n}"

        # 5. Spot-check value integrity: every row has all the expected
        # columns populated.
        bad = psql(
            f"SELECT count(*) FROM {TARGET} "
            f"WHERE event_id IS NULL OR user_id IS NULL "
            f"OR event_type IS NULL OR event_ts IS NULL"
        )
        assert bad == "0", f"{bad} rows have NULL primary columns"

        # 6. The 5 event_type values are well-known constants from
        # seed.py — verify the streaming bridge didn't corrupt strings.
        types = psql(
            f"SELECT array_agg(DISTINCT event_type ORDER BY event_type) "
            f"FROM {TARGET}"
        )
        for t in ("click", "logout", "page_view", "purchase", "signup"):
            assert t in types, f"missing event_type {t!r} in {types}"
    finally:
        pipeline.send_signal(signal.SIGINT)
        try:
            pipeline.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pipeline.kill()
            pipeline.wait(timeout=3)
