"""Demo 09 E2E — Kafka → Postgres.

Starts the streaming consumer + producer, lets ~50 events flow, and
asserts the row count in `analytics.clicks` reaches 50.
"""

from __future__ import annotations

import os
import signal
import subprocess
import time

from .conftest import (
    EXAMPLES, FLOW, PYTHON, psql, psql_count, truncate, wait_for_rows,
)

DEMO = EXAMPLES / "09_streaming_clickstream"
TARGET = "analytics.clicks"


def test_demo_09_kafka_to_postgres(docker_stack):
    # 1. Ensure schema is initialised + table empty.
    subprocess.run(
        ["docker", "exec", "-i", "ematix-flow-pg",
         "psql", "-U", "postgres", "-f", "-"],
        input=(DEMO / "init.sql").read_text(),
        text=True, check=True, capture_output=True,
    )
    truncate(TARGET)

    # 2. Start the consumer in the background.
    env = {**os.environ, "PYTHONPATH": "."}
    consumer = subprocess.Popen(
        [str(FLOW), "consume", "--module", "pipeline", "clicks-to-pg"],
        cwd=str(DEMO),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    try:
        # 3. Give the consumer time to subscribe before producing.
        time.sleep(4)

        # 4. Run the producer for ~5s (≈50 events).
        producer = subprocess.Popen(
            [str(PYTHON), str(DEMO / "producer.py")],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        time.sleep(5)
        producer.terminate()
        producer.wait(timeout=3)

        # 5. Wait for at least 30 rows (producer emits ~10/s; pipeline
        # should drain quickly).
        n = wait_for_rows(TARGET, target=30, timeout=15)
        assert n >= 30, f"expected ≥30 rows after producer flush, got {n}"

        # 6. Spot-check: every row has a non-null url and parseable
        # event_ts (the Arrow → TIMESTAMP cast worked).
        bad = psql(
            f"SELECT count(*) FROM {TARGET} "
            f"WHERE url IS NULL OR event_ts IS NULL"
        )
        assert bad == "0", f"{bad} rows have NULL url/event_ts"
    finally:
        consumer.send_signal(signal.SIGINT)
        try:
            consumer.wait(timeout=5)
        except subprocess.TimeoutExpired:
            consumer.kill()
            consumer.wait(timeout=3)
