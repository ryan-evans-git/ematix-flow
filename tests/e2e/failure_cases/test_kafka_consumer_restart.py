"""Kafka consumer restart must preserve at-least-once semantics:
after a mid-stream kill + restart with the same group_id, every
seq_id the producer emitted must appear at least once in the
target table. Duplicates are allowed (at-least-once); gaps are not.

Producer emits seq_id = 0..N-1. Consumer dies mid-stream. Restart
with same group_id picks up from the last *committed* offset, so
some events get re-delivered. The set of distinct seq_ids in
Postgres at the end must equal the full {0..N-1}.
"""

from __future__ import annotations

import os
import signal
import subprocess
import time

from ..conftest import (
    EXAMPLES,
    FLOW,
    PYTHON,
    psql,
    psql_count,
    reset_consumer_group,
    reset_kafka_topic,
    truncate,
    wait_for_rows,
)

DEMO = EXAMPLES / "09_streaming_clickstream"
TARGET = "analytics.clicks"
TOPIC = "clicks"
GROUP = "ematix-demo-09"
NUM_EVENTS = 60  # 6 seconds at 10 events/sec — fast enough for a 60s budget


def _consumer() -> subprocess.Popen:
    env = {**os.environ, "PYTHONPATH": "."}
    return subprocess.Popen(
        [str(FLOW), "consume", "--module", "pipeline", "clicks-to-pg"],
        cwd=str(DEMO),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def _stop(p: subprocess.Popen) -> None:
    p.send_signal(signal.SIGINT)
    try:
        p.wait(timeout=5)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=3)


def test_kafka_consumer_restart_preserves_at_least_once(docker_stack):
    # Init schema + clear table.
    subprocess.run(
        ["docker", "exec", "-i", "ematix-flow-pg",
         "psql", "-U", "postgres", "-f", "-"],
        input=(DEMO / "init.sql").read_text(),
        text=True, check=True, capture_output=True,
    )
    truncate(TARGET)

    # Hard-reset Kafka state: delete + recreate the topic (clears
    # any leftover events from prior test runs), and delete the
    # consumer group's committed offsets so this run starts fresh
    # at auto_offset_reset=earliest.
    reset_consumer_group(GROUP)
    reset_kafka_topic(TOPIC)

    # Pre-flight: bounded producer emits seq_id 0..NUM_EVENTS-1 to
    # the topic and exits. Producer doesn't need a consumer running
    # — Kafka retains messages on the broker for the consumer to
    # find on subscribe (default retention is 7 days).
    producer = subprocess.run(
        [str(PYTHON), str(DEMO / "producer.py"),
         "--num-events", str(NUM_EVENTS),
         "--rate", "30"],   # fast emit (~2s for 60 events)
        check=True, capture_output=True, text=True, timeout=15,
    )
    assert "produced" in producer.stdout

    # First consumer: run for ~6s (enough to subscribe + drain at
    # least some events). Same group_id as pipeline.py declares
    # (`ematix-demo-09`), so offsets commit to that group on flush.
    c1 = _consumer()
    try:
        time.sleep(6)
        partial = psql_count(TARGET)
    finally:
        _stop(c1)

    # The test's value is in restart behavior. Whether the first
    # consumer got 0 / partial / all events all valid setups for
    # the at-least-once assertion below — what matters is the
    # SECOND consumer doesn't drop anything between the kill point
    # and the topic end.

    # Restart the consumer (same code, same group_id). It resumes
    # from the last committed offset.
    c2 = _consumer()
    try:
        wait_for_rows(TARGET, target=NUM_EVENTS, timeout=20)
    finally:
        _stop(c2)

    # Now the bar: distinct seq_ids = {0..NUM_EVENTS-1}, every event
    # delivered at least once. Duplicates allowed.
    total = psql_count(TARGET)
    distinct = int(psql(f"SELECT count(DISTINCT seq_id) FROM {TARGET}"))
    min_seq = int(psql(f"SELECT min(seq_id) FROM {TARGET}"))
    max_seq = int(psql(f"SELECT max(seq_id) FROM {TARGET}"))

    assert distinct == NUM_EVENTS, (
        f"at-least-once violated: producer emitted {NUM_EVENTS} "
        f"distinct seq_ids but only {distinct} reached the target "
        f"(min={min_seq}, max={max_seq}, total_rows={total})"
    )
    assert min_seq == 0
    assert max_seq == NUM_EVENTS - 1
    # Duplicates are fine (at-least-once); just log how many.
    print(
        f"\n  ok: {distinct} distinct, {total} total rows "
        f"({total - distinct} duplicates from restart)"
    )
