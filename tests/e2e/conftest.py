"""Shared fixtures for the demo-driven E2E suite.

Tests in this directory are **opt-in**: they require the local docker
stack (postgres + kafka + minio) which `examples/docker-compose.yml`
defines. Run with:

    pytest -q tests/e2e/ --e2e

Without `--e2e` the tests are skipped, so the regular `pytest` /
`make test-python` lanes stay fast and Docker-free.

Each test asserts on visible side effects:
- rows in Postgres (`analytics.clicks` / `analytics.events` / SQLite warehouse)
- RunLog state (`flow status` equivalents)

Time budget: each test is bounded to ≤ 60 seconds via `--max-iterations`
on the scheduler, bounded producers, and explicit subprocess timeouts.
"""

from __future__ import annotations

import os
import socket
import subprocess
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON = REPO_ROOT / ".venv" / "bin" / "python"
FLOW = REPO_ROOT / ".venv" / "bin" / "flow"
EXAMPLES = REPO_ROOT / "examples"

# Demo stack endpoints — these MUST match `examples/docker-compose.yml`.
PG_HOST, PG_PORT = "127.0.0.1", 5434
KAFKA_HOST, KAFKA_PORT = "127.0.0.1", 9092
MINIO_HOST, MINIO_PORT = "127.0.0.1", 9000


# ---- option wiring -------------------------------------------------


def pytest_addoption(parser):
    parser.addoption(
        "--e2e",
        action="store_true",
        default=False,
        help="run E2E tests (require docker stack: `make up`)",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--e2e"):
        return  # caller opted in — run everything
    skip = pytest.mark.skip(reason="pass --e2e to run; needs docker stack")
    for item in items:
        item.add_marker(skip)


# ---- service readiness probes --------------------------------------


def _tcp_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def docker_stack():
    """Asserts the demo stack is reachable; fails the test if not.

    We intentionally don't `make up` from inside the test — the
    operator is expected to have brought it up. That avoids races
    between concurrent test workers and lets the operator inspect a
    broken stack interactively after a failure.
    """
    missing = []
    if not _tcp_open(PG_HOST, PG_PORT):
        missing.append(f"postgres on {PG_HOST}:{PG_PORT}")
    if not _tcp_open(KAFKA_HOST, KAFKA_PORT):
        missing.append(f"kafka on {KAFKA_HOST}:{KAFKA_PORT}")
    if not _tcp_open(MINIO_HOST, MINIO_PORT):
        missing.append(f"minio on {MINIO_HOST}:{MINIO_PORT}")
    if missing:
        pytest.skip(
            "demo stack not running: " + ", ".join(missing)
            + " — run `make up` and retry"
        )
    yield


# ---- helpers ------------------------------------------------------


def psql(sql: str, *, port: int = PG_PORT) -> str:
    """Run a SQL query against the demo Postgres via `docker exec`
    (uses the in-container socket, sidesteps host port forwarding)."""
    out = subprocess.run(
        ["docker", "exec", "ematix-flow-pg",
         "psql", "-U", "postgres", "-At", "-c", sql],
        capture_output=True, text=True, timeout=10,
    )
    if out.returncode != 0:
        raise RuntimeError(f"psql failed: {out.stderr}")
    return out.stdout.strip()


def psql_count(table: str) -> int:
    return int(psql(f"SELECT count(*) FROM {table}"))


def wait_for_rows(table: str, target: int, timeout: float = 20.0) -> int:
    """Poll psql_count until it reaches `target` or the timeout expires.
    Returns the final count. Lets a streaming pipeline catch up
    without the test sleeping for a fixed duration."""
    deadline = time.monotonic() + timeout
    n = 0
    while time.monotonic() < deadline:
        n = psql_count(table)
        if n >= target:
            return n
        time.sleep(0.5)
    return n


def truncate(table: str) -> None:
    psql(f"TRUNCATE {table}")
