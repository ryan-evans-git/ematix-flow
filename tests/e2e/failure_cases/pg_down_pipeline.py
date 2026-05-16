"""Test fixture: a pipeline that targets a Postgres instance which is
not reachable (port 5499 has no listener in the demo stack).

Mimics the "Postgres is down" scenario without having to actually
stop the container mid-test (which would race with concurrent
fixtures). The connection attempt fails with a socket error inside
the worker subprocess; the scheduler records the failure and retries
per `retry={...}`. After `max_attempts` it must mark the pipeline
`gave_up` and stop dispatching — and the scheduler itself must stay
alive across all of this.
"""

from __future__ import annotations

import psycopg2

from ematix_flow.pipeline import register

# Deliberately unreachable: no service binds 5499 in docker-compose.yml.
DEAD_PG_URL = "postgres://postgres:postgres@127.0.0.1:5499/postgres"


@register(
    name="pg_writer_against_down_db",
    schedule="* * * * *",
    retry={"max_attempts": 2, "backoff": "fixed", "base_secs": 1},
)
def pg_writer_against_down_db() -> dict:
    """Try to open a connection to a Postgres URL with no listener.
    psycopg2 raises `OperationalError` after the TCP refusal; the
    worker propagates that as a failed run."""
    # Short connect timeout so each attempt fails fast — the test has
    # a 60s budget and we want at least 2 attempts inside it.
    conn = psycopg2.connect(DEAD_PG_URL, connect_timeout=2)
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
    conn.close()
    return {"ok": True}
