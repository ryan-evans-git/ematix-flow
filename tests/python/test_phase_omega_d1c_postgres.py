"""Phase Ω.D1c — PostgresRunLog backend.

Tests require a running PostgreSQL. Set $EMATIX_FLOW_TEST_PG_DSN to
opt in (e.g. `postgresql://localhost/flowtest`). Without the env var
the tests skip — there's no point pulling psycopg or a docker fixture
into CI just to run a backend nobody is using yet.

Each test uses a unique schema (per-test) so a shared dev DB doesn't
accumulate cross-test bleed.
"""

from __future__ import annotations

import datetime as _dt
import os
import uuid

import pytest

from ematix_flow import pipeline as p


PG_DSN = os.environ.get("EMATIX_FLOW_TEST_PG_DSN")
PSYCOPG_AVAILABLE = False
try:
    import psycopg  # noqa: F401
    PSYCOPG_AVAILABLE = True
except ImportError:
    PSYCOPG_AVAILABLE = False


pytestmark = pytest.mark.skipif(
    not (PG_DSN and PSYCOPG_AVAILABLE),
    reason="set EMATIX_FLOW_TEST_PG_DSN and install psycopg to run these tests",
)


_SIDE_TABLES = (
    "_REGISTRY",
    "_DEPENDS_ON",
    "_UPSTREAM_FRESHNESS",
    "_LAST_RUN",
    "_RETRY_POLICY",
    "_ATTEMPT_STATE",
)


@pytest.fixture(autouse=True)
def _clean_registry():
    for tbl in _SIDE_TABLES:
        d = getattr(p, tbl, None)
        if d is not None:
            d.clear()
    yield
    for tbl in _SIDE_TABLES:
        d = getattr(p, tbl, None)
        if d is not None:
            d.clear()


@pytest.fixture
def pg_schema():
    """A throwaway Postgres schema for each test, cleaned up after."""
    name = "flowtest_" + uuid.uuid4().hex[:12]
    import psycopg

    with psycopg.connect(PG_DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{name}"')
    yield name
    with psycopg.connect(PG_DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA "{name}" CASCADE')


def test_protocol_check(pg_schema):
    from ematix_flow.run_log import PostgresRunLog, RunLog

    log = PostgresRunLog(PG_DSN, schema=pg_schema)
    try:
        assert isinstance(log, RunLog)
    finally:
        log.close()


def test_round_trip(pg_schema):
    from ematix_flow.run_log import PostgresRunLog

    log = PostgresRunLog(PG_DSN, schema=pg_schema)
    try:
        ts = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.timezone.utc)
        log.record_run("alpha", ts, success=True)
        log.record_attempt(
            "flaky",
            p.AttemptState(attempt_count=2, last_attempt_at=ts, gave_up=False),
        )

        p._LAST_RUN.clear()
        p._ATTEMPT_STATE.clear()
        PostgresRunLog(PG_DSN, schema=pg_schema).restore_into_process()

        assert p._LAST_RUN["alpha"] == (ts, True)
        assert p._ATTEMPT_STATE["flaky"].attempt_count == 2
        assert p._ATTEMPT_STATE["flaky"].gave_up is False
    finally:
        log.close()


def test_clear_attempt(pg_schema):
    from ematix_flow.run_log import PostgresRunLog

    log = PostgresRunLog(PG_DSN, schema=pg_schema)
    try:
        ts = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.timezone.utc)
        log.record_attempt("flaky", p.AttemptState(1, ts, False))
        log.clear_attempt_state("flaky")

        p._ATTEMPT_STATE.clear()
        log.restore_into_process()
        assert "flaky" not in p._ATTEMPT_STATE
    finally:
        log.close()


def test_run_due_writes_through(pg_schema):
    from ematix_flow.run_log import PostgresRunLog

    @p.register(
        name="fail",
        schedule="@hourly",
        retry={"max_attempts": 3, "backoff": "fixed", "base_secs": 30},
    )
    def _fail():
        raise RuntimeError("boom")

    log = PostgresRunLog(PG_DSN, schema=pg_schema)
    try:
        t = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.timezone.utc)
        p.run_due_with_dag(["fail"], now=t, run_log=log)

        p._LAST_RUN.clear()
        p._ATTEMPT_STATE.clear()
        PostgresRunLog(PG_DSN, schema=pg_schema).restore_into_process()
        assert p._LAST_RUN["fail"][1] is False
        assert p._ATTEMPT_STATE["fail"].attempt_count == 1
    finally:
        log.close()
