"""Phase Ω.W.2 — Postgres + MySQL real CAS lease impls.

Same lease-layer contract that Ω.W.1 pinned on SQLite/InMemory/DuckDB,
but executed against real Postgres / MySQL backends. Gated on the
same env vars the Ω.D1c / Ω.D1g tests use:

  - $EMATIX_FLOW_TEST_PG_DSN     (Postgres)
  - $EMATIX_FLOW_TEST_MYSQL_URL  (MySQL / MariaDB)

The cross-backend contract tests live alongside the W.1 set; this
file adds the concurrent-claim contention coverage that proves the
real CAS works under simultaneous workers — the whole point of
running this layer on a SQL backend.
"""

from __future__ import annotations

import os
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta

import pytest

PG_DSN = os.environ.get("EMATIX_FLOW_TEST_PG_DSN")
MYSQL_URL = os.environ.get("EMATIX_FLOW_TEST_MYSQL_URL")

try:
    import psycopg

    PSYCOPG_AVAILABLE = True
except ImportError:
    PSYCOPG_AVAILABLE = False

try:
    import pymysql  # noqa: F401

    PYMYSQL_AVAILABLE = True
except ImportError:
    PYMYSQL_AVAILABLE = False


# ---- fixtures: real Postgres + MySQL backends ----------------------


def _make_pg(tmp_path):
    from ematix_flow.run_log import PostgresRunLog

    schema = "flowtest_" + uuid.uuid4().hex[:12]
    # Pre-create the schema so PostgresRunLog DDL is idempotent.
    with psycopg.connect(PG_DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA "{schema}"')
    log = PostgresRunLog(PG_DSN, schema=schema)
    yield log
    log.close()
    with psycopg.connect(PG_DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def _make_mysql(tmp_path):
    from ematix_flow.run_log import MySQLRunLog

    prefix = f"w2_{uuid.uuid4().hex[:8]}_"
    log = MySQLRunLog(MYSQL_URL, table_prefix=prefix)
    yield log
    log.close()
    # Drop tables manually since MySQL has no schema-level cleanup.
    import pymysql

    from ematix_flow.run_log.mysql import _parse_mysql_url

    with pymysql.connect(**_parse_mysql_url(MYSQL_URL)) as conn, conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{prefix}run_log`")
        cur.execute(f"DROP TABLE IF EXISTS `{prefix}attempt_state`")
        cur.execute(f"DROP TABLE IF EXISTS `{prefix}pipeline_claims`")


_PG_PARAM = pytest.param(
    "pg",
    marks=pytest.mark.skipif(
        not (PG_DSN and PSYCOPG_AVAILABLE),
        reason="set EMATIX_FLOW_TEST_PG_DSN and install psycopg",
    ),
)
_MYSQL_PARAM = pytest.param(
    "mysql",
    marks=pytest.mark.skipif(
        not (MYSQL_URL and PYMYSQL_AVAILABLE),
        reason="set EMATIX_FLOW_TEST_MYSQL_URL and install PyMySQL",
    ),
)


@pytest.fixture(params=[_PG_PARAM, _MYSQL_PARAM])
def backend(request, tmp_path):
    factory = {"pg": _make_pg, "mysql": _make_mysql}[request.param]
    yield from factory(tmp_path)


# ---- core CAS contract (parallels the W.1 set) ---------------------


def test_claim_acquires_on_empty_store(backend):
    r = backend.claim("p1", "worker-A", lease_seconds=300)
    assert r.acquired is True
    assert r.token is not None
    assert r.holder == "worker-A"


def test_claim_busy_when_other_holds_unexpired_lease(backend):
    a = backend.claim("p1", "worker-A", lease_seconds=300)
    assert a.acquired
    b = backend.claim("p1", "worker-B", lease_seconds=300)
    assert b.acquired is False
    assert b.holder == "worker-A"
    assert b.token is None


def test_claim_after_expiry_succeeds(backend):
    backend.claim("p1", "worker-A", lease_seconds=0)
    b = backend.claim("p1", "worker-B", lease_seconds=300)
    assert b.acquired
    assert b.holder == "worker-B"


def test_heartbeat_extends_lease(backend):
    a = backend.claim("p1", "worker-A", lease_seconds=2)
    backend.heartbeat(a.token, lease_seconds=300)
    time.sleep(3)
    b = backend.claim("p1", "worker-B", lease_seconds=300)
    assert b.acquired is False
    assert b.holder == "worker-A"


def test_heartbeat_with_stale_token_is_noop(backend):
    a = backend.claim("p1", "worker-A", lease_seconds=0)
    b = backend.claim("p1", "worker-B", lease_seconds=300)
    assert b.acquired
    backend.heartbeat(a.token, lease_seconds=999)
    c = backend.claim("p1", "worker-C", lease_seconds=300)
    assert c.acquired is False
    assert c.holder == "worker-B"


def test_release_frees_claim(backend):
    a = backend.claim("p1", "worker-A", lease_seconds=300)
    backend.release(a.token)
    b = backend.claim("p1", "worker-B", lease_seconds=300)
    assert b.acquired


def test_release_with_stale_token_is_noop(backend):
    a = backend.claim("p1", "worker-A", lease_seconds=0)
    b = backend.claim("p1", "worker-B", lease_seconds=300)
    assert b.acquired
    backend.release(a.token)
    c = backend.claim("p1", "worker-C", lease_seconds=300)
    assert c.acquired is False
    assert c.holder == "worker-B"


def test_sweep_returns_only_expired(backend):
    backend.claim("p_expired", "worker-A", lease_seconds=0)
    backend.claim("p_fresh", "worker-B", lease_seconds=600)
    now = datetime.now(UTC) + timedelta(seconds=1)
    expired = backend.sweep_expired_leases(now)
    names = {e.pipeline for e in expired}
    assert "p_expired" in names
    assert "p_fresh" not in names


# ---- contention coverage (the whole point of SQL CAS) --------------
#
# Production dispatch is multi-process: each worker has its own DB
# connection. We model that with one fresh backend instance per
# thread (psycopg connections are mostly thread-safe for sequential
# use, but pymysql is not). Each thread targets the same schema /
# table_prefix so they race for the same row.


def _backend_factory_for(request):
    """Return a zero-arg callable that builds a fresh backend
    pointing at the shared per-test schema/prefix the `backend`
    fixture already created."""
    param = request.node.callspec.params["backend"]
    if param == "pg":
        from ematix_flow.run_log import PostgresRunLog

        # Pull the schema from the fixture's already-built instance.
        schema = request.getfixturevalue("backend")._schema
        return lambda: PostgresRunLog(PG_DSN, schema=schema, create_tables=False)
    if param == "mysql":
        from ematix_flow.run_log import MySQLRunLog

        prefix = request.getfixturevalue("backend")._prefix
        return lambda: MySQLRunLog(
            MYSQL_URL, table_prefix=prefix, create_tables=False
        )
    raise ValueError(f"unknown backend {param!r}")


def test_concurrent_claim_only_one_wins(backend, request):
    """Twelve workers race on the same pipeline. Exactly one should
    end up with an `acquired` token; everyone else should see `busy`.

    This is the real test for the CAS implementation — if the
    Postgres/MySQL conditional upsert is wrong, you'd see two
    `acquired` results and the scheduler would dispatch the same
    pipeline twice.
    """
    make_backend = _backend_factory_for(request)
    n_workers = 12
    results = [None] * n_workers
    start_gate = threading.Event()

    def worker(i: int):
        b = make_backend()
        try:
            start_gate.wait()
            results[i] = b.claim("p_race", f"worker-{i}", lease_seconds=300)
        finally:
            b.close()

    threads = [
        threading.Thread(target=worker, args=(i,)) for i in range(n_workers)
    ]
    for t in threads:
        t.start()
    start_gate.set()  # release the herd
    for t in threads:
        t.join()

    acquired = [r for r in results if r.acquired]
    busy = [r for r in results if not r.acquired]
    assert len(acquired) == 1, f"expected exactly one winner, got {len(acquired)}"
    assert len(busy) == n_workers - 1
    winner = acquired[0]
    # All busy responses should agree on who holds the claim.
    assert all(b.holder == winner.holder for b in busy)


def test_concurrent_claim_after_expiry(backend, request):
    """First worker gets an immediately-expiring lease; then 8 workers
    race for the now-stale slot. Exactly one of those 8 should win."""
    backend.claim("p_race", "worker-original", lease_seconds=0)
    # Wall-clock gap so the truncated-to-second `claimed_at` of the
    # new claims is strictly after the original lease's truncated
    # `expires_at`.
    time.sleep(1)

    make_backend = _backend_factory_for(request)
    n_workers = 8
    results = [None] * n_workers
    start_gate = threading.Event()

    def worker(i: int):
        b = make_backend()
        try:
            start_gate.wait()
            results[i] = b.claim("p_race", f"worker-{i}", lease_seconds=300)
        finally:
            b.close()

    threads = [
        threading.Thread(target=worker, args=(i,)) for i in range(n_workers)
    ]
    for t in threads:
        t.start()
    start_gate.set()
    for t in threads:
        t.join()

    acquired = [r for r in results if r.acquired]
    assert len(acquired) == 1


def test_concurrent_claims_across_pipelines(backend, request):
    """Different pipelines must not contend with each other — workers
    claiming p1, p2, p3 in parallel should all succeed."""
    make_backend = _backend_factory_for(request)
    n_pipelines = 6
    results = [None] * n_pipelines
    start_gate = threading.Event()

    def worker(i: int):
        b = make_backend()
        try:
            start_gate.wait()
            results[i] = b.claim(f"p_{i}", f"worker-{i}", lease_seconds=300)
        finally:
            b.close()

    threads = [
        threading.Thread(target=worker, args=(i,)) for i in range(n_pipelines)
    ]
    for t in threads:
        t.start()
    start_gate.set()
    for t in threads:
        t.join()

    assert all(r.acquired for r in results)
    tokens = {r.token for r in results}
    assert len(tokens) == n_pipelines
