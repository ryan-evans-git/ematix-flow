"""Phase Ω.D1g — MySQLRunLog backend.

Tests require a running MySQL / MariaDB. Set
$EMATIX_FLOW_TEST_MYSQL_URL (e.g. `mysql://root@localhost/flowtest`)
to opt in. Without it, the tests skip — there's no point pulling
PyMySQL or a docker fixture into CI just to exercise an idle backend.

Each test uses a unique `table_prefix` so a shared dev DB doesn't
accumulate cross-test bleed.
"""

from __future__ import annotations

import datetime as _dt
import os
import uuid

import pytest

from ematix_flow import pipeline as p


MYSQL_URL = os.environ.get("EMATIX_FLOW_TEST_MYSQL_URL")
PYMYSQL_AVAILABLE = False
try:
    import pymysql  # noqa: F401
    PYMYSQL_AVAILABLE = True
except ImportError:
    PYMYSQL_AVAILABLE = False


needs_mysql = pytest.mark.skipif(
    not (MYSQL_URL and PYMYSQL_AVAILABLE),
    reason="set EMATIX_FLOW_TEST_MYSQL_URL and install PyMySQL to run these tests",
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
def mysql_prefix():
    """A unique table_prefix per test for isolation, with cleanup."""
    name = "flowtest_" + uuid.uuid4().hex[:12] + "_"
    yield name
    import pymysql
    from ematix_flow.run_log.mysql import _parse_mysql_url

    kwargs = _parse_mysql_url(MYSQL_URL)
    kwargs.setdefault("autocommit", True)
    with pymysql.connect(**kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{name}run_log`")
            cur.execute(f"DROP TABLE IF EXISTS `{name}attempt_state`")


@needs_mysql
def test_protocol_check(mysql_prefix):
    from ematix_flow.run_log import MySQLRunLog, RunLog

    log = MySQLRunLog(url=MYSQL_URL, table_prefix=mysql_prefix)
    try:
        assert isinstance(log, RunLog)
    finally:
        log.close()


@needs_mysql
def test_round_trip(mysql_prefix):
    from ematix_flow.run_log import MySQLRunLog

    log = MySQLRunLog(url=MYSQL_URL, table_prefix=mysql_prefix)
    try:
        ts = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.timezone.utc)
        log.record_run("alpha", ts, success=True)
        log.record_attempt(
            "flaky",
            p.AttemptState(attempt_count=2, last_attempt_at=ts, gave_up=False),
        )

        p._LAST_RUN.clear()
        p._ATTEMPT_STATE.clear()
        MySQLRunLog(url=MYSQL_URL, table_prefix=mysql_prefix).restore_into_process()
        assert p._LAST_RUN["alpha"] == (ts, True)
        assert p._ATTEMPT_STATE["flaky"].attempt_count == 2
        assert p._ATTEMPT_STATE["flaky"].gave_up is False
    finally:
        log.close()


@needs_mysql
def test_clear_attempt(mysql_prefix):
    from ematix_flow.run_log import MySQLRunLog

    log = MySQLRunLog(url=MYSQL_URL, table_prefix=mysql_prefix)
    try:
        ts = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.timezone.utc)
        log.record_attempt("flaky", p.AttemptState(1, ts, False))
        log.clear_attempt_state("flaky")

        p._ATTEMPT_STATE.clear()
        log.restore_into_process()
        assert "flaky" not in p._ATTEMPT_STATE
    finally:
        log.close()


@needs_mysql
def test_run_due_writes_through(mysql_prefix):
    from ematix_flow.run_log import MySQLRunLog

    @p.register(
        name="fail",
        schedule="@hourly",
        retry={"max_attempts": 3, "backoff": "fixed", "base_secs": 30},
    )
    def _fail():
        raise RuntimeError("boom")

    log = MySQLRunLog(url=MYSQL_URL, table_prefix=mysql_prefix)
    try:
        t = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.timezone.utc)
        p.run_due_with_dag(["fail"], now=t, run_log=log)

        p._LAST_RUN.clear()
        p._ATTEMPT_STATE.clear()
        MySQLRunLog(url=MYSQL_URL, table_prefix=mysql_prefix).restore_into_process()
        assert p._LAST_RUN["fail"][1] is False
        assert p._ATTEMPT_STATE["fail"].attempt_count == 1
    finally:
        log.close()


def test_url_parser():
    """Smoke-test the URL parser regardless of whether MySQL is up."""
    from ematix_flow.run_log.mysql import _parse_mysql_url

    out = _parse_mysql_url("mysql://u:p@h:3307/db")
    assert out["host"] == "h"
    assert out["port"] == 3307
    assert out["user"] == "u"
    assert out["password"] == "p"
    assert out["database"] == "db"

    # mariadb:// scheme is accepted.
    out2 = _parse_mysql_url("mariadb://h/db")
    assert out2["host"] == "h"
    assert out2["database"] == "db"

    # Wrong scheme raises.
    with pytest.raises(ValueError):
        _parse_mysql_url("postgresql://h/db")
