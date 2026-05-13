"""Phase Ω.D1d — S3RunLog backend.

Tests use `moto` (in-process AWS mock) so they run without real AWS
credentials. Skipped if moto isn't installed.
"""

from __future__ import annotations

import datetime as _dt
import pytest

from ematix_flow import pipeline as p

# moto + boto3 are optional dev deps. The S3RunLog test suite is
# gated on both being importable.
moto = pytest.importorskip("moto")
boto3 = pytest.importorskip("boto3")
from moto import mock_aws  # noqa: E402


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
def s3_bucket():
    """A fresh, in-process moto S3 bucket for each test."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        bucket = "ematix-flow-test"
        client.create_bucket(Bucket=bucket)
        yield bucket, client


def test_protocol_check(s3_bucket):
    from ematix_flow.run_log import RunLog, S3RunLog

    bucket, client = s3_bucket
    log = S3RunLog(bucket, prefix="flow/", client=client)
    assert isinstance(log, RunLog)


def test_round_trip(s3_bucket):
    from ematix_flow.run_log import S3RunLog

    bucket, client = s3_bucket
    log = S3RunLog(bucket, prefix="flow/", client=client)
    ts = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.timezone.utc)
    log.record_run("alpha", ts, success=True)
    log.record_attempt(
        "flaky",
        p.AttemptState(attempt_count=2, last_attempt_at=ts, gave_up=False),
    )

    # Fresh in-memory state + fresh S3RunLog instance against the same
    # bucket = the cross-tick scenario.
    p._LAST_RUN.clear()
    p._ATTEMPT_STATE.clear()
    S3RunLog(bucket, prefix="flow/", client=client).restore_into_process()
    assert p._LAST_RUN["alpha"] == (ts, True)
    assert p._ATTEMPT_STATE["flaky"].attempt_count == 2


def test_clear_attempt_idempotent(s3_bucket):
    from ematix_flow.run_log import S3RunLog

    bucket, client = s3_bucket
    log = S3RunLog(bucket, prefix="flow/", client=client)
    # clear_attempt_state on a non-existent key must not raise.
    log.clear_attempt_state("nonexistent")

    ts = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.timezone.utc)
    log.record_attempt("flaky", p.AttemptState(1, ts, False))
    log.clear_attempt_state("flaky")

    p._ATTEMPT_STATE.clear()
    log.restore_into_process()
    assert "flaky" not in p._ATTEMPT_STATE


def test_run_due_writes_through(s3_bucket):
    from ematix_flow.run_log import S3RunLog

    @p.register(
        name="fail",
        schedule="@hourly",
        retry={"max_attempts": 3, "backoff": "fixed", "base_secs": 30},
    )
    def _fail():
        raise RuntimeError("boom")

    bucket, client = s3_bucket
    log = S3RunLog(bucket, prefix="flow/", client=client)
    t = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.timezone.utc)
    p.run_due_with_dag(["fail"], now=t, run_log=log)

    p._LAST_RUN.clear()
    p._ATTEMPT_STATE.clear()
    S3RunLog(bucket, prefix="flow/", client=client).restore_into_process()
    assert p._LAST_RUN["fail"][1] is False
    assert p._ATTEMPT_STATE["fail"].attempt_count == 1


def test_prefix_isolation(s3_bucket):
    """Two RunLogs writing under different prefixes don't see each
    other — important when sharing a bucket across environments
    (e.g. prod/ vs staging/)."""
    from ematix_flow.run_log import S3RunLog

    bucket, client = s3_bucket
    prod = S3RunLog(bucket, prefix="prod/", client=client)
    stg = S3RunLog(bucket, prefix="staging/", client=client)

    ts = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.timezone.utc)
    prod.record_run("alpha", ts, success=True)
    stg.record_run("alpha", ts, success=False)

    # Restore via prod → success=True.
    p._LAST_RUN.clear()
    prod.restore_into_process()
    assert p._LAST_RUN["alpha"][1] is True

    # Restore via staging → success=False.
    p._LAST_RUN.clear()
    stg.restore_into_process()
    assert p._LAST_RUN["alpha"][1] is False
