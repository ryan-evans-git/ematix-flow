"""Phase Ω.D1f — GcsRunLog backend.

Tests use a hand-rolled in-process mock of the
`google.cloud.storage.Bucket` interface. No real GCP credentials or
docker emulator needed; tests run even without `google-cloud-storage`
installed because the constructor accepts a pre-built bucket_client.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterator

import pytest

from ematix_flow import pipeline as p

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


class _Blob:
    """Stand-in for `google.cloud.storage.Blob`."""

    def __init__(self, store: dict[str, bytes], name: str):
        self._store = store
        self.name = name

    def upload_from_string(self, data, *, content_type: str = "text/plain"):
        if isinstance(data, str):
            data = data.encode("utf-8")
        self._store[self.name] = data

    def download_as_bytes(self) -> bytes:
        return self._store[self.name]

    def delete(self):
        try:
            del self._store[self.name]
        except KeyError as e:
            err = type("NotFound", (Exception,), {})(str(e))
            raise err from e


class _Bucket:
    """Stand-in for `google.cloud.storage.Bucket`."""

    def __init__(self):
        self._store: dict[str, bytes] = {}

    def blob(self, name: str) -> _Blob:
        return _Blob(self._store, name)

    def list_blobs(self, *, prefix: str = "") -> Iterator[_Blob]:
        return iter(
            _Blob(self._store, k) for k in sorted(self._store) if k.startswith(prefix)
        )


@pytest.fixture
def mock_bucket():
    return _Bucket()


def test_protocol_check(mock_bucket):
    from ematix_flow.run_log import GcsRunLog, RunLog

    log = GcsRunLog(bucket="ignored", bucket_client=mock_bucket)
    assert isinstance(log, RunLog)


def test_round_trip(mock_bucket):
    from ematix_flow.run_log import GcsRunLog

    log = GcsRunLog(bucket="ignored", prefix="flow/", bucket_client=mock_bucket)
    ts = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    log.record_run("alpha", ts, success=True)
    log.record_attempt(
        "flaky",
        p.AttemptState(attempt_count=2, last_attempt_at=ts, gave_up=False),
    )

    p._LAST_RUN.clear()
    p._ATTEMPT_STATE.clear()
    GcsRunLog(
        bucket="ignored", prefix="flow/", bucket_client=mock_bucket
    ).restore_into_process()
    assert p._LAST_RUN["alpha"] == (ts, True)
    assert p._ATTEMPT_STATE["flaky"].attempt_count == 2


def test_clear_attempt_idempotent(mock_bucket):
    from ematix_flow.run_log import GcsRunLog

    log = GcsRunLog(bucket="ignored", bucket_client=mock_bucket)
    log.clear_attempt_state("nonexistent")  # no error

    ts = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    log.record_attempt("flaky", p.AttemptState(1, ts, False))
    log.clear_attempt_state("flaky")
    p._ATTEMPT_STATE.clear()
    log.restore_into_process()
    assert "flaky" not in p._ATTEMPT_STATE


def test_run_due_writes_through(mock_bucket):
    from ematix_flow.run_log import GcsRunLog

    @p.register(
        name="fail",
        schedule="@hourly",
        retry={"max_attempts": 3, "backoff": "fixed", "base_secs": 30},
    )
    def _fail():
        raise RuntimeError("boom")

    log = GcsRunLog(bucket="ignored", bucket_client=mock_bucket)
    t = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    p.run_due_with_dag(["fail"], now=t, run_log=log)

    p._LAST_RUN.clear()
    p._ATTEMPT_STATE.clear()
    GcsRunLog(
        bucket="ignored", bucket_client=mock_bucket
    ).restore_into_process()
    assert p._LAST_RUN["fail"][1] is False
    assert p._ATTEMPT_STATE["fail"].attempt_count == 1


def test_prefix_isolation(mock_bucket):
    """Same bucket, different prefixes → isolated namespaces.
    Matches the S3 test of the same name."""
    from ematix_flow.run_log import GcsRunLog

    prod = GcsRunLog(bucket="ignored", prefix="prod/", bucket_client=mock_bucket)
    stg = GcsRunLog(bucket="ignored", prefix="staging/", bucket_client=mock_bucket)

    ts = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    prod.record_run("alpha", ts, success=True)
    stg.record_run("alpha", ts, success=False)

    p._LAST_RUN.clear()
    prod.restore_into_process()
    assert p._LAST_RUN["alpha"][1] is True

    p._LAST_RUN.clear()
    stg.restore_into_process()
    assert p._LAST_RUN["alpha"][1] is False
