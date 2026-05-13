"""Phase Ω.D1e — AzureBlobRunLog backend.

Tests use a hand-rolled in-process mock of the azure-storage-blob
ContainerClient (the SDK's test fakes are heavyweight; Azurite is a
docker fixture). The mock has just the surface AzureBlobRunLog calls:
get_blob_client / list_blobs / download_blob / close.

If `azure-storage-blob` isn't installed, the tests still run — they
construct AzureBlobRunLog with a mock `container_client=`, which
bypasses the SDK entirely.
"""

from __future__ import annotations

import datetime as _dt
from typing import Iterator

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


class _BlobItem:
    """Minimal stand-in for `azure.storage.blob.BlobProperties`."""

    def __init__(self, name: str):
        self.name = name


class _StreamingBlob:
    """Stand-in for the value returned by `download_blob`."""

    def __init__(self, data: bytes):
        self._data = data

    def readall(self) -> bytes:
        return self._data


class _BlobClient:
    """Stand-in for `azure.storage.blob.BlobClient`."""

    def __init__(self, store: dict[str, bytes], key: str):
        self._store = store
        self._key = key

    def upload_blob(self, data: bytes, *, overwrite: bool = False):
        if not overwrite and self._key in self._store:
            raise RuntimeError("blob exists")
        self._store[self._key] = data

    def delete_blob(self):
        try:
            del self._store[self._key]
        except KeyError as e:
            # Mimic the SDK's exception name so AzureBlobRunLog's
            # swallow-by-name path is exercised.
            err = type("ResourceNotFoundError", (Exception,), {})(str(e))
            raise err


class _ContainerClient:
    """Stand-in for `azure.storage.blob.ContainerClient`."""

    def __init__(self):
        self._store: dict[str, bytes] = {}

    def get_blob_client(self, key: str) -> _BlobClient:
        return _BlobClient(self._store, key)

    def list_blobs(self, *, name_starts_with: str = "") -> Iterator[_BlobItem]:
        return iter(
            _BlobItem(k) for k in sorted(self._store)
            if k.startswith(name_starts_with)
        )

    def download_blob(self, key: str) -> _StreamingBlob:
        return _StreamingBlob(self._store[key])

    def close(self):
        pass


@pytest.fixture
def mock_container():
    return _ContainerClient()


def test_protocol_check(mock_container):
    from ematix_flow.run_log import AzureBlobRunLog, RunLog

    log = AzureBlobRunLog(container="ignored", container_client=mock_container)
    assert isinstance(log, RunLog)


def test_round_trip(mock_container):
    from ematix_flow.run_log import AzureBlobRunLog

    log = AzureBlobRunLog(
        container="ignored", prefix="flow/", container_client=mock_container
    )
    ts = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.timezone.utc)
    log.record_run("alpha", ts, success=True)
    log.record_attempt(
        "flaky",
        p.AttemptState(attempt_count=2, last_attempt_at=ts, gave_up=False),
    )

    p._LAST_RUN.clear()
    p._ATTEMPT_STATE.clear()
    AzureBlobRunLog(
        container="ignored", prefix="flow/", container_client=mock_container
    ).restore_into_process()
    assert p._LAST_RUN["alpha"] == (ts, True)
    assert p._ATTEMPT_STATE["flaky"].attempt_count == 2


def test_clear_attempt_idempotent(mock_container):
    from ematix_flow.run_log import AzureBlobRunLog

    log = AzureBlobRunLog(container="ignored", container_client=mock_container)
    log.clear_attempt_state("nonexistent")  # must not raise

    ts = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.timezone.utc)
    log.record_attempt("flaky", p.AttemptState(1, ts, False))
    log.clear_attempt_state("flaky")
    p._ATTEMPT_STATE.clear()
    log.restore_into_process()
    assert "flaky" not in p._ATTEMPT_STATE


def test_run_due_writes_through(mock_container):
    from ematix_flow.run_log import AzureBlobRunLog

    @p.register(
        name="fail",
        schedule="@hourly",
        retry={"max_attempts": 3, "backoff": "fixed", "base_secs": 30},
    )
    def _fail():
        raise RuntimeError("boom")

    log = AzureBlobRunLog(container="ignored", container_client=mock_container)
    t = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.timezone.utc)
    p.run_due_with_dag(["fail"], now=t, run_log=log)

    p._LAST_RUN.clear()
    p._ATTEMPT_STATE.clear()
    AzureBlobRunLog(
        container="ignored", container_client=mock_container
    ).restore_into_process()
    assert p._LAST_RUN["fail"][1] is False
    assert p._ATTEMPT_STATE["fail"].attempt_count == 1


def test_requires_account_url_or_container_client():
    """Without either parameter and without azure-storage-blob
    installed, AzureBlobRunLog raises a helpful error rather than
    a cryptic AttributeError on a missing module."""
    from ematix_flow.run_log import AzureBlobRunLog

    with pytest.raises((ImportError, ValueError)) as ei:
        AzureBlobRunLog(container="my-container")
    msg = str(ei.value).lower()
    assert "azure" in msg or "account_url" in msg or "container_client" in msg
