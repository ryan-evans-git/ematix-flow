"""Phase Ω.W.1 — lease semantics on the RunLog Protocol.

Contract tests for `claim` / `heartbeat` / `release` /
`sweep_expired_leases`. Parametrized across every backend that
implements the lease layer:

  - InMemoryRunLog (always available)
  - SqliteRunLog
  - DuckDBRunLog (skipped when the `duckdb` extra isn't installed)

Postgres / MySQL get real CAS impls in Ω.W.2; their fixtures are
gated on `EMATIX_FLOW_TEST_*` env vars per the existing
test_phase_omega_d1{c,g}_*.py pattern. Blob backends (S3 / Azure /
GCS) explicitly raise `NotImplementedError` — that's the only
contract this phase needs from them.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from ematix_flow.run_log import InMemoryRunLog, SqliteRunLog
from ematix_flow.run_log.protocol import ClaimResult, ExpiredClaim

# ---- fixtures: every single-process backend ------------------------


def _make_inmemory(tmp_path):
    log = InMemoryRunLog()
    yield log
    log.close()


def _make_sqlite(tmp_path):
    log = SqliteRunLog(str(tmp_path / "run.db"))
    yield log
    log.close()


def _make_duckdb(tmp_path):
    pytest.importorskip("duckdb", reason="DuckDBRunLog requires the duckdb extra")
    from ematix_flow.run_log import DuckDBRunLog

    log = DuckDBRunLog(str(tmp_path / "run.duckdb"))
    yield log
    log.close()


@pytest.fixture(params=["inmemory", "sqlite", "duckdb"])
def backend(request, tmp_path):
    factory = {
        "inmemory": _make_inmemory,
        "sqlite": _make_sqlite,
        "duckdb": _make_duckdb,
    }[request.param]
    yield from factory(tmp_path)


# ---- claim ----------------------------------------------------------


def test_claim_acquires_on_empty_store(backend):
    """First worker to claim a pipeline gets the lease."""
    result = backend.claim("p1", "worker-A", lease_seconds=300)
    assert result.acquired is True
    assert result.token is not None
    assert result.holder == "worker-A"
    # Lease should be ~now + 300s.
    delta = result.expires_at - datetime.now(UTC)
    assert timedelta(seconds=290) <= delta <= timedelta(seconds=310)


def test_claim_busy_when_other_holds_unexpired_lease(backend):
    """Second worker to claim the same pipeline sees the existing
    holder and does NOT get a token."""
    a = backend.claim("p1", "worker-A", lease_seconds=300)
    assert a.acquired

    b = backend.claim("p1", "worker-B", lease_seconds=300)
    assert b.acquired is False
    assert b.token is None
    assert b.holder == "worker-A"
    # busy returns the OLD lease's expiry so the second worker knows
    # when to retry.
    assert b.expires_at == a.expires_at


def test_claim_after_expiry_succeeds(backend):
    """Once a lease expires, another worker can take it."""
    backend.claim("p1", "worker-A", lease_seconds=0)
    # Sleep-free: lease_seconds=0 means expires_at == claimed_at,
    # which is already <= "now" by the next instruction.
    b = backend.claim("p1", "worker-B", lease_seconds=300)
    assert b.acquired
    assert b.holder == "worker-B"


def test_claim_different_pipelines_are_independent(backend):
    """Per-pipeline scoping — claiming p1 doesn't lock out p2."""
    a = backend.claim("p1", "worker-A", lease_seconds=300)
    b = backend.claim("p2", "worker-B", lease_seconds=300)
    assert a.acquired and b.acquired
    assert a.token != b.token


# ---- heartbeat ------------------------------------------------------


def test_heartbeat_extends_lease(backend):
    """Heartbeating before expiry keeps the claim warm."""
    a = backend.claim("p1", "worker-A", lease_seconds=2)
    backend.heartbeat(a.token, lease_seconds=300)
    # Even after the original 2-second window, another worker still
    # can't claim because the heartbeat pushed expiry out.
    import time
    time.sleep(3)
    b = backend.claim("p1", "worker-B", lease_seconds=300)
    assert b.acquired is False
    assert b.holder == "worker-A"


def test_heartbeat_with_stale_token_is_noop(backend):
    """A worker that lost its lease and tries to heartbeat shouldn't
    crash or clobber the new holder."""
    a = backend.claim("p1", "worker-A", lease_seconds=0)
    b = backend.claim("p1", "worker-B", lease_seconds=300)
    assert b.acquired

    # worker-A's token is stale; heartbeat must not extend B's lease
    # to A's worker_id or anything weird.
    backend.heartbeat(a.token, lease_seconds=999)

    # B still holds.
    c = backend.claim("p1", "worker-C", lease_seconds=300)
    assert c.acquired is False
    assert c.holder == "worker-B"


# ---- release --------------------------------------------------------


def test_release_frees_claim(backend):
    """Voluntary release lets the next pass claim immediately."""
    a = backend.claim("p1", "worker-A", lease_seconds=300)
    backend.release(a.token)

    b = backend.claim("p1", "worker-B", lease_seconds=300)
    assert b.acquired


def test_release_with_stale_token_is_noop(backend):
    """Releasing a stale token must not free the current holder's
    lease."""
    a = backend.claim("p1", "worker-A", lease_seconds=0)
    b = backend.claim("p1", "worker-B", lease_seconds=300)
    assert b.acquired

    # worker-A's stale release shouldn't free B's claim.
    backend.release(a.token)

    c = backend.claim("p1", "worker-C", lease_seconds=300)
    assert c.acquired is False
    assert c.holder == "worker-B"


# ---- sweep_expired_leases ------------------------------------------


def test_sweep_returns_only_expired(backend):
    """sweep_expired_leases lists claims whose lease expired before
    `now` — not unexpired ones."""
    backend.claim("p_expired", "worker-A", lease_seconds=0)
    backend.claim("p_fresh", "worker-B", lease_seconds=600)

    now = datetime.now(UTC) + timedelta(seconds=1)
    expired = backend.sweep_expired_leases(now)

    names = {e.pipeline for e in expired}
    assert "p_expired" in names
    assert "p_fresh" not in names

    [e] = [e for e in expired if e.pipeline == "p_expired"]
    assert isinstance(e, ExpiredClaim)
    assert e.worker_id == "worker-A"


def test_sweep_empty_on_no_claims(backend):
    assert backend.sweep_expired_leases(datetime.now(UTC)) == []


# ---- ClaimResult dataclass -----------------------------------------


def test_claim_result_acquired_helper():
    now = datetime.now(UTC)
    r = ClaimResult.acquired_by(token="t1", worker_id="w1", expires_at=now)
    assert r.acquired
    assert r.token == "t1"
    assert r.holder == "w1"
    assert r.expires_at == now


def test_claim_result_busy_helper():
    now = datetime.now(UTC)
    r = ClaimResult.busy(holder="w1", expires_at=now)
    assert r.acquired is False
    assert r.token is None
    assert r.holder == "w1"


# ---- not-supported backends still satisfy Protocol shape ----------


class _BlobStub:
    """Minimal fixture: subclass NoLeaseBlobBackend without doing the
    real backend's I/O setup. We only care that the lease layer
    raises with a useful message."""

    def __init__(self):
        pass


def test_blob_backend_claim_raises_not_implemented():
    """Object stores can't do CAS without an external lock service.
    Lease methods must fail loud, not silently swallow the claim."""
    from ematix_flow.run_log._no_lease import NoLeaseBlobBackend

    class StubBlob(NoLeaseBlobBackend, _BlobStub):
        pass

    stub = StubBlob()
    with pytest.raises(NotImplementedError, match="external lock service"):
        stub.claim("p1", "worker-A", lease_seconds=300)
    with pytest.raises(NotImplementedError, match="external lock service"):
        stub.heartbeat("tok", lease_seconds=300)
    with pytest.raises(NotImplementedError, match="external lock service"):
        stub.release("tok")
    with pytest.raises(NotImplementedError, match="external lock service"):
        stub.sweep_expired_leases(datetime.now(UTC))
