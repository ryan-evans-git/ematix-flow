"""Phase Ω.W.3 — Executor Protocol + SubprocessExecutor.

Three layers of coverage:

  1. Protocol-shape tests: DispatchSpec / DispatchHandle dataclasses
     accept the expected kwargs and SubprocessExecutor satisfies the
     `Executor` runtime protocol.

  2. `_build_run_argv` unit tests: the CLI invocation the executor
     would produce matches the expected `flow run --module ... `
     shape, with all worker-side flags populated from the spec.

  3. End-to-end: spawn a real `python -m ematix_flow.cli run`
     subprocess pointing at a SQLite-backed RunLog. The pipeline
     records its run; the executor sees the worker exit; the
     RunLog ends up with the right outcome AND the claim row
     gets released.

The end-to-end test is the proof — if the SubprocessExecutor + the
new `flow run` worker-side flags + the heartbeat thread + the
release-on-exit all line up, the scheduler can dispatch work
fire-and-forget and read the result from the RunLog.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime

import pytest

from ematix_flow.executors import (
    DispatchError,
    DispatchHandle,
    DispatchSpec,
    Executor,
    SubprocessExecutor,
    python_subprocess_executor,
)
from ematix_flow.executors.subprocess import _SubprocessRef

# ---- DispatchSpec / DispatchHandle dataclasses --------------------


def test_dispatch_spec_minimal():
    """Just the required fields — everything else defaults."""
    spec = DispatchSpec(
        pipeline_name="p1",
        module="my_pipelines",
        claim_token="t1",
        lease_seconds=300,
        run_log_url="memory://",
    )
    assert spec.pipeline_name == "p1"
    assert spec.alerter_urls == []
    assert spec.metrics_url is None
    assert spec.env == {}


def test_dispatch_spec_full():
    spec = DispatchSpec(
        pipeline_name="p1",
        module="my_pipelines",
        claim_token="t1",
        lease_seconds=300,
        run_log_url="postgres://flow@logdb/history",
        alerter_urls=["stdout://", "slack://hooks.slack.com/services/..."],
        metrics_url="prometheus://:9100",
        env={"PIPELINE_OWNER": "team-data"},
    )
    assert len(spec.alerter_urls) == 2
    assert spec.metrics_url == "prometheus://:9100"
    assert spec.env["PIPELINE_OWNER"] == "team-data"


# ---- SubprocessExecutor.protocol shape ----------------------------


def test_subprocess_executor_satisfies_protocol():
    ex = python_subprocess_executor()
    assert isinstance(ex, Executor)


def test_subprocess_executor_no_binary_raises():
    """Without flow on PATH AND no python= override, construction
    should fail loud — not silently dispatch with `None` argv."""
    # Force the resolver to find nothing by passing flow_binary=None
    # and overriding which() to return None.
    import shutil

    real_which = shutil.which
    shutil.which = lambda name: None
    try:
        with pytest.raises(DispatchError, match="no `flow` binary"):
            SubprocessExecutor()
    finally:
        shutil.which = real_which


# ---- _build_run_argv ----------------------------------------------


def test_build_run_argv_minimal_spec():
    spec = DispatchSpec(
        pipeline_name="ingest_events",
        module="my_pipelines",
        claim_token="abc123",
        lease_seconds=300,
        run_log_url="memory://",
    )
    argv = SubprocessExecutor._build_run_argv(spec)
    assert argv[0] == "run"
    assert "--module" in argv
    assert "my_pipelines" in argv
    assert "--claim-token" in argv
    assert "abc123" in argv
    assert "--lease-seconds" in argv
    assert "300" in argv
    assert "--heartbeat-interval" in argv
    # Default heartbeat = lease // 3 = 100.
    assert "100" in argv
    assert "--run-log-url" in argv
    assert "memory://" in argv
    assert argv[-1] == "ingest_events"


def test_build_run_argv_with_alerters_and_metrics():
    spec = DispatchSpec(
        pipeline_name="p1",
        module="m",
        claim_token="t",
        lease_seconds=60,
        run_log_url="sqlite:///tmp/log.db",
        alerter_urls=["stdout://", "slack://x"],
        metrics_url="prometheus://:9100",
    )
    argv = SubprocessExecutor._build_run_argv(spec)
    # Both alerters appear as repeated --alerter flags.
    assert argv.count("--alerter") == 2
    assert "stdout://" in argv
    assert "slack://x" in argv
    assert "--metrics" in argv
    assert "prometheus://:9100" in argv
    # Heartbeat at lease // 3 = 20 (max 1).
    hb_idx = argv.index("--heartbeat-interval")
    assert argv[hb_idx + 1] == "20"


def test_build_run_argv_heartbeat_floor_is_one():
    """A 1-second lease shouldn't produce a zero-second heartbeat."""
    spec = DispatchSpec(
        pipeline_name="p",
        module="m",
        claim_token="t",
        lease_seconds=1,
        run_log_url="memory://",
    )
    argv = SubprocessExecutor._build_run_argv(spec)
    hb_idx = argv.index("--heartbeat-interval")
    assert argv[hb_idx + 1] == "1"


# ---- DispatchHandle shape ------------------------------------------


def test_dispatch_handle_carries_backend_and_ref():
    h = DispatchHandle(
        pipeline_name="p1",
        backend="subprocess",
        ref="opaque-token",
    )
    assert h.pipeline_name == "p1"
    assert h.backend == "subprocess"
    assert h.ref == "opaque-token"


# ---- end-to-end: real subprocess + SQLite run-log ------------------


@pytest.fixture
def user_module(tmp_path, monkeypatch):
    """Drop a tiny user module on the path with one trivial
    @register'd pipeline that returns success."""
    mod_path = tmp_path / "ω_w3_user.py"
    mod_path.write_text(
        "from ematix_flow.pipeline import register\n"
        "\n"
        "@register(name='hello', schedule=None)\n"
        "def hello():\n"
        "    return {'ok': True}\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    yield "ω_w3_user"


def test_subprocess_executor_end_to_end_releases_claim(user_module, tmp_path):
    """Spawn a real `flow run` worker via SubprocessExecutor and
    verify the claim is released after the pipeline completes."""
    from ematix_flow.run_log import SqliteRunLog

    log_path = str(tmp_path / "w3.db")
    run_log_url = f"sqlite://{log_path}"

    # Pre-create the claim row so the worker has something to release.
    log = SqliteRunLog(log_path)
    claim = log.claim("hello", "test-worker", lease_seconds=60)
    assert claim.acquired
    log.close()

    spec = DispatchSpec(
        pipeline_name="hello",
        module=user_module,
        claim_token=claim.token,
        lease_seconds=60,
        run_log_url=run_log_url,
        env={"PYTHONPATH": str(tmp_path)},
    )
    executor = python_subprocess_executor()
    handle = executor.dispatch(spec)
    assert handle.backend == "subprocess"
    assert isinstance(handle.ref, _SubprocessRef)

    # Wait for the worker to exit (it's a trivial pipeline; should
    # finish in a few hundred ms).
    rc = handle.ref.popen.wait(timeout=30)
    assert rc == 0, (
        f"worker exited {rc}; stderr:\n"
        f"{handle.ref.popen.stderr.read().decode(errors='replace')}"
    )

    # The release-on-exit path should have removed the claim row,
    # so a fresh claim succeeds.
    log = SqliteRunLog(log_path)
    try:
        next_claim = log.claim("hello", "next-worker", lease_seconds=60)
        assert next_claim.acquired, (
            "worker didn't release its claim; "
            f"holder={next_claim.holder}, expires_at={next_claim.expires_at}"
        )
    finally:
        log.close()


def test_subprocess_executor_cancel_terminates_worker(user_module, tmp_path):
    """cancel() on a still-running worker should SIGTERM it."""
    # Use a pipeline that sleeps so the test can race the cancel.
    sleeper = tmp_path / "ω_w3_sleeper.py"
    sleeper.write_text(
        "import time\n"
        "from ematix_flow.pipeline import register\n"
        "\n"
        "@register(name='sleeper', schedule=None)\n"
        "def sleeper():\n"
        "    time.sleep(30)\n"
        "    return {'ok': True}\n"
    )
    import sys as _sys
    _sys.path.insert(0, str(tmp_path))

    try:
        spec = DispatchSpec(
            pipeline_name="sleeper",
            module="ω_w3_sleeper",
            claim_token="dont-care",
            lease_seconds=60,
            run_log_url=f"sqlite://{tmp_path}/cancel.db",
            env={"PYTHONPATH": str(tmp_path)},
        )
        # Pre-create the claim so the worker can find it on release.
        from ematix_flow.run_log import SqliteRunLog

        log = SqliteRunLog(f"{tmp_path}/cancel.db")
        log.claim("sleeper", "test", lease_seconds=60)
        log.close()

        executor = python_subprocess_executor()
        handle = executor.dispatch(spec)
        # Let the worker boot.
        time.sleep(1)
        assert handle.ref.popen.poll() is None, "worker exited too early"
        executor.cancel(handle)
        rc = handle.ref.popen.wait(timeout=10)
        # SIGTERM produces rc != 0 on most platforms.
        assert rc != 0, "expected terminated rc, got clean exit"
    finally:
        _sys.path.remove(str(tmp_path))


# ---- heartbeat thread ----------------------------------------------


def test_heartbeat_thread_calls_run_log_periodically():
    """The thread should call run_log.heartbeat(token, lease) on each
    interval, and stop cleanly when stop() is set."""
    from ematix_flow.executors.heartbeat import HeartbeatThread

    calls = []

    class FakeRunLog:
        def heartbeat(self, token, lease_seconds):
            calls.append((token, lease_seconds, datetime.now(UTC)))

    hb = HeartbeatThread(
        FakeRunLog(),
        "tok-1",
        lease_seconds=10,
        interval_seconds=1,
    )
    hb.start()
    time.sleep(2.5)
    hb.stop()
    # Expect 2 calls in ~2.5s with 1s interval.
    assert 1 <= len(calls) <= 3, f"got {len(calls)} heartbeats"
    assert all(c[0] == "tok-1" for c in calls)
    assert all(c[1] == 10 for c in calls)


def test_heartbeat_thread_swallows_run_log_errors():
    """A flaky RunLog (transient network blip) shouldn't crash the
    worker — heartbeat exceptions are logged + swallowed."""
    from ematix_flow.executors.heartbeat import HeartbeatThread

    class FlakeyRunLog:
        def heartbeat(self, token, lease_seconds):
            raise RuntimeError("boom")

    hb = HeartbeatThread(
        FlakeyRunLog(),
        "tok-1",
        lease_seconds=10,
        interval_seconds=1,
    )
    hb.start()
    time.sleep(2)
    hb.stop()
    # Survival is the only assertion — no exception bubbled up.


def test_heartbeat_stop_is_idempotent():
    from ematix_flow.executors.heartbeat import HeartbeatThread

    class NoopRunLog:
        def heartbeat(self, token, lease_seconds):
            pass

    hb = HeartbeatThread(
        NoopRunLog(), "tok", lease_seconds=10, interval_seconds=1
    )
    hb.stop()  # no start
    hb.start()
    hb.stop()
    hb.stop()  # second stop is safe


def test_heartbeat_start_is_idempotent():
    from ematix_flow.executors.heartbeat import HeartbeatThread

    class NoopRunLog:
        def heartbeat(self, token, lease_seconds):
            pass

    hb = HeartbeatThread(
        NoopRunLog(), "tok", lease_seconds=10, interval_seconds=1
    )
    hb.start()
    hb.start()  # second start is a no-op (won't spawn a second thread)
    hb.stop()
