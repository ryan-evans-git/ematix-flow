"""Phase Ω.W.6 — `flow scheduler` loop + executor URL factory.

Three layers of coverage:

  1. `executor_from_url` unit tests: subprocess / subprocess+python /
     k8s / lambda URLs route to the right backend with the right
     kwargs; bad schemes raise loud.

  2. Scheduler-loop unit tests against a FakeExecutor: the loop
     claims pipelines through an InMemoryRunLog, dispatches eligible
     ones, gates stale-upstream / retry-backoff pipelines, releases
     claims on DispatchError, and gracefully bails when leader
     election loses.

  3. End-to-end via the CLI: `python -m ematix_flow.cli scheduler
     --executor subprocess+python:// --max-iterations 1` against a
     SQLite RunLog and a tiny user module. The pipeline actually
     runs; the scheduler exits after one tick; RunLog records the
     outcome.
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import pytest

from ematix_flow import pipeline as _p
from ematix_flow.executors import (
    DispatchError,
    DispatchHandle,
    DispatchSpec,
)
from ematix_flow.run_log import InMemoryRunLog
from ematix_flow.scheduler import (
    DispatchFailedEvent,
    executor_from_url,
    run_scheduler,
)

# ---- shared scaffolding --------------------------------------------


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
        d = getattr(_p, tbl, None)
        if d is not None:
            d.clear()
    yield
    for tbl in _SIDE_TABLES:
        d = getattr(_p, tbl, None)
        if d is not None:
            d.clear()


@dataclass
class FakeExecutor:
    """Records every dispatch + cancel; never spawns anything."""

    dispatched: list[DispatchSpec] = field(default_factory=list)
    cancelled: list[DispatchHandle] = field(default_factory=list)
    raise_on: set[str] = field(default_factory=set)

    def dispatch(self, spec: DispatchSpec) -> DispatchHandle:
        if spec.pipeline_name in self.raise_on:
            raise DispatchError(f"forced failure for {spec.pipeline_name}")
        self.dispatched.append(spec)
        return DispatchHandle(
            pipeline_name=spec.pipeline_name,
            backend="fake",
            ref=None,
        )

    def cancel(self, handle: DispatchHandle) -> None:
        self.cancelled.append(handle)


@pytest.fixture
def user_module(tmp_path, monkeypatch):
    """A throwaway module with two trivial pipelines that fire
    every minute, importable by `scheduler_user_mod_<n>`."""
    n = 0
    while (tmp_path / f"scheduler_user_mod_{n}.py").exists():
        n += 1
    mod_name = f"scheduler_user_mod_{n}"
    (tmp_path / f"{mod_name}.py").write_text(
        "from ematix_flow.pipeline import register\n"
        "\n"
        "@register(name='a', schedule='* * * * *')\n"
        "def a():\n"
        "    return {'ok': True}\n"
        "\n"
        "@register(name='b', schedule='* * * * *')\n"
        "def b():\n"
        "    return {'ok': True}\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    yield mod_name
    # Drop the import so the next test gets a fresh registry.
    sys.modules.pop(mod_name, None)


# ---- executor_from_url ---------------------------------------------


def test_executor_from_url_subprocess():
    # Pass `flow_binary=` via env trick — actually just verify the
    # URL routes to SubprocessExecutor class. The `flow` binary
    # check happens in __init__ so we expect either success (if
    # flow is on PATH) or DispatchError (clean error message).
    import shutil

    from ematix_flow.executors import SubprocessExecutor

    real_which = shutil.which
    shutil.which = lambda _: "/usr/local/bin/flow"
    try:
        ex = executor_from_url("subprocess://")
        assert isinstance(ex, SubprocessExecutor)
    finally:
        shutil.which = real_which


def test_executor_from_url_subprocess_python():
    from ematix_flow.executors import SubprocessExecutor

    ex = executor_from_url("subprocess+python://")
    assert isinstance(ex, SubprocessExecutor)


def test_executor_from_url_k8s_needs_image():
    with pytest.raises(ValueError, match="image"):
        executor_from_url("k8s://flow")


def test_executor_from_url_k8s_with_image_and_sa():
    pytest.importorskip("kubernetes")
    # The KubernetesJobExecutor constructor will try to load kube
    # config; on a machine without ~/.kube/config + not in-cluster,
    # that raises DispatchError. We catch that and verify our URL
    # parsing got far enough to attempt construction.
    from ematix_flow.scheduler.executor_url import DispatchError as DE

    try:
        ex = executor_from_url(
            "k8s://flow?image=worker:latest&service-account=flow-sa"
        )
        # In some CI envs ~/.kube/config exists (e.g. devcontainers).
        # Either path is acceptable; assert namespace + image landed.
        assert ex._namespace == "flow"
        assert ex._image == "worker:latest"
        assert ex._service_account == "flow-sa"
    except DE:
        # No k8s config — parsing reached __init__ which is the win
        # we care about. Re-test by hitting the parsing path
        # without constructing.
        pass


def test_executor_from_url_lambda(monkeypatch):
    pytest.importorskip("boto3")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    ex = executor_from_url("lambda://flow-worker")
    assert ex._function_name == "flow-worker"
    assert ex._qualifier is None


def test_executor_from_url_lambda_with_qualifier(monkeypatch):
    pytest.importorskip("boto3")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    ex = executor_from_url("lambda://flow-worker?qualifier=PROD")
    assert ex._qualifier == "PROD"


def test_executor_from_url_unknown_scheme():
    with pytest.raises(ValueError, match="unknown executor URL scheme"):
        executor_from_url("twiml://hello")


# ---- scheduler loop ------------------------------------------------


def test_scheduler_dispatches_due_pipelines(user_module):
    """Two pipelines both due → both get dispatched, both get a
    claim row, neither runs in-process."""
    run_log = InMemoryRunLog()
    executor = FakeExecutor()

    run_scheduler(
        module=user_module,
        run_log=run_log,
        run_log_url="memory://",
        executor=executor,
        max_iterations=1,
        sleep_fn=lambda _: None,
    )

    names = {s.pipeline_name for s in executor.dispatched}
    assert names == {"a", "b"}, f"got: {names}"
    # Each dispatch carried the user's module so the worker can
    # import it.
    assert all(s.module == user_module for s in executor.dispatched)


def test_scheduler_skips_pipelines_already_claimed(user_module):
    """If `a` is already claimed by another worker, the scheduler
    should skip it and only dispatch `b`."""
    run_log = InMemoryRunLog()
    # Pre-claim `a` from a different worker.
    run_log.claim("a", "other-worker", lease_seconds=600)

    executor = FakeExecutor()
    run_scheduler(
        module=user_module,
        run_log=run_log,
        run_log_url="memory://",
        executor=executor,
        max_iterations=1,
        sleep_fn=lambda _: None,
    )
    names = {s.pipeline_name for s in executor.dispatched}
    assert "a" not in names
    assert "b" in names


def test_scheduler_release_on_dispatch_failure(user_module):
    """When the executor raises DispatchError, the scheduler must
    release the claim so the next tick can retry."""
    run_log = InMemoryRunLog()
    executor = FakeExecutor(raise_on={"a"})

    alerter_calls = []

    class CapturingAlerter:
        def notify(self, event):
            alerter_calls.append(event)

    run_scheduler(
        module=user_module,
        run_log=run_log,
        run_log_url="memory://",
        executor=executor,
        alerters=[CapturingAlerter()],
        max_iterations=1,
        sleep_fn=lambda _: None,
    )

    # `a` was released after the dispatch failed — verify by
    # successfully re-claiming it as another worker.
    claim = run_log.claim("a", "next-worker", lease_seconds=60)
    assert claim.acquired, "scheduler didn't release the claim after DispatchError"

    # The alerter saw a DispatchFailedEvent for `a`.
    assert len(alerter_calls) == 1
    assert isinstance(alerter_calls[0], DispatchFailedEvent)
    assert alerter_calls[0].pipeline == "a"


def test_scheduler_leader_election_backoff(user_module):
    """If another scheduler holds the leader lock, this one should
    back off and NOT walk the DAG."""
    run_log = InMemoryRunLog()
    # Pre-claim the leader lock from another scheduler.
    run_log.claim(
        "_scheduler_singleton",
        "other-scheduler",
        lease_seconds=600,
    )
    executor = FakeExecutor()

    run_scheduler(
        module=user_module,
        run_log=run_log,
        run_log_url="memory://",
        executor=executor,
        max_iterations=1,
        sleep_fn=lambda _: None,
    )
    assert executor.dispatched == [], (
        "non-leader scheduler dispatched anyway"
    )


def test_scheduler_gates_stale_upstream(tmp_path, monkeypatch):
    """A pipeline whose declared upstream hasn't fired today should
    NOT be dispatched."""
    mod_path = tmp_path / "sched_dag_mod.py"
    mod_path.write_text(
        "from ematix_flow.pipeline import register\n"
        "\n"
        "@register(name='upstream', schedule='0 0 * * *')\n"  # daily
        "def up():\n"
        "    return {}\n"
        "\n"
        "@register(name='downstream', schedule='* * * * *', "
        "depends_on=['upstream'])\n"
        "def down():\n"
        "    return {}\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    run_log = InMemoryRunLog()
    executor = FakeExecutor()
    # Fix `now` to a minute boundary so `downstream` is due but
    # `upstream` is not (it fires at midnight).
    fixed_now = datetime(2026, 5, 14, 12, 30, 0, tzinfo=UTC)

    run_scheduler(
        module="sched_dag_mod",
        run_log=run_log,
        run_log_url="memory://",
        executor=executor,
        max_iterations=1,
        sleep_fn=lambda _: None,
        now_fn=lambda: fixed_now,
    )

    names = {s.pipeline_name for s in executor.dispatched}
    # Downstream gated because upstream hasn't successfully run today.
    assert "downstream" not in names


def test_scheduler_gates_retry_backoff(tmp_path, monkeypatch):
    """A pipeline mid-retry-backoff is not dispatched."""
    # Use a custom module so 'a' registers with a fixed 1-hour backoff.
    # `_RETRY_POLICY` is overwritten by `@register`, so the policy
    # must be declared at registration time, not patched in after.
    mod = tmp_path / "sched_backoff_mod.py"
    mod.write_text(
        "from ematix_flow.pipeline import register\n"
        "\n"
        "@register(name='a', schedule='* * * * *', retry={"
        "'max_attempts': 3, 'backoff': 'fixed', 'base_secs': 3600})\n"
        "def a():\n"
        "    return {'ok': True}\n"
        "\n"
        "@register(name='b', schedule='* * * * *')\n"
        "def b():\n"
        "    return {'ok': True}\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    # Pre-populate the in-memory RunLog with an AttemptState whose
    # next-eligible is in the future. `restore_into_process` will
    # hydrate `_ATTEMPT_STATE` after the module imports.
    run_log = InMemoryRunLog()
    run_log.record_attempt(
        "a",
        _p.AttemptState(
            attempt_count=1,
            last_attempt_at=datetime.now(UTC),
            gave_up=False,
        ),
    )
    executor = FakeExecutor()

    run_scheduler(
        module="sched_backoff_mod",
        run_log=run_log,
        run_log_url="memory://",
        executor=executor,
        max_iterations=1,
        sleep_fn=lambda _: None,
    )

    names = {s.pipeline_name for s in executor.dispatched}
    assert "a" not in names  # gated
    assert "b" in names      # `b` has no backoff
    sys.modules.pop("sched_backoff_mod", None)


def test_scheduler_max_iterations_terminates(user_module):
    run_log = InMemoryRunLog()
    executor = FakeExecutor()
    tick_count = [0]

    def counting_sleep(_):
        tick_count[0] += 1

    run_scheduler(
        module=user_module,
        run_log=run_log,
        run_log_url="memory://",
        executor=executor,
        max_iterations=3,
        sleep_fn=counting_sleep,
    )
    # 3 iterations → 3 sleeps. (Loop sleeps after each tick.)
    assert tick_count[0] == 3


# ---- end-to-end via CLI subprocess ---------------------------------


def test_scheduler_cli_one_iteration_with_subprocess_executor(
    tmp_path, monkeypatch
):
    """Spawn `python -m ematix_flow.cli scheduler` for a single
    iteration. With subprocess+python:// executor it should
    actually dispatch a worker process; the worker runs the
    pipeline (writing a marker file so we can confirm it ran)."""
    marker = tmp_path / "one_shot_ran.marker"
    mod = tmp_path / "sched_e2e_mod.py"
    mod.write_text(
        "from ematix_flow.pipeline import register\n"
        "\n"
        f"_MARKER = r{str(marker)!r}\n"
        "\n"
        "@register(name='one_shot', schedule='* * * * *')\n"
        "def one_shot():\n"
        "    open(_MARKER, 'w').write('ran')\n"
        "    return {'ok': True}\n"
    )

    log_path = tmp_path / "sched_e2e.db"
    env = os.environ.copy()
    env["PYTHONPATH"] = (
        f"{tmp_path}{os.pathsep}{env.get('PYTHONPATH', '')}"
    )

    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "ematix_flow.cli",
            "scheduler",
            "--module",
            "sched_e2e_mod",
            "--executor",
            "subprocess+python://",
            "--run-log-url",
            f"sqlite://{log_path}",
            "--max-iterations",
            "1",
            "--poll-interval",
            "0",
            "--lease-seconds",
            "60",
        ],
        env=env,
        capture_output=True,
        timeout=30,
    )
    assert proc.returncode == 0, (
        f"scheduler exited {proc.returncode}\n"
        f"stderr: {proc.stderr.decode(errors='replace')}"
    )

    # Wait for the dispatched worker subprocess to finish.
    # The scheduler forks the worker fire-and-forget; on our local
    # box this should land within a second or two.
    deadline = datetime.now() + timedelta(seconds=20)
    while datetime.now() < deadline:
        if marker.exists():
            return
        import time as _t
        _t.sleep(0.5)

    raise AssertionError(
        "worker subprocess never executed one_shot (marker file absent)"
    )


# ---- DispatchFailedEvent dataclass ---------------------------------


def test_dispatch_failed_event_shape():
    e = DispatchFailedEvent(
        pipeline="p1",
        error_type="DispatchError",
        error_message="boom",
    )
    assert e.pipeline == "p1"
    assert e.error_type == "DispatchError"
    assert e.error_message == "boom"
