"""Scheduler loop — claim pipelines, dispatch to an Executor."""

from __future__ import annotations

import logging
import os
import socket
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from ematix_flow import pipeline
from ematix_flow.executors import DispatchError, DispatchSpec, Executor
from ematix_flow.run_log.history import RunHistoryStore
from ematix_flow.run_log.protocol import RunLog

log = logging.getLogger(__name__)

# Reserved RunLog row used for leader-election; the underscore prefix
# is unlikely to collide with a real pipeline name. If a user *does*
# register a pipeline named `_scheduler_singleton`, the lease layer
# would refuse to dispatch it while a scheduler holds the lock — a
# loud failure mode the user can notice and rename.
_SCHEDULER_LEADER_KEY = "_scheduler_singleton"


@dataclass(frozen=True)
class DispatchFailedEvent:
    """Surfaced to alerters when `executor.dispatch()` raises.

    Mirrors the FiredEvent / FailedEvent shape from
    `ematix_flow.pipeline` so existing alerter implementations can
    pattern-match on attribute names. The scheduler-level alerter
    surface is intentionally narrow — actual pipeline failures are
    surfaced by the worker, not by the scheduler.
    """

    pipeline: str
    error_type: str
    error_message: str


def run_scheduler(
    *,
    module: str,
    run_log: RunLog,
    run_log_url: str,
    executor: Executor,
    alerter_urls: list[str] | None = None,
    metrics_url: str | None = None,
    alerters: list | None = None,
    metrics=None,
    poll_interval_seconds: int = 10,
    lease_seconds: int = 300,
    leader_lease_seconds: int = 60,
    interval_seconds: int = 60,
    worker_id: str | None = None,
    # Phase 4b-3: optional Web-UI run-history store. When provided
    # (typically the same backend as `run_log`), each tick walks
    # `history.pending_actions()` and dispatches any user-enqueued
    # restart / rerun rows via the same executor as cron-fired
    # pipelines. Without it the scheduler ignores Web-UI actions
    # (legacy behavior).
    history: RunHistoryStore | None = None,
    # Test-injection hooks. Production callers don't pass these.
    max_iterations: int | None = None,
    now_fn: Callable[[], datetime] | None = None,
    sleep_fn: Callable[[float], None] | None = None,
) -> None:
    """Long-running scheduler loop.

    Production callers invoke this without `max_iterations` /
    `now_fn` / `sleep_fn` — they run forever until SIGTERM and use
    real wall-clock time. The hooks exist so tests can drive the
    loop deterministically without spawning threads or wasting
    seconds on sleep().

    Args:
        module: importable Python module holding the @register'd
            pipelines. Will be imported on entry (decorators fire).
        run_log: the scheduler's own RunLog connection. Used for
            claim/heartbeat/release/sweep and the leader lock.
        run_log_url: URL string handed to spawned workers so they
            can write outcome back. Often the same backend the
            scheduler reads from.
        executor: a concrete `Executor` (subprocess / k8s / lambda /
            custom). The scheduler hands every claimed pipeline to
            this object.
        alerter_urls: URLs that *workers* should notify on
            pipeline-level failures. Distinct from the
            scheduler-level `alerters` (which surface dispatch
            failures only).
        metrics_url: metrics-sink URL for workers, same distinction.
        alerters: list of Alerter instances for scheduler-level
            events (currently just DispatchFailedEvent).
        metrics: optional MetricsSink for scheduler counters.
        poll_interval_seconds: sleep between ticks.
        lease_seconds: per-pipeline claim lease. Worker heartbeats
            keep it pushed out.
        leader_lease_seconds: scheduler-singleton lease. Should be
            > poll_interval so the leader's own lease doesn't expire
            mid-tick.
        interval_seconds: cron-match window. Matches the
            `--interval` semantics of `flow run-due`.
        worker_id: identifier this scheduler reports to the RunLog.
            Default: `scheduler-<hostname>-<pid>`.
    """
    if worker_id is None:
        worker_id = f"scheduler-{socket.gethostname()}-{os.getpid()}"

    now_fn = now_fn or (lambda: datetime.now(UTC))
    sleep_fn = sleep_fn or time.sleep

    _import_user_module(module)
    run_log.restore_into_process()

    log.info(
        "scheduler started: worker_id=%s module=%s executor=%s "
        "poll=%ss lease=%ss",
        worker_id, module, type(executor).__name__,
        poll_interval_seconds, lease_seconds,
    )

    iteration = 0
    while max_iterations is None or iteration < max_iterations:
        iteration += 1
        now = now_fn()

        _sweep_and_log(run_log, now, metrics)

        # Leader election: only the holder of the scheduler-singleton
        # walks the DAG. Multiple replicas back off to a sleep until
        # the leader's lease expires.
        leader_claim = run_log.claim(
            _SCHEDULER_LEADER_KEY, worker_id, lease_seconds=leader_lease_seconds
        )
        if not leader_claim.acquired:
            log.debug(
                "scheduler %s: leader is %s (expires %s), backing off",
                worker_id, leader_claim.holder, leader_claim.expires_at,
            )
            sleep_fn(poll_interval_seconds)
            continue

        try:
            # Phase 4b-3: Web-UI-enqueued actions (restart / rerun)
            # run before the cron-driven walk so user-initiated work
            # gets first dibs on the executor on each tick. Pause /
            # resume transitions are also surfaced here for telemetry,
            # but the actual transition happens worker-side (4b-4).
            if history is not None:
                _pickup_pending_actions(
                    history=history,
                    module=module,
                    run_log=run_log,
                    run_log_url=run_log_url,
                    executor=executor,
                    alerter_urls=alerter_urls or [],
                    metrics_url=metrics_url,
                    alerters=alerters or [],
                    metrics=metrics,
                    worker_id=worker_id,
                    lease_seconds=lease_seconds,
                )
            _walk_and_dispatch(
                module=module,
                run_log=run_log,
                run_log_url=run_log_url,
                executor=executor,
                alerter_urls=alerter_urls or [],
                metrics_url=metrics_url,
                alerters=alerters or [],
                metrics=metrics,
                worker_id=worker_id,
                lease_seconds=lease_seconds,
                interval_seconds=interval_seconds,
                now=now,
            )
        finally:
            # Always release the leader lock so another replica can
            # take over if we die. The lease would expire anyway, but
            # voluntary release shortens the failover window.
            run_log.release(leader_claim.token)

        sleep_fn(poll_interval_seconds)


def _import_user_module(module: str) -> None:
    """Import the user's pipelines module so the @register decorators
    fire and populate the in-process registry.

    Adds the current working directory to `sys.path` before importing
    so users can `flow scheduler --module pipelines` from the dir
    containing `pipelines.py` without manually exporting `PYTHONPATH`.
    Setuptools entry-point scripts (like the installed `flow` bin)
    don't get cwd on sys.path automatically; this restores the
    `python script.py` behavior most users expect.
    """
    import os
    import sys

    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    __import__(module)


def _sweep_and_log(run_log: RunLog, now: datetime, metrics) -> None:
    """Sweep expired leases — produces metrics + logs only. The
    actual recovery (re-marking the row as claimable) happens
    automatically inside the next claim() call's WHERE-expired
    branch."""
    try:
        expired = run_log.sweep_expired_leases(now)
    except Exception as e:
        log.warning("sweep_expired_leases failed: %s: %s", type(e).__name__, e)
        return
    for e in expired:
        if e.pipeline == _SCHEDULER_LEADER_KEY:
            continue  # internal lock; not a real pipeline
        log.warning(
            "lease expired: pipeline=%s held_by=%s expires_at=%s",
            e.pipeline, e.worker_id, e.expires_at,
        )
        _safe_metric(metrics, "inc_runs", e.pipeline, "lease_expired")


def _walk_and_dispatch(
    *,
    module: str,
    run_log: RunLog,
    run_log_url: str,
    executor: Executor,
    alerter_urls: list[str],
    metrics_url: str | None,
    alerters: list,
    metrics,
    worker_id: str,
    lease_seconds: int,
    interval_seconds: int,
    now: datetime,
) -> None:
    """One scheduling tick: find due pipelines, gate, claim, dispatch."""
    # Refresh _LAST_RUN + _ATTEMPT_STATE from the RunLog every tick so
    # workers' outcome writes (`run_log.record_run` in `flow run`) are
    # visible to the upstream-freshness + retry-backoff gates. Without
    # this, `depends_on=[...]` downstream pipelines stay gated forever
    # because the scheduler never sees the worker's success.
    try:
        run_log.restore_into_process()
    except Exception as e:
        log.warning(
            "scheduler: restore_into_process failed: %s: %s",
            type(e).__name__, e,
        )

    due = [
        sp.name for sp in pipeline.list_pipelines()
        if pipeline.is_due(sp.schedule, now, interval_seconds)
    ]
    if not due:
        return
    try:
        ordered = pipeline.order_for_run_due(due)
    except ValueError as e:
        log.error("scheduler: order_for_run_due failed: %s", e)
        return

    for name in ordered:
        if not _eligible_to_fire(name, now):
            continue

        claim = run_log.claim(name, worker_id, lease_seconds=lease_seconds)
        if not claim.acquired:
            log.debug(
                "scheduler: %s already claimed by %s", name, claim.holder
            )
            continue

        spec = DispatchSpec(
            pipeline_name=name,
            module=module,
            claim_token=claim.token,
            lease_seconds=lease_seconds,
            run_log_url=run_log_url,
            alerter_urls=list(alerter_urls),
            metrics_url=metrics_url,
            env={},
        )
        _dispatch_one(
            executor=executor,
            spec=spec,
            run_log=run_log,
            claim_token=claim.token,
            alerters=alerters,
            metrics=metrics,
        )


def _eligible_to_fire(name: str, now: datetime) -> bool:
    """Apply the upstream-freshness + retry-backoff gates the
    existing `run_due_with_dag_detailed` uses, returning True iff
    the pipeline is ready to fire right now."""
    if not pipeline._upstream_is_fresh(name, now):
        log.debug("scheduler: %s gated — upstream stale", name)
        return False
    st = pipeline._ATTEMPT_STATE.get(name)
    if st is None:
        return True
    if st.gave_up:
        log.debug("scheduler: %s gated — gave up", name)
        return False
    policy = pipeline._RETRY_POLICY.get(name, pipeline.RetryPolicy())
    backoff_secs = pipeline._compute_backoff_secs(policy, st.attempt_count)
    next_eligible = st.last_attempt_at + timedelta(seconds=backoff_secs)
    if now < next_eligible:
        log.debug(
            "scheduler: %s gated — retry backoff until %s",
            name, next_eligible,
        )
        return False
    return True


def _dispatch_one(
    *,
    executor: Executor,
    spec: DispatchSpec,
    run_log: RunLog,
    claim_token: str,
    alerters: list,
    metrics,
) -> None:
    try:
        executor.dispatch(spec)
        log.info(
            "scheduler: dispatched pipeline=%s claim=%s",
            spec.pipeline_name, claim_token[:8],
        )
        _safe_metric(metrics, "inc_runs", spec.pipeline_name, "dispatched")
    except DispatchError as e:
        # Spawn failed — release the claim so the next tick can retry.
        run_log.release(claim_token)
        event = DispatchFailedEvent(
            pipeline=spec.pipeline_name,
            error_type=type(e).__name__,
            error_message=str(e),
        )
        log.error(
            "scheduler: dispatch failed for pipeline=%s: %s: %s",
            event.pipeline, event.error_type, event.error_message,
        )
        for a in alerters:
            try:
                a.notify(event)
            except Exception as exc:
                log.warning(
                    "scheduler: alerter %s failed: %s",
                    type(a).__name__, exc,
                )
        _safe_metric(metrics, "inc_runs", spec.pipeline_name, "dispatch_failed")


def _pickup_pending_actions(
    *,
    history: RunHistoryStore,
    module: str,
    run_log: RunLog,
    run_log_url: str,
    executor: Executor,
    alerter_urls: list[str],
    metrics_url: str | None,
    alerters: list,
    metrics,
    worker_id: str,
    lease_seconds: int,
) -> None:
    """Phase 4b-3: dispatch Web-UI-enqueued restart / rerun requests.

    Walks ``history.pending_actions()`` and, for each
    ``status == "requested"`` row:

    1. Transitions the row to ``"running"`` via
       ``consume_requested_run`` (CAS-style atomic; returns False if
       another worker already grabbed it on a concurrent tick).
    2. Claims the underlying pipeline via the same RunLog claim
       call cron-fired pipelines use, so a Web-UI-enqueued run
       doesn't double-dispatch alongside a scheduled one.
    3. Dispatches via the same Executor, carrying the row's
       ``extras["restart_from_step"]`` or ``extras["rerun_full"]``
       through the env so the worker can honor the action.

    Pause / resume transitions are also surfaced in
    ``pending_actions`` for symmetry, but the actual run-state
    flip happens worker-side (Phase 4b-4 — the worker checks for
    pause_requested at the next step / watermark boundary).
    """
    try:
        pending = history.pending_actions()
    except Exception as e:
        log.warning(
            "scheduler: history.pending_actions failed: %s: %s",
            type(e).__name__, e,
        )
        return
    for record in pending:
        if record.status != "requested":
            # Pause / resume row — handled by the worker.
            continue
        if not history.consume_requested_run(record.run_id):
            # Another scheduler instance grabbed it first, or the
            # row state changed since pending_actions() ran.
            log.debug(
                "scheduler: pending action %s no longer requested",
                record.run_id,
            )
            continue
        claim = run_log.claim(
            record.pipeline, worker_id, lease_seconds=lease_seconds
        )
        if not claim.acquired:
            log.info(
                "scheduler: web-enqueued run %s for pipeline %s deferred "
                "— pipeline is already claimed by %s",
                record.run_id, record.pipeline, claim.holder,
            )
            continue
        env: dict[str, str] = {}
        from_step = record.extras.get("restart_from_step")
        if from_step is not None:
            env["EMATIX_FLOW_RESTART_FROM_STEP"] = str(from_step)
        if record.extras.get("rerun_full"):
            env["EMATIX_FLOW_RERUN_FULL"] = "1"
        prior = record.extras.get("prior_run_id")
        if prior:
            env["EMATIX_FLOW_PRIOR_RUN_ID"] = str(prior)
        spec = DispatchSpec(
            pipeline_name=record.pipeline,
            module=module,
            claim_token=claim.token,
            lease_seconds=lease_seconds,
            run_log_url=run_log_url,
            alerter_urls=list(alerter_urls),
            metrics_url=metrics_url,
            env=env,
        )
        log.info(
            "scheduler: dispatching web-enqueued run %s (pipeline=%s, "
            "restart_from_step=%s, rerun_full=%s)",
            record.run_id,
            record.pipeline,
            from_step,
            bool(record.extras.get("rerun_full")),
        )
        _dispatch_one(
            executor=executor,
            spec=spec,
            run_log=run_log,
            claim_token=claim.token,
            alerters=alerters,
            metrics=metrics,
        )


def _safe_metric(metrics, method: str, *args, **kwargs) -> None:
    """Metrics calls must not crash the scheduler. Sinks are
    expected to swallow their own errors, but defensively-wrap
    here too because the scheduler is the long-running daemon
    where a single bad metric call would kill the whole loop."""
    if metrics is None:
        return
    try:
        getattr(metrics, method)(*args, **kwargs)
    except Exception as e:
        log.warning("scheduler: metrics call %s failed: %s", method, e)
