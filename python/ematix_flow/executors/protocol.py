"""Executor Protocol — how the scheduler spawns a per-fire worker.

The scheduler half of Ω.W (still to land — Ω.W.6) walks the DAG,
claims pipelines through the RunLog lease layer, and hands each
fire to an Executor. The Executor's job is to make one
`flow run --module M <pipeline>` happen on whatever compute
backend the operator picked: subprocess, k8s Job, Lambda invoke,
ECS Run-Task, Cloud Run Job.

Two principles guide the Protocol shape:

1. **Fire-and-forget dispatch.** `dispatch()` returns when the
   worker has been *spawned*, not when the pipeline has finished.
   The scheduler doesn't wait on the worker — it polls the RunLog
   on its next tick to learn the outcome.

2. **No back-channel.** The worker writes its own outcome to the
   RunLog (success/failure + attempt state); the scheduler reads
   the RunLog. Executor handles are only used for cancel /
   liveness — never for the result.

Implementations live in `ematix_flow.executors.subprocess`,
`...kubernetes`, `...lambda`, etc.; each is gated on its own
optional install extra.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class DispatchSpec:
    """Everything the worker needs to actually run one pipeline-fire.

    Constructed by the scheduler after `RunLog.claim()` succeeds.
    The Executor passes this to the worker via env vars / CLI flags
    / event payload — whatever its compute backend supports.
    """

    pipeline_name: str
    module: str
    """Importable Python module containing the @register'd pipelines."""

    claim_token: str
    """Returned by `RunLog.claim()`. The worker passes this on
    heartbeat() and release() so the lease layer can join its
    activity to the right claim row."""

    lease_seconds: int
    """Initial lease duration. Worker heartbeats at a fraction of
    this so the scheduler's sweep_expired_leases() doesn't
    spuriously decide the worker died."""

    run_log_url: str
    """RunLog backend URL the worker reports outcome to."""

    alerter_urls: list[str] = field(default_factory=list)
    """Optional alerter URLs (stdout / slack / ...) for failures."""

    metrics_url: str | None = None
    """Optional metrics sink URL (prometheus / otlp / ...)."""

    env: dict[str, str] = field(default_factory=dict)
    """Extra env vars to merge into the worker's environment.
    Secrets stay out of the spec — pass them via the real env."""


@dataclass(frozen=True)
class DispatchHandle:
    """Opaque-to-the-scheduler reference to one spawned worker.

    The shape varies per Executor:
      - SubprocessExecutor: holds a PID + the original Popen
      - K8sJobExecutor: holds {namespace, job_name}
      - LambdaExecutor: holds the AWS request ID

    The scheduler treats this as opaque — it only uses it to call
    `cancel(handle)` on lease expiry, never to read state.
    """

    pipeline_name: str
    """Echoed back for logging / metrics; matches DispatchSpec."""

    backend: str
    """Short identifier for the executor type, e.g. "subprocess",
    "k8s", "lambda". Useful for status displays."""

    ref: object
    """Backend-specific reference (PID, k8s Job name, etc.).
    Opaque — only the Executor that produced it understands it."""


@runtime_checkable
class Executor(Protocol):
    """Spawn one worker per dispatch. Stateless from the scheduler's
    view — every state-of-the-pipeline question goes through the
    RunLog, not the Executor."""

    def dispatch(self, spec: DispatchSpec) -> DispatchHandle:
        """Spawn one worker for this fire. Returns when the worker
        process / pod / Lambda invocation has been *started*; does
        NOT wait for completion. Should raise `DispatchError` on
        spawn failure so the scheduler can release the claim and
        retry on the next tick."""

    def cancel(self, handle: DispatchHandle) -> None:
        """Best-effort cancel — used when a lease expires and the
        scheduler is about to re-claim. Workers should be
        idempotent enough that double-fire is safe (load strategies
        like merge / append-by-event-id already guarantee this),
        but cancelling reduces wasted compute on the dead worker."""


class DispatchError(RuntimeError):
    """Raised when an Executor can't spawn the requested worker.

    Examples:
      - SubprocessExecutor: OS errno (ENOMEM, ENOENT for `flow` binary)
      - K8sJobExecutor: API server unreachable, quota exceeded
      - LambdaExecutor: throttled, deployment package mismatch

    The scheduler catches this, releases the claim via
    `RunLog.release(token)`, and retries on the next loop tick.
    """
