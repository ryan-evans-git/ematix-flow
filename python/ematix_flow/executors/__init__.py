"""Executors — pluggable backends for spawning per-fire pipeline workers.

The Ω.W central scheduler walks the DAG, claims pipelines through
the RunLog lease layer, and hands each fire to an Executor.
Executors translate "fire pipeline X" into a concrete spawn:

  - SubprocessExecutor      — local process; default for dev + tests.
  - KubernetesJobExecutor   — k8s `batch/v1.Job`; needs `executor-k8s` extra.
  - LambdaExecutor          — AWS Lambda async invoke; needs `executor-lambda` extra.
  - ECSRunTaskExecutor      — AWS ECS RunTask; deferred.
  - CloudRunJobExecutor     — GCP Cloud Run Job; deferred.

The Executor Protocol is in `protocol`; pick a concrete backend
from this package or implement your own with the same shape.
"""

from __future__ import annotations

from .protocol import DispatchError, DispatchHandle, DispatchSpec, Executor
from .subprocess import SubprocessExecutor, python_subprocess_executor


def __getattr__(name: str):
    """Lazy import for optional-dep executors so importing
    `ematix_flow.executors` doesn't pull in kubernetes/boto3 when
    only SubprocessExecutor is needed."""
    if name == "KubernetesJobExecutor":
        from .kubernetes import KubernetesJobExecutor

        return KubernetesJobExecutor
    if name == "LambdaExecutor":
        from .lambda_ import LambdaExecutor

        return LambdaExecutor
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "DispatchError",
    "DispatchHandle",
    "DispatchSpec",
    "Executor",
    "KubernetesJobExecutor",
    "LambdaExecutor",
    "SubprocessExecutor",
    "python_subprocess_executor",
]
