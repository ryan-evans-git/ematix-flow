"""Executors — pluggable backends for spawning per-fire pipeline workers.

The Ω.W central scheduler walks the DAG, claims pipelines through
the RunLog lease layer, and hands each fire to an Executor.
Executors translate "fire pipeline X" into a concrete spawn:

  - SubprocessExecutor    — local process; default for dev + tests.
  - K8sJobExecutor        — k8s `batch/v1.Job`; deferred (Ω.W.4).
  - LambdaExecutor        — AWS Lambda invoke; deferred (Ω.W.5).
  - ECSRunTaskExecutor    — AWS ECS RunTask; deferred.
  - CloudRunJobExecutor   — GCP Cloud Run Job; deferred.

The Executor Protocol is in `protocol`; pick a concrete backend
from this package or implement your own with the same shape.
"""

from __future__ import annotations

from .protocol import DispatchError, DispatchHandle, DispatchSpec, Executor
from .subprocess import SubprocessExecutor, python_subprocess_executor

__all__ = [
    "DispatchError",
    "DispatchHandle",
    "DispatchSpec",
    "Executor",
    "SubprocessExecutor",
    "python_subprocess_executor",
]
