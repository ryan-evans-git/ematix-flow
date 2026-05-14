"""Scheduler — long-running daemon that claims pipelines and
dispatches them to an Executor.

This is the **central scheduler** half of the Ω.W arc. The other
half — lease semantics in the RunLog (Ω.W.1/W.2), an Executor
Protocol (Ω.W.3), and concrete Executors for subprocess / k8s /
Lambda (Ω.W.3-5) — landed in earlier sub-phases. This module
wires them into a runnable loop.

The flow per tick:

  1. **Sweep expired leases.** A worker that died mid-run leaves a
     stale claim; the next sweep observes it for metrics + logs.
     The claim() call's WHERE-expired branch is what actually
     re-marks the row, so no separate "release" step is needed.

  2. **Acquire the scheduler singleton lease.** Lightweight leader
     election: at most one scheduler replica walks the DAG per
     tick. The claim CAS in the RunLog backend is strict, so a
     transient two-leader window (during a network partition) is
     still safe — the per-pipeline claim CAS catches it.

  3. **Walk the DAG.** Topo-order pipelines whose cron matched this
     interval; apply the upstream-freshness and retry-backoff
     gates that the existing `run_due_with_dag_detailed` uses.

  4. **Claim + dispatch.** For each eligible pipeline, `claim()`
     the row in the RunLog (proving no other worker has it), then
     `executor.dispatch()` the work. On `DispatchError`, release
     the claim so the next tick retries.

  5. **Sleep.** Then loop.
"""

from __future__ import annotations

from .executor_url import executor_from_url
from .loop import DispatchFailedEvent, run_scheduler

__all__ = ["DispatchFailedEvent", "executor_from_url", "run_scheduler"]
