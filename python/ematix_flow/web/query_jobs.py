"""In-process async query jobs, for queries too slow to hold an HTTP
connection open. Submit returns a job id immediately; the query runs on
a daemon thread and the client polls for the result.

Deliberately in-process (no external queue): the web server is a single
uvicorn process and results are ephemeral. A distributed variant would
swap this registry for a shared store.
"""
from __future__ import annotations

import os
import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

# Jobs older than this (since completion) are pruned on access.
_JOB_TTL_S = 600.0


def _int_env(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(name, str(default))))
    except (TypeError, ValueError):
        return default


class QueryJobCapacityError(RuntimeError):
    """Raised by ``submit`` when the in-flight async-query limit is hit.
    The server maps this to HTTP 429 so a burst of slow queries can't
    spawn unbounded threads / retain unbounded results."""


class QueryJobRegistry:
    def __init__(
        self,
        max_inflight: int | None = None,
        max_jobs: int | None = None,
    ):
        self._jobs: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()
        # Cap concurrent background query threads, and the total number
        # of retained job records, so the in-process registry can't grow
        # without bound under load.
        self._max_inflight = max_inflight or _int_env(
            "EMATIX_FLOW_MAX_ASYNC_QUERIES", 8
        )
        self._max_jobs = max_jobs or _int_env("EMATIX_FLOW_MAX_ASYNC_JOBS", 256)

    def submit(self, fn: Callable[[], dict[str, Any]]) -> str:
        job_id = uuid.uuid4().hex
        with self._lock:
            self._prune_locked()
            inflight = sum(
                1
                for j in self._jobs.values()
                if j["status"] in ("pending", "running")
            )
            if inflight >= self._max_inflight:
                raise QueryJobCapacityError(
                    f"too many async queries in flight "
                    f"(limit {self._max_inflight}); retry shortly"
                )
            if len(self._jobs) >= self._max_jobs:
                raise QueryJobCapacityError(
                    f"async query job registry is full "
                    f"(limit {self._max_jobs}); retry shortly"
                )
            self._jobs[job_id] = {
                "id": job_id,
                "status": "pending",
                "created": time.monotonic(),
            }

        def run() -> None:
            self._set(job_id, status="running")
            try:
                result = fn()
                self._set(job_id, status="done", result=result)
            except Exception as exc:
                self._set(job_id, status="error", error=str(exc))

        threading.Thread(target=run, name=f"query-job-{job_id[:8]}", daemon=True).start()
        return job_id

    def _set(self, job_id: str, **fields: Any) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is not None:
                job.update(fields)
                job["finished"] = time.monotonic()

    def get(self, job_id: str) -> dict[str, Any] | None:
        self._prune()
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            out = {"id": job["id"], "status": job["status"]}
            if job["status"] == "done":
                out["result"] = job["result"]
            elif job["status"] == "error":
                out["error"] = job["error"]
            return out

    def _prune(self) -> None:
        with self._lock:
            self._prune_locked()

    def _prune_locked(self) -> None:
        """Drop TTL-expired jobs. Caller must hold ``self._lock``."""
        now = time.monotonic()
        stale = [
            jid
            for jid, j in self._jobs.items()
            if "finished" in j and now - j["finished"] > _JOB_TTL_S
        ]
        for jid in stale:
            self._jobs.pop(jid, None)
