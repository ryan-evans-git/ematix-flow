"""In-process async query jobs, for queries too slow to hold an HTTP
connection open. Submit returns a job id immediately; the query runs on
a daemon thread and the client polls for the result.

Deliberately in-process (no external queue): the web server is a single
uvicorn process and results are ephemeral. A distributed variant would
swap this registry for a shared store.
"""
from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

# Jobs older than this (since completion) are pruned on access.
_JOB_TTL_S = 600.0


class QueryJobRegistry:
    def __init__(self):
        self._jobs: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def submit(self, fn: Callable[[], dict[str, Any]]) -> str:
        job_id = uuid.uuid4().hex
        with self._lock:
            self._jobs[job_id] = {"id": job_id, "status": "pending", "created": time.monotonic()}

        def run() -> None:
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
        now = time.monotonic()
        with self._lock:
            stale = [
                jid
                for jid, j in self._jobs.items()
                if "finished" in j and now - j["finished"] > _JOB_TTL_S
            ]
            for jid in stale:
                self._jobs.pop(jid, None)
