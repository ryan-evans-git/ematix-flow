"""Async query-job registry bounds: concurrency + total-jobs caps so a
burst of slow queries can't spawn unbounded threads / retain unbounded
results."""
from __future__ import annotations

import threading

import pytest

from ematix_flow.web.query_jobs import QueryJobCapacityError, QueryJobRegistry


def test_rejects_when_max_inflight_reached():
    reg = QueryJobRegistry(max_inflight=2, max_jobs=100)
    gate = threading.Event()

    def _block():
        gate.wait(timeout=5)
        return {"columns": [], "rows": [], "stats": {}}

    # Two in-flight jobs fill the concurrency budget.
    reg.submit(_block)
    reg.submit(_block)
    with pytest.raises(QueryJobCapacityError):
        reg.submit(_block)
    # Release the blocked workers so the daemon threads exit.
    gate.set()


def test_capacity_frees_after_completion():
    reg = QueryJobRegistry(max_inflight=1, max_jobs=100)

    done = threading.Event()

    def _quick():
        done.set()
        return {"columns": [], "rows": [], "stats": {}}

    jid = reg.submit(_quick)
    assert done.wait(timeout=5)
    # Poll until the job leaves the in-flight set.
    for _ in range(500):
        job = reg.get(jid)
        if job and job["status"] in ("done", "error"):
            break
    # A fresh submit now succeeds (budget freed).
    reg.submit(lambda: {"columns": [], "rows": [], "stats": {}})
