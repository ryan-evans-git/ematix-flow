"""Test fixture: a pipeline that sleeps longer than its
`--lease-seconds` so the heartbeat thread is the only thing keeping
the claim alive."""

from __future__ import annotations

import time

from ematix_flow.pipeline import register


@register(name="slow", schedule="* * * * *")
def slow() -> dict:
    # Pipeline runs for ~8s. With `--lease-seconds 5 --heartbeat-interval 1`
    # in the test, the heartbeat thread must extend the lease at least
    # 7 times (every ~1s) — otherwise the lease expires after 5s and
    # the scheduler dispatches a SECOND worker against the same pipeline,
    # which the claim CAS would block (but indicates a bug).
    time.sleep(8)
    return {"slept_secs": 8}
