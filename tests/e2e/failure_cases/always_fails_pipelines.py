"""Test fixture: a pipeline that always fails, with a tight retry
policy (max_attempts=2, base_secs=1) so the give-up branch is
reachable in <30s of scheduler runtime."""

from __future__ import annotations

from ematix_flow.pipeline import register


@register(
    name="always_fails",
    schedule="* * * * *",
    retry={"max_attempts": 2, "backoff": "fixed", "base_secs": 1},
)
def always_fails() -> dict:
    raise RuntimeError("always fails on purpose — exercises retry give-up")
