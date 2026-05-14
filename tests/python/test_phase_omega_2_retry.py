"""Phase Ω.2 — declarative retry policy.

Tests `retry=` argument on the registration decorator + the in-process
re-fire logic it enables. Covers:

  - `retry=` annotates the registry entry with a RetryPolicy.
  - Defaults to no retries (max_attempts=1).
  - On failure, an in-process re-run within the same process backs off
    until the policy's next-eligible-at window passes.
  - Three backoff shapes: fixed, linear, exponential.
  - `max_backoff_secs` caps exponential growth.
  - A successful attempt resets the attempt counter.
  - Exceeding `max_attempts` gives up — no further invocations even if
    the window has passed.
  - Invalid policies (max_attempts=0, unknown backoff) rejected at
    registration time with a clear error.

In-process scope only. Durable per-attempt history is Ω.D1a.
"""

from __future__ import annotations

import datetime as _dt

import pytest

from ematix_flow import pipeline as p


@pytest.fixture(autouse=True)
def _clean_registry():
    """Reset all process-global registry state before each test."""
    for tbl in (
        "_REGISTRY",
        "_DEPENDS_ON",
        "_UPSTREAM_FRESHNESS",
        "_LAST_RUN",
        "_RETRY_POLICY",
        "_ATTEMPT_STATE",
    ):
        d = getattr(p, tbl, None)
        if d is not None:
            d.clear()
    yield
    for tbl in (
        "_REGISTRY",
        "_DEPENDS_ON",
        "_UPSTREAM_FRESHNESS",
        "_LAST_RUN",
        "_RETRY_POLICY",
        "_ATTEMPT_STATE",
    ):
        d = getattr(p, tbl, None)
        if d is not None:
            d.clear()


# ---- Registration --------------------------------------------------


def test_retry_annotates_registry():
    @p.register(
        name="flaky",
        schedule="@hourly",
        retry={"max_attempts": 3, "backoff": "exponential", "base_secs": 1, "max_backoff_secs": 60},
    )
    def _flaky():
        return {}

    pol = p.retry_policy_of("flaky")
    assert pol.max_attempts == 3
    assert pol.backoff == "exponential"
    assert pol.base_secs == 1
    assert pol.max_backoff_secs == 60


def test_no_retry_by_default():
    @p.register(name="simple", schedule="@hourly")
    def _simple():
        return {}

    assert p.retry_policy_of("simple").max_attempts == 1


def test_max_attempts_zero_rejected():
    with pytest.raises(ValueError) as ei:
        @p.register(name="bad", schedule="@hourly", retry={"max_attempts": 0})
        def _bad():
            return {}
    msg = str(ei.value).lower()
    assert "max_attempts" in msg


def test_unknown_backoff_rejected():
    with pytest.raises(ValueError) as ei:
        @p.register(
            name="bad",
            schedule="@hourly",
            retry={"max_attempts": 2, "backoff": "logarithmic"},
        )
        def _bad():
            return {}
    msg = str(ei.value).lower()
    assert "backoff" in msg and "logarithmic" in msg


# ---- In-process retry behaviour -----------------------------------


def test_successful_run_resets_attempt_state():
    calls: list[str] = []

    @p.register(
        name="ok",
        schedule="@hourly",
        retry={"max_attempts": 3, "backoff": "fixed", "base_secs": 5},
    )
    def _ok():
        calls.append("ok")
        return {}

    p.run_due_with_dag(["ok"])
    assert calls == ["ok"]
    # No attempt state retained on success — the next-attempt logic
    # would otherwise gate the next normal fire.
    assert p._ATTEMPT_STATE.get("ok") is None


def test_failure_records_attempt_and_blocks_immediate_retry():
    """First failure: attempt_count=1, next-eligible-at = now + backoff.
    A re-invocation within that window must NOT re-run the function.
    """
    calls: list[str] = []

    @p.register(
        name="fail",
        schedule="@hourly",
        retry={"max_attempts": 3, "backoff": "fixed", "base_secs": 30},
    )
    def _fail():
        calls.append("attempt")
        raise RuntimeError("boom")

    t0 = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    p.run_due_with_dag(["fail"], now=t0)
    assert calls == ["attempt"]
    state = p._ATTEMPT_STATE["fail"]
    assert state.attempt_count == 1

    # Re-invoke 5 seconds later — still inside the 30s backoff window.
    p.run_due_with_dag(["fail"], now=t0 + _dt.timedelta(seconds=5))
    # No new attempt; the window blocked it.
    assert calls == ["attempt"]
    assert p._ATTEMPT_STATE["fail"].attempt_count == 1


def test_failure_retries_after_backoff_window():
    calls: list[str] = []

    @p.register(
        name="fail",
        schedule="@hourly",
        retry={"max_attempts": 3, "backoff": "fixed", "base_secs": 10},
    )
    def _fail():
        calls.append("attempt")
        raise RuntimeError("boom")

    t0 = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    p.run_due_with_dag(["fail"], now=t0)
    # After 11s, the 10s fixed window has elapsed — re-run.
    p.run_due_with_dag(["fail"], now=t0 + _dt.timedelta(seconds=11))
    assert calls == ["attempt", "attempt"]
    assert p._ATTEMPT_STATE["fail"].attempt_count == 2


def test_exponential_backoff_grows():
    """attempt 1 fails → next-at = t0 + base_secs.
    attempt 2 fails → next-at = t1 + base_secs * 2.
    attempt 3 fails → next-at = t2 + base_secs * 4."""
    calls: list[str] = []

    @p.register(
        name="exp",
        schedule="@hourly",
        retry={"max_attempts": 5, "backoff": "exponential", "base_secs": 1, "max_backoff_secs": 1000},
    )
    def _exp():
        calls.append("attempt")
        raise RuntimeError("boom")

    t = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    # 1st attempt: fires at t.
    p.run_due_with_dag(["exp"], now=t)
    # After 0.5s: still inside the 1s window.
    p.run_due_with_dag(["exp"], now=t + _dt.timedelta(seconds=0.5))
    # After 1.5s: 1s window elapsed → 2nd attempt fires.
    p.run_due_with_dag(["exp"], now=t + _dt.timedelta(seconds=1.5))
    # After 2s (since 2nd attempt fired at 1.5s, next window is 2s
    # long): still inside the 2s window from the 2nd-attempt timestamp.
    p.run_due_with_dag(["exp"], now=t + _dt.timedelta(seconds=2.5))
    # After 4s (1.5 + 2s elapsed): 3rd attempt fires.
    p.run_due_with_dag(["exp"], now=t + _dt.timedelta(seconds=3.6))
    assert calls == ["attempt", "attempt", "attempt"]


def test_max_backoff_secs_caps_exponential():
    @p.register(
        name="capped",
        schedule="@hourly",
        retry={
            "max_attempts": 100,
            "backoff": "exponential",
            "base_secs": 1,
            "max_backoff_secs": 4,
        },
    )
    def _capped():
        return {}

    # base=1, max=4 → windows are 1, 2, 4, 4, 4, ...
    pol = p.retry_policy_of("capped")
    assert p._compute_backoff_secs(pol, attempt_count=1) == 1
    assert p._compute_backoff_secs(pol, attempt_count=2) == 2
    assert p._compute_backoff_secs(pol, attempt_count=3) == 4
    assert p._compute_backoff_secs(pol, attempt_count=4) == 4
    assert p._compute_backoff_secs(pol, attempt_count=10) == 4


def test_linear_backoff_grows():
    @p.register(
        name="lin",
        schedule="@hourly",
        retry={"max_attempts": 5, "backoff": "linear", "base_secs": 7},
    )
    def _lin():
        return {}

    pol = p.retry_policy_of("lin")
    assert p._compute_backoff_secs(pol, attempt_count=1) == 7
    assert p._compute_backoff_secs(pol, attempt_count=2) == 14
    assert p._compute_backoff_secs(pol, attempt_count=3) == 21


def test_giving_up_after_max_attempts():
    """attempt_count reaches max_attempts → no further runs even
    after backoff window elapses. State is preserved so observers
    can see we gave up."""
    calls: list[str] = []

    @p.register(
        name="dies",
        schedule="@hourly",
        retry={"max_attempts": 2, "backoff": "fixed", "base_secs": 1},
    )
    def _dies():
        calls.append("attempt")
        raise RuntimeError("boom")

    t = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    p.run_due_with_dag(["dies"], now=t)
    p.run_due_with_dag(["dies"], now=t + _dt.timedelta(seconds=2))
    assert calls == ["attempt", "attempt"]
    assert p._ATTEMPT_STATE["dies"].attempt_count == 2
    assert p._ATTEMPT_STATE["dies"].gave_up is True

    # Far in the future — still no new attempt.
    p.run_due_with_dag(["dies"], now=t + _dt.timedelta(seconds=600))
    assert calls == ["attempt", "attempt"]


def test_success_after_partial_failures_resets_state():
    calls: list[str] = []

    @p.register(
        name="recovers",
        schedule="@hourly",
        retry={"max_attempts": 5, "backoff": "fixed", "base_secs": 1},
    )
    def _recovers():
        calls.append("attempt")
        if len(calls) < 3:
            raise RuntimeError("not yet")
        return {}

    t = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.UTC)
    # Iterate only until the function succeeds. After that the
    # pipeline is back in the "no retry cycle in flight" state, so
    # additional run_due_with_dag calls would fire as regular scheduled
    # runs, not retries — orthogonal to what this test is about.
    p.run_due_with_dag(["recovers"], now=t)
    p.run_due_with_dag(["recovers"], now=t + _dt.timedelta(seconds=2))
    p.run_due_with_dag(["recovers"], now=t + _dt.timedelta(seconds=4))
    assert calls == ["attempt", "attempt", "attempt"]
    # State cleared on success.
    assert p._ATTEMPT_STATE.get("recovers") is None
