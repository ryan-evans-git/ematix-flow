"""Data-quality hardening regressions (unit level, no ematix-probe).

Covers:
  * #8 parse_duration rejects negative durations.
  * #6 evaluate_freshness tolerates a naive `now`.
  * #2 an empty assertion suite is not a silent green "pass".
  * #4 an unsupported target connection under on_quality_failure="fail"
       fails loud instead of silently passing.
  * #5 checks_errored is persisted and surfaced by the store.
"""
from __future__ import annotations

import datetime as _dt

import pytest

from ematix_flow import quality as q


# ---- #8 negative duration ------------------------------------------
def test_parse_duration_rejects_negative_string():
    with pytest.raises(ValueError):
        q.parse_duration("-5m")


def test_parse_duration_rejects_negative_number():
    with pytest.raises(ValueError):
        q.parse_duration(-300)


# ---- #6 naive now --------------------------------------------------
def test_evaluate_freshness_naive_now_does_not_crash():
    st = q.evaluate_freshness(
        pipeline="p",
        last_success=_dt.datetime(2026, 7, 9, 11, 0, 0),  # naive
        sla="6h",
        now=_dt.datetime(2026, 7, 9, 12, 0, 0),  # naive
    )
    assert st.lag_seconds == 3600
    assert st.state == "healthy"


# ---- #2 empty suite is not "pass" ----------------------------------
def test_reduce_verdict_empty_is_not_pass():
    assert q._reduce_verdict(()) == "empty"


def _outcome(assertions, verdict):
    return q.QualityOutcome(
        pipeline="p", table="t", schema=None,
        verdict=verdict, assertions=tuple(assertions),
    )


def test_empty_suite_under_fail_policy_raises(monkeypatch):
    monkeypatch.setattr(q, "source_for_connection", lambda c: object())
    monkeypatch.setattr(q, "run_expectations", lambda **k: _outcome([], "empty"))
    with pytest.raises(q.QualityError):
        q.run_quality_stage(
            target_connection=object(),
            pipeline_name="p",
            expectations=lambda t: None,
            on_quality_failure="fail",
        )


def test_empty_suite_under_warn_policy_returns_outcome(monkeypatch):
    monkeypatch.setattr(q, "source_for_connection", lambda c: object())
    monkeypatch.setattr(q, "run_expectations", lambda **k: _outcome([], "empty"))
    out = q.run_quality_stage(
        target_connection=object(),
        pipeline_name="p",
        expectations=lambda t: None,
        on_quality_failure="warn",
    )
    assert out is not None and out.checks_total == 0


# ---- #4 unsupported connection -------------------------------------
class _FakeConn:
    kind = "mysql"
    url = "mysql://u:p@h/db"


def test_unsupported_connection_under_fail_raises(monkeypatch):
    monkeypatch.setattr(q, "source_for_connection", lambda c: None)
    with pytest.raises(q.QualityError):
        q.run_quality_stage(
            target_connection=_FakeConn(),
            pipeline_name="p",
            expectations=lambda t: None,
            on_quality_failure="fail",
        )


def test_unsupported_connection_under_warn_returns_none(monkeypatch):
    monkeypatch.setattr(q, "source_for_connection", lambda c: None)
    out = q.run_quality_stage(
        target_connection=_FakeConn(),
        pipeline_name="p",
        expectations=lambda t: None,
        on_quality_failure="warn",
    )
    assert out is None


# ---- #5 checks_errored persisted -----------------------------------
def test_store_persists_and_returns_checks_errored(tmp_path):
    pytest.importorskip("fastapi")  # store lives under web.*
    from ematix_flow.web.analytics_store import AnalyticsStore

    store = AnalyticsStore(":memory:")
    try:
        outcome = q.QualityOutcome(
            pipeline="p", table="t", schema=None, verdict="warn",
            assertions=(
                q.QualityAssertion("a", "pass"),
                q.QualityAssertion("b", "error", "boom"),
            ),
        )
        assert outcome.checks_errored == 1
        store.record_quality_run(outcome, run_id="r1")
        rows = store.list_quality_runs(pipeline="p", limit=10)
        assert rows[0]["checks_errored"] == 1
        assert rows[0]["checks_total"] == 2
        assert rows[0]["checks_failed"] == 0
    finally:
        store.close()
