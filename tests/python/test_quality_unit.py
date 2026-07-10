"""Unit tests for the pure data-quality logic (no ematix-probe needed):
duration parsing, freshness evaluation + de-duped edges, verdict
reduction, and the SLA registry."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from ematix_flow import quality as q

# ---- parse_duration -----------------------------------------------------


@pytest.mark.parametrize(
    "value,seconds",
    [
        ("30s", 30),
        ("15m", 900),
        ("6h", 21600),
        ("2d", 172800),
        ("3600", 3600),
        (3600, 3600),
        (timedelta(hours=1), 3600),
    ],
)
def test_parse_duration(value, seconds):
    assert q.parse_duration(value).total_seconds() == seconds


@pytest.mark.parametrize("bad", ["", "abc", "10x", "h"])
def test_parse_duration_rejects_garbage(bad):
    with pytest.raises(ValueError):
        q.parse_duration(bad)


# ---- evaluate_freshness -------------------------------------------------


def _now():
    return datetime(2026, 7, 9, 12, 0, 0, tzinfo=UTC)


def test_freshness_healthy():
    st = q.evaluate_freshness(
        pipeline="p", last_success=_now() - timedelta(hours=1), sla="6h", now=_now()
    )
    assert st.state == "healthy"
    assert st.lag_seconds == 3600


def test_freshness_warning_band():
    # 5h lag on a 6h SLA → >= 80% of window → warning.
    st = q.evaluate_freshness(
        pipeline="p", last_success=_now() - timedelta(hours=5), sla="6h", now=_now()
    )
    assert st.state == "warning"


def test_freshness_breached():
    st = q.evaluate_freshness(
        pipeline="p", last_success=_now() - timedelta(hours=7), sla="6h", now=_now()
    )
    assert st.state == "breached"
    assert st.lag_seconds == 7 * 3600


def test_freshness_never_succeeded_is_breached():
    st = q.evaluate_freshness(pipeline="p", last_success=None, sla="6h", now=_now())
    assert st.state == "breached"
    assert st.lag_seconds is None


def test_freshness_accepts_naive_datetime_as_utc():
    naive = datetime(2026, 7, 9, 11, 0, 0)  # no tzinfo
    st = q.evaluate_freshness(pipeline="p", last_success=naive, sla="6h", now=_now())
    assert st.state == "healthy"


# ---- FreshnessEvaluator edges ------------------------------------------


def test_freshness_evaluator_fires_breach_once_and_recovers():
    q.register_freshness_sla("p", "6h")
    try:
        successes = {"p": _now() - timedelta(hours=7)}  # start breached
        breaches: list[str] = []
        recovers: list[str] = []
        writes: list[str] = []
        ev = q.FreshnessEvaluator(
            last_success_fn=lambda name: successes.get(name),
            on_breach=lambda s: breaches.append(s.pipeline),
            on_recover=lambda s: recovers.append(s.pipeline),
            store_writer=lambda s: writes.append(s.state),
        )
        ev.evaluate_all(now=_now())
        ev.evaluate_all(now=_now())  # still breached — must NOT re-fire
        assert breaches == ["p"]  # de-duped to the edge
        assert len(writes) == 2  # store written every tick

        # Now a fresh success clears it → recover edge fires once.
        successes["p"] = _now() - timedelta(minutes=1)
        ev.evaluate_all(now=_now())
        assert recovers == ["p"]
    finally:
        q.register_freshness_sla("p", None)


def test_sla_registry_register_and_clear():
    q.register_freshness_sla("x", "1h")
    assert "x" in q.registered_slas()
    assert q.registered_slas()["x"].sla_seconds == 3600
    q.register_freshness_sla("x", None)
    assert "x" not in q.registered_slas()


# ---- verdict reduction + outcome ---------------------------------------


def _a(name, verdict):
    return q.QualityAssertion(name=name, verdict=verdict)


def test_reduce_verdict_fail_wins():
    assert q._reduce_verdict([_a("a", "pass"), _a("b", "fail"), _a("c", "error")]) == "fail"


def test_reduce_verdict_error_becomes_warn():
    assert q._reduce_verdict([_a("a", "pass"), _a("b", "error")]) == "warn"


def test_reduce_verdict_all_pass():
    assert q._reduce_verdict([_a("a", "pass"), _a("b", "pass")]) == "pass"


def test_outcome_counts_and_to_dict():
    out = q.QualityOutcome(
        pipeline="p",
        table="t",
        schema="public",
        verdict="fail",
        assertions=(_a("a", "pass"), _a("b", "fail"), _a("c", "error")),
        duration_seconds=0.5,
    )
    assert out.checks_total == 3
    assert out.checks_failed == 1
    assert out.checks_errored == 1
    assert out.qualified_table == "public.t"
    d = out.to_dict()
    assert d["verdict"] == "fail"
    assert d["checks_failed"] == 1
    assert len(d["assertions"]) == 3
