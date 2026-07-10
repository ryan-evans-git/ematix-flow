"""Phase 2: freshness SLO orchestration — evaluate_freshness_once
persists state, de-dupes breach alerts across invocations, and notifies
on the breach + recover edges."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from ematix_flow import quality as q
from ematix_flow.web.analytics_store import AnalyticsStore


class _FakeAlerter:
    def __init__(self):
        self.events = []

    def notify(self, event):
        self.events.append((event.kind, event.pipeline))


def _now():
    return datetime(2026, 7, 9, 12, 0, 0, tzinfo=UTC)


@pytest.fixture()
def sla_p():
    q.register_freshness_sla("p", "6h")
    yield
    q.register_freshness_sla("p", None)


def test_evaluate_once_persists_and_dedupes(sla_p):
    store = AnalyticsStore(":memory:")
    alerter = _FakeAlerter()
    # Start breached (last success 8h ago).
    successes = {"p": _now() - timedelta(hours=8)}

    def last_success(name):
        return successes.get(name)

    # First run: breach edge fires + state persisted.
    states = q.evaluate_freshness_once(
        last_success_fn=last_success,
        analytics_store=store,
        alerters=[alerter],
        now=_now(),
    )
    assert states[0].state == "breached"
    assert alerter.events == [("sla_breached", "p")]
    assert store.list_freshness()[0]["state"] == "breached"

    # Second run, still breached: prior state seeded from the store →
    # NO duplicate alert.
    q.evaluate_freshness_once(
        last_success_fn=last_success,
        analytics_store=store,
        alerters=[alerter],
        now=_now(),
    )
    assert alerter.events == [("sla_breached", "p")]  # unchanged

    # Now it recovers.
    successes["p"] = _now() - timedelta(minutes=5)
    q.evaluate_freshness_once(
        last_success_fn=last_success,
        analytics_store=store,
        alerters=[alerter],
        now=_now(),
    )
    assert ("recovered", "p") in alerter.events
    assert store.list_freshness()[0]["state"] == "healthy"
    store.close()


def test_evaluate_once_no_store_still_works(sla_p):
    alerter = _FakeAlerter()
    states = q.evaluate_freshness_once(
        last_success_fn=lambda _n: None,  # never succeeded → breached
        analytics_store=None,
        alerters=[alerter],
        now=_now(),
    )
    assert states[0].state == "breached"
    assert alerter.events == [("sla_breached", "p")]
