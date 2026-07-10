"""Tests for the analytics_store v5 data-quality + freshness tables."""

from __future__ import annotations

from datetime import UTC, datetime

from ematix_flow import quality as q
from ematix_flow.web.analytics_store import AnalyticsStore


def _outcome(pipeline="p", verdict="fail"):
    return q.QualityOutcome(
        pipeline=pipeline,
        table="customers",
        schema="main",
        verdict=verdict,
        assertions=(
            q.QualityAssertion("id.not_null", "pass"),
            q.QualityAssertion("email.not_null", "fail", "1 NULL row(s)"),
        ),
        duration_seconds=0.1,
        started_at=datetime(2026, 7, 9, 12, 0, tzinfo=UTC),
        finished_at=datetime(2026, 7, 9, 12, 0, 1, tzinfo=UTC),
    )


def test_schema_is_v5():
    store = AnalyticsStore(":memory:")
    ver = store._conn.execute(
        "SELECT version FROM analytics_schema_version"
    ).fetchone()[0]
    assert ver == 5
    store.close()


def test_record_and_list_quality_runs():
    store = AnalyticsStore(":memory:")
    store.record_quality_run(_outcome(verdict="fail"), run_id="r1")
    store.record_quality_run(_outcome(pipeline="other", verdict="pass"))
    all_runs = store.list_quality_runs()
    assert len(all_runs) == 2
    only_p = store.list_quality_runs(pipeline="p")
    assert len(only_p) == 1
    row = only_p[0]
    assert row["verdict"] == "fail"
    assert row["checks_total"] == 2
    assert row["checks_failed"] == 1
    assert row["run_id"] == "r1"
    assert row["table"] == "customers"
    assert len(row["assertions"]) == 2
    store.close()


def test_upsert_freshness_state_replaces():
    store = AnalyticsStore(":memory:")
    now = datetime(2026, 7, 9, 12, 0, tzinfo=UTC)
    breached = q.FreshnessState(
        pipeline="p", sla_seconds=3600, lag_seconds=7200, state="breached",
        last_success=None, evaluated_at=now,
    )
    healthy = q.FreshnessState(
        pipeline="p", sla_seconds=3600, lag_seconds=60, state="healthy",
        last_success=now, evaluated_at=now,
    )
    store.upsert_freshness_state(breached)
    store.upsert_freshness_state(healthy)  # same pipeline → replace
    rows = store.list_freshness()
    assert len(rows) == 1
    assert rows[0]["state"] == "healthy"
    assert rows[0]["lag_seconds"] == 60
    store.close()


def test_quality_stage_persists_to_configured_db(tmp_path, monkeypatch):
    db = str(tmp_path / "analytics.db")
    monkeypatch.setenv("EMATIX_FLOW_ANALYTICS_DB", db)
    # Drive _record_quality directly (no probe needed).
    q._record_quality(_outcome(pipeline="pl", verdict="fail"), run_id="rX")
    store = AnalyticsStore(db)
    rows = store.list_quality_runs(pipeline="pl")
    assert len(rows) == 1
    assert rows[0]["run_id"] == "rX"
    store.close()
