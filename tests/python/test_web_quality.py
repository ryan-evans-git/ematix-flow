"""Phase 3: /api/quality + /api/freshness read endpoints."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient

from ematix_flow import quality as q
from ematix_flow.web.analytics_store import AnalyticsStore
from ematix_flow.web.auth import RBACConfig
from ematix_flow.web.server import create_app


def _seed(store: AnalyticsStore) -> None:
    store.record_quality_run(
        q.QualityOutcome(
            pipeline="load_customers",
            table="customers",
            schema="main",
            verdict="fail",
            assertions=(q.QualityAssertion("email.not_null", "fail", "1 null"),),
            started_at=datetime(2026, 7, 9, 12, 0, tzinfo=UTC),
            finished_at=datetime(2026, 7, 9, 12, 0, 1, tzinfo=UTC),
        ),
        run_id="r1",
    )
    store.upsert_freshness_state(
        q.FreshnessState(
            pipeline="load_customers",
            sla_seconds=21600,
            lag_seconds=30000,
            state="breached",
            last_success=None,
            evaluated_at=datetime(2026, 7, 9, 12, 0, tzinfo=UTC),
        )
    )


@pytest.fixture
def store() -> AnalyticsStore:
    s = AnalyticsStore(":memory:")
    _seed(s)
    return s


@pytest.fixture
def client(store) -> TestClient:
    return TestClient(create_app(analytics_store=store))


def test_list_quality(client: TestClient):
    r = client.get("/api/quality")
    assert r.status_code == 200, r.text
    runs = r.json()["quality_runs"]
    assert len(runs) == 1
    assert runs[0]["verdict"] == "fail"
    assert runs[0]["pipeline"] == "load_customers"
    assert runs[0]["checks_failed"] == 1


def test_list_quality_filtered(client: TestClient):
    assert client.get("/api/quality?pipeline=load_customers").json()["quality_runs"]
    assert client.get("/api/quality?pipeline=nope").json()["quality_runs"] == []


def test_list_freshness(client: TestClient):
    r = client.get("/api/freshness")
    assert r.status_code == 200
    fr = r.json()["freshness"]
    assert len(fr) == 1
    assert fr[0]["state"] == "breached"
    assert fr[0]["pipeline"] == "load_customers"


def test_quality_requires_read_permission(store):
    """With RBAC on, an unauthenticated request is rejected; a viewer
    (read) is allowed."""
    rbac = RBACConfig(
        identity_header="x-forwarded-email",
        groups_header="x-forwarded-groups",
        group_roles={"analysts": "viewer"},
        admin_identities=frozenset(),
        default_role=None,
    )
    client = TestClient(create_app(analytics_store=store, rbac=rbac))
    # No identity → 401.
    assert client.get("/api/quality").status_code == 401
    # Viewer identity → 200.
    ok = client.get(
        "/api/quality",
        headers={
            "x-forwarded-email": "a@b.io",
            "x-forwarded-groups": "analysts",
        },
    )
    assert ok.status_code == 200, ok.text
