"""Bearer-token auth on the Web UI (#6)."""
from __future__ import annotations

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from ematix_flow.web.server import create_app


class TestNoTokenConfigured:
    """Backwards-compat: when bearer_token=None, every route is open."""

    def test_health_open(self) -> None:
        client = TestClient(create_app())
        assert client.get("/api/health").status_code == 200

    def test_runs_open(self) -> None:
        client = TestClient(create_app())
        # Stub fallback returns 200 (no history configured).
        assert client.get("/api/runs").status_code == 200


class TestTokenRequired:
    def test_missing_header_returns_401(self) -> None:
        client = TestClient(create_app(bearer_token="s3cret"))
        resp = client.get("/api/runs")
        assert resp.status_code == 401
        assert "Authorization" in resp.json()["detail"]

    def test_wrong_token_returns_401(self) -> None:
        client = TestClient(create_app(bearer_token="s3cret"))
        resp = client.get(
            "/api/runs", headers={"Authorization": "Bearer wrong"},
        )
        assert resp.status_code == 401
        assert "invalid" in resp.json()["detail"]

    def test_right_token_passes(self) -> None:
        client = TestClient(create_app(bearer_token="s3cret"))
        resp = client.get(
            "/api/runs", headers={"Authorization": "Bearer s3cret"},
        )
        assert resp.status_code == 200

    def test_health_remains_open(self) -> None:
        # /api/health must NOT require auth — load balancer / readiness
        # probes need to reach it without configuring the token.
        client = TestClient(create_app(bearer_token="s3cret"))
        assert client.get("/api/health").status_code == 200

    def test_mutating_route_requires_token(self) -> None:
        client = TestClient(create_app(bearer_token="s3cret"))
        # POST without token fails the gate (not 400-ish from missing
        # body — 401 from auth gate, which fires before route logic).
        resp = client.post("/api/runs/01HQXXX/restart")
        assert resp.status_code == 401

    def test_non_api_paths_open(self) -> None:
        # SPA static files / index.html stay reachable without auth so
        # the UI can fetch its JS bundle, then prompt the user for the
        # token via the existing API surface.
        client = TestClient(create_app(bearer_token="s3cret"))
        resp = client.get("/")
        # 200 (placeholder HTML) or 404 (no ui_dist) — never 401.
        assert resp.status_code != 401

    def test_malformed_authorization_header(self) -> None:
        # Anything that doesn't start with `Bearer ` is rejected with
        # the "missing Bearer header" message — `Basic ...`, raw
        # token without scheme, etc.
        client = TestClient(create_app(bearer_token="s3cret"))
        resp = client.get(
            "/api/runs", headers={"Authorization": "Basic dXNlcjpwYXNz"},
        )
        assert resp.status_code == 401
        assert "Authorization" in resp.json()["detail"]
