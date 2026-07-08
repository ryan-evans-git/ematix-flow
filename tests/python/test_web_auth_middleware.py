"""RBAC middleware end-to-end: 401/403 gating, /api/me, ownership."""
from __future__ import annotations

import sqlite3

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from ematix_flow.web.analytics_store import AnalyticsStore  # noqa: E402
from ematix_flow.web.auth import RBACConfig  # noqa: E402
from ematix_flow.web.server import create_app  # noqa: E402


@pytest.fixture
def sqlite_db(tmp_path):
    path = tmp_path / "a.db"
    conn = sqlite3.connect(str(path))
    conn.executescript("CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1);")
    conn.commit()
    conn.close()
    return path


@pytest.fixture
def client(sqlite_db):
    rbac = RBACConfig(
        identity_header="x-forwarded-email",
        groups_header="x-forwarded-groups",
        group_roles={"analysts": "editor"},
        admin_identities=frozenset({"boss@x.io"}),
        default_role="viewer",
    )
    app = create_app(
        datasources={"db": f"sqlite:///{sqlite_db}"},
        analytics_store=AnalyticsStore(":memory:"),
        rbac=rbac,
    )
    return TestClient(app)


VIEWER = {"x-forwarded-email": "v@x.io"}
EDITOR = {"x-forwarded-email": "e@x.io", "x-forwarded-groups": "analysts"}
ADMIN = {"x-forwarded-email": "boss@x.io"}


class TestGating:
    def test_health_open_without_identity(self, client):
        assert client.get("/api/health").status_code == 200

    def test_unauthenticated_401(self, client):
        assert client.get("/api/charts").status_code == 401
        assert client.post("/api/query", json={"datasource_id": "db", "sql": "SELECT 1"}).status_code == 401

    def test_viewer_can_read_not_write_or_query(self, client):
        assert client.get("/api/charts", headers=VIEWER).status_code == 200
        # ad-hoc query needs 'query'
        assert client.post("/api/query", headers=VIEWER, json={"datasource_id": "db", "sql": "SELECT 1"}).status_code == 403
        # create needs 'write'
        r = client.post("/api/charts", headers=VIEWER, json={"name": "n", "datasource_id": "db", "sql": "SELECT 1", "viz_type": "table"})
        assert r.status_code == 403

    def test_editor_can_query_and_write(self, client):
        assert client.post("/api/query", headers=EDITOR, json={"datasource_id": "db", "sql": "SELECT 1"}).status_code == 200
        r = client.post("/api/charts", headers=EDITOR, json={"name": "n", "datasource_id": "db", "sql": "SELECT 1", "viz_type": "table"})
        assert r.status_code == 200
        # owner recorded from the trusted identity
        assert r.json()["owner"] == "e@x.io"

    def test_admin_all(self, client):
        assert client.post("/api/query", headers=ADMIN, json={"datasource_id": "db", "sql": "SELECT 1"}).status_code == 200


class TestWhoami:
    def test_viewer(self, client):
        me = client.get("/api/me", headers=VIEWER).json()
        assert me == {
            "authenticated": True, "identity": "v@x.io", "role": "viewer",
            "permissions": ["read"], "rbac_enabled": True,
        }

    def test_editor(self, client):
        me = client.get("/api/me", headers=EDITOR).json()
        assert me["role"] == "editor"
        assert set(me["permissions"]) == {"read", "query", "write"}

    def test_admin(self, client):
        assert client.get("/api/me", headers=ADMIN).json()["role"] == "admin"

    def test_anonymous(self, client):
        me = client.get("/api/me").json()
        assert me["authenticated"] is False and me["role"] is None


class TestNoRbac:
    def test_open_when_rbac_absent(self, sqlite_db):
        c = TestClient(create_app(datasources={"db": f"sqlite:///{sqlite_db}"}))
        assert c.get("/api/charts").status_code == 200
        # /api/me reports the single-tenant operator as admin-capable
        me = c.get("/api/me").json()
        assert me["rbac_enabled"] is False
