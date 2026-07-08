"""RBAC core: identity/role resolution + permission table."""
from __future__ import annotations

from ematix_flow.web.auth import (
    RBACConfig,
    permissions_for,
    required_permission,
    resolve_identity,
    resolve_role,
    role_has,
)


class Headers(dict):
    """Case-insensitive header mapping (like Starlette Headers)."""

    def get(self, key, default=None):
        for k, v in self.items():
            if k.lower() == key.lower():
                return v
        return default


class TestIdentity:
    def test_from_header(self):
        cfg = RBACConfig(identity_header="x-forwarded-email")
        assert resolve_identity(Headers({"X-Forwarded-Email": "a@x.io"}), cfg) == "a@x.io"

    def test_absent(self):
        assert resolve_identity(Headers({}), RBACConfig()) is None

    def test_blank_is_none(self):
        assert resolve_identity(Headers({"x-forwarded-email": "  "}), RBACConfig()) is None


class TestRole:
    def test_admin_allowlist_wins(self):
        cfg = RBACConfig(admin_identities=frozenset({"boss@x.io"}), default_role="viewer")
        assert resolve_role("boss@x.io", Headers({}), cfg) == "admin"

    def test_group_mapping_highest(self):
        cfg = RBACConfig(
            groups_header="x-forwarded-groups",
            group_roles={"analysts": "editor", "leads": "admin"},
            default_role="viewer",
        )
        h = Headers({"x-forwarded-groups": "analysts, leads, other"})
        assert resolve_role("u@x.io", h, cfg) == "admin"

    def test_default_role_when_unmapped(self):
        cfg = RBACConfig(default_role="editor", group_roles={"a": "admin"})
        assert resolve_role("u@x.io", Headers({"x-forwarded-groups": "nomatch"}), cfg) == "editor"


class TestPermissions:
    def test_role_has(self):
        assert role_has("viewer", "read")
        assert not role_has("viewer", "write")
        assert role_has("editor", "write")
        assert role_has("editor", "query")
        assert not role_has("editor", "admin")
        assert role_has("admin", "admin")

    def test_permissions_for(self):
        assert permissions_for("editor") == ["query", "read", "write"]


class TestRequiredPermission:
    def test_open_paths(self):
        assert required_permission("GET", "/") is None
        assert required_permission("GET", "/api/health") is None
        assert required_permission("GET", "/api/me") is None
        assert required_permission("GET", "/assets/app.js") is None

    def test_reads(self):
        assert required_permission("GET", "/api/charts") == "read"
        assert required_permission("GET", "/api/runs") == "read"

    def test_adhoc_query_needs_query_perm(self):
        assert required_permission("POST", "/api/query") == "query"
        assert required_permission("POST", "/api/query/async") == "query"
        assert required_permission("POST", "/api/cache/clear") == "query"

    def test_dashboard_run_and_alert_check_are_read(self):
        assert required_permission("POST", "/api/dashboards/abc/query") == "read"
        assert required_permission("POST", "/api/alerts/abc/check") == "read"

    def test_mutations_need_write(self):
        assert required_permission("POST", "/api/charts") == "write"
        assert required_permission("PUT", "/api/dashboards/abc") == "write"
        assert required_permission("DELETE", "/api/saved-queries/abc") == "write"
        assert required_permission("POST", "/api/runs/abc/restart") == "write"
