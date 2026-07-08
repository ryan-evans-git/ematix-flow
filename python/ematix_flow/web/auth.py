"""RBAC for the web API, in the reverse-proxy / SSO-trust model.

An upstream proxy (oauth2-proxy, Cloudflare Access, an ingress SSO
gateway, …) authenticates the user and forwards a trusted identity
header (default ``X-Forwarded-Email``) — and optionally a groups header.
The app trusts those headers (it must only be reachable *through* the
proxy) and maps the identity to a role.

Roles gate *actions*, not per-object access: saved queries / charts /
dashboards are shared org-wide (everyone with ``read`` sees them), and
``owner`` is recorded for attribution only.

    viewer  -> read
    editor  -> read, query, write
    admin   -> read, query, write, admin
"""
from __future__ import annotations

from dataclasses import dataclass, field

# Role -> granted permissions.
ROLE_PERMISSIONS: dict[str, frozenset[str]] = {
    "viewer": frozenset({"read"}),
    "editor": frozenset({"read", "query", "write"}),
    "admin": frozenset({"read", "query", "write", "admin"}),
}
# Ordering for "highest role wins" when several groups map.
_ROLE_RANK = {"viewer": 0, "editor": 1, "admin": 2}


@dataclass(frozen=True)
class RBACConfig:
    identity_header: str = "x-forwarded-email"
    groups_header: str | None = "x-forwarded-groups"
    # group name (as sent by the proxy) -> role
    group_roles: dict[str, str] = field(default_factory=dict)
    # identities always treated as admin, regardless of groups
    admin_identities: frozenset[str] = frozenset()
    # role for an authenticated user not otherwise mapped
    default_role: str = "viewer"


def resolve_identity(headers, cfg: RBACConfig) -> str | None:
    """The trusted identity from the proxy header, or None if absent.

    ``headers`` is any case-insensitive mapping (Starlette Headers).
    """
    value = headers.get(cfg.identity_header)
    return value.strip() if value and value.strip() else None


def _highest(roles: list[str]) -> str:
    return max(roles, key=lambda r: _ROLE_RANK.get(r, -1))


def resolve_role(identity: str, headers, cfg: RBACConfig) -> str:
    """Map an authenticated identity to a role: explicit admin allowlist
    wins, then any group mapping (highest role), else the default role."""
    if identity in cfg.admin_identities:
        return "admin"
    if cfg.groups_header:
        raw = headers.get(cfg.groups_header) or ""
        groups = [g.strip() for g in raw.split(",") if g.strip()]
        mapped = [cfg.group_roles[g] for g in groups if g in cfg.group_roles]
        if mapped:
            return _highest(mapped)
    return cfg.default_role


def role_has(role: str, permission: str) -> bool:
    return permission in ROLE_PERMISSIONS.get(role, frozenset())


def permissions_for(role: str) -> list[str]:
    return sorted(ROLE_PERMISSIONS.get(role, frozenset()))


def required_permission(method: str, path: str) -> str | None:
    """The permission an API call needs, or None if it is open (static
    assets, health, and the identity probe).

    - GET (and HEAD) -> read
    - POST /api/query , /api/query/async , /api/cache/clear -> query
    - POST .../query  (dashboard batch) and .../check (alert) -> read
    - any other mutation -> write
    """
    if not path.startswith("/api/") or path in ("/api/health", "/api/me"):
        return None
    if method in ("GET", "HEAD", "OPTIONS"):
        return "read"
    if path in ("/api/query", "/api/query/async", "/api/cache/clear"):
        return "query"
    if path.endswith("/query") or path.endswith("/check"):
        return "read"
    return "write"
