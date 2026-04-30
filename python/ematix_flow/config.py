"""Phase 21: connection registry.

Pipelines declare *symbolic* connection names; this module resolves them
into Postgres DSNs (and live `_core.Connection` handles).

Resolution order (highest priority first):
  1. `EMATIX_FLOW_DSN_<NAME>` env var (`<NAME>` uppercased).
  2. `EMATIX_FLOW_DSN` env var (only for the connection named `default`).
  3. Project-local `./.ematix-flow.toml` `[connections.<name>]`.
  4. User-global `~/.ematix-flow/connections.toml` `[connections.<name>]`.
  5. Explicit `connect(url=...)` in code (low-level escape hatch).

TOML values support `${VAR}` env-var interpolation, so a config file can
reference secrets stored in env without inlining them.
"""

from __future__ import annotations

import os
import re
import sys
import tomllib
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse

from ematix_flow import _core

_PROJECT_CONFIG = ".ematix-flow.toml"
_USER_CONFIG_DIR = ".ematix-flow"
_USER_CONFIG_FILE = "connections.toml"
_INTERPOLATION = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)\}")


def _user_config_path() -> Path:
    """`~/.ematix-flow/connections.toml`. Honors `$HOME`."""
    return Path(os.path.expanduser("~")) / _USER_CONFIG_DIR / _USER_CONFIG_FILE


def _project_config_path() -> Path:
    return Path.cwd() / _PROJECT_CONFIG


def _interpolate(value: str) -> str:
    """Replace `${VAR}` with `os.environ[VAR]`. Raise `KeyError` if missing."""

    def replace(match: re.Match[str]) -> str:
        var = match.group(1)
        if var not in os.environ:
            raise KeyError(f"environment variable {var!r} referenced in config is not set")
        return os.environ[var]

    return _INTERPOLATION.sub(replace, value)


def _read_toml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("rb") as f:
        return tomllib.load(f)


def _config_connections(path: Path) -> dict[str, str]:
    """Return `{name: dsn}` from a TOML file, with interpolation applied."""
    data = _read_toml(path)
    section = data.get("connections", {})
    if not isinstance(section, dict):
        return {}
    out: dict[str, str] = {}
    for name, entry in section.items():
        if isinstance(entry, dict) and "dsn" in entry:
            out[name] = _interpolate(entry["dsn"])
    return out


def _env_var_for(name: str) -> str:
    if name == "default":
        return "EMATIX_FLOW_DSN"
    return f"EMATIX_FLOW_DSN_{name.upper()}"


def resolve_dsn(name: str = "default") -> str:
    """Resolve a connection name to a DSN string.

    Searches env vars first, then config files. Raises `LookupError` with
    an actionable message if the name is unknown.
    """
    # 1. Environment.
    env_var = _env_var_for(name)
    if env_var in os.environ:
        return os.environ[env_var]

    # 2. Project-local config.
    project = _config_connections(_project_config_path())
    if name in project:
        return project[name]

    # 3. User-global config.
    user = _config_connections(_user_config_path())
    if name in user:
        return user[name]

    raise LookupError(
        f"connection {name!r} is not configured. "
        f"Looked for env var {env_var}, "
        f"{_project_config_path()} → [connections.{name}], and "
        f"{_user_config_path()} → [connections.{name}]."
    )


def connect(name: str = "default", *, url: str | None = None) -> Any:
    """Open a Postgres connection by name (or by explicit URL).

    `connect()` resolves `default`. `connect("warehouse")` resolves the
    named entry. `connect(url="postgres://...")` is the low-level escape
    hatch and bypasses the registry entirely.
    """
    dsn = url if url is not None else resolve_dsn(name)
    return _core.connect(dsn)


def _redact(dsn: str) -> str:
    """Strip the password component from a DSN for safe display."""
    try:
        parsed = urlparse(dsn)
    except Exception:
        return dsn
    if parsed.password is None:
        return dsn
    netloc = parsed.netloc
    # netloc is `user:pass@host:port`; rewrite without the pass.
    if parsed.username is not None:
        host_port = netloc.split("@", 1)[1] if "@" in netloc else netloc
        netloc = f"{parsed.username}:***@{host_port}"
    else:
        netloc = re.sub(r":[^@]*@", ":***@", netloc, count=1)
    return urlunparse(parsed._replace(netloc=netloc))


def list_connections() -> dict[str, str]:
    """Return `{name: redacted_dsn}` for every configured connection.

    Merges env vars, project-local config, and user-global config in the
    same precedence order as `resolve_dsn`.
    """
    result: dict[str, str] = {}

    # User-global (lowest precedence).
    for name, dsn in _config_connections(_user_config_path()).items():
        result[name] = _redact(dsn)

    # Project-local overrides user-global.
    for name, dsn in _config_connections(_project_config_path()).items():
        result[name] = _redact(dsn)

    # Env vars override config files.
    for env_key, value in os.environ.items():
        if env_key == "EMATIX_FLOW_DSN":
            result["default"] = _redact(value)
        elif env_key.startswith("EMATIX_FLOW_DSN_"):
            name = env_key[len("EMATIX_FLOW_DSN_"):].lower()
            result[name] = _redact(value)

    return result


def set_connection(name: str, dsn: str) -> Path:
    """Persist a named connection to the user-global config file.

    Creates the config dir if needed. Preserves other entries. Returns
    the path written.
    """
    path = _user_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    # Read existing config (preserving other entries).
    if path.exists():
        with path.open("rb") as f:
            data = tomllib.load(f)
    else:
        data = {}
    section = data.setdefault("connections", {})
    if not isinstance(section, dict):
        section = {}
        data["connections"] = section
    section[name] = {"dsn": dsn}

    # Write back as TOML. We use a tiny hand-rolled writer because tomllib
    # is read-only; tomli-w would be a new dep we don't need.
    lines: list[str] = []
    for conn_name, entry in section.items():
        if not isinstance(entry, dict) or "dsn" not in entry:
            continue
        lines.append(f"[connections.{conn_name}]")
        lines.append(f'dsn = "{entry["dsn"]}"')
        lines.append("")
    path.write_text("\n".join(lines).rstrip() + "\n")
    return path


def check_connection(name: str) -> tuple[bool, str]:
    """Try to open a connection by name. Returns `(ok, message)`."""
    try:
        dsn = resolve_dsn(name)
    except LookupError as e:
        return False, str(e)
    try:
        conn = _core.connect(dsn)
        # Connect already issues SELECT 1 internally; we can also ping.
        conn.ping()
        return True, _redact(dsn)
    except Exception as e:
        return False, f"{_redact(dsn)}: {e}"


__all__ = [
    "resolve_dsn",
    "connect",
    "list_connections",
    "set_connection",
    "check_connection",
]
