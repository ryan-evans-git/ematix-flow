"""Phase 21: connection registry resolver — env vars + TOML config.

Pure tests; no DB connection required for the resolver itself.
Connection-reachability is exercised by the matching integration test
that round-trips through testcontainers.
"""

from __future__ import annotations

import os
import textwrap
from collections.abc import Iterator
from pathlib import Path

import pytest

from ematix_flow import config


@pytest.fixture
def clean_env(monkeypatch) -> Iterator[None]:
    """Strip every EMATIX_FLOW_* env var so tests are deterministic."""
    for key in list(os.environ):
        if key.startswith("EMATIX_FLOW_"):
            monkeypatch.delenv(key, raising=False)
    yield


@pytest.fixture
def config_home(tmp_path: Path, monkeypatch) -> Path:
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.delenv("EMATIX_FLOW_HOME", raising=False)
    return home


@pytest.fixture
def project_dir(tmp_path: Path, monkeypatch) -> Path:
    proj = tmp_path / "proj"
    proj.mkdir()
    monkeypatch.chdir(proj)
    return proj


# --- env-var resolution -----------------------------------------------------


def test_default_connection_reads_ematix_flow_dsn(
    clean_env, monkeypatch
) -> None:
    monkeypatch.setenv("EMATIX_FLOW_DSN", "postgres://u:p@h/db")
    dsn = config.resolve_dsn("default")
    assert dsn == "postgres://u:p@h/db"


def test_named_connection_reads_uppercase_env_var(clean_env, monkeypatch) -> None:
    monkeypatch.setenv("EMATIX_FLOW_DSN_WAREHOUSE", "postgres://wh/db")
    dsn = config.resolve_dsn("warehouse")
    assert dsn == "postgres://wh/db"


def test_named_connection_uppercases_lower_input(clean_env, monkeypatch) -> None:
    monkeypatch.setenv("EMATIX_FLOW_DSN_WAREHOUSE", "postgres://wh/db")
    # User wrote "warehouse" lowercase; we still find the upper-case env var.
    dsn = config.resolve_dsn("warehouse")
    assert dsn == "postgres://wh/db"


def test_env_var_takes_priority_over_config_file(
    clean_env, config_home, project_dir, monkeypatch
) -> None:
    (project_dir / ".ematix-flow.toml").write_text(
        textwrap.dedent("""
            [connections.warehouse]
            dsn = "postgres://from-config/db"
        """)
    )
    monkeypatch.setenv("EMATIX_FLOW_DSN_WAREHOUSE", "postgres://from-env/db")
    dsn = config.resolve_dsn("warehouse")
    assert dsn == "postgres://from-env/db"


# --- config file resolution -------------------------------------------------


def test_project_local_toml_resolves_named_connection(
    clean_env, config_home, project_dir
) -> None:
    (project_dir / ".ematix-flow.toml").write_text(
        textwrap.dedent("""
            [connections.warehouse]
            dsn = "postgres://from-project/db"
        """)
    )
    dsn = config.resolve_dsn("warehouse")
    assert dsn == "postgres://from-project/db"


def test_user_global_toml_resolves_named_connection(
    clean_env, config_home, project_dir
) -> None:
    cfg_dir = config_home / ".ematix-flow"
    cfg_dir.mkdir()
    (cfg_dir / "connections.toml").write_text(
        textwrap.dedent("""
            [connections.warehouse]
            dsn = "postgres://from-user-global/db"
        """)
    )
    dsn = config.resolve_dsn("warehouse")
    assert dsn == "postgres://from-user-global/db"


def test_project_local_takes_priority_over_user_global(
    clean_env, config_home, project_dir
) -> None:
    cfg_dir = config_home / ".ematix-flow"
    cfg_dir.mkdir()
    (cfg_dir / "connections.toml").write_text(
        textwrap.dedent("""
            [connections.warehouse]
            dsn = "postgres://user-global/db"
        """)
    )
    (project_dir / ".ematix-flow.toml").write_text(
        textwrap.dedent("""
            [connections.warehouse]
            dsn = "postgres://project-local/db"
        """)
    )
    dsn = config.resolve_dsn("warehouse")
    assert dsn == "postgres://project-local/db"


# --- env-var interpolation in TOML ------------------------------------------


def test_env_var_interpolation_in_toml(
    clean_env, config_home, project_dir, monkeypatch
) -> None:
    monkeypatch.setenv("WAREHOUSE_PASSWORD", "s3cret")
    (project_dir / ".ematix-flow.toml").write_text(
        textwrap.dedent("""
            [connections.warehouse]
            dsn = "postgres://wh:${WAREHOUSE_PASSWORD}@host/db"
        """)
    )
    dsn = config.resolve_dsn("warehouse")
    assert dsn == "postgres://wh:s3cret@host/db"


def test_env_var_interpolation_missing_raises(
    clean_env, config_home, project_dir
) -> None:
    (project_dir / ".ematix-flow.toml").write_text(
        textwrap.dedent("""
            [connections.warehouse]
            dsn = "postgres://wh:${MISSING_VAR}@host/db"
        """)
    )
    with pytest.raises(KeyError, match="MISSING_VAR"):
        config.resolve_dsn("warehouse")


# --- error paths ------------------------------------------------------------


def test_unknown_connection_name_raises(clean_env, config_home, project_dir) -> None:
    with pytest.raises(LookupError) as excinfo:
        config.resolve_dsn("nonexistent")
    msg = str(excinfo.value)
    assert "nonexistent" in msg
    # Surface the resolution order so users know how to fix it.
    assert "EMATIX_FLOW_DSN_NONEXISTENT" in msg


def test_default_connection_with_no_env_or_config_raises(
    clean_env, config_home, project_dir
) -> None:
    with pytest.raises(LookupError):
        config.resolve_dsn("default")


# --- connect() facade -------------------------------------------------------


def test_connect_with_explicit_url_bypasses_resolver(clean_env) -> None:
    """`connect(url=...)` is the low-level escape hatch — no name lookup."""
    # Will raise because URL is unreachable; we just want to confirm the
    # call path doesn't try to resolve a name.
    with pytest.raises(ValueError):
        config.connect(url="postgres://nobody@127.0.0.1:1/nope")


def test_connect_with_no_args_resolves_default(clean_env, monkeypatch) -> None:
    monkeypatch.setenv("EMATIX_FLOW_DSN", "postgres://nobody@127.0.0.1:1/nope")
    # Resolves name "default" → tries to connect → fails (unreachable).
    # We just assert the resolver was used (not an "unknown name" error).
    with pytest.raises(ValueError) as excinfo:
        config.connect()
    msg = str(excinfo.value).lower()
    # Connection failure, not a config lookup error.
    assert "unknown" not in msg or "127.0.0.1" in msg


# --- list_connections -------------------------------------------------------


def test_list_connections_merges_env_and_config(
    clean_env, config_home, project_dir, monkeypatch
) -> None:
    monkeypatch.setenv("EMATIX_FLOW_DSN_FROM_ENV", "postgres://env/db")
    (project_dir / ".ematix-flow.toml").write_text(
        textwrap.dedent("""
            [connections.from_config]
            dsn = "postgres://config/db"
        """)
    )
    names = config.list_connections()
    assert "from_env" in names
    assert "from_config" in names


def test_list_connections_redacts_passwords(
    clean_env, config_home, project_dir
) -> None:
    (project_dir / ".ematix-flow.toml").write_text(
        textwrap.dedent("""
            [connections.warehouse]
            dsn = "postgres://user:s3cret@host/db"
        """)
    )
    info = config.list_connections()
    assert "warehouse" in info
    rendered = info["warehouse"]
    assert "s3cret" not in rendered
    assert "user" in rendered  # username preserved
    assert "host" in rendered  # host preserved


# --- set_connection ---------------------------------------------------------


def test_set_connection_writes_to_user_global_config(
    clean_env, config_home, project_dir
) -> None:
    config.set_connection("warehouse", "postgres://wh/db")
    cfg_path = config_home / ".ematix-flow" / "connections.toml"
    assert cfg_path.exists()
    contents = cfg_path.read_text()
    assert "warehouse" in contents
    assert "postgres://wh/db" in contents
    # Round-trip: resolve_dsn finds it.
    assert config.resolve_dsn("warehouse") == "postgres://wh/db"


def test_set_connection_overwrites_existing(
    clean_env, config_home, project_dir
) -> None:
    config.set_connection("warehouse", "postgres://old/db")
    config.set_connection("warehouse", "postgres://new/db")
    assert config.resolve_dsn("warehouse") == "postgres://new/db"


def test_set_connection_preserves_other_entries(
    clean_env, config_home, project_dir
) -> None:
    config.set_connection("a", "postgres://a/db")
    config.set_connection("b", "postgres://b/db")
    assert config.resolve_dsn("a") == "postgres://a/db"
    assert config.resolve_dsn("b") == "postgres://b/db"
