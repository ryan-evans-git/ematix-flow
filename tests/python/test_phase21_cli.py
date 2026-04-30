"""Phase 21: `flow connections` CLI subcommands."""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from ematix_flow.cli import main


@pytest.fixture
def clean_env(monkeypatch) -> Iterator[None]:
    for key in list(os.environ):
        if key.startswith("EMATIX_FLOW_"):
            monkeypatch.delenv(key, raising=False)
    yield


@pytest.fixture
def config_home(tmp_path: Path, monkeypatch) -> Path:
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    return home


@pytest.fixture
def project_dir(tmp_path: Path, monkeypatch) -> Path:
    proj = tmp_path / "proj"
    proj.mkdir()
    monkeypatch.chdir(proj)
    return proj


def test_connections_list_shows_configured(
    clean_env, config_home, project_dir, monkeypatch, capsys
) -> None:
    monkeypatch.setenv(
        "EMATIX_FLOW_DSN_WAREHOUSE", "postgres://wh_user:s3cret@host/db"
    )
    rc = main(["connections", "list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "warehouse" in out
    assert "host" in out
    assert "wh_user" in out         # username preserved
    assert "s3cret" not in out      # password redacted


def test_connections_list_with_no_configured_connections_is_empty(
    clean_env, config_home, project_dir, capsys
) -> None:
    rc = main(["connections", "list"])
    assert rc == 0
    out = capsys.readouterr().out.strip()
    # Some "no connections configured" message; tolerant of phrasing.
    assert "no" in out.lower() or out == ""


def test_connections_set_persists_to_user_global_config(
    clean_env, config_home, project_dir, capsys
) -> None:
    rc = main(["connections", "set", "warehouse=postgres://wh/db"])
    assert rc == 0
    cfg = config_home / ".ematix-flow" / "connections.toml"
    assert cfg.exists()
    assert "warehouse" in cfg.read_text()
    # Subsequent list call sees it.
    rc = main(["connections", "list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "warehouse" in out


def test_connections_set_rejects_malformed_arg(
    clean_env, config_home, project_dir, capsys
) -> None:
    rc = main(["connections", "set", "no-equals-sign"])
    assert rc != 0


def test_connections_check_unknown_name_fails(
    clean_env, config_home, project_dir, capsys
) -> None:
    rc = main(["connections", "check", "nonexistent"])
    assert rc != 0
    err = capsys.readouterr().err
    assert "nonexistent" in err


def test_connections_check_unreachable_url_fails(
    clean_env, config_home, project_dir, monkeypatch, capsys
) -> None:
    # Configured but unreachable.
    monkeypatch.setenv(
        "EMATIX_FLOW_DSN_DEAD", "postgres://nobody@127.0.0.1:1/nope"
    )
    rc = main(["connections", "check", "dead"])
    assert rc != 0


@pytest.mark.integration
def test_connections_check_reachable_succeeds(
    clean_env, config_home, project_dir, monkeypatch, capsys, pg_url: str
) -> None:
    monkeypatch.setenv("EMATIX_FLOW_DSN_LIVE", pg_url)
    rc = main(["connections", "check", "live"])
    assert rc == 0
    out = capsys.readouterr().out.lower()
    assert "ok" in out or "reachable" in out
