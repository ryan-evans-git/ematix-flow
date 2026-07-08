"""CLI `web --datasource NAME=URL` parsing."""
from __future__ import annotations

from ematix_flow.cli import _parse_datasource_specs


def test_parses_name_url_pairs():
    out = _parse_datasource_specs(
        ["warehouse=postgres://u:p@h/db", "local=duckdb:///data.db"]
    )
    assert out == {
        "warehouse": "postgres://u:p@h/db",
        "local": "duckdb:///data.db",
    }


def test_keeps_url_with_equals_intact():
    # Only the first '=' splits name from url; query strings survive.
    out = _parse_datasource_specs(["db=postgres://h/db?opt=1&x=2"])
    assert out == {"db": "postgres://h/db?opt=1&x=2"}


def test_skips_malformed(capsys):
    out = _parse_datasource_specs(["noequals", "=nourl", "name=", "ok=sqlite:///a.db"])
    assert out == {"ok": "sqlite:///a.db"}
    err = capsys.readouterr().err
    assert err.count("not NAME=URL") == 3


def test_empty_and_none():
    assert _parse_datasource_specs(None) == {}
    assert _parse_datasource_specs([]) == {}
