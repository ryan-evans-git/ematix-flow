"""Phase 1: PipelineSpec round-trip across the Rust↔Python bridge."""

from __future__ import annotations

import json

import pytest

from ematix_flow import _core
from ematix_flow.pipeline import Pipeline, Source, Target


def _raw(**overrides):
    spec = {
        "name": "customers",
        "source": {"connection": "postgres://src/db", "query": "SELECT * FROM customers"},
        "target": {
            "connection": "postgres://dst/db",
            "schema": "warehouse",
            "table": "customer_dim",
        },
        "mode": "append",
        "keys": [],
    }
    spec.update(overrides)
    return json.dumps(spec)


def test_pipeline_dataclass_round_trip() -> None:
    p = Pipeline(
        name="customers",
        source=Source(connection="postgres://src/db", query="SELECT * FROM customers"),
        target=Target(
            connection="postgres://dst/db", schema="warehouse", table="customer_dim"
        ),
        mode="append",
    )
    assert p.to_normalized_dict() == p.to_spec_dict()


def test_merge_round_trip_preserves_keys() -> None:
    p = Pipeline(
        name="customers",
        source=Source(connection="postgres://src/db", query="SELECT * FROM customers"),
        target=Target(
            connection="postgres://dst/db", schema="warehouse", table="customer_dim"
        ),
        mode="merge",
        keys=("customer_id",),
    )
    out = p.to_normalized_dict()
    assert out["mode"] == "merge"
    assert out["keys"] == ["customer_id"]


def test_whitespace_trimmed_and_keys_deduped() -> None:
    raw = _raw(
        name="  customers  ",
        source={"connection": " a ", "query": "  SELECT 1  "},
        target={"connection": "a", "schema": " s ", "table": " t "},
        mode="merge",
        keys=["id", " id ", "name"],
    )
    out = json.loads(_core.parse_spec(raw))
    assert out["name"] == "customers"
    assert out["source"]["query"] == "SELECT 1"
    assert out["target"]["schema"] == "s"
    assert out["keys"] == ["id", "name"]


def test_unknown_field_rejected() -> None:
    raw = _raw(bogus=True)
    with pytest.raises(ValueError, match="unknown field"):
        _core.parse_spec(raw)


def test_invalid_mode_rejected() -> None:
    raw = _raw(mode="wat")
    with pytest.raises(ValueError):
        _core.parse_spec(raw)


def test_merge_without_keys_rejected() -> None:
    raw = _raw(mode="merge", keys=[])
    with pytest.raises(ValueError, match="key"):
        _core.parse_spec(raw)


def test_scd2_without_keys_rejected() -> None:
    raw = _raw(mode="scd2", keys=[])
    with pytest.raises(ValueError, match="key"):
        _core.parse_spec(raw)


def test_empty_query_rejected() -> None:
    raw = _raw(source={"connection": "c", "query": "   "})
    with pytest.raises(ValueError, match="query"):
        _core.parse_spec(raw)


def test_malformed_json_rejected() -> None:
    with pytest.raises(ValueError):
        _core.parse_spec("{not json")
