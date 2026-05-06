"""Phase Δ PR 1 — CDC dataclass scaffolding tests.

Locks the typed-Python surface of `ematix_flow.CDC` so it stays in
lockstep with the Rust core's `CdcConfig` and the CLI's
`[transform.cdc]` TOML parser. No execution path yet (PRs 2-5);
these tests exercise only the dataclass shape + __post_init__
validation.
"""

from __future__ import annotations

import pytest

import ematix_flow
from ematix_flow import CDC


def test_cdc_imports_from_package_root() -> None:
    """`from ematix_flow import CDC` is the published entry point."""
    assert CDC is ematix_flow.CDC


def test_cdc_debezium_default_takes_one_keyword() -> None:
    """The simplest config — `CDC(envelope="debezium")` — is the
    one-liner the docs promise."""
    cdc = CDC(envelope="debezium")
    assert cdc.envelope == "debezium"
    assert cdc.delete_mode == "hard"
    assert cdc.schema_evolution == "skip"
    assert cdc.out_of_order_tolerance_ms == 5_000
    # Field-path overrides default to None — the canonical mapping
    # fills them in via the Rust-core lowering.
    assert cdc.op_field is None
    assert cdc.after_field is None


def test_cdc_maxwell_envelope_accepts() -> None:
    cdc = CDC(envelope="maxwell")
    assert cdc.envelope == "maxwell"


def test_cdc_unknown_envelope_rejected_at_construction() -> None:
    with pytest.raises(ValueError, match="envelope must be"):
        CDC(envelope="outbox")


def test_cdc_unknown_delete_mode_rejected() -> None:
    with pytest.raises(ValueError, match="delete_mode must be"):
        CDC(envelope="debezium", delete_mode="purge")


def test_cdc_soft_delete_requires_column() -> None:
    """`delete_mode='soft'` with no `soft_delete_column` is the
    classic "I forgot to set the target column" footgun. Catch
    at decoration time, not at first batch."""
    with pytest.raises(ValueError, match="soft_delete_column"):
        CDC(envelope="debezium", delete_mode="soft")


def test_cdc_soft_delete_with_column_accepts() -> None:
    cdc = CDC(
        envelope="debezium",
        delete_mode="soft",
        soft_delete_column="deleted_at",
    )
    assert cdc.delete_mode == "soft"
    assert cdc.soft_delete_column == "deleted_at"


def test_cdc_unknown_schema_evolution_rejected() -> None:
    with pytest.raises(ValueError, match="schema_evolution must be"):
        CDC(envelope="debezium", schema_evolution="alter_table")


def test_cdc_custom_envelope_requires_explicit_paths() -> None:
    """Custom envelopes have no canonical mapping to fall back on
    — the dataclass refuses to construct without every required
    field path + an op_map."""
    with pytest.raises(ValueError, match="envelope='custom' requires"):
        CDC(envelope="custom")


def test_cdc_custom_envelope_lists_specific_missing_fields() -> None:
    """Error message names the missing fields so users don't have
    to consult the source to find what's required."""
    with pytest.raises(ValueError) as exc:
        CDC(envelope="custom", op_field="action")
    msg = str(exc.value)
    assert "after_field" in msg
    assert "key_field" in msg
    assert "op_map" in msg


def test_cdc_custom_envelope_full_overrides_accepts() -> None:
    cdc = CDC(
        envelope="custom",
        op_field="action",
        before_field="old_state",
        after_field="new_state",
        key_field="new_state.id",
        ts_field="changed_at_ms",
        op_map={"INSERT": "create", "UPDATE": "update", "DELETE": "delete"},
        delete_mode="soft",
        soft_delete_column="deleted_at",
        schema_evolution="fail",
        out_of_order_tolerance_ms=60_000,
    )
    assert cdc.envelope == "custom"
    assert cdc.op_field == "action"
    assert cdc.op_map is not None
    assert cdc.op_map["INSERT"] == "create"
    assert cdc.out_of_order_tolerance_ms == 60_000


def test_cdc_is_frozen_dataclass() -> None:
    """Pipelines may share a single CDC config across multiple
    decorations — freezing prevents accidental mutation."""
    cdc = CDC(envelope="debezium")
    with pytest.raises(Exception):  # FrozenInstanceError or AttributeError
        cdc.envelope = "maxwell"  # type: ignore[misc]
