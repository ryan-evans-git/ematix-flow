"""`Pipeline` declarative spec + `pipeline.sync(...)` executor.

Phase 1 introduced the `Pipeline` / `Source` / `Target` round-trip dataclasses;
Phase 5 adds `sync(...)` for executing a load against a live database.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from ematix_flow import _core
from ematix_flow.source import Source as _ImperativeSource
from ematix_flow.table import ManagedTable

Mode = Literal["append", "truncate", "merge", "scd1", "scd2"]


@dataclass(frozen=True)
class Source:
    """Phase 1 declarative spec source: a URL string + SELECT query.

    Phase 5+ uses `ematix_flow.source.Source` for imperative `pipeline.sync`,
    which carries a live `Connection` object instead of a URL string.
    """

    connection: str
    query: str


@dataclass(frozen=True)
class Target:
    connection: str
    schema: str
    table: str


@dataclass(frozen=True)
class Pipeline:
    name: str
    source: Source
    target: Target
    mode: Mode
    keys: tuple[str, ...] = field(default_factory=tuple)

    def to_spec_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "source": asdict(self.source),
            "target": asdict(self.target),
            "mode": self.mode,
            "keys": list(self.keys),
        }

    def to_normalized_dict(self) -> dict[str, Any]:
        return json.loads(_core.parse_spec(json.dumps(self.to_spec_dict())))


def _same_database(a: Any, b: Any) -> bool:
    return a.connection_info() == b.connection_info()


_METADATA_COLS = ("_loaded_at", "_batch_id")
_SCD2_META_COLS = ("valid_from", "valid_to", "is_current", "row_hash")


def sync(
    *,
    target: type[ManagedTable],
    source: _ImperativeSource,
    target_connection: Any,
    mode: Mode = "append",
    pipeline_name: str | None = None,
    on_drift: str = "error",
    keys: tuple[str, ...] | None = None,
    update_columns: tuple[str, ...] | None = None,
    compare_columns: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """Execute a load. Phases 5–8 support 'append', 'truncate', 'merge'/'scd1', 'scd2'."""
    if mode not in ("append", "truncate", "merge", "scd1", "scd2"):
        raise NotImplementedError(f"mode={mode!r} is not yet implemented")

    name = pipeline_name or f"{target.__schema__}.{target.__tablename__}"

    if mode == "scd2":
        augmented_json = _core.augment_table_spec_scd2(json.dumps(target._to_spec()))
    else:
        augmented_json = _core.augment_table_spec(json.dumps(target._to_spec()))

    target_connection.ensure_table(augmented_json, on_drift)

    same_db = _same_database(source.connection, target_connection)
    src_arg = None if same_db else source.connection

    if mode == "append":
        return target_connection.run_append(augmented_json, source.query, name, src_arg)
    if mode == "truncate":
        return target_connection.run_truncate(augmented_json, source.query, name, src_arg)

    if mode == "scd2":
        resolved_keys = list(keys) if keys else target._primary_keys()
        if not resolved_keys:
            raise ValueError(
                "mode='scd2' requires keys; pass keys=... or declare primary_key columns"
            )
        if compare_columns is None:
            all_cols = [n for n, _ in target._columns()]
            resolved_compares = [
                c
                for c in all_cols
                if c not in resolved_keys
                and c not in _METADATA_COLS
                and c not in _SCD2_META_COLS
            ]
        else:
            resolved_compares = list(compare_columns)
        if not resolved_compares:
            raise ValueError(
                "mode='scd2' requires at least one compare column; pass compare_columns=..."
            )
        return target_connection.run_scd2(
            augmented_json,
            source.query,
            name,
            resolved_keys,
            resolved_compares,
            src_arg,
        )

    # merge / scd1
    resolved_keys = list(keys) if keys else target._primary_keys()
    if not resolved_keys:
        raise ValueError(
            f"mode={mode!r} requires keys; pass keys=... or declare primary_key columns"
        )
    if update_columns is None:
        all_cols = [n for n, _ in target._columns()]
        resolved_updates = [
            c for c in all_cols if c not in resolved_keys and c not in _METADATA_COLS
        ]
    else:
        resolved_updates = list(update_columns)
    return target_connection.run_merge(
        augmented_json,
        source.query,
        name,
        resolved_keys,
        resolved_updates,
        mode,
        src_arg,
    )
