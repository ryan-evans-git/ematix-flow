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


def _column_type_to_sql(target: type[ManagedTable], column: str) -> str:
    """Look up the Postgres SQL type for `column` on `target`. Used to
    cast a watermark text value back to the column's native type."""
    for name, col in target._columns():
        if name == column:
            spec = col.type.to_spec()
            kind = spec["kind"]
            mapping = {
                "small_int": "smallint",
                "integer": "integer",
                "big_int": "bigint",
                "float": "real",
                "double": "double precision",
                "boolean": "boolean",
                "text": "text",
                "date": "date",
                "timestamp": "timestamp",
                "timestamp_tz": "timestamptz",
                "json": "json",
                "jsonb": "jsonb",
                "uuid": "uuid",
                "bytes": "bytea",
            }
            if kind in mapping:
                return mapping[kind]
            if kind == "string":
                return f"varchar({spec['length']})"
            if kind == "numeric":
                return f"numeric({spec['precision']},{spec['scale']})"
            raise ValueError(f"unsupported incremental column type: {kind}")
    raise ValueError(
        f"incremental_column={column!r} is not declared on {target.__name__}"
    )


def _build_last_value_literal(value: str, sql_type: str) -> str:
    # Postgres accepts E'...' for text-quoted values; \\ → \\, ' → ''.
    escaped = value.replace("\\", "\\\\").replace("'", "''")
    return f"E'{escaped}'::{sql_type}"


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
    force_path: str | None = None,
    incremental_column: str | None = None,
) -> dict[str, Any]:
    """Execute a load. Phases 5–8 support 'append', 'truncate', 'merge'/'scd1', 'scd2'.
    Phase 10: pass `incremental_column='col'` for watermarked append loads."""
    if mode not in ("append", "truncate", "merge", "scd1", "scd2"):
        raise NotImplementedError(f"mode={mode!r} is not yet implemented")
    if force_path is not None and force_path not in ("same_db", "cross_db"):
        raise ValueError(
            f"force_path must be 'same_db', 'cross_db', or None (got {force_path!r})"
        )
    if incremental_column is not None and mode != "append":
        raise ValueError(
            f"incremental_column is only supported for mode='append'; got mode={mode!r}"
        )

    name = pipeline_name or f"{target.__schema__}.{target.__tablename__}"

    if mode == "scd2":
        augmented_json = _core.augment_table_spec_scd2(json.dumps(target._to_spec()))
    else:
        augmented_json = _core.augment_table_spec(json.dumps(target._to_spec()))

    target_connection.ensure_table(augmented_json, on_drift)

    if force_path == "same_db":
        same_db = True
    elif force_path == "cross_db":
        same_db = False
    else:
        same_db = _same_database(source.connection, target_connection)
    src_arg = None if same_db else source.connection

    if mode == "append":
        last_literal: str | None = None
        if incremental_column is not None:
            sql_type = _column_type_to_sql(target, incremental_column)
            existing = target_connection.read_watermark(name)
            if existing is not None and existing.get("column_name") == incremental_column:
                last_literal = _build_last_value_literal(
                    existing["last_value"], sql_type
                )
        return target_connection.run_append(
            augmented_json,
            source.query,
            name,
            src_arg,
            incremental_column,
            last_literal,
        )
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
