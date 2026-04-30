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


def sync(
    *,
    target: type[ManagedTable],
    source: _ImperativeSource,
    target_connection: Any,
    mode: Mode = "append",
    pipeline_name: str | None = None,
    on_drift: str = "error",
) -> dict[str, Any]:
    """Execute a load. Phases 5–6 support `mode="append"` and `"truncate"`."""
    if mode not in ("append", "truncate"):
        raise NotImplementedError(f"mode={mode!r} is not yet implemented")

    name = pipeline_name or f"{target.__schema__}.{target.__tablename__}"
    augmented_json = _core.augment_table_spec(json.dumps(target._to_spec()))

    target_connection.ensure_table(augmented_json, on_drift)

    same_db = _same_database(source.connection, target_connection)
    src_arg = None if same_db else source.connection
    if mode == "append":
        return target_connection.run_append(augmented_json, source.query, name, src_arg)
    return target_connection.run_truncate(augmented_json, source.query, name, src_arg)
