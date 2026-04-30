"""`Pipeline` declarative spec.

Phase 1: a minimal dataclass that ships a JSON-serialized spec to the Rust
core and returns the normalized version. Phase 5 will add `pipeline.sync(...)`
on top of this same spec shape.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from ematix_flow import _core

Mode = Literal["append", "truncate", "merge", "scd1", "scd2"]


@dataclass(frozen=True)
class Source:
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
        """Round-trip through the Rust core: parse, normalize, validate."""
        return json.loads(_core.parse_spec(json.dumps(self.to_spec_dict())))
