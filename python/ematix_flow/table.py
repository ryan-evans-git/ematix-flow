"""`ManagedTable` declarative base.

Subclasses declare `__schema__`, `__tablename__`, and one or more `Column`
attributes. The class collects columns in declaration order and exposes
`_to_spec()` / `_to_normalized_spec()` for the Rust DDL planner.
"""

from __future__ import annotations

import json
from typing import Any, ClassVar

from ematix_flow import _core
from ematix_flow.types import Column


class ManagedTable:
    __schema__: ClassVar[str]
    __tablename__: ClassVar[str]
    __columns__: ClassVar[tuple[tuple[str, Column], ...]]
    __unique_constraints__: ClassVar[tuple[tuple[str, ...], ...]] = ()
    # Phase 23: explicit class-level merge-key default. None = unset, fall
    # through to __unique_constraints__[0] then primary keys.
    __merge_keys__: ClassVar[tuple[str, ...] | None] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Abstract bases (e.g., FeatureView) opt out of validation by
        # setting `__abstract__ = True` in the class body; concrete
        # subclasses still go through the full check.
        if cls.__dict__.get("__abstract__"):
            return
        if "__schema__" not in {k for c in cls.__mro__ for k in c.__dict__}:
            raise TypeError(f"{cls.__name__} must define __schema__")
        if "__tablename__" not in {k for c in cls.__mro__ for k in c.__dict__}:
            raise TypeError(f"{cls.__name__} must define __tablename__")

        columns: list[tuple[str, Column]] = []
        for name, value in cls.__dict__.items():
            if isinstance(value, Column):
                value.name = name
                columns.append((name, value))
        if not columns:
            raise TypeError(f"{cls.__name__} must declare at least one Column")
        cls.__columns__ = tuple(columns)

    @classmethod
    def _columns(cls) -> tuple[tuple[str, Column], ...]:
        return cls.__columns__

    @classmethod
    def _primary_keys(cls) -> list[str]:
        return [name for name, col in cls.__columns__ if col.primary_key]

    @classmethod
    def _unique_constraints(cls) -> list[list[str]]:
        return [list(uc) for uc in cls.__unique_constraints__]

    @classmethod
    def _to_spec(cls) -> dict[str, Any]:
        return {
            "schema": cls.__schema__,
            "name": cls.__tablename__,
            "columns": [col.to_spec() for _, col in cls.__columns__],
            "unique_constraints": cls._unique_constraints(),
        }

    @classmethod
    def _to_normalized_spec(cls) -> dict[str, Any]:
        return json.loads(_core.parse_table_spec(json.dumps(cls._to_spec())))

    @classmethod
    def ensure(cls, conn: Any, on_drift: str = "error") -> dict[str, Any]:
        """Pre-flight: create the target table if missing, or compare against
        the live schema. Raises ValueError if drift is detected and
        `on_drift="error"` (the default).
        """
        return conn.ensure_table(json.dumps(cls._to_spec()), on_drift)
