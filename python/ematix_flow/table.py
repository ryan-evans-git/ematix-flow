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

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
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
    def _to_spec(cls) -> dict[str, Any]:
        return {
            "schema": cls.__schema__,
            "name": cls.__tablename__,
            "columns": [col.to_spec() for _, col in cls.__columns__],
        }

    @classmethod
    def _to_normalized_spec(cls) -> dict[str, Any]:
        return json.loads(_core.parse_table_spec(json.dumps(cls._to_spec())))
