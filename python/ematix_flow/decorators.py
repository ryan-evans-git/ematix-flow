"""Phase 24: the `ematix` decorator namespace.

`@ematix.table(schema=...)` is a class decorator that reads PEP 593
`Annotated[T, pk()]` annotations and produces a `ManagedTable` subclass
with the columns and constraints inferred from the type hints.

`@ematix.pipeline(...)` lands in Phase 24b alongside `ematix.target(...)`
and the multi-target / source_table support.
"""

from __future__ import annotations

import re
import sys
import types as _types
from typing import Any, Union, get_args, get_origin

from ematix_flow.markers import (
    _NaturalKeyMarker,
    _NullableMarker,
    _PkMarker,
)
from ematix_flow.table import ManagedTable
from ematix_flow.types import Column, ColumnType, _Nullable


_SNAKE_CASE_BOUNDARY = re.compile(r"(.)([A-Z][a-z]+)")
_SNAKE_CASE_LOWER_UPPER = re.compile(r"([a-z0-9])([A-Z])")


def _snake_case(name: str) -> str:
    """`CustomerDim` -> `customer_dim`, `HTTPServer` -> `http_server`."""
    s1 = _SNAKE_CASE_BOUNDARY.sub(r"\1_\2", name)
    return _SNAKE_CASE_LOWER_UPPER.sub(r"\1_\2", s1).lower()


def _is_optional(annotation: Any) -> tuple[bool, Any]:
    """Return `(is_optional, inner)` for `T | None` / `Optional[T]` / plain `T`."""
    origin = get_origin(annotation)
    if origin is Union or origin is _types.UnionType:
        args = get_args(annotation)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1 and len(args) == len(non_none) + 1:
            return True, non_none[0]
    return False, annotation


def _resolve_column_type(annotation: Any) -> tuple[ColumnType, list[Any]]:
    """Extract a `ColumnType` instance and any marker metadata from a type
    annotation. Handles `Annotated[T, ...]`, `T | None`, and bare types.

    Returns `(column_type, markers)` where `markers` is the list of
    metadata extras from `Annotated`.
    """
    markers: list[Any] = []

    # Unwrap Annotated. `Annotated[T, *meta]` exposes `.__metadata__` and
    # `.__origin__`. (`get_origin` returns the wrapped type, not Annotated
    # itself, so we detect via attribute presence.)
    if hasattr(annotation, "__metadata__"):
        markers.extend(annotation.__metadata__)
        annotation = annotation.__origin__

    # Unwrap our `_Nullable` wrapper from `ColumnType_instance | None`.
    is_optional = False
    if isinstance(annotation, _Nullable):
        is_optional = True
        annotation = annotation.inner

    # Unwrap typing-style `T | None` / `Optional[T]` for class-typed cases
    # like `BigInt | None`.
    typing_optional, inner = _is_optional(annotation)
    if typing_optional:
        is_optional = True
        annotation = inner

    # `annotation` is now either a parameter-less `ColumnType` subclass
    # (e.g., `BigInt`, `Boolean`, `Text`) or an instance (e.g., `String(64)`,
    # `String[64]` which evaluates to an instance).
    if isinstance(annotation, ColumnType):
        ty = annotation
    elif isinstance(annotation, type) and issubclass(annotation, ColumnType):
        # Try to instantiate parameter-less types.
        try:
            ty = annotation()
        except TypeError as e:
            raise TypeError(
                f"column type {annotation.__name__} requires arguments — "
                f"use `{annotation.__name__}[...]` or `{annotation.__name__}(...)`"
            ) from e
    else:
        raise TypeError(
            f"unsupported column type annotation {annotation!r}; "
            "expected a ColumnType subclass or instance"
        )

    if is_optional:
        markers.append(_NullableMarker())

    return ty, markers


def _check_no_competing_decorators(cls: type) -> None:
    """Reject `@dataclass` / `@attrs.define` / Pydantic `BaseModel` stacking.

    All three competing decorators introspect annotations and store
    their own per-field metadata that conflicts with our marker model.
    """
    if hasattr(cls, "__dataclass_fields__"):
        raise TypeError(
            f"{cls.__name__} is already decorated with @dataclass. "
            "@ematix.table provides field-collection on its own; "
            "remove @dataclass or use the imperative `class X(ManagedTable)` "
            "form instead."
        )
    if hasattr(cls, "__attrs_attrs__"):
        raise TypeError(
            f"{cls.__name__} is already decorated with @attrs.define. "
            "Remove it or use the imperative `class X(ManagedTable)` form."
        )
    # Pydantic v2 BaseModel exposes __pydantic_fields__; v1 used __fields__.
    if hasattr(cls, "__pydantic_fields__") or (
        hasattr(cls, "__fields__")
        and any(t.__name__ == "BaseModel" for t in cls.__mro__)
    ):
        raise TypeError(
            f"{cls.__name__} is a pydantic BaseModel. "
            "@ematix.table can't compose with pydantic; use a plain class "
            "annotated with our markers, or the imperative "
            "`class X(ManagedTable)` form."
        )


class _EmatixNamespace:
    """The `ematix` import handle. `from ematix_flow import ematix`."""

    def table(
        self,
        *,
        schema: str,
        name: str | None = None,
    ):
        """Class decorator. Build a `ManagedTable` subclass from PEP 593
        type annotations.
        """

        def decorate(cls: type) -> type[ManagedTable]:
            _check_no_competing_decorators(cls)

            tablename = name or _snake_case(cls.__name__)

            # Resolve annotations. We don't use `typing.get_type_hints`
            # because our parameterized types (`String[256]`,
            # `Numeric[12, 2]`) evaluate to ColumnType *instances*, which
            # `get_type_hints` rejects as not-a-type. Instead we eval each
            # annotation directly in the class's module globals, which
            # handles `from __future__ import annotations` (PEP 563)
            # transparently.
            module = sys.modules.get(cls.__module__)
            globalns: dict[str, Any] = vars(module) if module else {}
            raw_annotations = getattr(cls, "__annotations__", {})
            annotations: dict[str, Any] = {}
            for ann_name, ann_value in raw_annotations.items():
                if isinstance(ann_value, str):
                    annotations[ann_name] = eval(ann_value, globalns)
                else:
                    annotations[ann_name] = ann_value

            # Build per-column attributes for the eventual ManagedTable.
            attrs: dict[str, Any] = {
                "__schema__": schema,
                "__tablename__": tablename,
            }
            unique_groups: dict[str, list[str]] = {}

            for col_name, annotation in annotations.items():
                ty, markers = _resolve_column_type(annotation)

                primary_key = any(
                    isinstance(m, _PkMarker) for m in markers
                )
                explicit_nullable = any(
                    isinstance(m, _NullableMarker) for m in markers
                )
                # nullable from `T | None` already added a marker; honor it.
                nullable_flag = explicit_nullable

                column = Column(
                    type=ty,
                    nullable=nullable_flag,
                    primary_key=primary_key,
                )
                attrs[col_name] = column

                # Collect natural-key markers into unique-constraint groups.
                for m in markers:
                    if isinstance(m, _NaturalKeyMarker):
                        unique_groups.setdefault(m.group, []).append(col_name)

            if unique_groups:
                # Stable order: default group first ("" sorts before others).
                attrs["__unique_constraints__"] = tuple(
                    tuple(unique_groups[g]) for g in sorted(unique_groups)
                )

            # Carry forward any non-annotation class-level dunders the user
            # might have set (`__merge_keys__`, `__unique_constraints__` if
            # they overrode it manually).
            for dunder in ("__merge_keys__",):
                if dunder in cls.__dict__:
                    attrs[dunder] = cls.__dict__[dunder]
            if "__unique_constraints__" in cls.__dict__:
                attrs["__unique_constraints__"] = cls.__dict__[
                    "__unique_constraints__"
                ]

            # Carry forward docstring + module so the result looks sensible
            # in introspection.
            attrs["__doc__"] = cls.__doc__
            attrs["__module__"] = cls.__module__
            attrs["__qualname__"] = cls.__qualname__

            new_cls = type(cls.__name__, (ManagedTable,), attrs)
            return new_cls

        return decorate


ematix = _EmatixNamespace()


__all__ = ["ematix"]
