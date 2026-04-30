"""Phase 24: the `ematix` decorator namespace.

`@ematix.table(schema=...)` is a class decorator that reads PEP 593
`Annotated[T, pk()]` annotations and produces a `ManagedTable` subclass.

`@ematix.pipeline(...)` is a function decorator that wraps `pipeline.sync`
and registers via `pipeline.register`. `ematix.target(Cls, mode=..., **kw)`
named-constructor wraps per-target options for multi-target pipelines.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import re
import sys
import types as _types
from dataclasses import dataclass, field
from typing import Any, Callable, Union, get_args, get_origin

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


def _target_user_columns(target_cls: type[ManagedTable]) -> list[str]:
    """Return the column names declared on a ManagedTable subclass.

    These are the columns the user wrote — not the auto-augmented
    metadata columns (`_loaded_at`, `_batch_id`, `valid_from`, etc.)
    that the strategies layer in at sync time.
    """
    return [name for name, _col in target_cls._columns()]


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


@dataclass(frozen=True)
class Target:
    """Phase 24b: per-target options for a multi-target pipeline.

    Built via `ematix.target(Cls, mode=..., **kwargs)` rather than directly.
    """

    target_class: type[ManagedTable]
    mode: str
    target_connection: str | None = None
    keys: tuple[str, ...] | None = None
    update_columns: tuple[str, ...] | None = None
    compare_columns: tuple[str, ...] | None = None
    event_timestamp_column: str | None = None
    handle_deletes: str | None = None
    column_map: dict[str, str] | None = None


def _validate_qualified_table(value: str) -> None:
    parts = value.split(".")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(
            f"source_table requires schema.table format (e.g., 'public.users'); "
            f"got {value!r}"
        )


def _signature_arity(fn: Callable[..., Any]) -> int:
    sig = inspect.signature(fn)
    params = [
        p
        for p in sig.parameters.values()
        if p.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]
    return len(params)


def _resolve_named_connection(name: str | None) -> Any:
    """Resolve a connection by name via Phase 21's registry. None = default."""
    from ematix_flow import config

    return config.connect(name) if name else config.connect()


def _synth_source_sql(
    source_table: str,
    target_columns: list[str],
    column_map: dict[str, str] | None,
) -> str:
    """Build `SELECT col1 [AS alias], col2, ... FROM schema.table` from a
    target's column list and an optional column_map (target → source name).
    Validates that each column_map key is a target column.
    """
    column_map = column_map or {}
    for target_col in column_map:
        if target_col not in target_columns:
            raise ValueError(
                f"column_map key {target_col!r} is not a declared target column "
                f"(have: {target_columns})"
            )
    selects: list[str] = []
    for col in target_columns:
        if col in column_map:
            selects.append(f"{column_map[col]} AS {col}")
        else:
            selects.append(col)
    return f"SELECT {', '.join(selects)} FROM {source_table}"


class _EmatixNamespace:
    """The `ematix` import handle. `from ematix_flow import ematix`."""

    @staticmethod
    def target(
        target_class: type[ManagedTable],
        *,
        mode: str,
        target_connection: str | None = None,
        keys: tuple[str, ...] | None = None,
        update_columns: tuple[str, ...] | None = None,
        compare_columns: tuple[str, ...] | None = None,
        event_timestamp_column: str | None = None,
        handle_deletes: str | None = None,
        column_map: dict[str, str] | None = None,
    ) -> Target:
        return Target(
            target_class=target_class,
            mode=mode,
            target_connection=target_connection,
            keys=keys,
            update_columns=update_columns,
            compare_columns=compare_columns,
            event_timestamp_column=event_timestamp_column,
            handle_deletes=handle_deletes,
            column_map=column_map,
        )

    def pipeline(
        self,
        *,
        target: type[ManagedTable] | None = None,
        targets: list[Target] | None = None,
        schedule: str,
        mode: str | None = None,
        name: str | None = None,
        source_connection: str | None = None,
        target_connection: str | None = None,
        source_table: str | None = None,
        column_map: dict[str, str] | None = None,
        keys: tuple[str, ...] | None = None,
        update_columns: tuple[str, ...] | None = None,
        compare_columns: tuple[str, ...] | None = None,
        event_timestamp_column: str | None = None,
        handle_deletes: str | None = None,
        incremental_column: str | None = None,
        on_drift: str = "error",
        force_path: str | None = None,
        continue_on_failure: bool = False,
    ):
        """Function decorator. Wraps `pipeline.sync` and registers via the
        Phase 12 scheduling registry.
        """
        # Mutual-exclusion checks.
        if target is None and targets is None:
            raise TypeError(
                "@ematix.pipeline requires either target= or targets=[...]"
            )
        if target is not None and targets is not None:
            raise TypeError(
                "@ematix.pipeline accepts target= OR targets=, not both"
            )
        if targets is not None and not targets:
            raise TypeError("targets=[...] must be a non-empty list")
        if source_table is not None:
            _validate_qualified_table(source_table)
        if target is not None and mode is None:
            raise TypeError(
                "@ematix.pipeline(target=...) requires mode=..."
            )

        def decorate(fn: Callable[..., Any]):
            arity = _signature_arity(fn)
            is_async = inspect.iscoroutinefunction(fn)
            if arity not in (0, 1, 2):
                raise TypeError(
                    f"@ematix.pipeline-decorated function must take 0, 1, or 2 "
                    f"connection arguments; {fn.__name__} takes {arity}"
                )
            if arity == 0 and source_table is None:
                raise TypeError(
                    f"@ematix.pipeline-decorated function {fn.__name__} takes 0 "
                    "args but no source_table= was given; either declare "
                    "source_table= or accept a connection argument and return SQL"
                )

            @functools.wraps(fn)
            def wrapped() -> Any:
                # Resolve connections.
                tgt_conn_name = target_connection
                src_conn_name = source_connection
                tgt_conn = _resolve_named_connection(tgt_conn_name)
                if arity == 2:
                    src_conn = _resolve_named_connection(src_conn_name)
                else:
                    src_conn = tgt_conn  # same-DB

                # Call the user's function with the matching args.
                call_args: tuple[Any, ...]
                if arity == 0:
                    call_args = ()
                elif arity == 1:
                    call_args = (tgt_conn,)
                else:
                    call_args = (src_conn, tgt_conn)
                result = fn(*call_args)
                if is_async:
                    result = asyncio.run(result)

                # Build the source SQL / Source object.
                from ematix_flow.source import Source as _Source

                if isinstance(result, str):
                    source_sql = result
                    source_obj = _Source.postgres_query(src_conn, source_sql)
                elif isinstance(result, _Source):
                    source_obj = result
                elif result is None:
                    if source_table is None:
                        raise RuntimeError(
                            f"pipeline {fn.__name__!r} returned None and has no "
                            "source_table=; declare source_table= or return SQL"
                        )
                    target_columns = (
                        _target_user_columns(target) if target else None
                    )
                    if target_columns is None:
                        # multi-target source_table is not currently supported;
                        # this path implies single target.
                        raise RuntimeError(
                            "source_table= with multi-target pipelines is not "
                            "supported; write the source SQL in the function body"
                        )
                    source_sql = _synth_source_sql(
                        source_table, target_columns, column_map
                    )
                    source_obj = _Source.postgres_query(src_conn, source_sql)
                elif isinstance(result, dict):
                    return result  # advanced escape hatch
                else:
                    raise TypeError(
                        f"pipeline {fn.__name__!r} returned unsupported value "
                        f"of type {type(result).__name__}; expected str / Source "
                        "/ None / dict"
                    )

                # Single vs multi-target dispatch.
                from ematix_flow import pipeline as _p

                if target is not None:
                    return _p.sync(
                        target=target,
                        source=source_obj,
                        target_connection=tgt_conn,
                        mode=mode,  # type: ignore[arg-type]
                        pipeline_name=name or fn.__name__,
                        keys=keys,
                        update_columns=update_columns,
                        compare_columns=compare_columns,
                        event_timestamp_column=event_timestamp_column,
                        handle_deletes=handle_deletes,
                        incremental_column=incremental_column,
                        on_drift=on_drift,
                        force_path=force_path,
                    )

                # Multi-target.
                results: dict[str, Any] = {}
                errors: dict[str, Exception] = {}
                for t in targets or []:
                    try:
                        per_tgt_conn = (
                            _resolve_named_connection(t.target_connection)
                            if t.target_connection
                            else tgt_conn
                        )
                        per_result = _p.sync(
                            target=t.target_class,
                            source=source_obj,
                            target_connection=per_tgt_conn,
                            mode=t.mode,  # type: ignore[arg-type]
                            pipeline_name=(
                                f"{name or fn.__name__}::"
                                f"{t.target_class.__schema__}."
                                f"{t.target_class.__tablename__}"
                            ),
                            keys=t.keys,
                            update_columns=t.update_columns,
                            compare_columns=t.compare_columns,
                            event_timestamp_column=t.event_timestamp_column,
                            handle_deletes=t.handle_deletes,
                        )
                        key = (
                            f"{t.target_class.__schema__}."
                            f"{t.target_class.__tablename__}"
                        )
                        results[key] = per_result
                    except Exception as e:
                        key = (
                            f"{t.target_class.__schema__}."
                            f"{t.target_class.__tablename__}"
                        )
                        errors[key] = e
                        if not continue_on_failure:
                            raise
                if errors:
                    return {"results": results, "errors": {k: str(v) for k, v in errors.items()}}
                return results

            wrapped.__wrapped__ = fn  # type: ignore[attr-defined]

            # Register with the Phase 12 scheduling registry.
            from ematix_flow import pipeline as _p

            _p.register(name=name or fn.__name__, schedule=schedule)(wrapped)

            return wrapped

        return decorate

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
