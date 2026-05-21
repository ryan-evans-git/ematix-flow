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
import json
import re
import sys
import types as _types
from collections.abc import Callable
from dataclasses import dataclass
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


def _connection_info_safe(name: str | None) -> tuple[str | None, dict[str, Any] | None]:
    """Resolve a named connection's (host, port, dbname, user) tuple
    without raising when the connection isn't configured. Used by
    preview() so unconfigured connections don't block plan rendering.
    """
    if name is None:
        return None, None
    try:
        conn = _resolve_named_connection(name)
        return name, conn.connection_info()
    except Exception:
        return name, None


def _resolve_keys_with_reason(
    target_cls: type[ManagedTable], passed: tuple[str, ...] | None, mode: str
) -> tuple[list[str], str]:
    """Mirror pipeline._resolve_merge_keys but also report the reason."""
    if passed:
        return list(passed), "from explicit keys="
    dunder = getattr(target_cls, "__merge_keys__", None)
    if dunder:
        return list(dunder), "from __merge_keys__"
    uniques = getattr(target_cls, "__unique_constraints__", ())
    if uniques:
        return list(uniques[0]), "from natural_key() / __unique_constraints__"
    pks = target_cls._primary_keys()
    return list(pks), "from primary_key=True"


def _build_preview(
    *,
    fn: Callable[..., Any],
    arity: int,
    is_async: bool,
    target: type[ManagedTable] | None,
    targets: list[Any] | None,
    mode: str | None,
    name: str,
    schedule: str | None,
    source_connection: str | None,
    target_connection: str | None,
    source_table: str | None,
    column_map: dict[str, str] | None,
    keys: tuple[str, ...] | None,
    update_columns: tuple[str, ...] | None,
    compare_columns: tuple[str, ...] | None,
    event_timestamp_column: str | None,
    handle_deletes: str | None,
    dry_run: bool,
    transforms_pre: list[Any] | None = None,
    transforms_post: list[Any] | None = None,
) -> Any:
    """Phase 25: synthesize the plan a pipeline would execute.

    For preview (dry_run=False): no DB calls. Connections are resolved
    only to fetch their info; if a connection is missing, we render the
    plan with a placeholder note instead of failing.

    For dry_run (dry_run=True): not implemented in this commit; raises
    NotImplementedError for now (lands in the Phase 25 dry_run commit).
    """
    from ematix_flow.preview import PreviewResult, TargetPlan

    if dry_run:
        return _execute_dry_run(
            fn=fn,
            arity=arity,
            is_async=is_async,
            target=target,
            targets=targets,
            mode=mode,
            name=name,
            schedule=schedule,
            source_connection=source_connection,
            target_connection=target_connection,
            source_table=source_table,
            column_map=column_map,
            keys=keys,
            update_columns=update_columns,
            compare_columns=compare_columns,
            event_timestamp_column=event_timestamp_column,
            handle_deletes=handle_deletes,
            transforms_pre=transforms_pre or [],
        )

    notes: list[str] = []

    src_name, src_info = _connection_info_safe(source_connection)
    _tgt_name, tgt_info = _connection_info_safe(target_connection)
    if source_connection and src_info is None:
        notes.append(
            f"source connection {source_connection!r} not currently configured"
        )
    if target_connection and tgt_info is None:
        notes.append(
            f"target connection {target_connection!r} not currently configured"
        )

    # Build the source SQL without invoking a real connection.
    source_sql: str
    base_source_sql: str
    if source_table is not None and arity == 0:
        # source_table-only path: synthesize SELECT directly.
        target_columns = (
            _target_user_columns(target) if target is not None else None
        )
        if target_columns is None:
            base_source_sql = f"SELECT * FROM {source_table}"
        else:
            base_source_sql = _synth_source_sql(
                source_table, target_columns, column_map
            )
        source_sql = base_source_sql
    else:
        # Function-body path: call the user's function with placeholder
        # connections. We pass simple sentinels — the function should
        # only use them to return SQL strings; actual queries happen at
        # sync time.
        try:
            placeholder = _PreviewConn()
            if arity == 0:
                ret = fn()
            elif arity == 1:
                ret = fn(placeholder)
            else:
                ret = fn(placeholder, placeholder)
            if is_async:
                import asyncio as _aio

                ret = _aio.run(ret)
            if isinstance(ret, str):
                source_sql = ret
            elif ret is None and source_table is not None:
                target_columns = (
                    _target_user_columns(target) if target is not None else None
                )
                source_sql = (
                    _synth_source_sql(source_table, target_columns or [], column_map)
                    if target_columns is not None
                    else f"SELECT * FROM {source_table}"
                )
            else:
                source_sql = str(ret) if ret is not None else "-- (no source query)"
                notes.append(
                    "function returned a non-string; preview shows the source as-is"
                )
        except Exception as e:
            source_sql = "-- (could not invoke function for preview)"
            notes.append(f"function call failed during preview: {e}")

    # Apply per-column normalization + transforms_pre to the source SQL
    # so preview() shows the same SQL the strategy will see at sync time.
    if target is not None and source_sql.strip() and not source_sql.startswith("--"):
        from ematix_flow.normalize import apply_normalization as _apply_norm

        try:
            source_sql = _apply_norm(target, source_sql, transforms_pre or [])
        except Exception as e:  # pragma: no cover — defensive
            notes.append(f"normalization synthesis failed: {e}")

    # Per-target plan synthesis.
    target_plans: list[TargetPlan] = []
    if target is not None:
        target_plans.append(
            _plan_one_target(
                target_cls=target,
                mode=mode or "merge",
                source_sql=source_sql,
                target_connection_name=target_connection,
                target_connection_info=tgt_info,
                keys=keys,
                update_columns=update_columns,
                compare_columns=compare_columns,
                event_timestamp_column=event_timestamp_column,
                column_map=column_map,
                source_connection=source_connection,
            )
        )
    elif targets is not None:
        for t in targets:
            per_tgt_name = t.target_connection or target_connection
            _, per_tgt_info = _connection_info_safe(per_tgt_name)
            target_plans.append(
                _plan_one_target(
                    target_cls=t.target_class,
                    mode=t.mode,
                    source_sql=source_sql,
                    target_connection_name=per_tgt_name,
                    target_connection_info=per_tgt_info,
                    keys=t.keys,
                    update_columns=t.update_columns,
                    compare_columns=t.compare_columns,
                    event_timestamp_column=t.event_timestamp_column,
                    column_map=t.column_map,
                    source_connection=source_connection,
                )
            )

    return PreviewResult(
        pipeline_name=name,
        schedule=schedule,
        mode=mode,
        source_connection_name=src_name,
        source_connection_info=src_info,
        source_sql=source_sql,
        targets=target_plans,
        is_dry_run=False,
        notes=notes,
        transforms_post=_summarize_transforms_post(transforms_post or []),
    )


def _summarize_transforms_post(entries: list[Any]) -> list[Any]:
    """Phase 27f: convert transforms_post entries into TransformPlan
    summaries for preview() rendering. No execution.
    """
    from ematix_flow.preview import TransformPlan

    plans = []
    for step in entries:
        if isinstance(step, str):
            summary = step.strip().splitlines()[0][:200]
            plans.append(TransformPlan(kind="sql", summary=summary))
        elif isinstance(step, TransformRef):
            plans.append(TransformPlan(kind="transform_ref", summary=step.name))
        elif callable(step):
            plans.append(
                TransformPlan(
                    kind="callable",
                    summary=getattr(step, "__name__", "<callable>"),
                )
            )
        else:
            plans.append(TransformPlan(kind="unknown", summary=repr(step)))
    return plans


class _PreviewConn:
    """Placeholder connection used during preview. The user's pipeline
    function receives this when constructing the source SQL; if they
    actually call methods on it, we surface a clear error.
    """

    def __getattr__(self, name: str) -> Any:
        raise RuntimeError(
            f"preview/dry_run cannot use Connection.{name} — pipeline functions "
            "should return a SQL string or Source without making DB calls "
            "themselves; for queries that need live data, use a function-body "
            "source SQL"
        )

    def connection_info(self) -> dict[str, Any]:
        return {"host": "?", "port": 0, "dbname": "?", "user": "?"}


def _plan_one_target(
    *,
    target_cls: type[ManagedTable],
    mode: str,
    source_sql: str,
    target_connection_name: str | None,
    target_connection_info: dict[str, Any] | None,
    keys: tuple[str, ...] | None,
    update_columns: tuple[str, ...] | None,
    compare_columns: tuple[str, ...] | None,
    event_timestamp_column: str | None,
    column_map: dict[str, str] | None,
    source_connection: str | None,
) -> Any:
    from ematix_flow import _core
    from ematix_flow.preview import TargetPlan

    # Augment the spec the way pipeline.sync would.
    if mode == "scd2":
        augmented_json = _core.augment_table_spec_scd2(
            json.dumps(target_cls._to_spec())
        )
    else:
        augmented_json = _core.augment_table_spec(json.dumps(target_cls._to_spec()))
    augmented = json.loads(augmented_json)

    declared = [name for name, _ in target_cls._columns()]
    augmented_columns = augmented["columns"]

    resolved_keys, keys_reason = _resolve_keys_with_reason(target_cls, keys, mode)

    compare_resolved: list[str] = []
    compare_reason = ""
    if mode == "scd2":
        if compare_columns is not None:
            compare_resolved = list(compare_columns)
            compare_reason = "from explicit compare_columns="
        else:
            _scd_metadata = (
                "_loaded_at",
                "_batch_id",
                "valid_from",
                "valid_to",
                "is_current",
                "row_hash",
            )
            compare_resolved = [
                c
                for c in declared
                if c not in resolved_keys and c not in _scd_metadata
            ]
            compare_reason = "auto-derived (non-key non-metadata)"
    elif mode in ("merge", "scd1"):
        if update_columns is not None:
            compare_resolved = list(update_columns)
            compare_reason = "from explicit update_columns="
        else:
            _merge_metadata = ("_loaded_at", "_batch_id")
            compare_resolved = [
                c
                for c in declared
                if c not in resolved_keys and c not in _merge_metadata
            ]
            compare_reason = "auto-derived (non-key non-metadata)"

    # Path decision: if source_connection != target_connection, cross-DB.
    if source_connection is not None and source_connection != target_connection_name:
        path = "cross_db"
        path_reason = (
            f"source connection {source_connection!r} != "
            f"target {target_connection_name!r}"
        )
    else:
        path = "same_db"
        path_reason = "source and target use the same connection"

    # SQL plan.
    plan_sql: list[str] = []
    plan_label = ""
    if mode == "append":
        plan_sql = [_core.plan_append_sql(augmented_json, source_sql)]
        plan_label = "INSERT...SELECT"
    elif mode == "truncate":
        plan_sql = list(_core.plan_truncate_sql(augmented_json, source_sql))
        plan_label = "TRUNCATE + INSERT"
    elif mode in ("merge", "scd1"):
        plan_sql = [
            _core.plan_merge_sql(
                augmented_json, source_sql, resolved_keys, compare_resolved
            )
        ]
        plan_label = "ON CONFLICT upsert"
    elif mode == "scd2":
        plan_sql = list(
            _core.plan_scd2_sql(
                augmented_json,
                source_sql,
                resolved_keys,
                compare_resolved,
                "preview_token",
                event_timestamp_column,
            )
        )
        plan_label = "SCD2 (CREATE TEMP / UPDATE / INSERT)"

    return TargetPlan(
        schema_qualified_name=f"{target_cls.__schema__}.{target_cls.__tablename__}",
        mode=mode,
        path=path,
        path_reason=path_reason,
        augmented_columns=augmented_columns,
        declared_columns=declared,
        merge_keys=resolved_keys,
        merge_keys_reason=keys_reason,
        compare_columns=compare_resolved,
        compare_columns_reason=compare_reason,
        target_connection_name=target_connection_name,
        target_connection_info=target_connection_info,
        plan_sql=plan_sql,
        plan_sql_label=plan_label,
    )


def _run_transforms_post(
    *,
    transforms_post: list[Any],
    target_connection: Any,
    target_cls: type[ManagedTable] | None,
    pipeline_name: str,
    parent_run_id: str | None,
    continue_on_failure: bool,
    sync_result: dict[str, Any] | None = None,
) -> None:
    """Phase 27a/b: run each transforms_post entry in its own transaction.

    Accepted entry types:
      - str: SQL string, executed on target_connection.
      - callable: invoked with (conn,) or (conn, parent) — arity
        auto-detected (Q3.2 γ). Optional dict return value is recorded
        as metrics_json (Q3.1 B).

    Halts on first failure unless continue_on_failure=True. Each step
    runs in its own transaction (failure of one doesn't roll back
    earlier successes).

    `transform_ref(...)` name-based lookups land in Phase 27c.
    """
    if not parent_run_id:
        # Defensive: no run_id means we can't link transforms back to a
        # parent. Skip rather than orphan rows.
        return

    target_schema = target_cls.__schema__ if target_cls else ""
    target_table = target_cls.__tablename__ if target_cls else ""

    parent_ctx = None
    if sync_result is not None:
        from ematix_flow.pipeline import ParentContext as _PC

        parent_ctx = _PC(
            pipeline_name=pipeline_name,
            run_id=parent_run_id,
            rows_inserted=int(sync_result.get("rows_inserted") or 0),
            rows_updated=int(sync_result.get("rows_updated") or 0),
            rows_unchanged=int(sync_result.get("rows_unchanged") or 0),
        )

    for i, step in enumerate(transforms_post):
        if isinstance(step, str):
            step_name = f"transform[{i}]:sql"
            try:
                target_connection.execute(step)
                target_connection.record_transform_history(
                    parent_run_id,
                    pipeline_name,
                    step_name,
                    "transform_success",
                    target_schema,
                    target_table,
                    None,
                    None,
                )
            except Exception as e:
                target_connection.record_transform_history(
                    parent_run_id,
                    pipeline_name,
                    step_name,
                    "transform_failed",
                    target_schema,
                    target_table,
                    str(e),
                    None,
                )
                if not continue_on_failure:
                    raise
        elif isinstance(step, TransformRef):
            from ematix_flow import pipeline as _p

            step_name = f"transform[{i}]:ref={step.name}"
            try:
                _resolve_and_run_transform_ref(
                    ref=step,
                    target_connection=target_connection,
                    parent_ctx=parent_ctx,
                    pipeline_name=pipeline_name,
                    step_name=step_name,
                    target_schema=target_schema,
                    target_table=target_table,
                    parent_run_id=parent_run_id,
                )
            except Exception:
                if not continue_on_failure:
                    raise
        elif callable(step):
            from ematix_flow import pipeline as _p

            arity = _transform_arity(step)
            step_name = f"transform[{i}]:{getattr(step, '__name__', 'callable')}"
            try:
                _p._invoke_transform_callable(
                    fn=step,
                    arity=arity,
                    conn=target_connection,
                    parent=parent_ctx,
                    pipeline_name=pipeline_name,
                    step_name=step_name,
                    target_schema=target_schema,
                    target_table=target_table,
                    parent_run_id=parent_run_id,
                )
            except Exception:
                if not continue_on_failure:
                    raise
        else:
            raise TypeError(
                f"transforms_post[{i}] is {type(step).__name__}; "
                "expected SQL string, callable, or transform_ref(...)"
            )


def _resolve_and_run_transform_ref(
    *,
    ref: TransformRef,
    target_connection: Any,
    parent_ctx: Any,
    pipeline_name: str,
    step_name: str,
    target_schema: str,
    target_table: str,
    parent_run_id: str | None,
) -> None:
    """Phase 27c: look up `ref.name` first in transforms registry, then
    pipelines registry. Transforms run with parent context (like a
    callable). Pipelines run their full sync independently (Q5 A).
    """
    from ematix_flow import pipeline as _p

    rt = _p._TRANSFORMS_REGISTRY.get(ref.name)
    if rt is not None:
        _p._invoke_transform_callable(
            fn=rt.fn,
            arity=rt.arity,
            conn=target_connection,
            parent=parent_ctx,
            pipeline_name=pipeline_name,
            step_name=step_name,
            target_schema=target_schema,
            target_table=target_table,
            parent_run_id=parent_run_id,
        )
        return

    sp = _p._REGISTRY.get(ref.name)
    if sp is not None:
        # Q5 A: chained pipelines run their full sync independently —
        # fresh connection resolution, own run_history row, recursive
        # transforms_post. We invoke the wrapped function directly.
        sp.fn()
        # Record a row tying the chained run to the parent for visibility.
        if parent_run_id:
            try:
                target_connection.record_transform_history(
                    parent_run_id,
                    pipeline_name,
                    step_name,
                    "transform_success",
                    target_schema,
                    target_table,
                    None,
                    None,
                )
            except Exception:
                pass
        return

    raise KeyError(
        f"transform_ref({ref.name!r}) not found in transforms or pipelines registry"
    )


def _execute_dry_run(
    *,
    fn: Callable[..., Any],
    arity: int,
    is_async: bool,
    target: type[ManagedTable] | None,
    targets: list[Any] | None,
    mode: str | None,
    name: str,
    schedule: str | None,
    source_connection: str | None,
    target_connection: str | None,
    source_table: str | None,
    column_map: dict[str, str] | None,
    keys: tuple[str, ...] | None,
    update_columns: tuple[str, ...] | None,
    compare_columns: tuple[str, ...] | None,
    event_timestamp_column: str | None,
    handle_deletes: str | None,
    transforms_pre: list[Any] | None = None,
) -> Any:
    """Phase 25b: execute the pipeline plan inside a transaction and
    rollback at the end. Returns a PreviewResult with `is_dry_run=True`
    and per-target `dry_run_rows_affected` populated.

    Builds the same plan as preview, then dispatches through pipeline.sync
    with `dry_run=True` for each target. Always continues through every
    target regardless of `continue_on_failure` so one bad target doesn't
    suppress others' errors (matches the locked plan §3.5).
    """
    from ematix_flow import pipeline as _p
    from ematix_flow.source import Source as _Source

    # Start from the standard preview plan.
    base = _build_preview(
        fn=fn,
        arity=arity,
        is_async=is_async,
        target=target,
        targets=targets,
        mode=mode,
        name=name,
        schedule=schedule,
        source_connection=source_connection,
        target_connection=target_connection,
        source_table=source_table,
        column_map=column_map,
        keys=keys,
        update_columns=update_columns,
        compare_columns=compare_columns,
        event_timestamp_column=event_timestamp_column,
        handle_deletes=handle_deletes,
        dry_run=False,  # building the metadata only
        transforms_pre=transforms_pre or [],
    )
    base.is_dry_run = True

    # Resolve the real connection (we'll actually run SQL through it).
    tgt_conn = _resolve_named_connection(target_connection)

    # Run the strategy with dry_run=True for each target.
    if target is not None:
        try:
            source_obj = _Source.postgres_query(tgt_conn, base.source_sql)
            counts = _p.sync(
                target=target,
                source=source_obj,
                target_connection=tgt_conn,
                mode=mode,  # type: ignore[arg-type]
                pipeline_name=name,
                keys=keys,
                update_columns=update_columns,
                compare_columns=compare_columns,
                event_timestamp_column=event_timestamp_column,
                handle_deletes=handle_deletes,
                dry_run=True,
            )
            tp = base.targets[0]
            for k in ("rows_inserted", "rows_updated", "rows_unchanged", "rows_closed"):
                if k in counts and isinstance(counts[k], int):
                    tp.dry_run_rows_affected[k] = counts[k]
        except Exception as e:
            base.targets[0].dry_run_error = str(e)
    elif targets is not None:
        for i, t in enumerate(targets):
            tp = base.targets[i]
            try:
                per_tgt_conn = (
                    _resolve_named_connection(t.target_connection)
                    if t.target_connection
                    else tgt_conn
                )
                source_obj = _Source.postgres_query(per_tgt_conn, base.source_sql)
                counts = _p.sync(
                    target=t.target_class,
                    source=source_obj,
                    target_connection=per_tgt_conn,
                    mode=t.mode,  # type: ignore[arg-type]
                    pipeline_name=f"{name}::{t.target_class.__schema__}.{t.target_class.__tablename__}",
                    keys=t.keys,
                    update_columns=t.update_columns,
                    compare_columns=t.compare_columns,
                    event_timestamp_column=t.event_timestamp_column,
                    handle_deletes=t.handle_deletes,
                    dry_run=True,
                )
                for k in ("rows_inserted", "rows_updated", "rows_unchanged", "rows_closed"):
                    if k in counts and isinstance(counts[k], int):
                        tp.dry_run_rows_affected[k] = counts[k]
            except Exception as e:
                tp.dry_run_error = str(e)
                # Always continue through every target (locked plan).

    return base


@dataclass(frozen=True)
class TransformRef:
    """Phase 27c: name-based reference to a registered pipeline or transform.

    `transforms_post=[ematix.transform_ref("name")]` looks the name up in
    `_TRANSFORMS_REGISTRY` first, then `_REGISTRY`. Pipelines run their
    full sync independently (Q5 A); transforms run with parent context.
    """

    name: str


def _transform_arity(fn: Callable[..., Any]) -> int:
    """Phase 27 Q3.2 (γ): auto-detect 1- or 2-arg transform callable."""
    arity = _signature_arity(fn)
    if arity not in (1, 2):
        raise TypeError(
            f"@ematix.transform-decorated function must take 1 (conn) or 2 "
            f"(conn, parent) arguments; {fn.__name__} takes {arity}"
        )
    return arity


class _EmatixNamespace:
    """The `ematix` import handle. `from ematix_flow import ematix`."""

    def feature_view(
        self,
        *,
        schema: str,
        feature_version: str,
        name: str | None = None,
        event_timestamp_column: str | None = None,
        ttl: Any = None,  # timedelta | None
        freshness_sla: Any = None,  # timedelta | None
        online: bool = False,
        description: str | None = None,
        owner: str | None = None,
    ):
        """Phase 17: declare a FeatureView (ManagedTable + ML dunders).

        Behaves like `@ematix.table` but also: registers in
        `pipeline._FEATURE_VIEWS_REGISTRY`, records ML dunders on the
        class, and signals that `pipeline.sync` should default
        `mode='scd2'` and inherit `event_timestamp_column` / `ttl` from
        the class when the user doesn't override.
        """
        from datetime import timedelta as _td

        def decorate(cls: type) -> type[ManagedTable]:
            built = self.table(schema=schema, name=name)(cls)

            # Validate at least one primary key.
            if not built._primary_keys():
                raise TypeError(
                    f"FeatureView {built.__name__} must declare at least one "
                    "primary key (use `pk()` on the entity-key column)"
                )

            # Phase 18: re-parent the class to FeatureView so PIT methods
            # (point_in_time / historical_features) are available.
            from ematix_flow.feature_view import FeatureView as _FV

            attrs = dict(built.__dict__)
            for skip in ("__dict__", "__weakref__"):
                attrs.pop(skip, None)
            built = type(built.__name__, (_FV,), attrs)
            # Validate event_timestamp_column references a real column.
            if event_timestamp_column is not None:
                cols = {n for n, _ in built._columns()}
                if event_timestamp_column not in cols:
                    raise ValueError(
                        f"event_timestamp_column={event_timestamp_column!r} "
                        f"is not a declared column on {built.__name__} "
                        f"(have: {sorted(cols)})"
                    )

            built.__feature_version__ = feature_version
            built.__event_timestamp_column__ = event_timestamp_column
            built.__ttl__ = ttl if isinstance(ttl, _td) or ttl is None else _td(seconds=int(ttl))
            built.__freshness_sla__ = (
                freshness_sla
                if isinstance(freshness_sla, _td) or freshness_sla is None
                else _td(seconds=int(freshness_sla))
            )
            built.__online__ = bool(online)
            built.__description__ = description
            built.__owner__ = owner
            built.__is_feature_view__ = True

            from ematix_flow import pipeline as _p

            entity_keys = built._primary_keys()
            ttl_seconds = (
                int(built.__ttl__.total_seconds()) if built.__ttl__ is not None else None
            )
            sla_seconds = (
                int(built.__freshness_sla__.total_seconds())
                if built.__freshness_sla__ is not None
                else None
            )
            fv = _p.RegisteredFeatureView(
                schema=schema,
                tablename=built.__tablename__,
                feature_version=feature_version,
                entity_keys=list(entity_keys),
                event_timestamp_column=event_timestamp_column,
                ttl_seconds=ttl_seconds,
                freshness_sla_seconds=sla_seconds,
                online=bool(online),
                description=description,
                owner=owner,
                cls=built,
            )
            _p._FEATURE_VIEWS_REGISTRY[fv.name] = fv
            return built

        return decorate

    @staticmethod
    def training_set(
        conn: Any,
        *,
        spine: list[dict[str, Any]],
        feature_views: list[type],
        columns: dict[type, list[str]] | None = None,
        prefer: str = "auto",
    ):
        """Phase 20: build a training dataset by asof-joining feature_views
        against spine. Returns a polars (default) or pandas DataFrame.
        See `ematix_flow.training` for details.
        """
        from ematix_flow.training import training_set as _ts

        return _ts(
            conn,
            spine=spine,
            feature_views=feature_views,
            columns=columns,
            prefer=prefer,
        )

    @staticmethod
    def transform_ref(name: str) -> TransformRef:
        """Reference a registered transform or pipeline by name.

        Used inside `transforms_post=[...]` when the target is registered
        elsewhere by name (rather than passed directly as a function).
        """
        if not isinstance(name, str):
            raise TypeError(
                f"transform_ref expects a str name, got {type(name).__name__}"
            )
        return TransformRef(name=name)

    def transform(
        self,
        *,
        name: str | None = None,
        target_connection: str | None = None,
        schedule: str | None = None,
    ):
        """Phase 27b: register a standalone-runnable post-load transformation.

        Decorated callable takes `(conn,)` or `(conn, parent)`; arity is
        auto-detected. Registered in `pipeline._TRANSFORMS_REGISTRY` and
        runnable via `flow transform run <name>` or by referencing the
        function directly inside `transforms_post=[...]`.
        """

        def decorate(fn: Callable[..., Any]) -> Callable[..., Any]:
            from ematix_flow import pipeline as _p

            tname = name or fn.__name__
            if tname in _p._TRANSFORMS_REGISTRY:
                raise ValueError(
                    f"a transform named {tname!r} is already registered"
                )
            arity = _transform_arity(fn)
            _p._TRANSFORMS_REGISTRY[tname] = _p.RegisteredTransform(
                name=tname,
                schedule=schedule,
                target_connection=target_connection,
                fn=fn,
                arity=arity,
            )
            return fn

        return decorate

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
        schedule: str | None,
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
        transforms_pre: list[Any] | None = None,
        transforms_post: list[Any] | None = None,
        continue_on_failure_post: bool = False,
        ttl: Any = None,
        # Phase Ω.1 — DAG dependencies + freshness gating.
        depends_on: list[str] | None = None,
        upstream_freshness_secs: int | None = None,
        # Phase Ω.2 — declarative retry policy. dict shape:
        #   {"max_attempts": N, "backoff": "fixed"|"linear"|"exponential",
        #    "base_secs": M, "max_backoff_secs": K}
        # See pipeline.RetryPolicy for field semantics.
        retry: dict | None = None,
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

        # Phase 17: FeatureView targets default to mode='scd2' and inherit
        # event_timestamp_column / ttl from class-level dunders unless
        # the user overrode them at the decorator level.
        if target is not None and getattr(target, "__is_feature_view__", False):
            if mode is None:
                mode = "scd2"
            if event_timestamp_column is None:
                event_timestamp_column = getattr(
                    target, "__event_timestamp_column__", None
                )
            if ttl is None:
                ttl = getattr(target, "__ttl__", None)

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
                from ematix_flow.normalize import apply_normalization
                from ematix_flow.source import Source as _Source

                if isinstance(result, str):
                    source_sql = result
                    if target is not None:
                        source_sql = apply_normalization(
                            target, source_sql, transforms_pre or []
                        )
                    source_obj = _Source.postgres_query(src_conn, source_sql)
                elif isinstance(result, _Source):
                    if transforms_pre:
                        raise RuntimeError(
                            "transforms_pre= is not supported when the pipeline "
                            "returns a Source object directly; return SQL instead"
                        )
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
                    if target is not None:
                        source_sql = apply_normalization(
                            target, source_sql, transforms_pre or []
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
                    sync_result = _p.sync(
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
                        ttl=ttl,
                    )
                    # Phase 17: refresh FeatureView metadata table.
                    if getattr(target, "__is_feature_view__", False):
                        try:
                            _p._upsert_feature_view_metadata_for_target(
                                target_connection=tgt_conn, target_cls=target
                            )
                        except Exception:
                            # Metadata is best-effort — don't fail the pipeline.
                            pass
                        # Phase 19: ensure + refresh online MV for online=True FVs.
                        try:
                            _p._ensure_online_artifacts(
                                target_connection=tgt_conn, target_cls=target
                            )
                            _p._refresh_online_view(
                                target_connection=tgt_conn, target_cls=target
                            )
                        except Exception:
                            pass

                    # Phase 27a/b: post-load transforms.
                    if transforms_post:
                        _run_transforms_post(
                            transforms_post=transforms_post,
                            target_connection=tgt_conn,
                            target_cls=target,
                            pipeline_name=name or fn.__name__,
                            parent_run_id=sync_result.get("run_id"),
                            continue_on_failure=continue_on_failure_post,
                            sync_result=sync_result,
                        )
                    return sync_result

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

            # Phase 25: hook for preview / dry-run rendering. Captures the
            # decoration-time configuration so pipeline.preview(name) can
            # synthesize the plan without re-running the user's function
            # against a real connection (or running it and rolling back).
            def _preview(*, dry_run: bool = False) -> Any:
                return _build_preview(
                    fn=fn,
                    arity=arity,
                    is_async=is_async,
                    target=target,
                    targets=targets,
                    mode=mode,
                    name=name or fn.__name__,
                    schedule=schedule,
                    source_connection=source_connection,
                    target_connection=target_connection,
                    source_table=source_table,
                    column_map=column_map,
                    keys=keys,
                    update_columns=update_columns,
                    compare_columns=compare_columns,
                    event_timestamp_column=event_timestamp_column,
                    handle_deletes=handle_deletes,
                    dry_run=dry_run,
                    transforms_pre=transforms_pre or [],
                    transforms_post=transforms_post or [],
                )

            wrapped._preview = _preview  # type: ignore[attr-defined]
            wrapped.preview = lambda: _preview(dry_run=False)  # type: ignore[attr-defined]
            wrapped.dry_run = lambda: _preview(dry_run=True)  # type: ignore[attr-defined]

            # Register with the Phase 12 scheduling registry.
            from ematix_flow import pipeline as _p

            _p.register(
                name=name or fn.__name__,
                schedule=schedule,
                depends_on=depends_on,
                upstream_freshness_secs=upstream_freshness_secs,
                retry=retry,
            )(wrapped)

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

            from ematix_flow.normalize import is_normalizer as _is_norm

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

                normalizers = tuple(m for m in markers if _is_norm(m))

                column = Column(
                    type=ty,
                    nullable=nullable_flag,
                    primary_key=primary_key,
                    normalizers=normalizers,
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

    @staticmethod
    def streaming_pipeline(
        *,
        name: str,
        source: Any = None,
        source_query: str | None = None,
        sources: list[Any] | None = None,
        target: Any = None,
        target_table: tuple[str, str] | None = None,
        target_topic: str | None = None,
        target_queue: str | None = None,
        target_partition_key_prefix: str | None = None,
        target_prefix: str | None = None,
        target_message_key_column: str | None = None,
        target_partition_by: list[str] | None = None,
        targets: list[Any] | None = None,
        idle_pause_ms: int = 500,
        dead_letter_topic: str | None = None,
        transform_sql: str | None = None,
        lookups: dict[str, Any] | None = None,
        window: Any = None,
        join: Any = None,
        state_store: Any = None,
        transform_on_error: str | None = None,
        watermark: Any = None,
        metrics_port: int | None = None,
    ):
        """Phase 39 / Π.4b: function decorator that wires a streaming
        pipeline declaratively.

        The decorator captures every kwarg at definition time and
        registers the pipeline by ``name``; calling the decorated
        function (with no arguments) runs the pipeline by delegating
        to :func:`ematix_flow.streaming.run_streaming_pipeline`. The
        decorated function's body is currently unused (reserved for
        future hooks like Python-side pre/post callbacks). It must
        accept zero arguments.

        Mirrors the kwargs of ``run_streaming_pipeline`` exactly:
        same source / target / multi-target / transform / lookup
        surface. See :class:`ematix_flow.streaming.Lookup` for the
        ``lookups=`` shape (Phase 39.2/39.3).

        Returns the decorated function unchanged in arity (still
        zero-arg) but with its call routed through the runner. The
        runner's metrics dict is the function's return value, so
        ``my_pipeline()`` is shorthand for
        ``run_streaming_pipeline(...)`` with the captured kwargs.
        """
        captured = {
            "name": name,
            "source": source,
            "source_query": source_query,
            "sources": sources,
            "target": target,
            "target_table": target_table,
            "target_topic": target_topic,
            "target_queue": target_queue,
            "target_partition_key_prefix": target_partition_key_prefix,
            "target_prefix": target_prefix,
            "target_message_key_column": target_message_key_column,
            "target_partition_by": target_partition_by,
            "targets": targets,
            "idle_pause_ms": idle_pause_ms,
            "dead_letter_topic": dead_letter_topic,
            "transform_sql": transform_sql,
            "lookups": lookups,
            "window": window,
            "join": join,
            "state_store": state_store,
            "transform_on_error": transform_on_error,
            "watermark": watermark,
            "metrics_port": metrics_port,
        }

        def decorate(fn: Callable[..., Any]):
            arity = _signature_arity(fn)
            if arity != 0:
                raise TypeError(
                    f"@ematix.streaming_pipeline-decorated function must take 0 "
                    f"arguments; {fn.__name__} takes {arity}"
                )

            @functools.wraps(fn)
            def wrapped() -> Any:
                from ematix_flow.streaming import run_streaming_pipeline

                # Hand off — the decorator is purely ergonomic.
                return run_streaming_pipeline(**captured)

            wrapped.__ematix_streaming_pipeline__ = captured  # type: ignore[attr-defined]

            # Π.3: register by name so `flow consume --module M name`
            # can look the pipeline up after importing the user's
            # module.
            from ematix_flow.streaming import register_streaming_pipeline

            register_streaming_pipeline(name, captured)
            return wrapped

        return decorate

    def warehouse_pipeline(
        self,
        *,
        source: Any,
        target: Any,
        schedule: str | None,
        name: str | None = None,
        depends_on: list[str] | None = None,
        upstream_freshness_secs: int | None = None,
        retry: dict | None = None,
    ):
        """Phase 2d slice 1: register a warehouse-to-warehouse pipeline
        for scheduling.

        ``source`` and ``target`` must be a
        :class:`~ematix_flow.warehouses.WarehouseSource` /
        :class:`~ematix_flow.warehouses.WarehouseTarget` respectively.
        The wrapped function runs once per schedule tick, executes
        :func:`~ematix_flow.warehouses.run_warehouse_pipeline`, and
        records a JSON-serializable summary to the configured RunLog.

        See :mod:`ematix_flow.warehouse_pipeline` for full surface
        documentation.
        """
        # Lazy import so the warehouses + decorator module pair don't
        # form an import-time cycle.
        from ematix_flow.warehouse_pipeline import warehouse_pipeline as _wp

        return _wp(
            source=source,
            target=target,
            schedule=schedule,
            name=name,
            depends_on=depends_on,
            upstream_freshness_secs=upstream_freshness_secs,
            retry=retry,
        )

    @staticmethod
    def connection(cls: type) -> Any:
        """Π.1: register a typed streaming connection from a declarative class.

        The class body declares the connection's fields (``kind``,
        ``bootstrap_servers``, etc.); the decorator builds the
        appropriate :class:`Connection` subclass instance, registers
        it under the class name, and *returns the instance* — so
        the user's module-level ``kafka_prod`` binding becomes the
        typed connection itself.

        Example::

            from ematix_flow import ematix

            @ematix.connection
            class kafka_prod:
                kind = "kafka"
                bootstrap_servers = "${KAFKA_BOOTSTRAP}"
                group_id = "ematix-flow"

        For instance-based registration (no decorator), use
        :func:`ematix_flow.register_connection` directly.
        """
        # Imported lazily so importing decorators doesn't require
        # connections.py at parse time.
        from ematix_flow import connections as _conn_mod

        return _conn_mod.connection(cls)


ematix = _EmatixNamespace()


__all__ = ["ematix"]
