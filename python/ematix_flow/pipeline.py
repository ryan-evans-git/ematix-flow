"""`Pipeline` declarative spec + `pipeline.sync(...)` executor + scheduling registry.

Phase 1 introduced the `Pipeline` / `Source` / `Target` round-trip dataclasses;
Phase 5 adds `sync(...)` for executing a load against a live database;
Phase 12 adds `@register(name=..., schedule=...)` and helpers used by the
`flow` CLI to list, run, and run-due against a user's pipeline module.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
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


def _resolve_merge_keys(
    target: type[ManagedTable],
    *,
    passed: tuple[str, ...] | None,
    pipeline_name: str | None = None,
) -> list[str]:
    """Phase 23: pick the merge target columns.

    Resolution order (highest priority first):
      1. explicit `passed` (from pipeline.sync(keys=...) or @ematix.pipeline)
      2. `__merge_keys__` class dunder
      3. first `__unique_constraints__` entry
      4. `__primary_keys__` (derived from primary_key=True columns)

    When the resolved keys come from steps 2 or 3 and differ from the
    primary key, emit a UserWarning so the user knows what got picked.
    Pass an explicit `keys=` to silence.
    """
    import warnings

    pks = target._primary_keys()

    if passed is not None:
        return list(passed)

    dunder = getattr(target, "__merge_keys__", None)
    if dunder:
        resolved = list(dunder)
        if list(resolved) != list(pks):
            warnings.warn(
                f"pipeline {pipeline_name!r}: merge keys resolved to {tuple(resolved)} "
                f"from __merge_keys__, which differs from declared primary key "
                f"{tuple(pks)}. Pass keys=... explicitly to silence.",
                UserWarning,
                stacklevel=3,
            )
        return resolved

    uniques = getattr(target, "__unique_constraints__", ())
    if uniques:
        resolved = list(uniques[0])
        if list(resolved) != list(pks):
            warnings.warn(
                f"pipeline {pipeline_name!r}: merge keys resolved to {tuple(resolved)} "
                f"from natural_key()/__unique_constraints__, which differs from declared "
                f"primary key {tuple(pks)}. Pass keys=... explicitly to silence.",
                UserWarning,
                stacklevel=3,
            )
        return resolved

    return list(pks)


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
    handle_deletes: str | None = None,
    event_timestamp_column: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Execute a load. Phases 5–8 support 'append', 'truncate', 'merge'/'scd1', 'scd2'.

    Phase 10: pass `incremental_column='col'` for watermarked append loads.
    Phase 11: pass `handle_deletes='hard'` (merge/scd1) or `'soft'` (scd2) to
    handle keys that have disappeared from the source.
    Phase 15: pass `event_timestamp_column='col'` (scd2 only) to take
    `valid_from` from a source column instead of `now()`.
    """
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
    if event_timestamp_column is not None and mode != "scd2":
        raise ValueError(
            f"event_timestamp_column is only supported for mode='scd2'; got mode={mode!r}"
        )
    if handle_deletes is not None:
        if handle_deletes not in ("hard", "soft"):
            raise ValueError(
                f"handle_deletes must be 'hard', 'soft', or None (got {handle_deletes!r})"
            )
        if mode in ("append", "truncate"):
            raise ValueError(
                f"handle_deletes is not supported for mode={mode!r}; "
                "use mode='merge'/'scd1' (hard) or mode='scd2' (soft)"
            )
        if incremental_column is not None:
            raise ValueError(
                "handle_deletes cannot be combined with incremental_column — "
                "an incremental load can't see absent keys"
            )
        if mode in ("merge", "scd1") and handle_deletes != "hard":
            raise NotImplementedError(
                f"merge/scd1 + handle_deletes={handle_deletes!r} is not yet implemented; "
                "Phase 11.5 will add soft-delete via auto _is_deleted column"
            )
        if mode == "scd2" and handle_deletes != "soft":
            raise ValueError(
                f"scd2 supports handle_deletes='soft' only (got {handle_deletes!r})"
            )

    name = pipeline_name or f"{target.__schema__}.{target.__tablename__}"

    if mode == "scd2":
        augmented_json = _core.augment_table_spec_scd2(json.dumps(target._to_spec()))
    else:
        augmented_json = _core.augment_table_spec(json.dumps(target._to_spec()))

    # ensure_table is idempotent; safe to call in dry_run too. The DDL
    # side effect of creating an empty target on first run is benign and
    # lets dry_run work against a freshly-cloned database.
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
            dry_run,
        )
    if mode == "truncate":
        return target_connection.run_truncate(
            augmented_json, source.query, name, src_arg, dry_run
        )

    if mode == "scd2":
        resolved_keys = _resolve_merge_keys(
            target, passed=keys, pipeline_name=pipeline_name
        )
        if not resolved_keys:
            raise ValueError(
                "mode='scd2' requires keys; pass keys=..., declare __merge_keys__, "
                "add a unique_constraint / natural_key, or declare primary_key columns"
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
            handle_deletes,
            event_timestamp_column,
            dry_run,
        )

    # merge / scd1
    resolved_keys = _resolve_merge_keys(
        target, passed=keys, pipeline_name=pipeline_name
    )
    if not resolved_keys:
        raise ValueError(
            f"mode={mode!r} requires keys; pass keys=..., declare __merge_keys__, "
            "add a unique_constraint / natural_key, or declare primary_key columns"
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
        handle_deletes,
        dry_run,
    )


# --- Phase 12: scheduling registry ------------------------------------------


@dataclass(frozen=True)
class ScheduledPipeline:
    """A named, scheduled callable. Registered via `@pipeline.register(...)`.

    `schedule=None` (Phase 27c) keeps a pipeline registered without a cron
    — only fires when invoked by another pipeline's transforms_post via
    `transform_ref(...)` or directly via `flow run`.
    """

    name: str
    schedule: str | None
    fn: Callable[[], dict[str, Any]]


_REGISTRY: dict[str, ScheduledPipeline] = {}


@dataclass(frozen=True)
class RegisteredTransform:
    """Phase 27b: a `@ematix.transform`-decorated standalone callable."""

    name: str
    schedule: str | None
    target_connection: str | None
    fn: Callable[..., Any]
    arity: int  # 1 (conn,) or 2 (conn, parent)


_TRANSFORMS_REGISTRY: dict[str, RegisteredTransform] = {}


@dataclass(frozen=True)
class ParentContext:
    """Phase 27b: passed as the second arg to 2-arg transform callables.

    `None` is passed when a transform runs standalone (`flow transform run`,
    its own scheduled cron) — there is no parent pipeline.
    """

    pipeline_name: str
    run_id: str
    rows_inserted: int
    rows_updated: int
    rows_unchanged: int


def run_transform(name: str) -> dict[str, Any]:
    """Run a `@ematix.transform`-decorated callable standalone.

    Resolves the transform's `target_connection` (or default), invokes
    the function with parent=None, records the outcome in run_history,
    returns a dict {status, metrics, run_id}.
    """
    rt = _TRANSFORMS_REGISTRY.get(name)
    if rt is None:
        raise KeyError(name)
    from ematix_flow import config as _config

    conn = _config.connect(rt.target_connection) if rt.target_connection else _config.connect()
    return _invoke_transform_callable(
        fn=rt.fn,
        arity=rt.arity,
        conn=conn,
        parent=None,
        pipeline_name=name,
        step_name="standalone",
        target_schema="",
        target_table="",
        parent_run_id=None,
    )


def _invoke_transform_callable(
    *,
    fn: Callable[..., Any],
    arity: int,
    conn: Any,
    parent: ParentContext | None,
    pipeline_name: str,
    step_name: str,
    target_schema: str,
    target_table: str,
    parent_run_id: str | None,
) -> dict[str, Any]:
    """Invoke a transform callable, record the outcome, return a result dict.

    Raises any exception the callable raises *after* recording
    transform_failed. Caller decides whether to propagate.
    """
    import uuid as _uuid

    own_run_id = parent_run_id or str(_uuid.uuid4())
    try:
        if arity == 1:
            ret = fn(conn)
        else:
            ret = fn(conn, parent)
    except Exception as e:
        if parent_run_id:
            try:
                conn.record_transform_history(
                    parent_run_id,
                    pipeline_name,
                    step_name,
                    "transform_failed",
                    target_schema,
                    target_table,
                    str(e),
                    None,
                )
            except Exception:
                pass
        raise

    metrics: dict[str, Any] | None = None
    if isinstance(ret, dict):
        metrics = ret
    metrics_json = json.dumps(metrics) if metrics is not None else None
    if parent_run_id:
        conn.record_transform_history(
            parent_run_id,
            pipeline_name,
            step_name,
            "transform_success",
            target_schema,
            target_table,
            None,
            metrics_json,
        )
    return {
        "status": "transform_success",
        "metrics": metrics,
        "run_id": own_run_id,
    }


def register(
    *, name: str, schedule: str | None
) -> Callable[[Callable[[], dict[str, Any]]], Callable[[], dict[str, Any]]]:
    """Decorator: register a callable as a scheduled pipeline.

    The function should perform a sync (or any work) and return a
    JSON-serializable dict describing what happened.
    """

    def decorator(
        fn: Callable[[], dict[str, Any]],
    ) -> Callable[[], dict[str, Any]]:
        if name in _REGISTRY:
            raise ValueError(
                f"a pipeline named {name!r} is already registered"
            )
        _REGISTRY[name] = ScheduledPipeline(name=name, schedule=schedule, fn=fn)
        return fn

    return decorator


def list_pipelines() -> list[ScheduledPipeline]:
    return list(_REGISTRY.values())


@dataclass(frozen=True)
class RegistryEntry:
    """Phase 27c Q4.2 β: unified view across pipelines + transforms for
    the merged `flow list` output."""

    name: str
    kind: Literal["pipeline", "transform"]
    schedule: str | None


def list_entries() -> list[RegistryEntry]:
    """Return pipelines + transforms together, sorted by name."""
    entries: list[RegistryEntry] = []
    for sp in _REGISTRY.values():
        entries.append(
            RegistryEntry(name=sp.name, kind="pipeline", schedule=sp.schedule)
        )
    for rt in _TRANSFORMS_REGISTRY.values():
        entries.append(
            RegistryEntry(name=rt.name, kind="transform", schedule=rt.schedule)
        )
    entries.sort(key=lambda e: e.name)
    return entries


def list_transforms() -> list[RegistryEntry]:
    return [e for e in list_entries() if e.kind == "transform"]


def get_pipeline(name: str) -> ScheduledPipeline:
    return _REGISTRY[name]


def run_pipeline(name: str) -> dict[str, Any]:
    return _REGISTRY[name].fn()


def is_due(schedule: str | None, now: datetime, interval_seconds: int) -> bool:
    """Return True if `schedule` would fire within the half-open window
    `(now - interval_seconds, now]`. The intended invocation pattern is
    an external cron / k8s CronJob calling `flow run-due` once per
    `interval_seconds`.

    `schedule=None` always returns False — unscheduled pipelines/transforms
    are only invoked explicitly (`flow run`, `flow transform run`) or
    indirectly via `transforms_post=[transform_ref(...)]`.
    """
    if schedule is None:
        return False
    from croniter import croniter

    try:
        base = now - timedelta(seconds=interval_seconds)
        cron = croniter(schedule, base)
        next_fire = cron.get_next(datetime)
    except Exception as e:
        raise ValueError(f"invalid cron expression {schedule!r}: {e}") from e
    return base < next_fire <= now


# --- Phase 25: preview / dry-run --------------------------------------------


def preview(name: str, *, dry_run: bool = False) -> Any:
    """Inspect what a registered pipeline would do without committing.

    Returns a `PreviewResult` (or a `DryRunResult` when `dry_run=True`).
    The returned value carries the resolved connections, augmented target
    spec, resolved merge / compare keys with the reasons they were picked,
    and the SQL the strategy would execute.

    With `dry_run=True`, also runs the strategy inside a transaction and
    rolls back at the end so the caller sees row counts that *would* have
    been affected. Run-history side effects are skipped.
    """
    sp = _REGISTRY.get(name)
    if sp is None:
        raise KeyError(name)
    fn = sp.fn
    # The wrapper installed by @ematix.pipeline carries a `_preview` hook
    # that knows the decorator-time configuration. Plain @register-only
    # pipelines don't support preview.
    hook = getattr(fn, "_preview", None)
    if hook is None:
        raise TypeError(
            f"pipeline {name!r} was not built with @ematix.pipeline; preview "
            "and dry_run are only supported for decorator-built pipelines"
        )
    return hook(dry_run=dry_run)


def dry_run(name: str) -> Any:
    """Convenience: `preview(name, dry_run=True)`."""
    return preview(name, dry_run=True)


@dataclass(frozen=True)
class ValidateResult:
    """Phase 26: result of `flow validate <pipeline>`.

    `ok` is True when every per-target EXPLAIN succeeded.
    `source_sql` is the synthesized SQL that was EXPLAINed.
    `errors` is a list of human-readable strings, one per failure.
    `notes` (Phase 27f) records skipped transform entries that can't be
    EXPLAINed (callables, transform_ref).
    """

    pipeline_name: str
    ok: bool
    source_sql: str
    errors: list[str]
    target_connection_name: str | None = None
    notes: list[str] = field(default_factory=list)


def validate(name: str) -> ValidateResult:
    """Validate a pipeline by running `EXPLAIN` against the target
    connection on the synthesized source SQL.

    Catches type/syntax errors at user-controlled times (CI, pre-deploy)
    without taxing decoration. Does not run any DML.
    """
    sp = _REGISTRY.get(name)
    if sp is None:
        raise KeyError(name)
    plan = preview(name)
    source_sql = plan.source_sql
    errors: list[str] = []

    # Resolve the target connection. Multi-target pipelines may have
    # per-target connection overrides; for now validate against each
    # distinct connection. Single-target uses the pipeline's
    # target_connection (or default).
    from ematix_flow import config as _config

    seen_conns: set[tuple[str | None, str | None]] = set()
    targets_info: list[tuple[str | None, dict | None]] = []
    if plan.targets:
        for t in plan.targets:
            key = (t.target_connection_name, str(t.target_connection_info))
            if key in seen_conns:
                continue
            seen_conns.add(key)
            targets_info.append((t.target_connection_name, t.target_connection_info))
    if not targets_info:
        targets_info = [(None, None)]

    primary_conn_name: str | None = None
    primary_conn = None
    for conn_name, _info in targets_info:
        try:
            conn = _config.connect(conn_name) if conn_name else _config.connect()
        except Exception as e:
            errors.append(f"could not resolve connection {conn_name!r}: {e}")
            continue
        if primary_conn_name is None:
            primary_conn_name = conn_name
            primary_conn = conn
        try:
            conn.execute(f"EXPLAIN {source_sql}")
        except Exception as e:
            errors.append(str(e))

    # Phase 27f Q10 B: EXPLAIN SQL-string transforms_post entries; note
    # skipped callables / transform_refs.
    notes: list[str] = []
    if primary_conn is not None and plan.transforms_post:
        for i, t in enumerate(plan.transforms_post):
            if t.kind == "sql":
                try:
                    primary_conn.execute(f"EXPLAIN {t.summary}")
                except Exception as e:
                    errors.append(f"transforms_post[{i}] (sql): {e}")
            else:
                notes.append(
                    f"transforms_post[{i}] ({t.kind}): skipped — not EXPLAIN-able"
                )

    return ValidateResult(
        pipeline_name=name,
        ok=not errors,
        source_sql=source_sql,
        errors=errors,
        target_connection_name=primary_conn_name,
        notes=notes,
    )
