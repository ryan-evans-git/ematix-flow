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
    ttl: timedelta | None = None,
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
    if ttl is not None and mode != "scd2":
        raise ValueError(
            f"ttl is only supported for mode='scd2'; got mode={mode!r}"
        )
    ttl_seconds = int(ttl.total_seconds()) if ttl is not None else None
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
            ttl_seconds,
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

# ---- Phase Ω.1 — pipeline DAG side-tables --------------------------
#
# Three small side-tables keep the DAG state out of `ScheduledPipeline`
# (which stays frozen + serializable). They live alongside `_REGISTRY`
# and are reset together. In-process only — durable run-history is
# Phase Ω.D1a's job.
#
#   _DEPENDS_ON[pipeline_name]              = list of upstream names
#   _UPSTREAM_FRESHNESS[pipeline_name]      = max age of upstream's
#                                             last successful run in
#                                             seconds, or None for ∞
#   _LAST_RUN[pipeline_name]                = (timestamp, success)
#                                             populated by
#                                             `run_due_with_dag`
_DEPENDS_ON: dict[str, list[str]] = {}
_UPSTREAM_FRESHNESS: dict[str, int | None] = {}
_LAST_RUN: dict[str, tuple[datetime, bool]] = {}


# Phase Ω.2 — declarative retry policy + in-process attempt state.
#
#   _RETRY_POLICY[name]     = RetryPolicy describing the retry shape
#   _ATTEMPT_STATE[name]    = AttemptState tracking the current retry
#                             cycle. Cleared on a successful run.
#
# In-process scope only. Durable per-attempt history is Phase Ω.D1a.


@dataclass(frozen=True)
class RetryPolicy:
    """Per-pipeline retry shape.

    `max_attempts`: total invocations allowed, including the first.
      1 means "no retries" (matches the default).
    `backoff`: one of "fixed" | "linear" | "exponential".
    `base_secs`: backoff window after the first failure. Linear and
      exponential variants scale this.
    `max_backoff_secs`: ceiling on the window for any single retry.
      None disables the ceiling.
    """

    max_attempts: int = 1
    backoff: str = "fixed"
    base_secs: int = 0
    max_backoff_secs: int | None = None


@dataclass
class AttemptState:
    """In-process state of an in-flight retry cycle.

    `attempt_count`: how many invocations have happened in this cycle,
      including the original failed attempt.
    `last_attempt_at`: timestamp of the most recent invocation.
    `gave_up`: True once `attempt_count` has reached `max_attempts`
      without success. A successful run clears the whole record.
    """

    attempt_count: int
    last_attempt_at: datetime
    gave_up: bool = False


_RETRY_POLICY: dict[str, RetryPolicy] = {}
_ATTEMPT_STATE: dict[str, AttemptState] = {}


_VALID_BACKOFFS = {"fixed", "linear", "exponential"}


def _build_retry_policy(name: str, raw: dict | None) -> RetryPolicy:
    """Validate a retry= kwarg and produce a RetryPolicy. Raises
    ValueError on shape problems so registration fails loud."""
    if raw is None:
        return RetryPolicy()
    max_attempts = int(raw.get("max_attempts", 1))
    if max_attempts < 1:
        raise ValueError(
            f"pipeline {name!r}: retry.max_attempts must be ≥ 1, got {max_attempts}"
        )
    backoff = str(raw.get("backoff", "fixed"))
    if backoff not in _VALID_BACKOFFS:
        raise ValueError(
            f"pipeline {name!r}: retry.backoff must be one of "
            f"{sorted(_VALID_BACKOFFS)}, got {backoff!r}"
        )
    base_secs = int(raw.get("base_secs", 0))
    if base_secs < 0:
        raise ValueError(
            f"pipeline {name!r}: retry.base_secs must be ≥ 0, got {base_secs}"
        )
    max_backoff_secs = raw.get("max_backoff_secs")
    if max_backoff_secs is not None:
        max_backoff_secs = int(max_backoff_secs)
        if max_backoff_secs < 0:
            raise ValueError(
                f"pipeline {name!r}: retry.max_backoff_secs must be ≥ 0 "
                f"or omitted, got {max_backoff_secs}"
            )
    return RetryPolicy(
        max_attempts=max_attempts,
        backoff=backoff,
        base_secs=base_secs,
        max_backoff_secs=max_backoff_secs,
    )


def _compute_backoff_secs(policy: RetryPolicy, attempt_count: int) -> int:
    """Backoff window after attempt `attempt_count`. attempt_count=1
    means "after the first failure", so the window scales from there.
    The ceiling, if set, caps every variant."""
    base = policy.base_secs
    if policy.backoff == "fixed":
        secs = base
    elif policy.backoff == "linear":
        secs = base * attempt_count
    elif policy.backoff == "exponential":
        # attempt_count=1 → base; =2 → 2*base; =3 → 4*base; ...
        secs = base * (2 ** (attempt_count - 1))
    else:  # defensive — registration filtered this already
        secs = base
    if policy.max_backoff_secs is not None and secs > policy.max_backoff_secs:
        secs = policy.max_backoff_secs
    return secs


def retry_policy_of(name: str) -> RetryPolicy:
    """Return the RetryPolicy for `name`. KeyError for unknown names."""
    if name not in _REGISTRY:
        raise KeyError(name)
    return _RETRY_POLICY.get(name, RetryPolicy())


# Phase Ω.D1a — durable run-history.
#
# The concrete RunLog backends live in `ematix_flow.run_log`. The
# default is the local-file SqliteRunLog; alternates (in-memory,
# Postgres, S3, Azure Blob, GCS) all satisfy the same Protocol and
# are interchangeable in `run_due_with_dag(run_log=...)`.
#
# `pipeline.RunLog` is kept as a backwards-compat alias for the
# SQLite backend — the original Ω.D1a name.


# Backwards-compat: the original Ω.D1a name lived here. New code
# should prefer `from ematix_flow.run_log import SqliteRunLog`, but
# the legacy spelling keeps working.
from ematix_flow.run_log import SqliteRunLog as RunLog  # noqa: E402


# Phase Ω.3 — operator-facing status view.
#
# `status_snapshot()` produces one dict per registered pipeline,
# summarising the in-process state that Ω.1 and Ω.2 already maintain.
# It does NOT touch durable run-history — that arrives with Ω.D1a's
# RunLog. The renderer below turns the snapshot into a fixed-width
# text table suitable for `flow status` CLI output.


def status_snapshot() -> list[dict]:
    """Return a list of per-pipeline state dicts. Stable order:
    alphabetical by name. Each row has keys:

      name          : str
      schedule      : str | None
      depends_on    : list[str]
      last_run      : (datetime, bool) | None — None if never run
      retry_policy  : dict mirroring RetryPolicy fields
      attempt_state : dict | None, with keys
                      attempt_count, last_attempt_at,
                      next_eligible_at, gave_up
                      (None when no retry cycle is in flight)
    """
    rows: list[dict] = []
    for name in sorted(_REGISTRY):
        sp = _REGISTRY[name]
        policy = _RETRY_POLICY.get(name, RetryPolicy())
        st = _ATTEMPT_STATE.get(name)
        attempt_state: dict | None = None
        if st is not None:
            backoff_secs = _compute_backoff_secs(policy, st.attempt_count)
            attempt_state = {
                "attempt_count": st.attempt_count,
                "last_attempt_at": st.last_attempt_at,
                "next_eligible_at": st.last_attempt_at + timedelta(seconds=backoff_secs),
                "gave_up": st.gave_up,
            }
        rows.append({
            "name": name,
            "schedule": sp.schedule,
            "depends_on": list(_DEPENDS_ON.get(name, [])),
            "last_run": _LAST_RUN.get(name),
            "retry_policy": {
                "max_attempts": policy.max_attempts,
                "backoff": policy.backoff,
                "base_secs": policy.base_secs,
                "max_backoff_secs": policy.max_backoff_secs,
            },
            "attempt_state": attempt_state,
        })
    return rows


def render_status_table(snapshot: list[dict]) -> str:
    """Render a `status_snapshot()` result as a fixed-width text table.

    The columns track what an operator wants to see at a glance:
    name, schedule, last run + outcome, retry status. depends_on is
    elided when empty to keep the row short.
    """
    if not snapshot:
        return "no pipelines registered"

    header = ("NAME", "SCHEDULE", "LAST RUN", "OUTCOME", "RETRY", "DEPENDS ON")
    rows: list[tuple[str, ...]] = [header]
    for r in snapshot:
        last_run = r["last_run"]
        if last_run is None:
            last_run_s = "never"
            outcome_s = "-"
        else:
            ts, ok = last_run
            # Truncate to seconds; UTC iso with trailing Z is the most
            # operator-friendly compact form.
            last_run_s = ts.replace(microsecond=0).isoformat().replace("+00:00", "Z")
            outcome_s = "ok" if ok else "fail"
        retry_s = _format_retry_cell(r["retry_policy"], r["attempt_state"])
        deps_s = ",".join(r["depends_on"]) if r["depends_on"] else "-"
        rows.append((
            r["name"],
            r["schedule"] or "-",
            last_run_s,
            outcome_s,
            retry_s,
            deps_s,
        ))

    widths = [max(len(row[i]) for row in rows) for i in range(len(header))]
    lines = []
    for i, row in enumerate(rows):
        line = "  ".join(cell.ljust(widths[j]) for j, cell in enumerate(row))
        lines.append(line)
        if i == 0:
            lines.append("  ".join("-" * w for w in widths))
    return "\n".join(lines)


def _format_retry_cell(policy: dict, state: dict | None) -> str:
    """One short cell describing retry status for the table."""
    max_attempts = policy.get("max_attempts", 1)
    if state is None:
        if max_attempts <= 1:
            return "no retry"
        return f"0/{max_attempts} ({policy['backoff']})"
    attempt = state["attempt_count"]
    if state["gave_up"]:
        return f"{attempt}/{max_attempts} gave up"
    next_at = state["next_eligible_at"]
    next_at_s = next_at.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return f"{attempt}/{max_attempts} next>{next_at_s}"


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
    *,
    name: str,
    schedule: str | None,
    depends_on: list[str] | None = None,
    upstream_freshness_secs: int | None = None,
    retry: dict | None = None,
) -> Callable[[Callable[[], dict[str, Any]]], Callable[[], dict[str, Any]]]:
    """Decorator: register a callable as a scheduled pipeline.

    The function should perform a sync (or any work) and return a
    JSON-serializable dict describing what happened.

    Phase Ω.1 keyword arguments:
      depends_on: list of names of pipelines that must run successfully
        before this one is eligible. Empty list / None = no upstreams.
        Each name must already be registered. Self-loops and cycles are
        rejected at registration time with a clear error pointing at
        both endpoints.
      upstream_freshness_secs: max age (seconds) of an upstream's last
        successful run for this pipeline to count it as fresh. None
        (the default) = ∞: any prior success counts. Used by
        `run_due_with_dag` to gate downstream execution.
    """

    def decorator(
        fn: Callable[[], dict[str, Any]],
    ) -> Callable[[], dict[str, Any]]:
        if name in _REGISTRY:
            raise ValueError(
                f"a pipeline named {name!r} is already registered"
            )
        upstreams = list(depends_on or [])
        # Self-loop.
        if name in upstreams:
            raise ValueError(
                f"pipeline {name!r} depends on itself; "
                f"a self-cycle of length 1 is not allowed"
            )
        # All upstreams must already be registered.
        for u in upstreams:
            if u not in _REGISTRY:
                raise ValueError(
                    f"pipeline {name!r} declares depends_on={u!r} but "
                    f"no pipeline named {u!r} has been registered yet — "
                    f"order your decorators so upstreams register first"
                )
        # Cycle detection. With name->upstreams as a forward graph,
        # check that none of `upstreams` (or anything they transitively
        # depend on) is `name`. Returns the offending cycle path on hit.
        cycle = _find_cycle_path(start=name, frontier=upstreams)
        if cycle is not None:
            raise ValueError(
                f"pipeline {name!r} would create a dependency cycle: "
                f"{' → '.join(cycle)} (rejected at registration time)"
            )
        # Validate retry= up front so a malformed policy doesn't make
        # it into the registry.
        policy = _build_retry_policy(name, retry)
        _REGISTRY[name] = ScheduledPipeline(name=name, schedule=schedule, fn=fn)
        _DEPENDS_ON[name] = upstreams
        if upstream_freshness_secs is not None:
            _UPSTREAM_FRESHNESS[name] = upstream_freshness_secs
        # Always populate so retry_policy_of returns the validated
        # policy (the default RetryPolicy for unset cases).
        _RETRY_POLICY[name] = policy
        return fn

    return decorator


def _find_cycle_path(*, start: str, frontier: list[str]) -> list[str] | None:
    """Returns a `start → ... → start` cycle path if one of `frontier`
    can transitively reach `start`. Returns None if the graph is
    acyclic when extended with edges `start → frontier`.
    """
    # DFS each frontier node; if any path reaches `start`, return the
    # path including the original `start`.
    for f in frontier:
        path = _dfs_path(src=f, target=start)
        if path is not None:
            return [start, *path]
    return None


def _dfs_path(*, src: str, target: str) -> list[str] | None:
    """DFS the _DEPENDS_ON forward graph from `src`; if any walk
    reaches `target`, return the path. None if no path exists.
    """
    stack: list[tuple[str, list[str]]] = [(src, [src])]
    seen: set[str] = set()
    while stack:
        node, path = stack.pop()
        if node == target:
            return path
        if node in seen:
            continue
        seen.add(node)
        for up in _DEPENDS_ON.get(node, []):
            stack.append((up, [*path, up]))
    return None


def depends_on_of(name: str) -> list[str]:
    """Return the upstream list for `name`. Empty list for pipelines
    with no declared dependencies. KeyError for unknown names."""
    if name not in _REGISTRY:
        raise KeyError(name)
    return list(_DEPENDS_ON.get(name, []))


def upstream_freshness_secs_of(name: str) -> int | None:
    """Return the `upstream_freshness_secs` for `name`, or None for ∞."""
    if name not in _REGISTRY:
        raise KeyError(name)
    return _UPSTREAM_FRESHNESS.get(name)


def order_for_run_due(names: list[str]) -> list[str]:
    """Topo-sort the subset `names` so upstreams appear before
    downstreams. Edges OUTSIDE the subset are ignored: e.g. if `b`
    depends on `a` and only `b` is in `names`, the returned order is
    `[b]` (caller's responsibility to handle the missing upstream
    via the freshness gate in `run_due_with_dag`).

    The input order is preserved among nodes with the same depth, so
    callers get stable output.
    """
    name_set = set(names)
    # Forward graph restricted to names; reverse edges = "downstreams
    # of x within the subset".
    in_degree: dict[str, int] = {n: 0 for n in names}
    downstream: dict[str, list[str]] = {n: [] for n in names}
    for n in names:
        for u in _DEPENDS_ON.get(n, []):
            if u in name_set:
                in_degree[n] += 1
                downstream[u].append(n)
    # Kahn's algorithm — preserve original input order across ties.
    queue: list[str] = [n for n in names if in_degree[n] == 0]
    out: list[str] = []
    while queue:
        n = queue.pop(0)
        out.append(n)
        for d in downstream[n]:
            in_degree[d] -= 1
            if in_degree[d] == 0:
                queue.append(d)
    if len(out) != len(names):
        # Should be impossible because cycles are rejected at
        # registration, but defend anyway with a clear error.
        leftover = [n for n in names if n not in out]
        raise ValueError(
            f"order_for_run_due: cycle detected involving {leftover} — "
            f"this shouldn't happen given registration-time checks; "
            f"please file a bug"
        )
    return out


def _upstream_is_fresh(downstream_name: str, now: datetime) -> bool:
    """For each declared upstream of `downstream_name`, check that
    `_LAST_RUN[upstream]` exists, is a success, and was within the
    declared freshness window. Returns True iff all upstreams pass.
    """
    upstreams = _DEPENDS_ON.get(downstream_name, [])
    if not upstreams:
        return True
    max_age_secs = _UPSTREAM_FRESHNESS.get(downstream_name)  # None = ∞
    for up in upstreams:
        rec = _LAST_RUN.get(up)
        if rec is None:
            return False
        ts, ok = rec
        if not ok:
            return False
        if max_age_secs is not None:
            age = (now - ts).total_seconds()
            if age > max_age_secs:
                return False
    return True


# ---- Detailed run-due result types (Phase Ω.D2) ------------------------


@dataclass(frozen=True)
class FiredEvent:
    """One pipeline that ran successfully in this invocation."""

    name: str
    result: Any  # whatever the callable returned (dict or otherwise)


@dataclass(frozen=True)
class FailedEvent:
    """One pipeline that ran but raised."""

    name: str
    error_message: str
    error_type: str  # e.g. "RuntimeError"
    attempt_count: int  # which attempt this was (1 = first)
    gave_up: bool      # True iff attempt_count == max_attempts


@dataclass(frozen=True)
class SkippedEvent:
    """One pipeline that didn't run at all (upstream stale, retry
    backoff, or gave-up)."""

    name: str
    reason: str  # human-readable; e.g. "retry backoff (next at ...)"


@dataclass(frozen=True)
class RunDueResult:
    """Per-pipeline outcomes from one `run_due_with_dag` invocation.

    Three disjoint lists. Order matches topo-sort order so an operator
    reading the JSON can see the DAG progression.
    """

    fired: list[FiredEvent]
    failed: list[FailedEvent]
    skipped: list[SkippedEvent]


def run_due_with_dag_detailed(
    due: list[str],
    *,
    now: datetime | None = None,
    run_log: "RunLog | None" = None,
    alerters: list | None = None,
) -> RunDueResult:
    """Like `run_due_with_dag` but returns a structured `RunDueResult`
    with full per-pipeline outcome detail. The CLI uses this to build
    its ran/failed/skipped JSON output.

    Same semantics as `run_due_with_dag`:
      1. Topo-sort `due` so upstreams-in-the-subset go first.
      2. For each pipeline:
         a. Skip if a declared upstream isn't fresh (Ω.1).
         b. Skip if mid retry-backoff or gave-up (Ω.2).
         c. Otherwise fire; record outcome to `_LAST_RUN` and (if
            provided) the durable `run_log`.
    """
    if now is None:
        now = datetime.now(_tz_utc())
    ordered = order_for_run_due(list(due))

    fired: list[FiredEvent] = []
    failed: list[FailedEvent] = []
    skipped: list[SkippedEvent] = []

    for name in ordered:
        if not _upstream_is_fresh(name, now):
            stale = [
                u for u in _DEPENDS_ON.get(name, [])
                if not _one_upstream_is_fresh(u, _UPSTREAM_FRESHNESS.get(name), now)
            ]
            skipped.append(SkippedEvent(
                name=name,
                reason=f"upstream not fresh: {','.join(stale) or '(unknown)'}",
            ))
            continue

        # Ω.2: retry-backoff gate.
        st = _ATTEMPT_STATE.get(name)
        policy = _RETRY_POLICY.get(name, RetryPolicy())
        if st is not None:
            if st.gave_up:
                skipped.append(SkippedEvent(
                    name=name,
                    reason=(
                        f"gave up after {st.attempt_count} retry attempts "
                        f"(max_attempts={policy.max_attempts})"
                    ),
                ))
                continue
            backoff_secs = _compute_backoff_secs(policy, st.attempt_count)
            next_eligible = st.last_attempt_at + timedelta(seconds=backoff_secs)
            if now < next_eligible:
                skipped.append(SkippedEvent(
                    name=name,
                    reason=(
                        f"retry backoff (attempt {st.attempt_count}/"
                        f"{policy.max_attempts}; next eligible at "
                        f"{next_eligible.replace(microsecond=0).isoformat().replace('+00:00', 'Z')})"
                    ),
                ))
                continue

        sp = _REGISTRY.get(name)
        if sp is None:
            raise KeyError(name)
        ret: Any = None
        success = False
        err: Exception | None = None
        try:
            ret = sp.fn()
            success = True
        except Exception as e:
            err = e
            success = False
        completed_at = datetime.now(_tz_utc())
        _LAST_RUN[name] = (completed_at, success)
        if run_log is not None:
            run_log.record_run(name, completed_at, success)
        if success:
            prev = _ATTEMPT_STATE.get(name)
            fired.append(FiredEvent(name=name, result=ret))
            _ATTEMPT_STATE.pop(name, None)
            if run_log is not None:
                run_log.clear_attempt_state(name)
            # Recovery event: if a retry cycle was in flight (prev had
            # attempt_count >= 1) and now we succeeded, push a
            # `recovered` event so alerters can close the loop.
            if prev is not None and prev.attempt_count >= 1:
                _fan_out_alert(
                    alerters,
                    AlertEvent_local(
                        kind="recovered",
                        pipeline=name,
                        timestamp=completed_at,
                        attempt_count=prev.attempt_count + 1,
                        max_attempts=policy.max_attempts,
                    ),
                )
        else:
            prev = _ATTEMPT_STATE.get(name)
            attempt_count = (prev.attempt_count if prev else 0) + 1
            gave_up = attempt_count >= policy.max_attempts
            new_state = AttemptState(
                attempt_count=attempt_count,
                last_attempt_at=now,
                gave_up=gave_up,
            )
            _ATTEMPT_STATE[name] = new_state
            if run_log is not None:
                run_log.record_attempt(name, new_state)
            err_msg = str(err) if err else ""
            err_type = type(err).__name__ if err else "Exception"
            failed.append(FailedEvent(
                name=name,
                error_message=err_msg,
                error_type=err_type,
                attempt_count=attempt_count,
                gave_up=gave_up,
            ))
            # Always emit a `failed` event; emit an additional
            # `gave_up` if this attempt exhausted max_attempts.
            _fan_out_alert(
                alerters,
                AlertEvent_local(
                    kind="failed",
                    pipeline=name,
                    timestamp=completed_at,
                    error_message=err_msg,
                    error_type=err_type,
                    attempt_count=attempt_count,
                    max_attempts=policy.max_attempts,
                    gave_up=gave_up,
                ),
            )
            if gave_up:
                _fan_out_alert(
                    alerters,
                    AlertEvent_local(
                        kind="gave_up",
                        pipeline=name,
                        timestamp=completed_at,
                        error_message=err_msg,
                        error_type=err_type,
                        attempt_count=attempt_count,
                        max_attempts=policy.max_attempts,
                        gave_up=True,
                    ),
                )

    return RunDueResult(fired=fired, failed=failed, skipped=skipped)


def _fan_out_alert(alerters, event) -> None:
    """Notify each alerter, swallowing any exceptions so a buggy
    alerter can't poison the orchestrator loop."""
    if not alerters:
        return
    import sys
    for a in alerters:
        try:
            a.notify(event)
        except Exception as e:
            print(
                f"warning: alerter {type(a).__name__} raised on notify: "
                f"{type(e).__name__}: {e}",
                file=sys.stderr,
            )


def AlertEvent_local(**kwargs):
    """Lazy import of AlertEvent to avoid loading alerters at
    pipeline.py module-import time."""
    from ematix_flow.alerters import AlertEvent
    return AlertEvent(**kwargs)


def _one_upstream_is_fresh(
    upstream: str, max_age_secs: int | None, now: datetime
) -> bool:
    """Per-upstream variant of `_upstream_is_fresh`. Surfacing this
    so the CLI / detailed result can name exactly which upstreams
    are stale."""
    rec = _LAST_RUN.get(upstream)
    if rec is None:
        return False
    ts, ok = rec
    if not ok:
        return False
    if max_age_secs is None:
        return True
    return (now - ts).total_seconds() <= max_age_secs


def run_due_with_dag(
    due: list[str],
    *,
    now: datetime | None = None,
    run_log: "RunLog | None" = None,
) -> list[str]:
    """Legacy entry point: run the pipelines in `due`, return the names
    that fired successfully. Built on `run_due_with_dag_detailed` —
    same semantics, narrower return type for back-compat with
    callers from Ω.1/Ω.2/Ω.D1a tests.
    """
    result = run_due_with_dag_detailed(due, now=now, run_log=run_log)
    return [e.name for e in result.fired]


def _tz_utc():
    """Return the UTC tzinfo without importing datetime.timezone at
    the module top (kept lean for import-time)."""
    from datetime import timezone
    return timezone.utc


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


# --- Phase 17: FeatureView registry ---------------------------------------


@dataclass(frozen=True)
class RegisteredFeatureView:
    """Process-local entry for a `@ematix.feature_view`-decorated class."""

    schema: str
    tablename: str
    feature_version: str
    entity_keys: list[str]
    event_timestamp_column: str | None
    ttl_seconds: int | None
    freshness_sla_seconds: int | None
    online: bool
    description: str | None
    owner: str | None
    cls: Any

    @property
    def name(self) -> str:
        return f"{self.schema}.{self.tablename}.{self.feature_version}"


_FEATURE_VIEWS_REGISTRY: dict[str, RegisteredFeatureView] = {}


def list_feature_views() -> list[RegisteredFeatureView]:
    return list(_FEATURE_VIEWS_REGISTRY.values())


def _ensure_online_artifacts(*, target_connection: Any, target_cls: Any) -> None:
    """Phase 19: ensure the partial index, online MATERIALIZED VIEW, and MV
    unique index exist for an `online=True` FeatureView. Idempotent —
    safe to call after every sync."""
    if not getattr(target_cls, "__is_feature_view__", False):
        return
    if not getattr(target_cls, "__online__", False):
        return
    schema = target_cls.__schema__
    table = target_cls.__tablename__
    online_name = f"{table}__online"
    keys = target_cls._primary_keys()
    if not keys:
        return
    keys_sql = ", ".join(f'"{k}"' for k in keys)
    quoted_schema = f'"{schema}"'

    # Partial index on the main table — speeds up WHERE is_current scans
    # and is the building block referenced in plan §3.4.
    target_connection.execute(
        f'CREATE INDEX IF NOT EXISTS '
        f'"idx__{table}__current_keys" '
        f'ON {quoted_schema}."{table}" ({keys_sql}) '
        f"WHERE is_current"
    )
    # MV containing only the current versions.
    target_connection.execute(
        f'CREATE MATERIALIZED VIEW IF NOT EXISTS '
        f'{quoted_schema}."{online_name}" '
        f'AS SELECT * FROM {quoted_schema}."{table}" WHERE is_current'
    )
    # Unique index on entity keys so REFRESH MATERIALIZED VIEW
    # CONCURRENTLY can run.
    target_connection.execute(
        f'CREATE UNIQUE INDEX IF NOT EXISTS '
        f'"uq__{online_name}__keys" '
        f'ON {quoted_schema}."{online_name}" ({keys_sql})'
    )


def _refresh_online_view(*, target_connection: Any, target_cls: Any) -> None:
    """Phase 19: REFRESH MATERIALIZED VIEW CONCURRENTLY for online FVs.

    CONCURRENTLY requires that the MV has at least one unique index,
    which `_ensure_online_artifacts` provides. First-time refresh of a
    freshly-created MV must NOT use CONCURRENTLY (Postgres requires the
    MV to have been populated at least once non-concurrently). We try
    CONCURRENTLY first and fall back to plain REFRESH.
    """
    if not getattr(target_cls, "__is_feature_view__", False):
        return
    if not getattr(target_cls, "__online__", False):
        return
    schema = target_cls.__schema__
    table = target_cls.__tablename__
    online_name = f"{table}__online"
    quoted = f'"{schema}"."{online_name}"'
    try:
        target_connection.execute(
            f"REFRESH MATERIALIZED VIEW CONCURRENTLY {quoted}"
        )
    except Exception:
        target_connection.execute(f"REFRESH MATERIALIZED VIEW {quoted}")


def _upsert_feature_view_metadata_for_target(
    *, target_connection: Any, target_cls: Any
) -> None:
    """Look up the registered FeatureView for `target_cls` and upsert its
    metadata row. No-op if not registered (e.g., user constructed a
    ManagedTable manually with __is_feature_view__ but didn't decorate)."""
    if not getattr(target_cls, "__is_feature_view__", False):
        return
    name = (
        f"{target_cls.__schema__}.{target_cls.__tablename__}."
        f"{getattr(target_cls, '__feature_version__', 'v0')}"
    )
    fv = _FEATURE_VIEWS_REGISTRY.get(name)
    if fv is None:
        return
    upsert_feature_view_metadata(target_connection, fv)


def upsert_feature_view_metadata(
    target_connection: Any, fv: RegisteredFeatureView
) -> None:
    """Record/refresh a FeatureView row in `ematix_flow.feature_views`.
    Lazy-creates the table on first call. Updates `last_synced_at` to now().
    """
    target_connection.execute("CREATE SCHEMA IF NOT EXISTS ematix_flow")
    target_connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ematix_flow.feature_views (
            name TEXT PRIMARY KEY,
            schema_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            feature_version TEXT NOT NULL,
            entity_keys TEXT[] NOT NULL,
            event_timestamp_column TEXT,
            ttl_seconds BIGINT,
            description TEXT,
            owner TEXT,
            freshness_sla_seconds BIGINT,
            last_synced_at TIMESTAMPTZ,
            registered_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    keys_array = "ARRAY[" + ", ".join(f"'{k}'" for k in fv.entity_keys) + "]::text[]"
    ets = f"'{fv.event_timestamp_column}'" if fv.event_timestamp_column else "NULL"
    ttl = str(fv.ttl_seconds) if fv.ttl_seconds is not None else "NULL"
    sla = (
        str(fv.freshness_sla_seconds) if fv.freshness_sla_seconds is not None else "NULL"
    )
    desc = "'" + (fv.description or "").replace("'", "''") + "'" if fv.description else "NULL"
    owner = "'" + (fv.owner or "").replace("'", "''") + "'" if fv.owner else "NULL"
    target_connection.execute(
        f"""
        INSERT INTO ematix_flow.feature_views
            (name, schema_name, table_name, feature_version, entity_keys,
             event_timestamp_column, ttl_seconds, description, owner,
             freshness_sla_seconds, last_synced_at)
        VALUES
            ('{fv.name}', '{fv.schema}', '{fv.tablename}', '{fv.feature_version}',
             {keys_array}, {ets}, {ttl}, {desc}, {owner}, {sla}, now())
        ON CONFLICT (name) DO UPDATE SET
            entity_keys = EXCLUDED.entity_keys,
            event_timestamp_column = EXCLUDED.event_timestamp_column,
            ttl_seconds = EXCLUDED.ttl_seconds,
            description = EXCLUDED.description,
            owner = EXCLUDED.owner,
            freshness_sla_seconds = EXCLUDED.freshness_sla_seconds,
            last_synced_at = now()
        """
    )


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
