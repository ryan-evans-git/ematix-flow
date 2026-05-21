"""Phase 2d slice 1 — ``@ematix.warehouse_pipeline`` decorator.

Wires :class:`WarehouseSource` / :class:`WarehouseTarget` into the
scheduler-registered pipeline registry so warehouse-shaped pipelines
get cron scheduling, retries, run history, and ``flow run-due``
participation the same way DB-backed ``@ematix.pipeline`` pipelines do.

Today's surface routes through the existing Python
:func:`run_warehouse_pipeline` orchestrator. Phase 2d slice 2 will
add a Rust PyO3 callback bridge so the worker process can invoke the
adapter without re-entering the Python interpreter from a Rust
Backend impl. The decorator-level API stays stable across that
migration.

Example::

    from ematix_flow import ematix
    from ematix_flow.warehouses import (
        SnowflakeConnection, WarehouseSource, WarehouseTarget,
    )

    src = SnowflakeConnection(name="src", account="...", user="...",
                              password="...", warehouse="WH")
    tgt = SnowflakeConnection(name="tgt", account="...", user="...",
                              password="...", warehouse="WH")

    @ematix.warehouse_pipeline(
        source=WarehouseSource(connection=src, sql="SELECT * FROM events",
                               kind="snowflake"),
        target=WarehouseTarget.snowflake_table(tgt, "events_copy"),
        schedule="0 * * * *",
    )
    def hourly_events_copy() -> None:
        return None

    # Optionally return a DuckDB transform SQL string:
    @ematix.warehouse_pipeline(
        source=WarehouseSource(connection=src, sql="SELECT * FROM raw",
                               kind="snowflake"),
        target=WarehouseTarget.bigquery_table(tgt, "events_clean",
                                              project="p", dataset="d"),
        schedule="*/15 * * * *",
    )
    def cleaned_events() -> str:
        return "SELECT id, lower(name) AS name FROM source WHERE id > 0"
"""
from __future__ import annotations

import functools
import inspect
from collections.abc import Callable
from typing import Any, Protocol

from ematix_flow.warehouses import (
    WarehouseSource,
    WarehouseTarget,
    bigquery_query_to_arrow,
    redshift_query_to_arrow,
    run_warehouse_pipeline,
    snowflake_query_to_arrow,
)

__all__ = [
    "InMemoryWatermarkStore",
    "WatermarkStore",
    "warehouse_pipeline",
]


# ---------------------------------------------------------------------------
# Watermark plumbing — task #559 slice 2
# ---------------------------------------------------------------------------


class WatermarkStore(Protocol):
    """Persistence surface for per-pipeline incremental-sync watermarks.

    Each pipeline that declares ``incremental_column=`` rewrites its
    next read as ``<source_sql> WHERE <col> > $watermark``. The
    watermark is stored as a string so it round-trips through any KV
    store without value-type coupling — the decorator stringifies on
    write and substitutes verbatim on read.

    Implementations only need read/write. ``None`` from :meth:`get` is
    interpreted as "no prior watermark — first run".

    The default :class:`InMemoryWatermarkStore` is process-local; for
    durable scheduling, point a SQLite / Postgres-backed implementation
    at the same table that holds run history.
    """

    def get(self, pipeline_name: str) -> str | None: ...
    def set(self, pipeline_name: str, value: str) -> None: ...


class InMemoryWatermarkStore:
    """Process-local watermark store. Loses state on restart — fine for
    tests and short-lived scheduler processes, NOT for production
    incremental syncs (those need a durable :class:`WatermarkStore`).
    """

    def __init__(self) -> None:
        self._values: dict[str, str] = {}

    def get(self, pipeline_name: str) -> str | None:
        return self._values.get(pipeline_name)

    def set(self, pipeline_name: str, value: str) -> None:
        self._values[pipeline_name] = value


def _query_to_arrow_for_kind(kind: str):
    """Return the matching ``*_query_to_arrow`` helper for a
    ``WarehouseSource.kind``. Defined once at module scope so the
    decorator wrapper doesn't re-import on every call."""
    if kind == "snowflake":
        return snowflake_query_to_arrow
    if kind == "bigquery":
        return bigquery_query_to_arrow
    if kind == "redshift":
        return redshift_query_to_arrow
    raise ValueError(f"unsupported warehouse source kind {kind!r}")


def _wrap_sql_with_watermark(
    source_sql: str, column: str, watermark: str
) -> str:
    """Rewrite ``source_sql`` as a derived subquery with a watermark
    filter. Avoids appending raw text to a query that might already
    end in ``ORDER BY`` / ``LIMIT`` / a semicolon — pushes those into
    the outer scope where they keep working.

    Watermark values are SQL-string-literal-escaped (`'` → `''`) so
    they go through verbatim regardless of the column's wire type;
    every warehouse we ship (Snowflake / BigQuery / Redshift) auto-
    casts a string literal in a comparison against a typed column.
    """
    safe = watermark.replace("'", "''")
    # Strip a trailing semicolon — `SELECT ... FROM (... ;) AS s` is a
    # syntax error on every backend.
    inner = source_sql.rstrip().rstrip(";")
    return (
        f"SELECT * FROM ({inner}) AS _ematix_src "
        f"WHERE \"{column}\" > '{safe}'"
    )


def _compute_new_watermark(arrow_table, column: str) -> str | None:
    """Return the string form of ``max(column)`` in ``arrow_table``,
    or ``None`` when the read was empty. ``None`` means "leave the
    stored watermark alone" — running an incremental sync that returns
    zero rows shouldn't blow away the previous high-water mark."""
    if arrow_table.num_rows == 0:
        return None
    if column not in arrow_table.column_names:
        raise ValueError(
            f"incremental_column={column!r} is not present in the source "
            f"result columns {arrow_table.column_names!r}; either rewrite "
            "the source SQL to project it, or drop incremental_column="
        )
    col = arrow_table.column(column)
    # Arrow's compute kernel is the same cost as a Python max over
    # `to_pylist()` for small results but Arrow's kernel is the right
    # default — and stays Arrow-native if the column is large.
    import pyarrow.compute as pc

    max_scalar = pc.max(col)
    if not max_scalar.is_valid:
        return None
    return str(max_scalar.as_py())


def warehouse_pipeline(
    *,
    source: WarehouseSource,
    target: WarehouseTarget,
    schedule: str | None,
    name: str | None = None,
    depends_on: list[str] | None = None,
    upstream_freshness_secs: int | None = None,
    retry: dict | None = None,
    # Task #559 slice 2 — warehouse-side watermarks for incremental sync.
    incremental_column: str | None = None,
    watermark_store: WatermarkStore | None = None,
) -> Callable[[Callable[..., Any]], Callable[[], dict[str, Any]]]:
    """Wrap a zero-arg function so it executes a warehouse-to-warehouse
    sync via :func:`run_warehouse_pipeline` and registers with the
    scheduler.

    Parameters mirror :meth:`_EmatixNamespace.pipeline` where they make
    sense. The wrapped function is zero-arg — source and target
    connections are bound at decorator time, not at call time. The
    function body is optional: if it returns a ``str``, that string is
    forwarded as the ``transform_sql`` argument to
    :func:`run_warehouse_pipeline` (executed via DuckDB on the in-flight
    Arrow table between source read and target write).

    Returns the wrapped callable, which is also registered in
    ``pipeline._REGISTRY`` so ``flow run-due`` and the long-running
    scheduler will fire it on schedule.
    """
    if not isinstance(source, WarehouseSource):
        raise TypeError(
            "@ematix.warehouse_pipeline requires source=WarehouseSource(...); "
            f"got {type(source).__name__}"
        )
    if not isinstance(target, WarehouseTarget):
        raise TypeError(
            "@ematix.warehouse_pipeline requires target=WarehouseTarget(...); "
            f"got {type(target).__name__}"
        )
    # Watermarked-append semantics need both halves declared.
    if incremental_column is not None and watermark_store is None:
        raise TypeError(
            "@ematix.warehouse_pipeline(incremental_column=...) requires "
            "watermark_store=... (an InMemoryWatermarkStore or a durable "
            "WatermarkStore-conforming object)"
        )
    if watermark_store is not None and incremental_column is None:
        raise TypeError(
            "@ematix.warehouse_pipeline(watermark_store=...) requires "
            "incremental_column=... (the column to track high-water mark on)"
        )

    def decorate(fn: Callable[..., Any]) -> Callable[[], dict[str, Any]]:
        sig = inspect.signature(fn)
        # Reject any positional/keyword params — warehouse pipelines
        # don't take a `conn` argument the way DB-backed pipelines do.
        # The transform body either takes zero args and returns a
        # transform SQL string (or None), nothing else.
        positional = [
            p
            for p in sig.parameters.values()
            if p.kind
            in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
            and p.default is inspect.Parameter.empty
        ]
        if positional:
            raise TypeError(
                f"@ematix.warehouse_pipeline-decorated function {fn.__name__!r} "
                f"must be zero-arg (took {len(positional)} required positional "
                "args); warehouse source/target connections are bound at "
                "decorator time. Return a transform SQL string from the body "
                "if a transform is needed, otherwise return None."
            )

        pipeline_name = name or fn.__name__

        @functools.wraps(fn)
        def wrapped() -> dict[str, Any]:
            transform_sql = fn()
            if transform_sql is not None and not isinstance(transform_sql, str):
                raise TypeError(
                    f"warehouse_pipeline {pipeline_name!r} body returned "
                    f"{type(transform_sql).__name__}; expected str (transform "
                    "SQL) or None"
                )

            # Non-incremental path — straight passthrough.
            if incremental_column is None:
                result = run_warehouse_pipeline(
                    source=source,
                    target=target,
                    transform_sql=transform_sql,
                )
                return {
                    "status": "succeeded",
                    "pipeline": pipeline_name,
                    "rows_read": result.rows_read,
                    "rows_written": result.rows_written,
                    "duration_ms": result.duration_ms,
                }

            # Incremental path. Rewrite source.sql with the prior
            # watermark (if any), do the read explicitly so we can
            # compute the new high-water mark, then hand the Arrow
            # table to the existing orchestrator via the same kind
            # dispatch it already uses on the write side.
            assert watermark_store is not None  # invariant: see decorator-time check
            prior = watermark_store.get(pipeline_name)
            effective_sql = (
                _wrap_sql_with_watermark(source.sql, incremental_column, prior)
                if prior is not None
                else source.sql
            )
            import time as _time

            started = _time.monotonic()
            reader = _query_to_arrow_for_kind(source.kind)
            arrow_in = reader(source.connection, effective_sql)
            rows_read = arrow_in.num_rows

            # Optional transform via DuckDB — same shape as
            # run_warehouse_pipeline, but inlined so we keep ownership
            # of the Arrow table for max() afterwards.
            if transform_sql:
                import duckdb  # type: ignore[import-not-found]
                con = duckdb.connect()
                try:
                    con.register("source", arrow_in)
                    arrow_out = con.execute(transform_sql).arrow()
                finally:
                    con.close()
            else:
                arrow_out = arrow_in

            # Write via the orchestrator's write half — reuse its
            # per-kind dispatch by handing it a synthetic "noop" read
            # would be wrong, so call the write helpers directly.
            from ematix_flow.warehouses import (
                bigquery_write_arrow,
                redshift_write_arrow,
                snowflake_write_arrow,
            )
            if target.kind == "snowflake":
                rows_written = snowflake_write_arrow(
                    target.connection, arrow_out,
                    table_name=target.table_name,
                    create_if_not_exists=target.create_if_not_exists,
                )
            elif target.kind == "bigquery":
                rows_written = bigquery_write_arrow(
                    target.connection, arrow_out,
                    table_name=target.table_name,
                    create_if_not_exists=target.create_if_not_exists,
                )
            elif target.kind == "redshift":
                rows_written = redshift_write_arrow(
                    target.connection, arrow_out,
                    table_name=target.table_name,
                    create_if_not_exists=target.create_if_not_exists,
                )
            else:
                raise ValueError(
                    f"unsupported warehouse target kind {target.kind!r}"
                )
            elapsed_ms = int((_time.monotonic() - started) * 1000)

            # Advance the watermark only after a successful write. A
            # failed write should leave the prior high-water mark in
            # place so the next run re-reads the same range.
            new_watermark = _compute_new_watermark(arrow_in, incremental_column)
            if new_watermark is not None:
                watermark_store.set(pipeline_name, new_watermark)

            return {
                "status": "succeeded",
                "pipeline": pipeline_name,
                "rows_read": rows_read,
                "rows_written": rows_written,
                "duration_ms": elapsed_ms,
                "watermark": watermark_store.get(pipeline_name),
            }

        # Register with the Phase 12 scheduling registry so
        # `flow run-due` + the long-running scheduler will pick it up.
        from ematix_flow import pipeline as _p

        _p.register(
            name=pipeline_name,
            schedule=schedule,
            depends_on=depends_on,
            upstream_freshness_secs=upstream_freshness_secs,
            retry=retry,
        )(wrapped)

        return wrapped

    return decorate
