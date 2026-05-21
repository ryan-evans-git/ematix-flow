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
from typing import Any

from ematix_flow.warehouses import (
    WarehouseSource,
    WarehouseTarget,
    run_warehouse_pipeline,
)

__all__ = ["warehouse_pipeline"]


def warehouse_pipeline(
    *,
    source: WarehouseSource,
    target: WarehouseTarget,
    schedule: str | None,
    name: str | None = None,
    depends_on: list[str] | None = None,
    upstream_freshness_secs: int | None = None,
    retry: dict | None = None,
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
