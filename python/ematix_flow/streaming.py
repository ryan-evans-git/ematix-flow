"""Streaming pipeline runner — Python facade over the Rust ``flow consume`` runtime.

Two equivalent entry points:

1. **``run_streaming_pipeline(...)``** (Π.1, recommended) — takes
   typed connection objects from
   :mod:`ematix_flow.connections`. The framework serializes the
   pipeline shape internally and hands off to the Rust runner.

2. **``run_pipeline(config_str=...)`` / ``run_pipeline(config=...)``**
   (Phase Py.1) — takes a TOML string or path. Useful for the
   ``flow consume <path.toml>`` CLI shape and for callers who
   already manage their own TOML.

Both invoke the same Rust runtime — the pipeline runs on
multi-thread tokio with no records crossing the Python FFI
boundary. Python only does one-time configuration; per-batch
work is pure Rust. ``${VAR}`` env interpolation in connection
fields happens at runner-build time.

Errors raise ``ValueError`` with the underlying error string;
SIGTERM / SIGINT exits cleanly with ``shutdown_triggered=True``.

Example (Π.1, decorator-based):

    from ematix_flow import ematix
    from ematix_flow.streaming import run_streaming_pipeline

    @ematix.connection
    class kafka_prod:
        kind = "kafka"
        bootstrap_servers = "${KAFKA_BOOTSTRAP}"
        group_id = "ematix-flow"

    @ematix.connection
    class warehouse:
        kind = "postgres"
        url = "${WAREHOUSE_DSN}"

    metrics = run_streaming_pipeline(
        name="events-to-pg",
        source=kafka_prod,                        # typed connection
        source_query="events",                    # topic / queue / sub / stream
        target=warehouse,                         # typed connection
        target_table=("public", "events"),
        metrics_port=9100,
    )
    print(metrics["total_rows"], metrics["shutdown_triggered"])
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

from ematix_flow._core import (
    run_pipeline_from_path,
    run_pipeline_from_toml_str,
)
from ematix_flow.connections import (
    Connection,
    DeltaLocalConnection,
    DeltaS3Connection,
    DuckDBConnection,
    GlueSchemaRegistryConnection,
    KafkaConnection,
    KinesisConnection,
    MySQLConnection,
    ObjectStoreLocalConnection,
    ObjectStoreS3Connection,
    PostgresConnection,
    PubSubConnection,
    RabbitMQConnection,
    SchemaRegistryConnection,
    SQLiteConnection,
    get_connection,
    resolve,
)

# `PipelineMetrics` is the dict shape returned by the Rust runner:
# {"total_rows": int, "iterations": int, "shutdown_triggered": bool}.
# Kept as a type alias so callers can annotate against it.
PipelineMetrics = dict[str, Any]


@dataclass(frozen=True)
class Target:
    """Π.4a: one entry in a multi-target fan-out pipeline.

    Bundles a connection (or registered name) with its per-kind
    placement spec — ``table`` for DB / Delta targets, ``topic`` /
    ``queue`` / ``partition_key_prefix`` for streaming targets, etc.
    Pass a ``list[Target]`` as ``targets=`` to
    :func:`run_streaming_pipeline` to fan a single source's
    batches out to multiple sinks; the framework writes to every
    target before advancing source offsets.
    """

    connection: Connection | str
    table: tuple[str, str] | None = None
    topic: str | None = None
    queue: str | None = None
    partition_key_prefix: str | None = None
    prefix: str | None = None
    message_key_column: str | None = None
    partition_by: list[str] | None = field(default=None)
    # Π.1.4: object-store per-format write options. Only consulted
    # for ObjectStoreLocalConnection / ObjectStoreS3Connection
    # targets; ignored for other kinds. Use Parquet compression in
    # production unless you have a specific reason to keep raw
    # bytes — `"zstd"` is a strong default.
    parquet_compression: str | None = None  # "uncompressed" | "snappy" | "gzip" | "zstd"
    csv_delimiter: str | None = None  # single-character string
    csv_header: bool | None = None
    # Π.4e: write-side CSV parity with the read options. Apply when
    # the object-store target is a CSV sink. Defaults reproduce
    # arrow-csv's library defaults:
    #   csv_quote      = '"'  (single ASCII char)
    #   csv_escape     = doubled-quote (i.e. write "" inside quoted values)
    #   csv_null_value = ""   (empty string)
    csv_quote: str | None = None
    csv_escape: str | None = None
    csv_null_value: str | None = None
    # Π.4b: per-format READ options. Used when an object-store target
    # is on the source side of a pipeline (less common, but the same
    # backend supports both directions). Shape mirrors the Rust
    # `CsvReadOptions` / `JsonReadOptions`:
    #
    #   csv_read_options = {
    #       "has_header":      bool,
    #       "delimiter":       str,           # single ASCII char
    #       "quote":           str,           # single ASCII char
    #       "escape":          str,           # single ASCII char
    #       "terminator":      str,           # single ASCII char
    #       "comment":         str,           # single ASCII char
    #       "null_regex":      str,
    #       "truncated_rows_ok": bool,
    #       "schema_infer_max_records": int,
    #   }
    #   json_read_options = {
    #       "schema_infer_max_records": int,
    #       "batch_size":      int,
    #   }
    csv_read_options: dict | None = None
    json_read_options: dict | None = None
    # Δ.X1.2: optional user-declared primary-key column list,
    # emitted as ``[target.table].primary_key = [...]`` in the
    # generated TOML. Required for CDC pipelines whose target
    # backend can't surface PK info via reflection (Delta, object
    # stores). Harmless on Postgres CDC.
    primary_key: list[str] | None = None


@dataclass(frozen=True)
class Source:
    """Phase 39.5b P2.18: one entry in a multi-source fan-in
    pipeline. Bundles a streaming connection with the per-source
    ``query`` field (Kafka topic, Pub/Sub subscription, RabbitMQ
    queue, Kinesis stream).

    Pass a ``list[Source]`` as ``sources=`` to
    :func:`run_streaming_pipeline` for multi-source pipelines —
    each source's batches arrive at the transform with
    ``BatchContext.source_id`` set to its ``query``, which lets
    stream-stream joins route to the correct side. Use the typed
    Python form for stream-stream joins instead of raw TOML.
    """

    connection: Connection | str
    query: str


@dataclass(frozen=True)
class Aggregation:
    """Phase 39.4: one entry in a windowed transform's aggregation list.

    Mirrors the Rust ``windowed::AggregationSpec`` and the
    ``[[transform.window.aggregations]]`` TOML block. The ``column``
    field is required for everything except ``"count"`` (which is
    ``COUNT(*)``).

    PR 2 supports: ``"count"``, ``"count_col"``, ``"sum"``, ``"min"``,
    ``"max"``, ``"avg"``, ``"first"``, ``"last"``. ``"count_distinct"``
    lands in PR 2b.
    """

    agg: str
    as_: str
    column: str | None = None


@dataclass(frozen=True)
class Window:
    """Phase 39.4 / 39.5a: windowed-aggregation config for the
    ``[transform.window]`` TOML block.

    Composes with the existing ``transform_sql=`` (filter / project /
    cast / lookup-join SQL pre-stage) — the pre-stage runs first, then
    the windowed aggregator buffers into per-window per-group state
    and emits when the pipeline's watermark crosses each window's end.

    ``kind`` is ``"tumbling"``, ``"hopping"``, or ``"session"``.

    - Tumbling/hopping: set ``duration_ms``; hopping additionally
      requires ``hop_ms`` ≤ ``duration_ms``.
    - Session (39.5a): set ``gap_ms`` and ``max_session_duration_ms``;
      ``group_by`` must be non-empty; ``duration_ms`` / ``hop_ms``
      must remain at their defaults; pipelines with session windows
      must also pass a ``state_store=`` argument to
      :func:`run_streaming_pipeline`.

    ``late_data`` is ``"drop"`` (default) or ``"reopen"``.
    ``"reopen"`` requires ``allowed_lateness_ms``. ``"dlq"`` is
    reserved for a future follow-on.
    """

    kind: str
    aggregations: list[Aggregation]
    duration_ms: int = 0
    hop_ms: int | None = None
    gap_ms: int | None = None
    max_session_duration_ms: int | None = None
    event_time_column: str = "_event_ts"
    group_by: tuple[str, ...] = ()
    late_data: str = "drop"
    allowed_lateness_ms: int | None = None
    max_groups_per_window: int = 1_000_000
    window_start_column: str = "window_start"
    window_end_column: str = "window_end"
    session_id_column: str = "session_id"


@dataclass(frozen=True)
class Join:
    """Phase 39.5b: keyed time-windowed stream-stream join config
    for the ``[transform.join]`` TOML block.

    Joins consume two distinct ``[[sources]]`` (left + right) and
    emit one output row per (left, right) pair whose event-times
    fall within the configured window.

    ``kind`` (P2.12):
      - ``"inner"`` (default) — only matched pairs emit.
      - ``"left_outer"`` — every left row emits at least once
        (NULL-padded right side at eviction if no match).
      - ``"right_outer"`` — symmetric.
      - ``"full_outer"`` — both sides' unmatched rows emit
        NULL-padded at eviction.

    Window:
      - Symmetric: ``time_window_ms=N`` → ``|right_ts - left_ts| ≤ N``.
      - Asymmetric (P2.14): ``min_delta_ms=A, max_delta_ms=B`` →
        ``right_ts - left_ts ∈ [A, B] ms`` (negative A allows
        right-before-left). Overrides ``time_window_ms``.

    Late data:
      - ``"drop"`` (default) — past-horizon rows dropped.
      - ``"reopen"`` (P2.13) — extends the per-side retention
        horizon by ``allowed_lateness_ms``.

    ``left_source`` / ``right_source`` must match the ``query``
    field of two distinct configured sources — the pipeline routes
    each per-source batch to the join transform via
    ``BatchContext.source_id``.

    Pipelines configured with a ``Join`` must also pass a
    ``state_store=`` argument to :func:`run_streaming_pipeline`.
    """

    left_source: str
    right_source: str
    left_keys: tuple[str, ...]
    right_keys: tuple[str, ...]
    time_window_ms: int = 0
    kind: str = "inner"
    min_delta_ms: int | None = None
    max_delta_ms: int | None = None
    event_time_column: str = "_event_ts"
    late_data: str = "drop"
    allowed_lateness_ms: int | None = None
    left_column_prefix: str = "left_"
    right_column_prefix: str = "right_"


@dataclass(frozen=True)
class CDC:
    """Phase Δ — change-data-capture source-mode config for the
    ``[transform.cdc]`` TOML block.

    The streaming pipeline interprets the source as a CDC envelope
    (Debezium / Maxwell / custom) and dispatches each event to the
    matching per-op strategy on the target — INSERT on ``c`` /
    ``r``, UPDATE on ``u``, DELETE (or soft-delete column flip)
    on ``d``.

    The simplest form picks a built-in envelope and accepts
    every default::

        @ematix.streaming_pipeline(
            source="kafka_prod",
            source_query="dbserver1.public.customers",
            target=CustomerMirror,
            target_connection="warehouse",
            cdc=CDC(envelope="debezium"),
        )
        def mirror_customers(): ...

    Override individual fields when the upstream wire format
    diverges from the spec, or pick ``envelope="custom"`` and
    supply every required path explicitly. ``op_map`` maps
    upstream op strings to the four canonical CDC ops:
    ``"create"``, ``"update"``, ``"delete"``, ``"read"``.

    Mutually exclusive with :class:`Window` and :class:`Join` at
    config-load — CDC applies per-event change semantics; both
    of those buffer events into time windows. The CLI rejects
    the combination with a clear error.

    Configurable via this dataclass on the
    ``@ematix.streaming_pipeline`` decorator OR via the
    ``[transform.cdc]`` TOML block. Both surfaces compile to the
    same internal ``CdcConfig`` Rust struct + execution path; pick
    whichever fits the workflow.

    Plan: ``docs/PHASE_DELTA_CDC_PLAN.md``.
    """

    envelope: str = "debezium"
    op_field: str | None = None
    before_field: str | None = None
    after_field: str | None = None
    key_field: str | None = None
    ts_field: str | None = None
    op_map: dict[str, str] | None = None
    delete_mode: str = "hard"
    soft_delete_column: str | None = None
    schema_evolution: str = "skip"
    out_of_order_tolerance_ms: int = 5_000

    def __post_init__(self) -> None:
        # Lock the upfront contract so mistakes surface at
        # decoration time, not at first batch. The Rust core's
        # `cdc_toml_to_core` does the deeper validation; here
        # we catch only the obvious shape issues so a typo in
        # `envelope=` doesn't silently use Debezium defaults.
        if self.envelope not in ("debezium", "maxwell", "custom"):
            raise ValueError(
                f"CDC envelope must be 'debezium' | 'maxwell' | 'custom'; "
                f"got {self.envelope!r}"
            )
        if self.delete_mode not in ("hard", "soft"):
            raise ValueError(
                f"CDC delete_mode must be 'hard' or 'soft'; got "
                f"{self.delete_mode!r}"
            )
        if self.delete_mode == "soft" and not self.soft_delete_column:
            raise ValueError(
                "CDC delete_mode='soft' requires soft_delete_column "
                "(the target column to flip)"
            )
        if self.schema_evolution not in ("skip", "fail"):
            raise ValueError(
                f"CDC schema_evolution must be 'skip' or 'fail'; got "
                f"{self.schema_evolution!r}"
            )
        if self.envelope == "custom":
            # Custom envelope requires explicit field paths +
            # op_map. Built-in envelopes' canonical mapping fills
            # these in via the Rust core lowering, so they can be
            # left None on the dataclass.
            missing: list[str] = []
            if not self.op_field:
                missing.append("op_field")
            if not self.after_field:
                missing.append("after_field")
            if not self.key_field:
                missing.append("key_field")
            if not self.op_map:
                missing.append("op_map")
            if missing:
                raise ValueError(
                    f"CDC envelope='custom' requires {', '.join(missing)} "
                    f"to be set explicitly"
                )


@dataclass(frozen=True)
class StateStore:
    """Phase 39.5a PR 3: durable per-pipeline state-store config.

    Required when :class:`Window` ``kind="session"``. Maps to the
    top-level ``[state_store]`` TOML block (sibling of
    ``[transform]``).

    ``kind`` is ``"postgres"`` or ``"in_memory"``.
    - Postgres: requires ``url``; ``schema`` defaults to ``"public"``.
    - In-memory: persistence is process-local — restart wipes state.
      Useful for tests; loud warning at config-load in production.

    ``checkpoint_interval_ms`` bounds replay-on-restart for
    idle-but-not-empty pipelines. Defaults to 60s.
    """

    kind: str
    url: str | None = None
    schema: str = "public"
    checkpoint_interval_ms: int = 60_000


@dataclass(frozen=True)
class Lookup:
    """Phase 39.2 / 39.3: a lookup table for the
    ``[transform.lookups.<name>]`` block.

    Reuses the same typed :class:`Connection` machinery as the
    pipeline's source / target — only DB-shaped backends
    (Postgres, MySQL, SQLite, DuckDB) make sense as lookup
    sources. The lookup is loaded with ``SELECT * FROM
    <schema>.<table>`` (or ``<table>`` when ``schema`` is empty)
    at pipeline startup; with ``refresh_interval_ms`` set, a
    background tokio task re-loads on that interval and
    atomically swaps the registered DataFusion ``MemTable`` so
    the in-flight plan never sees a torn read.

    Pass a ``dict[str, Lookup]`` as ``lookups=`` to
    :func:`run_streaming_pipeline`. The dict key becomes the
    name visible in the transform SQL.
    """

    connection: Connection | str
    table: str
    schema: str = ""
    refresh_interval_ms: int | None = None


__all__ = [
    "Aggregation",
    "Join",
    "Lookup",
    "PipelineMetrics",
    "Source",
    "StateStore",
    "Target",
    "Watermark",
    "Window",
    "get_streaming_pipeline",
    "list_streaming_pipelines",
    "register_streaming_pipeline",
    "render_streaming_pipeline_toml",
    "run_pipeline",
    "run_streaming_pipeline",
]


_VALID_TRANSFORM_ON_ERROR = ("fail", "drop", "dlq")


@dataclass(frozen=True)
class Watermark:
    """Tuning knobs for the per-source watermark machinery.

    The framework auto-enables watermarking for windowed pipelines
    and stream-stream joins; this dataclass exposes the two knobs
    that were previously hardcoded to defaults at the Rust layer.

    - ``lateness_ms`` — per-source watermark slack, applied as
      ``wm_i = max(_event_ts_i) - lateness_ms``. Set to 0 if the
      source produces strictly in-order data; raise it when
      cross-partition or cross-broker reordering is expected.
    - ``source_idleness_ms`` — a source is excluded from the
      multi-source ``min(wm_i)`` after going this long without a
      batch. Defaults to 60s in the Rust core; set higher for
      bursty sources whose silent stretches shouldn't pin the
      pipeline's watermark.
    """

    lateness_ms: int | None = None
    source_idleness_ms: int | None = None

    def __post_init__(self) -> None:
        if self.lateness_ms is not None and self.lateness_ms < 0:
            raise ValueError(
                f"Watermark.lateness_ms must be ≥ 0, got {self.lateness_ms}"
            )
        if (
            self.source_idleness_ms is not None
            and self.source_idleness_ms < 0
        ):
            raise ValueError(
                f"Watermark.source_idleness_ms must be ≥ 0, got "
                f"{self.source_idleness_ms}"
            )


# ---------- Π.3: name-keyed streaming-pipeline registry -----------
#
# `@ematix.streaming_pipeline(name=..., ...)` populates this so the
# Python `flow consume --module M name` CLI can look the pipeline up
# by name and render the equivalent TOML the Rust runner already
# parses. Decoupling the registry from the function reference lets
# the CLI work with any pipeline declared in the imported module
# without the user having to expose / call the decorated function.


_STREAMING_PIPELINES: dict[str, dict[str, Any]] = {}


def register_streaming_pipeline(name: str, captured: dict[str, Any]) -> None:
    """Register a streaming pipeline's captured kwargs under ``name``.

    Called by ``@ematix.streaming_pipeline``; not normally invoked
    directly. Raises ``ValueError`` on a duplicate name so two
    pipelines in the same module can't accidentally collide.
    """
    if name in _STREAMING_PIPELINES:
        raise ValueError(
            f"a streaming pipeline named {name!r} is already registered"
        )
    _STREAMING_PIPELINES[name] = captured


def get_streaming_pipeline(name: str) -> dict[str, Any] | None:
    """Return the captured kwargs for a registered streaming pipeline,
    or ``None`` if no pipeline with that name was registered."""
    return _STREAMING_PIPELINES.get(name)


def list_streaming_pipelines() -> list[str]:
    """Return the names of all registered streaming pipelines, sorted."""
    return sorted(_STREAMING_PIPELINES.keys())


def _clear_streaming_pipelines() -> None:
    """Test-only: clear the registry between tests."""
    _STREAMING_PIPELINES.clear()


def render_streaming_pipeline_toml(name: str) -> str:
    """Look a registered streaming pipeline up by name and render the
    equivalent TOML the Rust ``flow consume`` runtime already parses.

    Used by ``flow consume --module my_pipelines <name>`` (see
    :mod:`ematix_flow.cli`) — the module-loading CLI form imports
    the user's module (which decorates pipelines into the registry),
    then this function turns the registered shape into a TOML
    string that flows through the existing TOML-driven runner.

    Raises ``KeyError`` when no pipeline named ``name`` is
    registered.
    """
    captured = _STREAMING_PIPELINES.get(name)
    if captured is None:
        known = ", ".join(sorted(_STREAMING_PIPELINES)) or "<none>"
        raise KeyError(
            f"no streaming pipeline named {name!r} is registered "
            f"(known: {known})"
        )
    return _build_toml_from_captured(captured)


def _build_toml_from_captured(captured: dict[str, Any]) -> str:
    """Apply the same validation + TOML emission ``run_streaming_pipeline``
    does, but stop one step short of handing the TOML to the Rust
    runner. Centralizes the shape so the runner path and the
    render-only path can't drift.
    """
    return _run_streaming_pipeline_emit_toml(**captured)


def run_pipeline(
    *,
    config: str | os.PathLike[str] | None = None,
    config_str: str | None = None,
    metrics_port: int | None = None,
) -> PipelineMetrics:
    """Run a streaming pipeline from a TOML config (string or path).

    Exactly one of ``config`` (path) or ``config_str`` (TOML
    contents) must be provided. Returns a dict with
    ``total_rows``, ``iterations``, and ``shutdown_triggered``.
    Blocks until SIGTERM / SIGINT or clean exit.

    For new code, prefer :func:`run_streaming_pipeline` with typed
    :class:`Connection` objects — this form stays for back-compat
    with the ``flow consume <toml>`` CLI shape.
    """
    if config is None and config_str is None:
        raise ValueError("run_pipeline: either `config` (path) or `config_str` is required")
    if config is not None and config_str is not None:
        raise ValueError("run_pipeline: pass `config` OR `config_str`, not both")
    if config_str is not None:
        return run_pipeline_from_toml_str(config_str, metrics_port)
    assert config is not None
    return run_pipeline_from_path(os.fspath(config), metrics_port)


# ---------- Π.1: typed-connection-based runner --------------------


def run_streaming_pipeline(
    *,
    name: str,
    # Single-source form (legacy + most-common). Mutually
    # exclusive with ``sources=``.
    source: Connection | str | None = None,
    source_query: str | None = None,
    # Phase 39.5b P2.18: typed multi-source form. Mutually
    # exclusive with the single ``source=``/``source_query=`` pair
    # above. Required for stream-stream joins (each `Source.query`
    # becomes the per-source `BatchContext.source_id` the join
    # routes on).
    sources: list[Source] | None = None,
    target: Connection | str | None = None,
    # Single-target placement (legacy shape; mutually exclusive with
    # ``targets=``). Exactly one of these applies depending on the
    # target connection's kind:
    target_table: tuple[str, str] | None = None,
    target_topic: str | None = None,
    target_queue: str | None = None,
    target_partition_key_prefix: str | None = None,
    target_prefix: str | None = None,
    target_message_key_column: str | None = None,
    target_partition_by: list[str] | None = None,
    # Δ.X1.2: optional user-declared PK column list, emitted as
    # ``[target.table].primary_key = [...]``. Required for CDC
    # pipelines whose target backend can't surface PK info via
    # reflection (Delta, object stores). Harmless on Postgres CDC.
    target_primary_key: list[str] | None = None,
    # Π.4a multi-target: list of typed Target specs. Mutually
    # exclusive with the single ``target=`` shape above.
    targets: list[Target] | None = None,
    idle_pause_ms: int = 500,
    dead_letter_topic: str | None = None,
    transform_sql: str | None = None,
    lookups: dict[str, Lookup] | None = None,
    window: Window | None = None,
    join: Join | None = None,
    state_store: StateStore | None = None,
    transform_on_error: str | None = None,
    watermark: Watermark | None = None,
    metrics_port: int | None = None,
    udfs: list | None = None,
    aggregate_udfs: list | None = None,
) -> PipelineMetrics:
    """Run a streaming pipeline driven by typed :class:`Connection` objects.

    ``source`` accepts either a :class:`Connection` instance
    (recommended — no string lookup) or a registry name string
    (resolved via :func:`get_connection`).

    **Single-target (legacy shape).** Pass ``target=`` plus the
    placement kwarg matching your target's kind:

    - DB targets (Postgres / MySQL / SQLite / DuckDB / Delta\\*):
      ``target_table=(schema, name)``.
    - Kafka target: ``target_topic="..."``; optional
      ``target_message_key_column="user_id"``.
    - RabbitMQ target: ``target_queue="..."``.
    - Pub/Sub target: ``target_topic="..."``.
    - Kinesis target: ``target_partition_key_prefix="..."``.
    - Object store: ``target_prefix="..."``.
    - Delta\\* targets also accept
      ``target_partition_by=["year", "month"]``.

    **Multi-target (Π.4a).** Pass ``targets=[Target(...), ...]`` to
    fan one source's batches out to N sinks. Each ``Target`` carries
    its connection plus the placement field for that sink's kind.
    The pipeline writes to every target before advancing source
    offsets — at-least-once across the fan-out.

    The two forms are mutually exclusive; pass one or the other.

    **Multi-source (P2.18).** Pass ``sources=[Source(connection=..., query=...), ...]``
    to fan in from N sources. Required for stream-stream joins —
    the ``Join`` references each source's ``query`` as
    ``left_source`` / ``right_source``.

    **UDFs (Π.5).** Pass ``udfs=[handle, ...]`` to register custom
    scalar UDFs on the SQL pre-stage. Each handle is built via the
    ``@ematix_flow.udf`` decorator and is callable from
    ``transform_sql``::

        @udf(args=("Float64", "Float64"), returns="Float64")
        def hypot(a, b):
            ...

        run_streaming_pipeline(
            ...,
            transform_sql="SELECT hypot(dx, dy) AS dist FROM source",
            udfs=[hypot],
        )

    UDFs are only consulted by the SQL pre-stage. Pure-CDC,
    pure-window-no-SQL, and pure-join pipelines silently ignore
    them — those code paths don't run a `LazySqlTransform`.

    **Aggregate UDFs.** Pass ``aggregate_udfs=[handle, ...]`` for
    per-group reductions DataFusion's stdlib doesn't ship (VWAP,
    custom percentiles, distinct-by-cardinality with custom merge
    semantics). Each handle is built via the ``@ematix_flow.udaf``
    decorator (Accumulator class with ``update_batch``,
    ``merge_batch``, ``evaluate``, ``state``)::

        @udaf(args=("Float64", "Float64"),
              state=("Float64", "Float64"),
              returns="Float64")
        class Vwap:
            ...

        run_streaming_pipeline(
            ...,
            transform_sql="SELECT vwap(price, qty) FROM source GROUP BY 1",
            aggregate_udfs=[Vwap],
        )
    """
    toml = _run_streaming_pipeline_emit_toml(
        name=name,
        source=source,
        source_query=source_query,
        sources=sources,
        target=target,
        target_table=target_table,
        target_topic=target_topic,
        target_queue=target_queue,
        target_partition_key_prefix=target_partition_key_prefix,
        target_prefix=target_prefix,
        target_message_key_column=target_message_key_column,
        target_partition_by=target_partition_by,
        target_primary_key=target_primary_key,
        targets=targets,
        idle_pause_ms=idle_pause_ms,
        dead_letter_topic=dead_letter_topic,
        transform_sql=transform_sql,
        lookups=lookups,
        window=window,
        join=join,
        state_store=state_store,
        transform_on_error=transform_on_error,
        watermark=watermark,
        metrics_port=metrics_port,
    )
    return run_pipeline_from_toml_str(toml, metrics_port, udfs, aggregate_udfs)


def _run_streaming_pipeline_emit_toml(
    *,
    name: str,
    source: Connection | str | None = None,
    source_query: str | None = None,
    sources: list[Source] | None = None,
    target: Connection | str | None = None,
    target_table: tuple[str, str] | None = None,
    target_topic: str | None = None,
    target_queue: str | None = None,
    target_partition_key_prefix: str | None = None,
    target_prefix: str | None = None,
    target_message_key_column: str | None = None,
    target_partition_by: list[str] | None = None,
    target_primary_key: list[str] | None = None,
    targets: list[Target] | None = None,
    idle_pause_ms: int = 500,
    dead_letter_topic: str | None = None,
    transform_sql: str | None = None,
    lookups: dict[str, Lookup] | None = None,
    window: Window | None = None,
    join: Join | None = None,
    state_store: StateStore | None = None,
    transform_on_error: str | None = None,
    watermark: Watermark | None = None,
    metrics_port: int | None = None,  # accepted+ignored — not in TOML
) -> str:
    """Validate the kwargs and emit the equivalent TOML the Rust
    runner already parses. Shared between
    :func:`run_streaming_pipeline` (which then invokes the runner)
    and :func:`render_streaming_pipeline_toml` (which only needs
    the rendered TOML).

    ``metrics_port`` is accepted for kwarg-shape parity with the
    runner entry point but isn't part of the TOML — it's a CLI flag
    on the run side.
    """
    del metrics_port  # not in the TOML

    if target is not None and targets is not None:
        raise ValueError(
            "run_streaming_pipeline: pass `target=` or `targets=`, not both"
        )
    if target is None and not targets:
        raise ValueError(
            "run_streaming_pipeline: a target is required — set `target=` "
            "(single-target shape) or `targets=[Target(...), ...]` (multi-target)"
        )
    # Source-shape validation: exactly one of (source, source_query)
    # pair OR `sources=[...]`.
    if (source is not None or source_query is not None) and sources is not None:
        raise ValueError(
            "run_streaming_pipeline: pass `source=`/`source_query=` OR `sources=`, not both"
        )
    single_source = source is not None or source_query is not None
    if not single_source and not sources:
        raise ValueError(
            "run_streaming_pipeline: a source is required — set `source=` + "
            "`source_query=` (single-source shape) or `sources=[Source(...), ...]` "
            "(multi-source fan-in / required for joins)"
        )
    if single_source and (source is None or source_query is None):
        raise ValueError(
            "run_streaming_pipeline: single-source shape needs both "
            "`source=` and `source_query=`"
        )
    if sources is not None and join is not None:
        queries: list[str] = [s.query for s in sources]
        if join.left_source not in queries:
            raise ValueError(
                f"Join.left_source = {join.left_source!r} doesn't match any "
                f"sources= query (got: {queries})"
            )
        if join.right_source not in queries:
            raise ValueError(
                f"Join.right_source = {join.right_source!r} doesn't match any "
                f"sources= query (got: {queries})"
            )

    source_specs: list[Source]
    if sources is not None:
        source_specs = list(sources)
    else:
        source_specs = [
            Source(
                connection=source,  # type: ignore[arg-type]
                query=source_query,  # type: ignore[arg-type]
            )
        ]

    if targets is not None:
        target_specs: list[Target] = list(targets)
    else:
        target_specs = [
            Target(
                connection=target,  # type: ignore[arg-type]
                table=target_table,
                topic=target_topic,
                queue=target_queue,
                partition_key_prefix=target_partition_key_prefix,
                prefix=target_prefix,
                message_key_column=target_message_key_column,
                partition_by=target_partition_by,
                primary_key=target_primary_key,
            )
        ]

    if lookups and not transform_sql:
        raise ValueError(
            "run_streaming_pipeline: lookups= requires transform_sql= "
            "(lookups are only meaningful joined from the transform SQL)"
        )

    if window is not None and transform_sql is None:
        transform_sql = ""

    return _build_toml_multi(
        name=name,
        sources=source_specs,
        targets=target_specs,
        idle_pause_ms=idle_pause_ms,
        dead_letter_topic=dead_letter_topic,
        transform_sql=transform_sql,
        lookups=lookups,
        window=window,
        join=join,
        state_store=state_store,
        transform_on_error=transform_on_error,
        watermark=watermark,
    )


# ---------- TOML emission (private) -------------------------------


def _build_toml(
    *,
    name: str,
    source: Connection,
    source_query: str,
    target: Connection,
    target_table: tuple[str, str] | None,
    target_topic: str | None,
    target_queue: str | None,
    target_partition_key_prefix: str | None,
    target_prefix: str | None,
    target_message_key_column: str | None,
    target_partition_by: list[str] | None,
    idle_pause_ms: int,
    dead_letter_topic: str | None,
    transform_sql: str | None = None,
    lookups: dict[str, Lookup] | None = None,
    window: Window | None = None,
    join: Join | None = None,
    state_store: StateStore | None = None,
) -> str:
    """Single-source / single-target back-compat shim. Wraps the
    flat kwargs into a one-element ``[Source]`` + one-element
    ``[Target]`` and delegates to :func:`_build_toml_multi`."""
    return _build_toml_multi(
        name=name,
        sources=[Source(connection=source, query=source_query)],
        targets=[
            Target(
                connection=target,
                table=target_table,
                topic=target_topic,
                queue=target_queue,
                partition_key_prefix=target_partition_key_prefix,
                prefix=target_prefix,
                message_key_column=target_message_key_column,
                partition_by=target_partition_by,
            )
        ],
        idle_pause_ms=idle_pause_ms,
        dead_letter_topic=dead_letter_topic,
        transform_sql=transform_sql,
        lookups=lookups,
        window=window,
        join=join,
        state_store=state_store,
    )


def _build_toml_multi(
    *,
    name: str,
    sources: list[Source],
    targets: list[Target],
    idle_pause_ms: int,
    dead_letter_topic: str | None,
    transform_sql: str | None = None,
    lookups: dict[str, Lookup] | None = None,
    window: Window | None = None,
    join: Join | None = None,
    state_store: StateStore | None = None,
    transform_on_error: str | None = None,
    watermark: Watermark | None = None,
) -> str:
    """Π.4a / P2.18: serialize a pipeline shape into the TOML
    config format.

    - 1 source → emits ``[source]`` + top-level ``source_query``.
    - 2+ sources → emits one ``[[sources]]`` block per spec
      (multi-source fan-in / required for stream-stream joins).
    - 1 target → emits ``[target]`` + ``[target.table]`` (legacy
      compact shape).
    - 2+ targets → emits one ``[[targets]]`` block per spec.

    The Rust runner accepts every combination. The TOML round-trip
    is the transport between the Python facade and the Rust runner;
    it happens once at pipeline start, per-batch work stays pure
    Rust.
    """
    if not sources:
        raise ValueError("_build_toml_multi: sources must be non-empty")
    if not targets:
        raise ValueError("_build_toml_multi: targets must be non-empty")

    lines: list[str] = [
        f"pipeline_name = {_q(name)}",
        f"idle_pause_ms = {int(idle_pause_ms)}",
    ]
    # Single-source: keep the historical compact shape (top-level
    # `source_query` + `[source]`). Multi-source: emit `[[sources]]`
    # array entries — the Rust runner branches on which form is
    # populated.
    if len(sources) == 1:
        lines.insert(1, f"source_query = {_q(sources[0].query)}")
    if dead_letter_topic is not None:
        lines.append(f"dead_letter_topic = {_q(dead_letter_topic)}")

    lines.append("")
    if len(sources) == 1:
        src_conn = (
            sources[0].connection
            if isinstance(sources[0].connection, Connection)
            else get_connection(sources[0].connection)
        )
        lines.append("[source]")
        lines.extend(_source_fields(src_conn))
    else:
        for spec in sources:
            src_conn = (
                spec.connection
                if isinstance(spec.connection, Connection)
                else get_connection(spec.connection)
            )
            lines.append("[[sources]]")
            lines.append(f"query = {_q(spec.query)}")
            lines.extend(_source_fields(src_conn))
            lines.append("")

    if len(targets) == 1:
        lines.extend(_emit_single_target_block(targets[0]))
    else:
        for spec in targets:
            lines.extend(_emit_targets_array_entry(spec))

    if (
        transform_sql is not None
        or window is not None
        or join is not None
        or transform_on_error is not None
    ):
        lines.extend(
            _emit_transform_block(
                transform_sql or "",
                lookups,
                window,
                join,
                on_error=transform_on_error,
            )
        )

    if state_store is not None:
        lines.extend(_emit_state_store_block(state_store))

    if watermark is not None:
        lines.extend(_emit_watermark_block(watermark))

    return "\n".join(lines) + "\n"


def _emit_watermark_block(wm: Watermark) -> list[str]:
    """Emit a `[watermark]` TOML block — only the fields the user
    set, so the Rust core's defaults stay authoritative for anything
    omitted."""
    lines: list[str] = ["", "[watermark]"]
    if wm.lateness_ms is not None:
        lines.append(f"lateness_ms = {int(wm.lateness_ms)}")
    if wm.source_idleness_ms is not None:
        lines.append(f"source_idleness_ms = {int(wm.source_idleness_ms)}")
    return lines


def _emit_transform_block(
    transform_sql: str,
    lookups: dict[str, Lookup] | None,
    window: Window | None = None,
    join: Join | None = None,
    *,
    on_error: str | None = None,
) -> list[str]:
    """Emit `[transform]` plus one `[transform.lookups.<name>]`
    block per entry, plus optional `[transform.window]` +
    `[[transform.window.aggregations]]` (Phase 39.4) or
    `[transform.join]` (Phase 39.5b). SQL is rendered as a TOML
    basic-multiline string so users can include newlines + double
    quotes without escaping every line.

    `[transform.window]` and `[transform.join]` are mutually
    exclusive at the Rust validator layer; we don't enforce here
    so users get a config-load error message rather than a
    pre-emit Python error.
    """
    if on_error is not None and on_error not in _VALID_TRANSFORM_ON_ERROR:
        raise ValueError(
            f"transform_on_error must be one of {_VALID_TRANSFORM_ON_ERROR!r}, "
            f"got {on_error!r}"
        )
    lines: list[str] = ["", "[transform]", f"sql = {_q_multiline(transform_sql)}"]
    if on_error is not None:
        lines.append(f"on_error = {_q(on_error)}")
    if lookups:
        for lookup_name, lookup in lookups.items():
            conn = (
                lookup.connection
                if isinstance(lookup.connection, Connection)
                else get_connection(lookup.connection)
            )
            lines.append("")
            lines.append(f"[transform.lookups.{lookup_name}]")
            lines.extend(_lookup_fields(conn, lookup))
    if window is not None:
        lines.extend(_emit_window_block(window))
    if join is not None:
        lines.extend(_emit_join_block(join))
    return lines


def _emit_window_block(window: Window) -> list[str]:
    """Emit `[transform.window]` plus one
    `[[transform.window.aggregations]]` block per Aggregation."""
    lines: list[str] = ["", "[transform.window]"]
    if window.kind not in ("tumbling", "hopping", "session"):
        raise ValueError(
            f"Window.kind must be 'tumbling', 'hopping', or 'session', got {window.kind!r}"
        )
    lines.append(f"kind = {_q(window.kind)}")
    if window.kind == "session":
        if window.gap_ms is None:
            raise ValueError("Window.gap_ms is required when kind='session'")
        if window.max_session_duration_ms is None:
            raise ValueError(
                "Window.max_session_duration_ms is required when kind='session'"
            )
        if window.duration_ms != 0:
            raise ValueError(
                "Window.duration_ms is not used when kind='session' (sessions are gap-based)"
            )
        if window.hop_ms is not None and window.hop_ms != 0:
            raise ValueError(
                "Window.hop_ms is not used when kind='session'"
            )
        if not window.group_by:
            raise ValueError(
                "Window.group_by must be non-empty when kind='session'"
            )
        lines.append(f"gap_ms = {int(window.gap_ms)}")
        lines.append(f"max_session_duration_ms = {int(window.max_session_duration_ms)}")
    else:
        lines.append(f"duration_ms = {int(window.duration_ms)}")
        if window.kind == "hopping":
            if window.hop_ms is None:
                raise ValueError("Window.hop_ms is required when kind='hopping'")
            lines.append(f"hop_ms = {int(window.hop_ms)}")
        elif window.hop_ms is not None and window.hop_ms != window.duration_ms:
            raise ValueError(
                "Window.hop_ms is only meaningful when kind='hopping'"
            )
    lines.append(f"event_time_column = {_q(window.event_time_column)}")
    if window.group_by:
        lines.append(f"group_by = {_toml_list(list(window.group_by))}")
    lines.append(f"late_data = {_q(window.late_data)}")
    if window.late_data == "reopen":
        if window.allowed_lateness_ms is None:
            raise ValueError(
                "Window.allowed_lateness_ms is required when late_data='reopen'"
            )
        lines.append(f"allowed_lateness_ms = {int(window.allowed_lateness_ms)}")
    elif window.allowed_lateness_ms is not None:
        raise ValueError(
            "Window.allowed_lateness_ms is only meaningful when late_data='reopen'"
        )
    lines.append(f"max_groups_per_window = {int(window.max_groups_per_window)}")
    if window.window_start_column != "window_start":
        lines.append(f"window_start_column = {_q(window.window_start_column)}")
    if window.window_end_column != "window_end":
        lines.append(f"window_end_column = {_q(window.window_end_column)}")
    if window.kind == "session" and window.session_id_column != "session_id":
        lines.append(f"session_id_column = {_q(window.session_id_column)}")
    if not window.aggregations:
        raise ValueError("Window.aggregations must be non-empty")
    for agg in window.aggregations:
        lines.append("")
        lines.append("[[transform.window.aggregations]]")
        lines.append(f"agg = {_q(agg.agg)}")
        if agg.column is not None:
            lines.append(f"column = {_q(agg.column)}")
        lines.append(f"as = {_q(agg.as_)}")
    return lines


def _emit_join_block(join: Join) -> list[str]:
    """Phase 39.5b: emit ``[transform.join]``.

    Handles inner / left_outer / right_outer / full_outer (P2.12),
    asymmetric windows (P2.14), and late_data = "reopen" (P2.13).
    """
    if not join.left_source or not join.right_source:
        raise ValueError("Join.left_source and Join.right_source must be non-empty")
    if join.left_source == join.right_source:
        raise ValueError("Join.left_source and Join.right_source must differ")
    if not join.left_keys or not join.right_keys:
        raise ValueError("Join.left_keys and Join.right_keys must be non-empty")
    if len(join.left_keys) != len(join.right_keys):
        raise ValueError(
            "Join.left_keys and Join.right_keys must have equal length"
        )
    if join.kind not in ("inner", "left_outer", "right_outer", "full_outer"):
        raise ValueError(
            f"Join.kind must be one of 'inner' / 'left_outer' / 'right_outer' / "
            f"'full_outer' (got {join.kind!r})"
        )
    # Window: either symmetric (`time_window_ms`) or asymmetric
    # (`min_delta_ms` + `max_delta_ms`). Validate locally so the
    # error fires before TOML emission.
    if join.min_delta_ms is None and join.max_delta_ms is None:
        if join.time_window_ms <= 0:
            raise ValueError(
                "Join.time_window_ms must be > 0 when min_delta_ms / "
                "max_delta_ms are not set"
            )
    elif join.min_delta_ms is not None and join.max_delta_ms is not None:
        if join.min_delta_ms >= join.max_delta_ms:
            raise ValueError(
                f"Join.min_delta_ms ({join.min_delta_ms}) must be < "
                f"max_delta_ms ({join.max_delta_ms})"
            )
    else:
        raise ValueError(
            "Join: min_delta_ms and max_delta_ms must be set together"
        )
    if join.late_data not in ("drop", "reopen"):
        raise ValueError(
            f"Join.late_data must be 'drop' or 'reopen' (got {join.late_data!r})"
        )
    if join.late_data == "reopen" and join.allowed_lateness_ms is None:
        raise ValueError(
            "Join.allowed_lateness_ms is required when late_data='reopen'"
        )
    if join.left_column_prefix == join.right_column_prefix:
        raise ValueError(
            "Join.left_column_prefix and Join.right_column_prefix must differ"
        )
    lines: list[str] = ["", "[transform.join]"]
    lines.append(f"kind = {_q(join.kind)}")
    lines.append(f"left_source = {_q(join.left_source)}")
    lines.append(f"right_source = {_q(join.right_source)}")
    lines.append(f"left_keys = {_toml_list(list(join.left_keys))}")
    lines.append(f"right_keys = {_toml_list(list(join.right_keys))}")
    if join.min_delta_ms is not None and join.max_delta_ms is not None:
        lines.append(f"min_delta_ms = {int(join.min_delta_ms)}")
        lines.append(f"max_delta_ms = {int(join.max_delta_ms)}")
    else:
        lines.append(f"time_window_ms = {int(join.time_window_ms)}")
    if join.event_time_column != "_event_ts":
        lines.append(f"event_time_column = {_q(join.event_time_column)}")
    if join.late_data != "drop":
        lines.append(f"late_data = {_q(join.late_data)}")
    if join.allowed_lateness_ms is not None:
        lines.append(f"allowed_lateness_ms = {int(join.allowed_lateness_ms)}")
    if join.left_column_prefix != "left_":
        lines.append(f"left_column_prefix = {_q(join.left_column_prefix)}")
    if join.right_column_prefix != "right_":
        lines.append(f"right_column_prefix = {_q(join.right_column_prefix)}")
    return lines


def _emit_state_store_block(state_store: StateStore) -> list[str]:
    """Phase 39.5a PR 3: emit the top-level ``[state_store]`` block."""
    if state_store.kind not in ("postgres", "in_memory"):
        raise ValueError(
            f"StateStore.kind must be 'postgres' or 'in_memory', got {state_store.kind!r}"
        )
    if state_store.checkpoint_interval_ms <= 0:
        raise ValueError(
            "StateStore.checkpoint_interval_ms must be > 0"
        )
    lines: list[str] = ["", "[state_store]", f"kind = {_q(state_store.kind)}"]
    if state_store.kind == "postgres":
        if not state_store.url:
            raise ValueError(
                "StateStore.url is required when kind='postgres'"
            )
        lines.append(f"url = {_q(state_store.url)}")
        if state_store.schema != "public":
            lines.append(f"schema = {_q(state_store.schema)}")
    if state_store.checkpoint_interval_ms != 60_000:
        lines.append(f"checkpoint_interval_ms = {int(state_store.checkpoint_interval_ms)}")
    return lines


def _lookup_fields(conn: Connection, lookup: Lookup) -> list[str]:
    """Per-kind fields for a `[transform.lookups.<name>]` block.
    Only DB-shaped backends are valid as lookup sources — the
    transform layer joins from a `MemTable` populated by
    `SELECT * FROM <table>`, which only DB backends provide
    natively."""
    out: list[str]
    if isinstance(conn, PostgresConnection):
        out = ['kind = "postgres"', f"url = {_q(_resolve_required(conn.url, 'url'))}"]
    elif isinstance(conn, MySQLConnection):
        out = ['kind = "mysql"', f"url = {_q(_resolve_required(conn.url, 'url'))}"]
    elif isinstance(conn, SQLiteConnection):
        out = ['kind = "sqlite"', f"path = {_q(_resolve_required(conn.path, 'path'))}"]
    elif isinstance(conn, DuckDBConnection):
        out = ['kind = "duckdb"', f"path = {_q(_resolve_required(conn.path, 'path'))}"]
    else:
        raise ValueError(
            f"connection {conn.name!r} of kind {conn.kind!r} cannot be used as a "
            "lookup (lookups: postgres, mysql, sqlite, duckdb)"
        )
    if lookup.schema:
        out.append(f"schema = {_q(lookup.schema)}")
    out.append(f"table = {_q(lookup.table)}")
    if lookup.refresh_interval_ms is not None and lookup.refresh_interval_ms > 0:
        out.append(f"refresh_interval_ms = {int(lookup.refresh_interval_ms)}")
    return out


def _q_multiline(s: str) -> str:
    """Quote `s` as a TOML basic *multiline* string. Escapes
    backslashes and triple-double-quotes; everything else passes
    through. Keeps newlines literal so user-readable SQL stays
    readable in the emitted TOML."""
    escaped = s.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')
    return f'"""\n{escaped}\n"""'


def _resolve_target_connection(spec: Target) -> Connection:
    return (
        spec.connection
        if isinstance(spec.connection, Connection)
        else get_connection(spec.connection)
    )


def _emit_single_target_block(spec: Target) -> list[str]:
    """Emit `[target]` (+ `[target.table]` for DB-shaped sinks)."""
    conn = _resolve_target_connection(spec)
    lines: list[str] = ["", "[target]"]
    lines.extend(
        _target_fields(
            conn,
            target_table=spec.table,
            target_topic=spec.topic,
            target_queue=spec.queue,
            target_partition_key_prefix=spec.partition_key_prefix,
            target_prefix=spec.prefix,
            target_message_key_column=spec.message_key_column,
            target_partition_by=spec.partition_by,
            parquet_compression=spec.parquet_compression,
            csv_delimiter=spec.csv_delimiter,
            csv_header=spec.csv_header,
            csv_quote=spec.csv_quote,
            csv_escape=spec.csv_escape,
            csv_null_value=spec.csv_null_value,
            csv_read_options=spec.csv_read_options,
            json_read_options=spec.json_read_options,
        )
    )
    if spec.table is not None:
        schema, table = spec.table
        lines.append("")
        lines.append("[target.table]")
        lines.append(f"schema = {_q(schema)}")
        lines.append(f"name = {_q(table)}")
        if spec.primary_key:
            pk_lits = ", ".join(_q(c) for c in spec.primary_key)
            lines.append(f"primary_key = [{pk_lits}]")
    return lines


def _emit_targets_array_entry(spec: Target) -> list[str]:
    """Emit one `[[targets]]` block for the multi-target shape."""
    conn = _resolve_target_connection(spec)
    lines: list[str] = ["", "[[targets]]"]
    lines.extend(
        _target_fields(
            conn,
            target_table=spec.table,
            target_topic=spec.topic,
            target_queue=spec.queue,
            target_partition_key_prefix=spec.partition_key_prefix,
            target_prefix=spec.prefix,
            target_message_key_column=spec.message_key_column,
            target_partition_by=spec.partition_by,
            parquet_compression=spec.parquet_compression,
            csv_delimiter=spec.csv_delimiter,
            csv_header=spec.csv_header,
            csv_quote=spec.csv_quote,
            csv_escape=spec.csv_escape,
            csv_null_value=spec.csv_null_value,
            csv_read_options=spec.csv_read_options,
            json_read_options=spec.json_read_options,
        )
    )
    if spec.table is not None:
        schema, table = spec.table
        lines.append("")
        lines.append("[targets.table]")
        lines.append(f"schema = {_q(schema)}")
        lines.append(f"name = {_q(table)}")
        if spec.primary_key:
            pk_lits = ", ".join(_q(c) for c in spec.primary_key)
            lines.append(f"primary_key = [{pk_lits}]")
    return lines


def _resolve_kafka_schema_registry(
    conn: KafkaConnection,
) -> SchemaRegistryConnection | GlueSchemaRegistryConnection | None:
    """Π.1: collapse ``KafkaConnection.schema_registry`` (typed) and
    ``schema_registry_url`` (legacy inline) into a single resolved
    connection, or ``None`` if SR isn't configured.

    Mutual exclusion is checked at dataclass construction time. A
    string in ``schema_registry`` is treated as a registered SR
    name; a missing registration raises ``KeyError``. The resolved
    object may be either ``SchemaRegistryConnection`` (Confluent /
    Apicurio wire format) or ``GlueSchemaRegistryConnection`` (AWS
    Glue wire format); the caller dispatches on type.
    """
    if conn.schema_registry is None and conn.schema_registry_url is None:
        return None
    if conn.schema_registry is not None:
        sr = conn.schema_registry
        if isinstance(sr, str):
            resolved = get_connection(sr)
            if not isinstance(
                resolved,
                (SchemaRegistryConnection, GlueSchemaRegistryConnection),
            ):
                raise TypeError(
                    f"KafkaConnection({conn.name!r}).schema_registry={sr!r} "
                    f"resolved to a {type(resolved).__name__}, not "
                    "SchemaRegistryConnection or GlueSchemaRegistryConnection"
                )
            return resolved
        return sr
    return SchemaRegistryConnection(
        name=f"{conn.name}_inline_sr", url=conn.schema_registry_url or ""
    )


def _kafka_payload_and_sr_lines(
    conn: KafkaConnection, *, as_source: bool = False
) -> list[str]:
    """Emit `payload_format = ...` + `schema_registry_url = ...` (and
    SR basic-auth, if configured) for a Kafka source or target.
    Shared between ``_source_fields`` and ``_target_fields`` so SR
    plumbing stays in one place.

    `auto_offset_reset` is consumer-only; only emitted when
    ``as_source=True``.
    """
    out: list[str] = []
    if conn.payload_format is not None:
        out.append(f"payload_format = {_q(conn.payload_format)}")
    if as_source and conn.auto_offset_reset is not None:
        out.append(f"auto_offset_reset = {_q(conn.auto_offset_reset)}")
    sr = _resolve_kafka_schema_registry(conn)
    if sr is None:
        return out
    if isinstance(sr, GlueSchemaRegistryConnection):
        # Task #556: Glue dispatch — emit `schema_registry_kind` as a
        # TOML inline table so the Rust side parses it into the
        # `SchemaRegistryKind::Glue { .. }` variant. No
        # `schema_registry_url` for the Glue path (the URL is
        # implicit in the IAM-backed Glue API endpoint).
        from ematix_flow.glue_schema_registry import (
            GLUE_SCHEMA_LOOKUP_BY_NAME_CALLBACK,
            GLUE_SCHEMA_LOOKUP_CALLBACK,
        )
        out.append(
            "schema_registry_kind = "
            "{ kind = \"glue\", "
            f"region = {_q(_resolve_required(sr.region, 'region'))}, "
            "registry_name = "
            f"{_q(_resolve_required(sr.registry_name, 'registry_name'))}, "
            f"schema_lookup_callback = {_q(GLUE_SCHEMA_LOOKUP_CALLBACK)}, "
            "schema_lookup_by_name_callback = "
            f"{_q(GLUE_SCHEMA_LOOKUP_BY_NAME_CALLBACK)}"
            " }"
        )
        return out
    out.append(
        f"schema_registry_url = {_q(_resolve_required(sr.url, 'url'))}"
    )
    # Basic auth is optional; both fields must be set together (the
    # `SchemaRegistryConnection` dataclass doesn't enforce pairing
    # itself so we check here).
    if sr.basic_auth_user is not None or sr.basic_auth_password is not None:
        if sr.basic_auth_user is None or sr.basic_auth_password is None:
            raise ValueError(
                f"SchemaRegistryConnection({sr.name!r}): basic_auth_user "
                "and basic_auth_password must both be set, not just one"
            )
        out.append(
            f"schema_registry_basic_auth_user = "
            f"{_q(resolve(sr.basic_auth_user))}"
        )
        out.append(
            f"schema_registry_basic_auth_password = "
            f"{_q(resolve(sr.basic_auth_password))}"
        )
    return out


def _kafka_auth_lines(conn: KafkaConnection) -> list[str]:
    """Emit Kafka auth fields (SASL/PLAIN, SASL/SCRAM, MSK-IAM)
    when configured. Mutually exclusive: at most one of the three
    may be set; the Rust runner re-validates this at backend-build
    time, but checking here gives users a faster error.
    """
    plain_set = (
        conn.sasl_plain_username is not None
        or conn.sasl_plain_password is not None
    )
    scram_set = (
        conn.sasl_scram_username is not None
        or conn.sasl_scram_password is not None
        or conn.sasl_scram_mechanism is not None
    )
    iam_set = conn.msk_iam_region is not None
    set_count = int(plain_set) + int(scram_set) + int(iam_set)
    if set_count > 1:
        raise ValueError(
            f"KafkaConnection({conn.name!r}): at most one of "
            "(SASL/PLAIN, SASL/SCRAM, MSK-IAM) may be configured; got "
            "fields from more than one"
        )
    out: list[str] = []
    if plain_set:
        if conn.sasl_plain_username is None or conn.sasl_plain_password is None:
            raise ValueError(
                f"KafkaConnection({conn.name!r}): SASL/PLAIN requires both "
                "sasl_plain_username and sasl_plain_password"
            )
        out.append(
            f"sasl_plain_username = {_q(resolve(conn.sasl_plain_username))}"
        )
        out.append(
            f"sasl_plain_password = {_q(resolve(conn.sasl_plain_password))}"
        )
    if scram_set:
        if (
            conn.sasl_scram_username is None
            or conn.sasl_scram_password is None
            or conn.sasl_scram_mechanism is None
        ):
            raise ValueError(
                f"KafkaConnection({conn.name!r}): SASL/SCRAM requires "
                "sasl_scram_username, sasl_scram_password, AND "
                "sasl_scram_mechanism (\"sha-256\" or \"sha-512\")"
            )
        out.append(
            f"sasl_scram_username = {_q(resolve(conn.sasl_scram_username))}"
        )
        out.append(
            f"sasl_scram_password = {_q(resolve(conn.sasl_scram_password))}"
        )
        out.append(
            f"sasl_scram_mechanism = {_q(conn.sasl_scram_mechanism)}"
        )
    if iam_set:
        out.append(f"msk_iam_region = {_q(resolve(conn.msk_iam_region))}")
    return out


def _source_fields(conn: Connection) -> list[str]:
    """Emit the `kind = ...` line + per-kind fields for [source]."""
    if isinstance(conn, KafkaConnection):
        bootstrap = _resolve_required(conn.bootstrap_servers, "bootstrap_servers")
        out = ['kind = "kafka"', f"bootstrap_servers = {_q(bootstrap)}"]
        if conn.group_id is not None:
            out.append(f"group_id = {_q(resolve(conn.group_id))}")
        out.extend(_kafka_payload_and_sr_lines(conn, as_source=True))
        out.extend(_kafka_auth_lines(conn))
        return out
    if isinstance(conn, RabbitMQConnection):
        amqp_url = _resolve_required(conn.amqp_url, "amqp_url")
        return ['kind = "rabbitmq"', f"amqp_url = {_q(amqp_url)}"]
    if isinstance(conn, PubSubConnection):
        project_id = _resolve_required(conn.project_id, "project_id")
        out = ['kind = "pubsub"', f"project_id = {_q(project_id)}"]
        if conn.endpoint:
            out.append(f"endpoint = {_q(resolve(conn.endpoint))}")
        if conn.anonymous_auth:
            out.append("anonymous_auth = true")
        return out
    if isinstance(conn, KinesisConnection):
        stream_name = _resolve_required(conn.stream_name, "stream_name")
        out = ['kind = "kinesis"', f"stream_name = {_q(stream_name)}"]
        if conn.region:
            out.append(f"region = {_q(resolve(conn.region))}")
        if conn.endpoint:
            out.append(f"endpoint = {_q(resolve(conn.endpoint))}")
        if conn.access_key_id:
            out.append(f"access_key_id = {_q(resolve(conn.access_key_id))}")
        if conn.secret_access_key:
            out.append(f"secret_access_key = {_q(resolve(conn.secret_access_key))}")
        return out
    # P4 #28: object-store as a streaming source. The pipeline's
    # `source_query` field carries the prefix to watch; the backend's
    # `last_seen_object_key` high-water mark + state-store recovery
    # provides at-least-once semantics. Format-specific write options
    # (parquet_compression, csv_delimiter / csv_header) live on the
    # target side and are not duplicated here — readers infer codec /
    # delimiter from file metadata.
    if isinstance(conn, ObjectStoreLocalConnection):
        return [
            'kind = "object_store_local"',
            f"path = {_q(_resolve_required(conn.path, 'path'))}",
            f"format = {_q(conn.format)}",
        ]
    if isinstance(conn, ObjectStoreS3Connection):
        return [
            'kind = "object_store_s3"',
            f"endpoint = {_q(_resolve_required(conn.endpoint, 'endpoint'))}",
            f"bucket = {_q(_resolve_required(conn.bucket, 'bucket'))}",
            f"region = {_q(_resolve_required(conn.region, 'region'))}",
            (
                "access_key_id = "
                f"{_q(_resolve_required(conn.access_key_id, 'access_key_id'))}"
            ),
            (
                "secret_access_key = "
                f"{_q(_resolve_required(conn.secret_access_key, 'secret_access_key'))}"
            ),
            f"format = {_q(conn.format)}",
        ]
    raise ValueError(
        f"connection {conn.name!r} of kind {conn.kind!r} cannot be used as a source "
        "(streaming sources: kafka, rabbitmq, pubsub, kinesis, "
        "object_store_local, object_store_s3)"
    )


_VALID_PARQUET_COMPRESSION = ("uncompressed", "snappy", "gzip", "zstd")


def _target_fields(
    conn: Connection,
    *,
    target_table: tuple[str, str] | None,
    target_topic: str | None,
    target_queue: str | None,
    target_partition_key_prefix: str | None,
    target_prefix: str | None,
    target_message_key_column: str | None,
    target_partition_by: list[str] | None,
    parquet_compression: str | None = None,
    csv_delimiter: str | None = None,
    csv_header: bool | None = None,
    csv_quote: str | None = None,
    csv_escape: str | None = None,
    csv_null_value: str | None = None,
    csv_read_options: dict | None = None,
    json_read_options: dict | None = None,
) -> list[str]:
    """Emit the `kind = ...` line + per-kind fields for [target]."""

    def need(label: str, value: Any) -> Any:
        if value is None:
            raise ValueError(
                f"target connection {conn.name!r} (kind={conn.kind!r}) requires {label}=... "
                "on run_streaming_pipeline()"
            )
        return value

    if isinstance(conn, PostgresConnection):
        need("target_table", target_table)
        return ['kind = "postgres"', f"url = {_q(_resolve_required(conn.url, 'url'))}"]
    if isinstance(conn, MySQLConnection):
        need("target_table", target_table)
        return ['kind = "mysql"', f"url = {_q(_resolve_required(conn.url, 'url'))}"]
    if isinstance(conn, SQLiteConnection):
        need("target_table", target_table)
        return ['kind = "sqlite"', f"path = {_q(_resolve_required(conn.path, 'path'))}"]
    if isinstance(conn, DuckDBConnection):
        need("target_table", target_table)
        return ['kind = "duckdb"', f"path = {_q(_resolve_required(conn.path, 'path'))}"]
    if isinstance(conn, KafkaConnection):
        topic = need("target_topic", target_topic)
        bootstrap = _resolve_required(conn.bootstrap_servers, "bootstrap_servers")
        out = [
            'kind = "kafka"',
            f"bootstrap_servers = {_q(bootstrap)}",
            f"topic = {_q(topic)}",
        ]
        if conn.group_id is not None:
            out.append(f"group_id = {_q(resolve(conn.group_id))}")
        if target_message_key_column is not None:
            out.append(f"message_key_column = {_q(target_message_key_column)}")
        out.extend(_kafka_payload_and_sr_lines(conn))
        out.extend(_kafka_auth_lines(conn))
        return out
    if isinstance(conn, RabbitMQConnection):
        queue = need("target_queue", target_queue)
        return [
            'kind = "rabbitmq"',
            f"amqp_url = {_q(_resolve_required(conn.amqp_url, 'amqp_url'))}",
            f"queue = {_q(queue)}",
        ]
    if isinstance(conn, PubSubConnection):
        topic = need("target_topic", target_topic)
        out = [
            'kind = "pubsub"',
            f"project_id = {_q(_resolve_required(conn.project_id, 'project_id'))}",
            f"topic = {_q(topic)}",
        ]
        if conn.endpoint:
            out.append(f"endpoint = {_q(resolve(conn.endpoint))}")
        if conn.anonymous_auth:
            out.append("anonymous_auth = true")
        return out
    if isinstance(conn, KinesisConnection):
        prefix = need("target_partition_key_prefix", target_partition_key_prefix)
        out = [
            'kind = "kinesis"',
            f"stream_name = {_q(_resolve_required(conn.stream_name, 'stream_name'))}",
            f"partition_key_prefix = {_q(prefix)}",
        ]
        if conn.region:
            out.append(f"region = {_q(resolve(conn.region))}")
        if conn.endpoint:
            out.append(f"endpoint = {_q(resolve(conn.endpoint))}")
        if conn.access_key_id:
            out.append(f"access_key_id = {_q(resolve(conn.access_key_id))}")
        if conn.secret_access_key:
            out.append(f"secret_access_key = {_q(resolve(conn.secret_access_key))}")
        return out
    if isinstance(conn, DeltaLocalConnection):
        need("target_table", target_table)
        out = ['kind = "delta_local"', f"path = {_q(_resolve_required(conn.path, 'path'))}"]
        if target_partition_by:
            out.append(f"partition_by = {_toml_list(target_partition_by)}")
        return out
    if isinstance(conn, DeltaS3Connection):
        need("target_table", target_table)
        out = [
            'kind = "delta_s3"',
            f"endpoint = {_q(_resolve_required(conn.endpoint, 'endpoint'))}",
            f"bucket = {_q(_resolve_required(conn.bucket, 'bucket'))}",
            f"region = {_q(_resolve_required(conn.region, 'region'))}",
            (
                "access_key_id = "
                f"{_q(_resolve_required(conn.access_key_id, 'access_key_id'))}"
            ),
            (
                "secret_access_key = "
                f"{_q(_resolve_required(conn.secret_access_key, 'secret_access_key'))}"
            ),
        ]
        if conn.prefix:
            out.append(f"prefix = {_q(resolve(conn.prefix))}")
        if target_partition_by:
            out.append(f"partition_by = {_toml_list(target_partition_by)}")
        return out
    if isinstance(conn, ObjectStoreLocalConnection):
        prefix = need("target_prefix", target_prefix)
        out = [
            'kind = "object_store_local"',
            f"path = {_q(_resolve_required(conn.path, 'path'))}",
            f"format = {_q(conn.format)}",
            f"prefix = {_q(prefix)}",
        ]
        out.extend(
            _object_store_format_lines(
                conn.format,
                parquet_compression,
                csv_delimiter,
                csv_header,
                csv_quote,
                csv_escape,
                csv_null_value,
                csv_read_options,
                json_read_options,
            )
        )
        return out
    if isinstance(conn, ObjectStoreS3Connection):
        prefix = need("target_prefix", target_prefix)
        out = [
            'kind = "object_store_s3"',
            f"endpoint = {_q(_resolve_required(conn.endpoint, 'endpoint'))}",
            f"bucket = {_q(_resolve_required(conn.bucket, 'bucket'))}",
            f"region = {_q(_resolve_required(conn.region, 'region'))}",
            (
                "access_key_id = "
                f"{_q(_resolve_required(conn.access_key_id, 'access_key_id'))}"
            ),
            (
                "secret_access_key = "
                f"{_q(_resolve_required(conn.secret_access_key, 'secret_access_key'))}"
            ),
            f"format = {_q(conn.format)}",
            f"prefix = {_q(prefix)}",
        ]
        out.extend(
            _object_store_format_lines(
                conn.format,
                parquet_compression,
                csv_delimiter,
                csv_header,
                csv_quote,
                csv_escape,
                csv_null_value,
                csv_read_options,
                json_read_options,
            )
        )
        return out
    raise ValueError(f"unknown target connection kind {conn.kind!r}")


def _object_store_format_lines(
    format_kind: str,
    parquet_compression: str | None,
    csv_delimiter: str | None,
    csv_header: bool | None,
    csv_quote: str | None = None,
    csv_escape: str | None = None,
    csv_null_value: str | None = None,
    csv_read_options: dict | None = None,
    json_read_options: dict | None = None,
) -> list[str]:
    """Π.1.4: emit the per-format option lines for object-store
    targets, with shape-correctness checks at the typed-Python
    boundary so users see the right error before TOML round-trip.

    Π.4b: also emits the [target.read_options.csv] / [target.read_options.json]
    nested tables when csv_read_options / json_read_options are set.
    """
    out: list[str] = []
    if parquet_compression is not None:
        if format_kind != "parquet":
            raise ValueError(
                f"Target.parquet_compression={parquet_compression!r} only "
                f"applies when format='parquet'; got format={format_kind!r}"
            )
        if parquet_compression not in _VALID_PARQUET_COMPRESSION:
            raise ValueError(
                f"Target.parquet_compression must be one of "
                f"{_VALID_PARQUET_COMPRESSION!r}, got {parquet_compression!r}"
            )
        out.append(f"parquet_compression = {_q(parquet_compression)}")
    if csv_delimiter is not None:
        if format_kind != "csv":
            raise ValueError(
                f"Target.csv_delimiter={csv_delimiter!r} only applies when "
                f"format='csv'; got format={format_kind!r}"
            )
        if len(csv_delimiter) != 1 or ord(csv_delimiter) > 127:
            raise ValueError(
                f"Target.csv_delimiter must be a single ASCII character, "
                f"got {csv_delimiter!r}"
            )
        out.append(f"csv_delimiter = {_q(csv_delimiter)}")
    if csv_header is not None:
        if format_kind != "csv":
            raise ValueError(
                f"Target.csv_header only applies when format='csv'; got "
                f"format={format_kind!r}"
            )
        out.append(f"csv_header = {'true' if csv_header else 'false'}")
    if csv_quote is not None:
        if format_kind != "csv":
            raise ValueError(
                f"Target.csv_quote={csv_quote!r} only applies when "
                f"format='csv'; got format={format_kind!r}"
            )
        if len(csv_quote) != 1 or ord(csv_quote) > 127:
            raise ValueError(
                f"Target.csv_quote must be a single ASCII character, "
                f"got {csv_quote!r}"
            )
        out.append(f"csv_quote = {_q(csv_quote)}")
    if csv_escape is not None:
        if format_kind != "csv":
            raise ValueError(
                f"Target.csv_escape={csv_escape!r} only applies when "
                f"format='csv'; got format={format_kind!r}"
            )
        if len(csv_escape) != 1 or ord(csv_escape) > 127:
            raise ValueError(
                f"Target.csv_escape must be a single ASCII character, "
                f"got {csv_escape!r}"
            )
        out.append(f"csv_escape = {_q(csv_escape)}")
    if csv_null_value is not None:
        if format_kind != "csv":
            raise ValueError(
                f"Target.csv_null_value={csv_null_value!r} only applies when "
                f"format='csv'; got format={format_kind!r}"
            )
        if not isinstance(csv_null_value, str):
            raise ValueError(
                f"Target.csv_null_value must be str, got {csv_null_value!r}"
            )
        out.append(f"csv_null_value = {_q(csv_null_value)}")
    if csv_read_options:
        if format_kind != "csv":
            raise ValueError(
                f"Target.csv_read_options only applies when format='csv'; "
                f"got format={format_kind!r}"
            )
        out.extend(_csv_read_options_lines(csv_read_options))
    if json_read_options:
        if format_kind not in ("json", "json_lines", "jsonl"):
            raise ValueError(
                f"Target.json_read_options only applies when format='json' "
                f"or 'json_lines'; got format={format_kind!r}"
            )
        out.extend(_json_read_options_lines(json_read_options))
    return out


# ---- Π.4b: read-option emitters ------------------------------------

_CSV_READ_OPTION_KEYS = (
    "has_header",
    "delimiter",
    "quote",
    "escape",
    "terminator",
    "comment",
    "null_regex",
    "truncated_rows_ok",
    "schema_infer_max_records",
)


def _csv_read_options_lines(opts: dict) -> list[str]:
    """Emit the `[target.read_options.csv]` block. Shape-checks each
    field so a typo here errors at decorator-evaluation time, not
    inside the Rust deserializer at runtime."""
    unknown = set(opts) - set(_CSV_READ_OPTION_KEYS)
    if unknown:
        raise ValueError(
            f"Target.csv_read_options has unknown keys {sorted(unknown)!r}; "
            f"valid: {list(_CSV_READ_OPTION_KEYS)!r}"
        )

    def _char(field_name: str, v: str) -> int:
        if not isinstance(v, str) or len(v) != 1 or ord(v) > 127:
            raise ValueError(
                f"Target.csv_read_options[{field_name!r}] must be a single "
                f"ASCII character, got {v!r}"
            )
        return ord(v)

    out: list[str] = ["", "[target.read_options.csv]"]
    if "has_header" in opts:
        v = opts["has_header"]
        if not isinstance(v, bool):
            raise ValueError(
                f"Target.csv_read_options.has_header must be bool, got {v!r}"
            )
        out.append(f"has_header = {'true' if v else 'false'}")
    # Rust side deserialises these as `u8`, so we emit the integer
    # code point (e.g. b'\t' = 9) rather than the character itself.
    for k in ("delimiter", "quote", "escape", "terminator", "comment"):
        if k in opts:
            out.append(f"{k} = {_char(k, opts[k])}")
    if "null_regex" in opts:
        v = opts["null_regex"]
        if not isinstance(v, str):
            raise ValueError(
                f"Target.csv_read_options.null_regex must be str, got {v!r}"
            )
        out.append(f"null_regex = {_q(v)}")
    if "truncated_rows_ok" in opts:
        v = opts["truncated_rows_ok"]
        if not isinstance(v, bool):
            raise ValueError(
                f"Target.csv_read_options.truncated_rows_ok must be bool, "
                f"got {v!r}"
            )
        out.append(f"truncated_rows_ok = {'true' if v else 'false'}")
    if "schema_infer_max_records" in opts:
        v = opts["schema_infer_max_records"]
        if not isinstance(v, int) or v < 1:
            raise ValueError(
                f"Target.csv_read_options.schema_infer_max_records must be "
                f"a positive int, got {v!r}"
            )
        out.append(f"schema_infer_max_records = {v}")
    return out


_JSON_READ_OPTION_KEYS = ("schema_infer_max_records", "batch_size")


def _json_read_options_lines(opts: dict) -> list[str]:
    """Emit the `[target.read_options.json]` block."""
    unknown = set(opts) - set(_JSON_READ_OPTION_KEYS)
    if unknown:
        raise ValueError(
            f"Target.json_read_options has unknown keys {sorted(unknown)!r}; "
            f"valid: {list(_JSON_READ_OPTION_KEYS)!r}"
        )
    out: list[str] = ["", "[target.read_options.json]"]
    for k in _JSON_READ_OPTION_KEYS:
        if k in opts:
            v = opts[k]
            if not isinstance(v, int) or v < 1:
                raise ValueError(
                    f"Target.json_read_options.{k} must be a positive int, "
                    f"got {v!r}"
                )
            out.append(f"{k} = {v}")
    return out


def _resolve_required(value: str, label: str) -> str:
    """Resolve env vars; treat empty/None as missing required field."""
    resolved = resolve(value)
    if not resolved:
        raise ValueError(
            f"connection field {label!r} resolves to empty after ${{VAR}} interpolation"
        )
    return resolved


def _q(s: str) -> str:
    """TOML basic-string quoting. Backslashes and quotes escaped;
    rest passes through. Sufficient for the URL / table / topic
    field shapes the runner sees — TOML basic strings handle these."""
    escaped = s.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _toml_list(items: list[str]) -> str:
    quoted = ", ".join(_q(s) for s in items)
    return f"[{quoted}]"
