"""Phase 39 / Π.4b: streaming transforms + lookups + decorator
exposed through the Python facade.

Tests the TOML-emission shim only — no Kafka / no Rust extension
load. End-to-end runs are covered by the Rust core's integration
tests in `crates/ematix-flow-cli`.
"""

from __future__ import annotations

import pytest

from ematix_flow import (
    Aggregation,
    KafkaConnection,
    Lookup,
    PostgresConnection,
    SQLiteConnection,
    Window,
    ematix,
    register_connection,
)
from ematix_flow.connections import clear_registry
from ematix_flow.streaming import _build_toml


@pytest.fixture(autouse=True)
def _isolated_registry():
    from ematix_flow.streaming import _clear_streaming_pipelines

    clear_registry()
    _clear_streaming_pipelines()
    yield
    clear_registry()
    _clear_streaming_pipelines()


class TestTransformBlockEmission:
    def _basic_kafka_to_sqlite(self) -> tuple[KafkaConnection, SQLiteConnection]:
        src = KafkaConnection(name="src", bootstrap_servers="b:9092", group_id="g")
        tgt = SQLiteConnection(name="tgt", path=":memory:")
        return src, tgt

    def test_no_transform_omits_block(self):
        src, tgt = self._basic_kafka_to_sqlite()
        toml = _build_toml(
            name="p",
            source=src,
            source_query="events",
            target=tgt,
            target_table=("main", "events"),
            target_topic=None,
            target_queue=None,
            target_partition_key_prefix=None,
            target_prefix=None,
            target_message_key_column=None,
            target_partition_by=None,
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        assert "[transform]" not in toml

    def test_transform_sql_emits_block(self):
        src, tgt = self._basic_kafka_to_sqlite()
        toml = _build_toml(
            name="p",
            source=src,
            source_query="events",
            target=tgt,
            target_table=("main", "events"),
            target_topic=None,
            target_queue=None,
            target_partition_key_prefix=None,
            target_prefix=None,
            target_message_key_column=None,
            target_partition_by=None,
            idle_pause_ms=500,
            dead_letter_topic=None,
            transform_sql="SELECT user_id FROM source WHERE event_type = 'click'",
        )
        assert "[transform]" in toml
        assert 'sql = """' in toml
        assert "user_id FROM source WHERE event_type = 'click'" in toml

    def test_lookups_emit_per_entry_with_refresh(self):
        src, tgt = self._basic_kafka_to_sqlite()
        users_pg = PostgresConnection(name="users", url="postgres://app@host/db")
        toml = _build_toml(
            name="p",
            source=src,
            source_query="events",
            target=tgt,
            target_table=("main", "events"),
            target_topic=None,
            target_queue=None,
            target_partition_key_prefix=None,
            target_prefix=None,
            target_message_key_column=None,
            target_partition_by=None,
            idle_pause_ms=500,
            dead_letter_topic=None,
            transform_sql="SELECT s.user_id, u.name FROM source s LEFT JOIN users u ON s.user_id = u.id",
            lookups={
                "users": Lookup(
                    connection=users_pg,
                    schema="public",
                    table="users",
                    refresh_interval_ms=30000,
                ),
            },
        )
        assert "[transform.lookups.users]" in toml
        assert 'kind = "postgres"' in toml
        assert 'url = "postgres://app@host/db"' in toml
        assert 'schema = "public"' in toml
        assert 'table = "users"' in toml
        assert "refresh_interval_ms = 30000" in toml

    def test_lookup_without_refresh_omits_field(self):
        src, tgt = self._basic_kafka_to_sqlite()
        toml = _build_toml(
            name="p",
            source=src,
            source_query="events",
            target=tgt,
            target_table=("main", "events"),
            target_topic=None,
            target_queue=None,
            target_partition_key_prefix=None,
            target_prefix=None,
            target_message_key_column=None,
            target_partition_by=None,
            idle_pause_ms=500,
            dead_letter_topic=None,
            transform_sql="SELECT * FROM source",
            lookups={
                "products": Lookup(
                    connection=SQLiteConnection(name="prod", path="/tmp/p.db"),
                    table="products",
                ),
            },
        )
        assert "[transform.lookups.products]" in toml
        assert "refresh_interval_ms" not in toml, (
            "no refresh_interval_ms set ⇒ field must be omitted"
        )

    def test_lookups_require_transform_sql(self):
        from ematix_flow.streaming import run_streaming_pipeline

        src, tgt = self._basic_kafka_to_sqlite()
        register_connection(src)
        register_connection(tgt)
        with pytest.raises(ValueError, match="lookups= requires transform_sql"):
            run_streaming_pipeline(
                name="p",
                source=src,
                source_query="events",
                target=tgt,
                target_table=("main", "events"),
                lookups={
                    "users": Lookup(
                        connection=SQLiteConnection(name="u", path="/tmp/u.db"),
                        table="users",
                    ),
                },
            )

    def test_non_db_connection_rejected_as_lookup_kind(self):
        src, tgt = self._basic_kafka_to_sqlite()
        # Kafka isn't a valid lookup source — DB only.
        with pytest.raises(ValueError, match="cannot be used as a lookup"):
            _build_toml(
                name="p",
                source=src,
                source_query="events",
                target=tgt,
                target_table=("main", "events"),
                target_topic=None,
                target_queue=None,
                target_partition_key_prefix=None,
                target_prefix=None,
                target_message_key_column=None,
                target_partition_by=None,
                idle_pause_ms=500,
                dead_letter_topic=None,
                transform_sql="SELECT * FROM source",
                lookups={
                    "x": Lookup(
                        connection=src,  # Kafka — wrong kind
                        table="x",
                    ),
                },
            )


class TestWindowBlockEmission:
    def _basic(self) -> tuple[KafkaConnection, SQLiteConnection]:
        src = KafkaConnection(name="src", bootstrap_servers="b:9092", group_id="g")
        tgt = SQLiteConnection(name="tgt", path=":memory:")
        return src, tgt

    def _emit(self, **kwargs) -> str:
        src, tgt = self._basic()
        return _build_toml(
            name="p",
            source=src,
            source_query="events",
            target=tgt,
            target_table=("main", "events"),
            target_topic=None,
            target_queue=None,
            target_partition_key_prefix=None,
            target_prefix=None,
            target_message_key_column=None,
            target_partition_by=None,
            idle_pause_ms=500,
            dead_letter_topic=None,
            **kwargs,
        )

    def test_tumbling_window_emits_block(self):
        toml = self._emit(
            transform_sql="SELECT user_id, amount, _event_ts FROM source",
            window=Window(
                kind="tumbling",
                duration_ms=60_000,
                group_by=("user_id",),
                aggregations=[
                    Aggregation(agg="count", as_="n"),
                    Aggregation(agg="sum", column="amount", as_="amount_sum"),
                ],
            ),
        )
        assert "[transform.window]" in toml
        assert 'kind = "tumbling"' in toml
        assert "duration_ms = 60000" in toml
        assert 'group_by = ["user_id"]' in toml
        assert 'late_data = "drop"' in toml
        assert "max_groups_per_window = 1000000" in toml
        assert "[[transform.window.aggregations]]" in toml
        assert 'agg = "count"' in toml
        assert 'as = "n"' in toml
        assert 'agg = "sum"' in toml
        assert 'column = "amount"' in toml
        assert 'as = "amount_sum"' in toml

    def test_hopping_window_requires_hop_ms(self):
        with pytest.raises(ValueError, match="hop_ms is required"):
            self._emit(
                transform_sql="SELECT user_id, _event_ts FROM source",
                window=Window(
                    kind="hopping",
                    duration_ms=60_000,
                    aggregations=[Aggregation(agg="count", as_="n")],
                ),
            )

    def test_hopping_window_emits_hop_ms(self):
        toml = self._emit(
            transform_sql="SELECT user_id, _event_ts FROM source",
            window=Window(
                kind="hopping",
                duration_ms=60_000,
                hop_ms=15_000,
                aggregations=[Aggregation(agg="count", as_="n")],
            ),
        )
        assert 'kind = "hopping"' in toml
        assert "hop_ms = 15000" in toml

    def test_invalid_window_kind_rejected(self):
        # Phase 39.5a PR 3: "session" is now valid; only truly
        # unknown kinds are rejected.
        with pytest.raises(ValueError, match="must be 'tumbling', 'hopping', or 'session'"):
            self._emit(
                transform_sql="SELECT _event_ts FROM source",
                window=Window(
                    kind="rolling",
                    duration_ms=60_000,
                    aggregations=[Aggregation(agg="count", as_="n")],
                ),
            )

    def test_window_without_transform_sql_synthesizes_empty_sql(self):
        # Π.4b — when only window= is set, no SQL pre-stage, the
        # framework still emits [transform] with sql="" so the Rust
        # side knows about the windowed transform.
        from ematix_flow.streaming import Source, _build_toml_multi

        src, tgt = self._basic()
        toml = _build_toml_multi(
            name="p",
            sources=[Source(connection=src, query="events")],
            targets=[
                __import__("ematix_flow.streaming", fromlist=["Target"]).Target(
                    connection=tgt, table=("main", "events")
                )
            ],
            idle_pause_ms=500,
            dead_letter_topic=None,
            window=Window(
                kind="tumbling",
                duration_ms=60_000,
                aggregations=[Aggregation(agg="count", as_="n")],
            ),
        )
        assert "[transform]" in toml
        assert "[transform.window]" in toml

    def test_window_renames_canonical_columns(self):
        toml = self._emit(
            transform_sql="SELECT _event_ts FROM source",
            window=Window(
                kind="tumbling",
                duration_ms=60_000,
                aggregations=[Aggregation(agg="count", as_="n")],
                window_start_column="ws",
                window_end_column="we",
            ),
        )
        assert 'window_start_column = "ws"' in toml
        assert 'window_end_column = "we"' in toml

    def test_reopen_late_data_emits_allowed_lateness_ms(self):
        toml = self._emit(
            transform_sql="SELECT user_id, _event_ts FROM source",
            window=Window(
                kind="tumbling",
                duration_ms=60_000,
                aggregations=[Aggregation(agg="count", as_="n")],
                late_data="reopen",
                allowed_lateness_ms=30_000,
            ),
        )
        assert 'late_data = "reopen"' in toml
        assert "allowed_lateness_ms = 30000" in toml

    def test_reopen_requires_allowed_lateness_ms(self):
        with pytest.raises(ValueError, match="allowed_lateness_ms is required"):
            self._emit(
                transform_sql="SELECT _event_ts FROM source",
                window=Window(
                    kind="tumbling",
                    duration_ms=60_000,
                    aggregations=[Aggregation(agg="count", as_="n")],
                    late_data="reopen",
                ),
            )

    def test_allowed_lateness_ms_only_with_reopen(self):
        with pytest.raises(ValueError, match="only meaningful when late_data='reopen'"):
            self._emit(
                transform_sql="SELECT _event_ts FROM source",
                window=Window(
                    kind="tumbling",
                    duration_ms=60_000,
                    aggregations=[Aggregation(agg="count", as_="n")],
                    late_data="drop",
                    allowed_lateness_ms=30_000,
                ),
            )

    def test_empty_aggregations_rejected(self):
        with pytest.raises(ValueError, match="aggregations must be non-empty"):
            self._emit(
                transform_sql="SELECT _event_ts FROM source",
                window=Window(
                    kind="tumbling",
                    duration_ms=60_000,
                    aggregations=[],
                ),
            )


class TestSessionWindowAndStateStoreEmission:
    """Phase 39.5a PR 3: session window + state_store TOML emission."""

    def _basic(self) -> tuple[KafkaConnection, SQLiteConnection]:
        src = KafkaConnection(name="src", bootstrap_servers="b:9092", group_id="g")
        tgt = SQLiteConnection(name="tgt", path=":memory:")
        return src, tgt

    def _emit(self, **kwargs) -> str:
        src, tgt = self._basic()
        return _build_toml(
            name="p",
            source=src,
            source_query="events",
            target=tgt,
            target_table=("main", "events"),
            target_topic=None,
            target_queue=None,
            target_partition_key_prefix=None,
            target_prefix=None,
            target_message_key_column=None,
            target_partition_by=None,
            idle_pause_ms=500,
            dead_letter_topic=None,
            **kwargs,
        )

    def test_session_window_emits_block(self):
        from ematix_flow.streaming import StateStore

        toml = self._emit(
            transform_sql="SELECT user_id, _event_ts FROM source",
            window=Window(
                kind="session",
                gap_ms=30_000,
                max_session_duration_ms=7_200_000,
                group_by=("user_id",),
                aggregations=[Aggregation(agg="count", as_="n")],
            ),
            state_store=StateStore(kind="in_memory"),
        )
        assert 'kind = "session"' in toml
        assert "gap_ms = 30000" in toml
        assert "max_session_duration_ms = 7200000" in toml
        assert 'group_by = ["user_id"]' in toml
        # state_store block emitted at top level (sibling of [transform]).
        assert "[state_store]" in toml
        assert 'kind = "in_memory"' in toml

    def test_session_window_requires_gap_ms(self):
        with pytest.raises(ValueError, match="gap_ms is required"):
            self._emit(
                transform_sql="SELECT _event_ts FROM source",
                window=Window(
                    kind="session",
                    max_session_duration_ms=1000,
                    group_by=("user_id",),
                    aggregations=[Aggregation(agg="count", as_="n")],
                ),
            )

    def test_session_window_requires_max_session_duration_ms(self):
        with pytest.raises(ValueError, match="max_session_duration_ms is required"):
            self._emit(
                transform_sql="SELECT _event_ts FROM source",
                window=Window(
                    kind="session",
                    gap_ms=100,
                    group_by=("user_id",),
                    aggregations=[Aggregation(agg="count", as_="n")],
                ),
            )

    def test_session_window_requires_non_empty_group_by(self):
        with pytest.raises(ValueError, match="group_by must be non-empty"):
            self._emit(
                transform_sql="SELECT _event_ts FROM source",
                window=Window(
                    kind="session",
                    gap_ms=100,
                    max_session_duration_ms=1000,
                    aggregations=[Aggregation(agg="count", as_="n")],
                ),
            )

    def test_postgres_state_store_emits_url_and_schema(self):
        from ematix_flow.streaming import StateStore

        toml = self._emit(
            transform_sql="SELECT user_id, _event_ts FROM source",
            window=Window(
                kind="session",
                gap_ms=100,
                max_session_duration_ms=1000,
                group_by=("user_id",),
                aggregations=[Aggregation(agg="count", as_="n")],
            ),
            state_store=StateStore(
                kind="postgres",
                url="postgres://localhost/ematix_state",
                schema="ematix",
            ),
        )
        assert 'kind = "postgres"' in toml
        assert 'url = "postgres://localhost/ematix_state"' in toml
        assert 'schema = "ematix"' in toml

    def test_postgres_state_store_requires_url(self):
        from ematix_flow.streaming import StateStore

        with pytest.raises(ValueError, match="url is required"):
            self._emit(
                transform_sql="SELECT user_id, _event_ts FROM source",
                window=Window(
                    kind="session",
                    gap_ms=100,
                    max_session_duration_ms=1000,
                    group_by=("user_id",),
                    aggregations=[Aggregation(agg="count", as_="n")],
                ),
                state_store=StateStore(kind="postgres"),
            )

    def test_invalid_state_store_kind_rejected(self):
        from ematix_flow.streaming import StateStore

        with pytest.raises(ValueError, match="must be 'postgres' or 'in_memory'"):
            self._emit(
                transform_sql="SELECT user_id, _event_ts FROM source",
                window=Window(
                    kind="session",
                    gap_ms=100,
                    max_session_duration_ms=1000,
                    group_by=("user_id",),
                    aggregations=[Aggregation(agg="count", as_="n")],
                ),
                state_store=StateStore(kind="redis"),
            )


class TestJoinBlockEmission:
    """Phase 39.5b: stream-stream join TOML emission."""

    def _basic(self) -> tuple[KafkaConnection, SQLiteConnection]:
        src = KafkaConnection(name="src", bootstrap_servers="b:9092", group_id="g")
        tgt = SQLiteConnection(name="tgt", path=":memory:")
        return src, tgt

    def _emit(self, **kwargs) -> str:
        src, tgt = self._basic()
        return _build_toml(
            name="p",
            source=src,
            source_query="events",
            target=tgt,
            target_table=("main", "events"),
            target_topic=None,
            target_queue=None,
            target_partition_key_prefix=None,
            target_prefix=None,
            target_message_key_column=None,
            target_partition_by=None,
            idle_pause_ms=500,
            dead_letter_topic=None,
            **kwargs,
        )

    def test_join_emits_block(self):
        from ematix_flow.streaming import Join, StateStore

        toml = self._emit(
            join=Join(
                left_source="orders",
                right_source="payments",
                left_keys=("order_id",),
                right_keys=("order_id",),
                time_window_ms=300_000,
            ),
            state_store=StateStore(kind="in_memory"),
        )
        assert "[transform.join]" in toml
        # P2.12 / P2.18: default kind is "inner" (was "stream_stream_join"
        # — the CLI keeps that as a legacy alias).
        assert 'kind = "inner"' in toml
        assert 'left_source = "orders"' in toml
        assert 'right_source = "payments"' in toml
        assert 'left_keys = ["order_id"]' in toml
        assert "time_window_ms = 300000" in toml

    def test_join_requires_non_empty_keys(self):
        from ematix_flow.streaming import Join

        with pytest.raises(ValueError, match="non-empty"):
            self._emit(
                join=Join(
                    left_source="l",
                    right_source="r",
                    left_keys=(),
                    right_keys=(),
                    time_window_ms=1000,
                ),
            )

    def test_join_requires_equal_key_lengths(self):
        from ematix_flow.streaming import Join

        with pytest.raises(ValueError, match="equal length"):
            self._emit(
                join=Join(
                    left_source="l",
                    right_source="r",
                    left_keys=("a", "b"),
                    right_keys=("a",),
                    time_window_ms=1000,
                ),
            )

    def test_join_requires_distinct_sources(self):
        from ematix_flow.streaming import Join

        with pytest.raises(ValueError, match="differ"):
            self._emit(
                join=Join(
                    left_source="same",
                    right_source="same",
                    left_keys=("k",),
                    right_keys=("k",),
                    time_window_ms=1000,
                ),
            )

    def test_join_requires_positive_time_window(self):
        from ematix_flow.streaming import Join

        with pytest.raises(ValueError, match="time_window_ms"):
            self._emit(
                join=Join(
                    left_source="l",
                    right_source="r",
                    left_keys=("k",),
                    right_keys=("k",),
                    time_window_ms=0,
                ),
            )

    def test_join_rejects_unknown_late_data(self):
        from ematix_flow.streaming import Join

        with pytest.raises(ValueError, match="late_data"):
            self._emit(
                join=Join(
                    left_source="l",
                    right_source="r",
                    left_keys=("k",),
                    right_keys=("k",),
                    time_window_ms=1000,
                    late_data="yolo",
                ),
            )

    def test_join_reopen_requires_allowed_lateness_ms(self):
        from ematix_flow.streaming import Join

        with pytest.raises(ValueError, match="allowed_lateness_ms"):
            self._emit(
                join=Join(
                    left_source="l",
                    right_source="r",
                    left_keys=("k",),
                    right_keys=("k",),
                    time_window_ms=1000,
                    late_data="reopen",
                    # missing allowed_lateness_ms
                ),
            )

    def test_join_emits_outer_kind(self):
        from ematix_flow.streaming import Join, StateStore
        toml = self._emit(
            join=Join(
                left_source="orders",
                right_source="payments",
                left_keys=("k",),
                right_keys=("k",),
                time_window_ms=1000,
                kind="left_outer",
            ),
            state_store=StateStore(kind="in_memory"),
        )
        assert 'kind = "left_outer"' in toml

    def test_join_emits_asymmetric_window(self):
        from ematix_flow.streaming import Join, StateStore
        toml = self._emit(
            join=Join(
                left_source="orders",
                right_source="payments",
                left_keys=("k",),
                right_keys=("k",),
                min_delta_ms=0,
                max_delta_ms=10_000,
            ),
            state_store=StateStore(kind="in_memory"),
        )
        assert "min_delta_ms = 0" in toml
        assert "max_delta_ms = 10000" in toml
        # When asymmetric is set, time_window_ms is omitted.
        assert "time_window_ms" not in toml


class TestMultiSourceTomlEmission:
    """Phase 39.5b P2.18: typed-Python multi-source TOML emission."""

    def _conns(self) -> tuple[KafkaConnection, KafkaConnection, SQLiteConnection]:
        a = KafkaConnection(name="kafka_a", bootstrap_servers="b:9092", group_id="g")
        b = KafkaConnection(name="kafka_b", bootstrap_servers="b:9092", group_id="g")
        tgt = SQLiteConnection(name="tgt", path=":memory:")
        return a, b, tgt

    def test_multi_source_emits_double_brackets_array(self):
        from ematix_flow.streaming import Source, Target, _build_toml_multi

        a, b, tgt = self._conns()
        toml = _build_toml_multi(
            name="merge",
            sources=[
                Source(connection=a, query="orders"),
                Source(connection=b, query="payments"),
            ],
            targets=[Target(connection=tgt, table=("main", "joined"))],
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        # Multi-source emits array entries, not [source]/source_query.
        assert "[[sources]]" in toml
        assert toml.count("[[sources]]") == 2
        assert 'query = "orders"' in toml
        assert 'query = "payments"' in toml
        # The single-source compact shape is NOT present.
        assert "\n[source]\n" not in toml
        assert "source_query =" not in toml

    def test_single_source_keeps_legacy_compact_shape(self):
        # P2.18 doesn't disturb the historical single-source emit:
        # the [source] block + top-level source_query stays.
        from ematix_flow.streaming import Source, Target, _build_toml_multi

        a, _b, tgt = self._conns()
        toml = _build_toml_multi(
            name="single",
            sources=[Source(connection=a, query="events")],
            targets=[Target(connection=tgt, table=("main", "events"))],
            idle_pause_ms=500,
            dead_letter_topic=None,
        )
        assert "\n[source]" in toml
        assert 'source_query = "events"' in toml
        assert "[[sources]]" not in toml

    def test_run_streaming_pipeline_join_via_typed_python(self):
        """The whole point of P2.18: stream-stream join driven from
        typed Python, not raw TOML. We don't actually run the
        pipeline (no Kafka); we verify the TOML produced has the
        expected multi-source + [transform.join] + [state_store]
        shape."""
        from ematix_flow.streaming import (
            Join,
            Source,
            StateStore,
            Target,
            _build_toml_multi,
        )

        a, b, tgt = self._conns()
        toml = _build_toml_multi(
            name="orders-payments",
            sources=[
                Source(connection=a, query="orders"),
                Source(connection=b, query="payments"),
            ],
            targets=[Target(connection=tgt, table=("main", "joined"))],
            idle_pause_ms=500,
            dead_letter_topic=None,
            join=Join(
                left_source="orders",
                right_source="payments",
                left_keys=("order_id",),
                right_keys=("order_id",),
                time_window_ms=300_000,
            ),
            state_store=StateStore(kind="in_memory"),
        )
        assert "[[sources]]" in toml
        assert "[transform.join]" in toml
        assert 'left_source = "orders"' in toml
        assert "[state_store]" in toml

    def test_run_streaming_pipeline_rejects_both_source_and_sources(self):
        from ematix_flow.streaming import Source, run_streaming_pipeline

        a, _b, tgt = self._conns()
        register_connection(a)
        register_connection(tgt)
        # Both single + multi source forms set → rejected upfront.
        with pytest.raises(ValueError, match="not both"):
            run_streaming_pipeline(
                name="bad",
                source=a,
                source_query="orders",
                sources=[Source(connection=a, query="orders")],
                target=tgt,
                target_table=("main", "x"),
            )

    def test_join_validates_left_right_source_against_sources_list(self):
        from ematix_flow.streaming import Join, Source, run_streaming_pipeline

        a, b, tgt = self._conns()
        register_connection(a)
        register_connection(b)
        register_connection(tgt)
        # `Join.left_source = "orders"` but `sources=[..."order_events"...]`.
        with pytest.raises(ValueError, match="left_source"):
            run_streaming_pipeline(
                name="typo",
                sources=[
                    Source(connection=a, query="order_events"),
                    Source(connection=b, query="payments"),
                ],
                target=tgt,
                target_table=("main", "joined"),
                join=Join(
                    left_source="orders",  # typo
                    right_source="payments",
                    left_keys=("k",),
                    right_keys=("k",),
                    time_window_ms=1000,
                ),
            )


class TestStreamingPipelineDecorator:
    def test_decorator_captures_kwargs(self):
        src = KafkaConnection(name="src", bootstrap_servers="b:9092")
        tgt = SQLiteConnection(name="tgt", path=":memory:")
        register_connection(src)
        register_connection(tgt)

        @ematix.streaming_pipeline(
            name="events-clean",
            source=src,
            source_query="events",
            target=tgt,
            target_table=("main", "events_clean"),
            transform_sql="SELECT * FROM source",
        )
        def events_clean():
            pass

        captured = events_clean.__ematix_streaming_pipeline__
        assert captured["name"] == "events-clean"
        assert captured["source"] is src
        assert captured["source_query"] == "events"
        assert captured["transform_sql"] == "SELECT * FROM source"
        assert captured["target_table"] == ("main", "events_clean")

    def test_decorator_captures_multi_source_join_kwargs(self):
        # Phase 39.5b P2.18: the decorator accepts the typed-Python
        # multi-source shape so stream-stream joins can be declared
        # without raw TOML.
        from ematix_flow.streaming import Join, Source, StateStore

        a = KafkaConnection(name="kafka_a", bootstrap_servers="b:9092", group_id="g")
        b = KafkaConnection(name="kafka_b", bootstrap_servers="b:9092", group_id="g")
        tgt = SQLiteConnection(name="tgt", path=":memory:")
        for c in (a, b, tgt):
            register_connection(c)

        @ematix.streaming_pipeline(
            name="orders-payments",
            sources=[
                Source(connection=a, query="orders"),
                Source(connection=b, query="payments"),
            ],
            target=tgt,
            target_table=("main", "joined"),
            join=Join(
                left_source="orders",
                right_source="payments",
                left_keys=("order_id",),
                right_keys=("order_id",),
                time_window_ms=300_000,
            ),
            state_store=StateStore(kind="in_memory"),
        )
        def orders_payments():
            pass

        captured = orders_payments.__ematix_streaming_pipeline__
        assert captured["sources"] is not None
        assert len(captured["sources"]) == 2
        assert captured["sources"][0].query == "orders"
        assert captured["join"].left_source == "orders"
        assert captured["state_store"].kind == "in_memory"

    def test_decorator_requires_zero_arg_function(self):
        src = KafkaConnection(name="src", bootstrap_servers="b:9092")
        tgt = SQLiteConnection(name="tgt", path=":memory:")

        with pytest.raises(TypeError, match="must take 0 arguments"):

            @ematix.streaming_pipeline(
                name="bad",
                source=src,
                source_query="events",
                target=tgt,
                target_table=("main", "events"),
            )
            def too_many_args(conn):  # type: ignore[unused-ignore]
                pass


class TestStreamingPipelineRegistry:
    """Π.3: name-keyed registry + ``render_streaming_pipeline_toml``
    so ``flow consume --module M name`` can look up a pipeline by
    name and render the equivalent TOML the Rust runner expects.
    """

    def test_decorator_registers_pipeline_by_name(self):
        from ematix_flow.streaming import (
            get_streaming_pipeline,
            list_streaming_pipelines,
        )

        src = KafkaConnection(
            name="src", bootstrap_servers="b:9092", group_id="g"
        )
        tgt = SQLiteConnection(name="tgt", path=":memory:")
        register_connection(src)
        register_connection(tgt)

        @ematix.streaming_pipeline(
            name="events-clean",
            source=src,
            source_query="events",
            target=tgt,
            target_table=("main", "events_clean"),
        )
        def events_clean():
            pass

        captured = get_streaming_pipeline("events-clean")
        assert captured is not None
        assert captured["name"] == "events-clean"
        assert captured["source"] is src
        assert captured["source_query"] == "events"
        assert "events-clean" in list_streaming_pipelines()

    def test_decorator_rejects_duplicate_name(self):
        src = KafkaConnection(
            name="src", bootstrap_servers="b:9092", group_id="g"
        )
        tgt = SQLiteConnection(name="tgt", path=":memory:")
        register_connection(src)
        register_connection(tgt)

        @ematix.streaming_pipeline(
            name="dupe",
            source=src,
            source_query="events",
            target=tgt,
            target_table=("main", "a"),
        )
        def first():
            pass

        with pytest.raises(ValueError, match="already registered"):

            @ematix.streaming_pipeline(
                name="dupe",
                source=src,
                source_query="events",
                target=tgt,
                target_table=("main", "b"),
            )
            def second():
                pass

    def test_render_streaming_pipeline_toml_emits_valid_toml(self):
        # The TOML must look like what `_build_toml` produces — same
        # shape `flow consume <toml>` already parses. The Rust
        # parser's correctness is covered by the CLI crate's
        # integration tests.
        from ematix_flow.streaming import render_streaming_pipeline_toml

        src = KafkaConnection(
            name="src", bootstrap_servers="b:9092", group_id="g"
        )
        tgt = SQLiteConnection(name="tgt", path=":memory:")
        register_connection(src)
        register_connection(tgt)

        @ematix.streaming_pipeline(
            name="events-render",
            source=src,
            source_query="events",
            target=tgt,
            target_table=("main", "events_clean"),
            transform_sql="SELECT * FROM source",
        )
        def events_render():
            pass

        toml = render_streaming_pipeline_toml("events-render")
        assert 'pipeline_name = "events-render"' in toml
        assert "[source]" in toml
        assert "[target]" in toml
        assert "events_clean" in toml
        assert "SELECT * FROM source" in toml

    def test_render_unknown_name_raises(self):
        from ematix_flow.streaming import render_streaming_pipeline_toml

        with pytest.raises(KeyError, match="no streaming pipeline"):
            render_streaming_pipeline_toml("does-not-exist")
