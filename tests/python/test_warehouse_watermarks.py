"""Task #559 slice 2 — warehouse-side watermarks for incremental sync.

Adds ``incremental_column=`` + ``watermark_store=`` to
``@ematix.warehouse_pipeline``. On each tick the decorator looks up
the previously-stored high-water mark, rewrites the source SQL as
``SELECT * FROM (<sql>) WHERE <col> > '<watermark>'``, runs the
sync, and on success stores ``max(<col>)`` of the rows that came
back as the new watermark.

Production-mode tests pin the wire shape — what SQL leaves the
decorator, what gets stored, and what happens on failures / empty
reads / first runs.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from ematix_flow import pipeline
from ematix_flow.warehouse_pipeline import (
    InMemoryWatermarkStore,
    WatermarkStore,
    _compute_new_watermark,
    _wrap_sql_with_watermark,
    warehouse_pipeline,
)
from ematix_flow.warehouses import (
    SnowflakeConnection,
    WarehouseSource,
    WarehouseTarget,
)


@pytest.fixture(autouse=True)
def _reset_registry():
    """Each test starts with an empty pipeline registry so the
    decorator's `register()` call doesn't collide across tests."""
    pipeline._REGISTRY.clear()
    pipeline._DEPENDS_ON.clear()
    pipeline._UPSTREAM_FRESHNESS.clear()
    pipeline._LAST_RUN.clear()
    pipeline._RETRY_POLICY.clear()
    yield
    pipeline._REGISTRY.clear()
    pipeline._DEPENDS_ON.clear()
    pipeline._UPSTREAM_FRESHNESS.clear()
    pipeline._LAST_RUN.clear()
    pipeline._RETRY_POLICY.clear()


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


class TestWrapSqlWithWatermark:
    def test_basic_wrap(self) -> None:
        wrapped = _wrap_sql_with_watermark(
            "SELECT id, updated_at FROM events", "updated_at",
            "2026-05-21T10:00:00Z",
        )
        # Outer SELECT must reach the inner query as a subquery, and
        # the watermark predicate must reference the same column.
        assert "SELECT * FROM (" in wrapped
        assert ") AS _ematix_src" in wrapped
        assert '"updated_at" > \'2026-05-21T10:00:00Z\'' in wrapped
        assert "SELECT id, updated_at FROM events" in wrapped

    def test_strips_trailing_semicolon(self) -> None:
        # A trailing `;` inside the parens would make every backend
        # error out on a syntax error.
        wrapped = _wrap_sql_with_watermark(
            "SELECT * FROM t;", "ts", "v",
        )
        assert ";)" not in wrapped

    def test_escapes_single_quote_in_watermark(self) -> None:
        # Watermark values are bound by string-literal escaping rather
        # than parameter binding — `'` MUST be doubled.
        wrapped = _wrap_sql_with_watermark(
            "SELECT * FROM t", "name", "O'Brien",
        )
        assert "'O''Brien'" in wrapped
        # And nothing escapes the watermark-literal scope.
        assert "; --" not in wrapped


class TestComputeNewWatermark:
    def test_returns_string_max_for_typed_column(self) -> None:
        t = pa.table({
            "id": [1, 2, 3],
            "updated_at": ["2026-05-20", "2026-05-22", "2026-05-21"],
        })
        assert _compute_new_watermark(t, "updated_at") == "2026-05-22"

    def test_empty_table_returns_none(self) -> None:
        # Zero rows means "no progress this tick"; the stored
        # watermark should be left alone, so we return None.
        t = pa.table({"id": pa.array([], type=pa.int64())})
        assert _compute_new_watermark(t, "id") is None

    def test_missing_column_raises_clear_error(self) -> None:
        t = pa.table({"id": [1, 2]})
        with pytest.raises(ValueError, match="incremental_column"):
            _compute_new_watermark(t, "updated_at")

    def test_all_null_returns_none(self) -> None:
        t = pa.table({"ts": pa.array([None, None], type=pa.string())})
        # Arrow's max() on an all-null column produces a null scalar;
        # we treat that the same as "no progress".
        assert _compute_new_watermark(t, "ts") is None


# ---------------------------------------------------------------------------
# InMemoryWatermarkStore — conforms to the Protocol
# ---------------------------------------------------------------------------


class TestInMemoryWatermarkStore:
    def test_get_returns_none_for_unknown(self) -> None:
        store = InMemoryWatermarkStore()
        assert store.get("nope") is None

    def test_round_trip(self) -> None:
        store = InMemoryWatermarkStore()
        store.set("p", "2026-05-21")
        assert store.get("p") == "2026-05-21"

    def test_conforms_to_protocol(self) -> None:
        # Static-typing-only check: runtime-protocol-conformance test
        # via isinstance only works on @runtime_checkable Protocols.
        # Verify the duck-typing surface instead.
        store = InMemoryWatermarkStore()
        assert hasattr(store, "get")
        assert hasattr(store, "set")
        assert callable(store.get)
        assert callable(store.set)
        # And it's a subtype of WatermarkStore at runtime via duck-
        # type (no isinstance check, but the decorator only needs
        # `.get()` / `.set()`).
        _: WatermarkStore = store  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Decorator-time validation
# ---------------------------------------------------------------------------


def _src() -> WarehouseSource:
    conn = SnowflakeConnection(
        name="src", account="a", user="u", password="p", warehouse="W",
    )
    return WarehouseSource(connection=conn, sql="SELECT * FROM events",
                            kind="snowflake")


def _tgt() -> WarehouseTarget:
    conn = SnowflakeConnection(
        name="tgt", account="a", user="u", password="p", warehouse="W",
    )
    return WarehouseTarget.snowflake_table(conn, "events_copy")


class TestDecoratorValidation:
    def test_incremental_column_without_store_rejected(self) -> None:
        with pytest.raises(TypeError, match="watermark_store"):

            @warehouse_pipeline(
                source=_src(), target=_tgt(),
                schedule="0 * * * *",
                incremental_column="updated_at",
            )
            def _fn():
                return None

    def test_store_without_incremental_column_rejected(self) -> None:
        with pytest.raises(TypeError, match="incremental_column"):

            @warehouse_pipeline(
                source=_src(), target=_tgt(),
                schedule="0 * * * *",
                watermark_store=InMemoryWatermarkStore(),
            )
            def _fn():
                return None

    def test_neither_works_today(self) -> None:
        # The non-incremental path — the existing slice 1 surface —
        # must keep working without watermark plumbing.
        @warehouse_pipeline(
            source=_src(), target=_tgt(),
            schedule="0 * * * *",
            name="non_incremental",
        )
        def _fn():
            return None

        assert "non_incremental" in pipeline._REGISTRY


# ---------------------------------------------------------------------------
# End-to-end: first run + second run with rewritten SQL
# ---------------------------------------------------------------------------


class TestWatermarkedRun:
    def test_first_run_uses_original_sql_and_stores_max(self) -> None:
        store = InMemoryWatermarkStore()

        with (
            patch(
                "ematix_flow.warehouse_pipeline.snowflake_query_to_arrow"
            ) as mock_read,
            patch(
                "ematix_flow.warehouses.snowflake_write_arrow"
            ) as mock_write,
        ):
            # Simulate a fresh sync — three rows, max(updated_at)=row 3.
            mock_read.return_value = pa.table({
                "id": [1, 2, 3],
                "updated_at": [
                    "2026-05-20T10:00:00Z",
                    "2026-05-20T11:00:00Z",
                    "2026-05-21T09:30:00Z",
                ],
            })
            mock_write.return_value = 3

            @warehouse_pipeline(
                source=_src(), target=_tgt(),
                schedule="0 * * * *",
                name="incr",
                incremental_column="updated_at",
                watermark_store=store,
            )
            def _fn():
                return None

            result = _fn()

        assert result["rows_read"] == 3
        assert result["rows_written"] == 3
        # First-run SQL is the original — no watermark to substitute.
        called_sql = mock_read.call_args.args[1]
        assert called_sql == "SELECT * FROM events"
        # Stored watermark = max(updated_at).
        assert store.get("incr") == "2026-05-21T09:30:00Z"
        assert result["watermark"] == "2026-05-21T09:30:00Z"

    def test_second_run_rewrites_sql_with_prior_watermark(self) -> None:
        store = InMemoryWatermarkStore()
        store.set("incr", "2026-05-21T09:30:00Z")

        with (
            patch(
                "ematix_flow.warehouse_pipeline.snowflake_query_to_arrow"
            ) as mock_read,
            patch(
                "ematix_flow.warehouses.snowflake_write_arrow"
            ) as mock_write,
        ):
            mock_read.return_value = pa.table({
                "id": [4, 5],
                "updated_at": [
                    "2026-05-21T10:15:00Z",
                    "2026-05-21T11:00:00Z",
                ],
            })
            mock_write.return_value = 2

            @warehouse_pipeline(
                source=_src(), target=_tgt(),
                schedule="0 * * * *",
                name="incr",
                incremental_column="updated_at",
                watermark_store=store,
            )
            def _fn():
                return None

            result = _fn()

        # SQL was rewritten with the prior watermark.
        called_sql = mock_read.call_args.args[1]
        assert "SELECT * FROM (SELECT * FROM events)" in called_sql
        assert '"updated_at" > \'2026-05-21T09:30:00Z\'' in called_sql
        # Watermark advanced.
        assert store.get("incr") == "2026-05-21T11:00:00Z"
        assert result["rows_read"] == 2

    def test_empty_read_leaves_watermark_alone(self) -> None:
        store = InMemoryWatermarkStore()
        store.set("incr", "2026-05-21T09:30:00Z")

        with (
            patch(
                "ematix_flow.warehouse_pipeline.snowflake_query_to_arrow"
            ) as mock_read,
            patch(
                "ematix_flow.warehouses.snowflake_write_arrow"
            ) as mock_write,
        ):
            mock_read.return_value = pa.table({
                "id": pa.array([], type=pa.int64()),
                "updated_at": pa.array([], type=pa.string()),
            })
            mock_write.return_value = 0

            @warehouse_pipeline(
                source=_src(), target=_tgt(),
                schedule="0 * * * *",
                name="incr",
                incremental_column="updated_at",
                watermark_store=store,
            )
            def _fn():
                return None

            result = _fn()

        assert result["rows_read"] == 0
        # Stored watermark untouched.
        assert store.get("incr") == "2026-05-21T09:30:00Z"

    def test_write_failure_does_not_advance_watermark(self) -> None:
        store = InMemoryWatermarkStore()
        store.set("incr", "2026-05-21T09:30:00Z")

        with (
            patch(
                "ematix_flow.warehouse_pipeline.snowflake_query_to_arrow"
            ) as mock_read,
            patch(
                "ematix_flow.warehouses.snowflake_write_arrow"
            ) as mock_write,
        ):
            mock_read.return_value = pa.table({
                "id": [4, 5],
                "updated_at": [
                    "2026-05-21T10:15:00Z",
                    "2026-05-21T11:00:00Z",
                ],
            })
            mock_write.side_effect = RuntimeError("snowflake unavailable")

            @warehouse_pipeline(
                source=_src(), target=_tgt(),
                schedule="0 * * * *",
                name="incr",
                incremental_column="updated_at",
                watermark_store=store,
            )
            def _fn():
                return None

            with pytest.raises(RuntimeError, match="snowflake unavailable"):
                _fn()

        # Watermark MUST NOT advance — the next run needs to retry
        # this same window. Re-running with an advanced watermark
        # would silently drop the in-flight rows.
        assert store.get("incr") == "2026-05-21T09:30:00Z"


class TestCustomWatermarkStore:
    def test_decorator_uses_arbitrary_protocol_conformer(self) -> None:
        # A custom store (e.g. SQLite-backed in production) just has
        # to satisfy the `.get(name) / .set(name, value)` shape.
        backend = MagicMock()
        backend.get.return_value = "2026-05-20T00:00:00Z"

        with (
            patch(
                "ematix_flow.warehouse_pipeline.snowflake_query_to_arrow"
            ) as mock_read,
            patch(
                "ematix_flow.warehouses.snowflake_write_arrow"
            ) as mock_write,
        ):
            mock_read.return_value = pa.table({
                "id": [1],
                "updated_at": ["2026-05-21T00:00:00Z"],
            })
            mock_write.return_value = 1

            @warehouse_pipeline(
                source=_src(), target=_tgt(),
                schedule="0 * * * *",
                name="incr_custom",
                incremental_column="updated_at",
                watermark_store=backend,
            )
            def _fn():
                return None

            _fn()

        backend.get.assert_called_with("incr_custom")
        backend.set.assert_called_with("incr_custom", "2026-05-21T00:00:00Z")
