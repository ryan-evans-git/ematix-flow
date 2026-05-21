"""Phase 2d slice 1 — @ematix.warehouse_pipeline decorator.

Wires WarehouseSource/WarehouseTarget into the scheduler-registered
pipeline registry so warehouse-shaped pipelines participate in cron
scheduling, retries, and ``flow run-due`` the same way DB-backed
``@ematix.pipeline`` pipelines do.

Tests use monkey-patched warehouse SDK call sites — the real
``run_warehouse_pipeline`` is exercised end-to-end via the existing
``test_warehouses.py`` suite. Here we test:

- decorator registers the wrapped fn in ``pipeline._REGISTRY``
- the wrapped callable invokes ``run_warehouse_pipeline`` with the
  correct args
- the optional fn body's return value (transform SQL string) is
  forwarded as ``transform_sql=``
- registration honors depends_on / retry / schedule like the
  existing ``@ematix.pipeline``
- name= override works; default is the function name
"""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from ematix_flow import ematix, pipeline
from ematix_flow.warehouses import (
    SnowflakeConnection,
    WarehouseSource,
    WarehouseTarget,
)


@pytest.fixture(autouse=True)
def _clear_registry():
    """Pipeline registry is module-global; clear before + after each test."""
    pipeline._REGISTRY.clear()
    pipeline._DEPENDS_ON.clear()
    pipeline._RETRY_POLICY.clear()
    pipeline._LAST_RUN.clear()
    pipeline._ATTEMPT_STATE.clear()
    yield
    pipeline._REGISTRY.clear()
    pipeline._DEPENDS_ON.clear()
    pipeline._RETRY_POLICY.clear()
    pipeline._LAST_RUN.clear()
    pipeline._ATTEMPT_STATE.clear()


def _stub_source() -> WarehouseSource:
    conn = SnowflakeConnection(
        name="src", account="a", user="u", password="p", warehouse="W"
    )
    return WarehouseSource(connection=conn, sql="SELECT 1 AS id", kind="snowflake")


def _stub_target() -> WarehouseTarget:
    conn = SnowflakeConnection(
        name="tgt", account="a", user="u", password="p", warehouse="W"
    )
    return WarehouseTarget.snowflake_table(conn, "out_table")


class TestWarehouseDecoratorRegistration:
    def test_decorator_registers_pipeline_by_function_name(self):
        @ematix.warehouse_pipeline(
            source=_stub_source(),
            target=_stub_target(),
            schedule="0 * * * *",
        )
        def my_etl() -> None:
            return None

        assert "my_etl" in pipeline._REGISTRY
        entry = pipeline._REGISTRY["my_etl"]
        assert entry.schedule == "0 * * * *"
        assert callable(entry.fn)

    def test_decorator_honors_explicit_name(self):
        @ematix.warehouse_pipeline(
            source=_stub_source(),
            target=_stub_target(),
            schedule="@hourly",
            name="custom_name",
        )
        def fn_with_diff_name() -> None:
            return None

        assert "custom_name" in pipeline._REGISTRY
        assert "fn_with_diff_name" not in pipeline._REGISTRY

    def test_decorator_honors_depends_on(self):
        @ematix.warehouse_pipeline(
            source=_stub_source(),
            target=_stub_target(),
            schedule="0 * * * *",
            name="upstream",
        )
        def upstream_etl() -> None:
            return None

        @ematix.warehouse_pipeline(
            source=_stub_source(),
            target=_stub_target(),
            schedule="0 * * * *",
            name="downstream",
            depends_on=["upstream"],
        )
        def downstream_etl() -> None:
            return None

        assert pipeline._DEPENDS_ON["downstream"] == ["upstream"]

    def test_decorator_honors_retry_policy(self):
        @ematix.warehouse_pipeline(
            source=_stub_source(),
            target=_stub_target(),
            schedule="0 * * * *",
            retry={"max_attempts": 5, "backoff": "exponential", "base_secs": 2},
        )
        def retry_etl() -> None:
            return None

        policy = pipeline._RETRY_POLICY["retry_etl"]
        assert policy.max_attempts == 5
        assert policy.backoff == "exponential"


class TestWarehouseDecoratorInvocation:
    """The wrapped callable should invoke run_warehouse_pipeline with the
    decorator-supplied source/target, optionally with transform_sql from
    the fn return value."""

    def _patch_run(self, monkeypatch) -> MagicMock:
        """Replace run_warehouse_pipeline with a MagicMock that returns a
        plausible WarehouseRunResult."""
        from ematix_flow import warehouse_pipeline as wp_mod
        from ematix_flow import warehouses as wh_mod

        fake = MagicMock(
            return_value=wh_mod.WarehouseRunResult(
                rows_read=3, rows_written=3, duration_ms=42
            )
        )
        # Patch in the warehouse_pipeline module since that's where the
        # decorator imports + calls it.
        monkeypatch.setattr(wp_mod, "run_warehouse_pipeline", fake)
        return fake

    def test_wrapped_callable_invokes_run_warehouse_pipeline(self, monkeypatch):
        fake = self._patch_run(monkeypatch)
        src = _stub_source()
        tgt = _stub_target()

        @ematix.warehouse_pipeline(source=src, target=tgt, schedule="0 * * * *")
        def my_etl() -> None:
            return None

        result = my_etl()
        fake.assert_called_once()
        kwargs = fake.call_args.kwargs
        assert kwargs["source"] is src
        assert kwargs["target"] is tgt
        # No transform_sql when fn returns None.
        assert kwargs.get("transform_sql") is None
        # Result dict is JSON-serializable + carries summary numbers.
        assert result["rows_read"] == 3
        assert result["rows_written"] == 3
        assert result["status"] == "succeeded"

    def test_fn_returning_string_passes_transform_sql(self, monkeypatch):
        fake = self._patch_run(monkeypatch)

        @ematix.warehouse_pipeline(
            source=_stub_source(),
            target=_stub_target(),
            schedule="0 * * * *",
        )
        def with_transform() -> str:
            return "SELECT id, name FROM source WHERE id > 0"

        with_transform()
        kwargs = fake.call_args.kwargs
        assert kwargs["transform_sql"] == "SELECT id, name FROM source WHERE id > 0"

    def test_wrapped_callable_records_failure(self, monkeypatch):
        from ematix_flow import warehouse_pipeline as wp_mod
        from ematix_flow.warehouses import WarehouseSyncError

        # Make run_warehouse_pipeline raise.
        def _boom(**_kwargs: Any) -> Any:
            raise WarehouseSyncError("snowflake outage")

        monkeypatch.setattr(wp_mod, "run_warehouse_pipeline", _boom)

        @ematix.warehouse_pipeline(
            source=_stub_source(),
            target=_stub_target(),
            schedule="0 * * * *",
        )
        def will_fail() -> None:
            return None

        with pytest.raises(WarehouseSyncError):
            will_fail()


class TestWarehouseDecoratorValidation:
    def test_rejects_non_warehouse_source(self):
        with pytest.raises(TypeError, match="WarehouseSource"):

            @ematix.warehouse_pipeline(
                source="not a warehouse source",  # type: ignore[arg-type]
                target=_stub_target(),
                schedule="0 * * * *",
            )
            def bad() -> None:
                return None

    def test_rejects_non_warehouse_target(self):
        with pytest.raises(TypeError, match="WarehouseTarget"):

            @ematix.warehouse_pipeline(
                source=_stub_source(),
                target="not a warehouse target",  # type: ignore[arg-type]
                schedule="0 * * * *",
            )
            def bad() -> None:
                return None

    def test_decorator_function_signature_must_be_zero_arg(self):
        """Warehouse pipelines don't take a `conn` argument — the source
        + target connections are bound at decorator time. A function
        that takes args is a misuse."""
        with pytest.raises(TypeError, match="zero-arg|0 arg"):

            @ematix.warehouse_pipeline(
                source=_stub_source(),
                target=_stub_target(),
                schedule="0 * * * *",
            )
            def bad_takes_args(conn: Any) -> None:
                return None
