"""Task #559 final slice — @ematix.warehouse_pipeline registers a
Rust-callable callback at decoration time.

Once the bridge is in place, the Rust scheduler can invoke warehouse
pipelines via the py_callbacks registry instead of spawning a Python
subprocess per tick. The Rust executor itself is a follow-up;
this slice puts the foundation in.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from ematix_flow import pipeline
from ematix_flow.warehouse_pipeline import (
    WAREHOUSE_PIPELINE_CALLBACK_PREFIX,
    warehouse_pipeline,
    warehouse_pipeline_callback_name,
)
from ematix_flow.warehouses import (
    SnowflakeConnection,
    WarehouseSource,
    WarehouseTarget,
)


@pytest.fixture(autouse=True)
def _reset_registry():
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


def _src() -> WarehouseSource:
    conn = SnowflakeConnection(
        name="s", account="a", user="u", password="p", warehouse="W",
    )
    return WarehouseSource(connection=conn, sql="SELECT 1", kind="snowflake")


def _tgt() -> WarehouseTarget:
    conn = SnowflakeConnection(
        name="t", account="a", user="u", password="p", warehouse="W",
    )
    return WarehouseTarget.snowflake_table(conn, "out")


class TestCallbackNameConvention:
    def test_callback_name_is_prefix_plus_pipeline_name(self) -> None:
        assert warehouse_pipeline_callback_name("orders_sync") == (
            WAREHOUSE_PIPELINE_CALLBACK_PREFIX + "orders_sync"
        )


class TestCallbackRegistrationOnDecoration:
    def test_registers_when_extension_available(self) -> None:
        # Patch the binding on the ematix_flow package — the decorator
        # imports via `from ematix_flow import _core`, which resolves
        # through the package's attribute, not sys.modules. The real
        # _core may or may not be loaded; either way patching the
        # attribute is the correct test seam.
        from unittest.mock import patch

        import ematix_flow as _pkg

        fake_core = MagicMock()
        with patch.object(_pkg, "_core", fake_core, create=True):
            @warehouse_pipeline(
                source=_src(), target=_tgt(),
                schedule="0 * * * *",
                name="my_sync",
            )
            def _fn():
                return None
        # The callback registration happened with the expected name.
        fake_core.register_python_callback.assert_called_once()
        callback_name, adapter = fake_core.register_python_callback.call_args.args
        assert callback_name == WAREHOUSE_PIPELINE_CALLBACK_PREFIX + "my_sync"
        assert callable(adapter)

    def test_silent_when_extension_missing(self) -> None:
        # Patch the ematix_flow package so `from ematix_flow import _core`
        # raises ImportError, simulating a build without the maturin
        # extension. Decoration must NOT raise — falls back to the
        # existing in-process scheduler path.
        from unittest.mock import patch

        import ematix_flow as _pkg

        # Temporarily remove the _core attribute so the `from ... import _core`
        # inside the decorator raises ImportError.
        saved = getattr(_pkg, "_core", None)
        if hasattr(_pkg, "_core"):
            delattr(_pkg, "_core")
        try:
            @warehouse_pipeline(
                source=_src(), target=_tgt(),
                schedule="0 * * * *",
                name="silent_pipeline",
            )
            def _fn():
                return None
        finally:
            if saved is not None:
                _pkg._core = saved
        # The pipeline still got registered via the scheduler.
        assert "silent_pipeline" in pipeline._REGISTRY
        del patch  # unused


class TestCallbackAdapterShape:
    """The adapter is `bytes → bytes` (JSON in, JSON out). The Rust
    side calls it with `b"{}"` and expects a JSON-encoded run result
    back. We exercise the adapter directly to lock the contract."""

    def test_adapter_invokes_wrapped_and_returns_json(self) -> None:
        from unittest.mock import patch

        import ematix_flow as _pkg
        from ematix_flow.warehouses import WarehouseRunResult

        fake_core = MagicMock()
        with patch.object(_pkg, "_core", fake_core, create=True):
            @warehouse_pipeline(
                source=_src(), target=_tgt(),
                schedule="0 * * * *",
                name="contract_pipeline",
            )
            def _fn():
                return None

        captured_adapter = (
            fake_core.register_python_callback.call_args.args[1]
        )

        # The adapter calls wrapped(), which executes run_warehouse_pipeline.
        # Patch the orchestrator so the test stays offline.
        with patch(
            "ematix_flow.warehouse_pipeline.run_warehouse_pipeline",
            return_value=WarehouseRunResult(
                rows_read=3, rows_written=3, duration_ms=10,
            ),
        ):
            resp_bytes = captured_adapter(b"{}")

        assert isinstance(resp_bytes, bytes)
        result = json.loads(resp_bytes.decode("utf-8"))
        assert result["status"] == "succeeded"
        assert result["pipeline"] == "contract_pipeline"
        assert result["rows_read"] == 3
