"""Scalar UDFs callable from ``transform_sql``.

Wraps a Python callable as a DataFusion ``ScalarUDF`` that user-supplied
SQL inside ``transform_sql`` can invoke. Per-batch dispatch through
PyArrow zero-copy: each invocation receives PyArrow ``Array`` objects
positionally and returns a single PyArrow ``Array``.

Vectorise inside the callable — the GIL acquisition + PyArrow round-trip
amortise across the batch (typically thousands of rows), but the
callable itself runs once per batch, so per-row Python loops will be
slow. Reach for ``numpy`` / ``pyarrow.compute`` instead.

Foundation cut: a UDF handle is constructable today via the decorator
below and invocable directly via :func:`apply_udf_to_batch` for tests
and exploration. Full ``run_streaming_pipeline(udfs=...)`` wiring is the
next iteration — same handle slots in once that lands.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from ematix_flow import _core


__all__ = ["udf", "PythonScalarUdfHandle", "apply_udf_to_batch"]


# Re-export the PyO3 class so users can type-annotate against it.
PythonScalarUdfHandle = _core.PythonScalarUdfHandle


def udf(
    *,
    args: Sequence[str],
    returns: str,
    name: str | None = None,
) -> Callable[[Callable[..., Any]], PythonScalarUdfHandle]:
    """Decorator that wraps a Python callable as a scalar UDF handle.

    ``args`` is the positional list of input ``DataType`` names — e.g.
    ``("Float64", "Float64", "Int32")``. ``returns`` is the output
    ``DataType``. Mismatched call sites raise at plan-compile time.

    The decorated function must accept and return PyArrow ``Array``
    objects. ``numpy``-vectorised math is the typical inside-the-function
    pattern (``arr.to_numpy()`` → numpy ops → ``pa.array(result)``).

    Example::

        from math import erf, sqrt
        import numpy as np
        import pyarrow as pa

        @udf(args=("Float64", "Float64", "Float64", "Float64", "Float64"),
             returns="Float64")
        def bs_delta(strike, spot, vol, rate, expiry):
            # Black-Scholes call delta. All five inputs are PyArrow
            # Float64 Arrays; convert once, do the math vectorised,
            # ship a PyArrow Array back.
            k = strike.to_numpy(zero_copy_only=False)
            s = spot.to_numpy(zero_copy_only=False)
            v = vol.to_numpy(zero_copy_only=False)
            r = rate.to_numpy(zero_copy_only=False)
            t = expiry.to_numpy(zero_copy_only=False)
            d1 = (np.log(s / k) + (r + 0.5 * v * v) * t) / (v * np.sqrt(t))
            cdf = 0.5 * (1.0 + np.vectorize(erf)(d1 / np.sqrt(2)))
            return pa.array(cdf, type=pa.float64())
    """
    arg_types = list(args)
    return_type = returns

    def decorator(fn: Callable[..., Any]) -> PythonScalarUdfHandle:
        udf_name = name or fn.__name__
        return _core.make_python_udf(udf_name, fn, arg_types, return_type)

    return decorator


def apply_udf_to_batch(
    handle: PythonScalarUdfHandle,
    batch: Any,
    arg_columns: Sequence[str],
    *,
    output_column: str = "result",
) -> Any:
    """Apply a UDF to a single PyArrow ``RecordBatch`` directly.

    Builds a one-shot DataFusion transform with the UDF registered,
    runs ``SELECT <udf>(<arg_cols>) AS result FROM source`` against
    the batch, and returns the result column as a PyArrow Array.

    **Foundation-cut helper** — production users will register a UDF
    on a streaming pipeline via ``run_streaming_pipeline(udfs=...)``
    once that wiring lands; this is the test harness for the round-trip
    until then.
    """
    return _core._apply_python_udf_to_batch(
        handle, batch, list(arg_columns), output_column
    )
