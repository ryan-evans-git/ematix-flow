"""Smoke + round-trip coverage for the Python @ematix.udf scalar UDF.

The UDF foundation: a Python callable wraps as a DataFusion ScalarUDF
that user-supplied SQL inside ``transform_sql`` can invoke. These
tests exercise the full GIL → PyArrow → callable → PyArrow → result
round-trip via the test-only ``apply_udf_to_batch`` helper. The
``run_streaming_pipeline(udfs=...)`` integration is the next iteration;
the same handle slots in there once that wiring lands.
"""

from __future__ import annotations

import math

import numpy as np
import pyarrow as pa

from ematix_flow import apply_udf_to_batch, udf


def test_simple_int_udf_round_trips_through_datafusion_sql():
    """A trivial integer UDF is enough to prove the registration +
    invocation path works end-to-end. Each Python invocation receives
    a PyArrow Int64 Array and returns one."""

    @udf(args=("Int64",), returns="Int64")
    def double_it(x):
        # Per-batch: vectorise via pyarrow.compute (or numpy). Returning
        # a Python list / scalar would fail; the wrapper expects a
        # pyarrow Array.
        return pa.array([v * 2 for v in x.to_pylist()], type=pa.int64())

    batch = pa.record_batch(
        [pa.array([1, 2, 3, 4], type=pa.int64())],
        names=["n"],
    )
    out = apply_udf_to_batch(double_it, batch, ["n"])
    assert out.to_pylist() == [2, 4, 6, 8]
    assert out.type == pa.int64()


def test_multi_arg_udf_passes_columns_positionally():
    """Multiple input columns become positional arguments to the
    Python callable. Order matches the ``arg_columns`` list, not the
    original RecordBatch column order."""

    @udf(args=("Float64", "Float64"), returns="Float64")
    def hypot(a, b):
        a_np = a.to_numpy(zero_copy_only=False)
        b_np = b.to_numpy(zero_copy_only=False)
        return pa.array(np.sqrt(a_np * a_np + b_np * b_np), type=pa.float64())

    batch = pa.record_batch(
        [
            pa.array([3.0, 5.0, 8.0], type=pa.float64()),
            pa.array([4.0, 12.0, 15.0], type=pa.float64()),
        ],
        names=["dx", "dy"],
    )
    out = apply_udf_to_batch(hypot, batch, ["dx", "dy"])
    assert out.to_pylist() == [5.0, 13.0, 17.0]


def test_realistic_black_scholes_call_delta():
    """The user-feedback shape: a Black-Scholes call delta UDF over
    realistic options inputs. Vectorised numpy inside the callable;
    one PyO3 call per batch — the GIL acquisition + PyArrow round-trip
    amortises across all rows."""

    @udf(
        args=("Float64", "Float64", "Float64", "Float64", "Float64"),
        returns="Float64",
    )
    def bs_call_delta(strike, spot, vol, rate, expiry):
        k = strike.to_numpy(zero_copy_only=False)
        s = spot.to_numpy(zero_copy_only=False)
        v = vol.to_numpy(zero_copy_only=False)
        r = rate.to_numpy(zero_copy_only=False)
        t = expiry.to_numpy(zero_copy_only=False)
        d1 = (np.log(s / k) + (r + 0.5 * v * v) * t) / (v * np.sqrt(t))
        cdf = 0.5 * (1.0 + np.vectorize(math.erf)(d1 / np.sqrt(2)))
        return pa.array(cdf, type=pa.float64())

    # ATM call, 30 days to expiry, 20% vol, 5% rate. Delta ≈ 0.55.
    batch = pa.record_batch(
        [
            pa.array([4500.0, 4500.0, 4500.0], type=pa.float64()),  # strike
            pa.array([4500.0, 4520.0, 4480.0], type=pa.float64()),  # spot
            pa.array([0.20, 0.20, 0.20], type=pa.float64()),  # vol
            pa.array([0.05, 0.05, 0.05], type=pa.float64()),  # rate
            pa.array([30 / 365] * 3, type=pa.float64()),  # expiry (years)
        ],
        names=["strike", "spot", "vol", "rate", "expiry"],
    )
    out = apply_udf_to_batch(
        bs_call_delta, batch, ["strike", "spot", "vol", "rate", "expiry"]
    )
    deltas = out.to_pylist()
    # ATM ≈ 0.55, ITM > ATM, OTM < ATM. The exact values depend on
    # the cumulative-normal approximation — assert the qualitative
    # shape rather than pin a specific number.
    assert 0.5 < deltas[0] < 0.65, f"ATM delta should be ~0.55, got {deltas[0]}"
    assert deltas[1] > deltas[0], "ITM call delta exceeds ATM"
    assert deltas[2] < deltas[0], "OTM call delta is below ATM"


def test_udf_handle_carries_user_chosen_name():
    @udf(args=("Int64",), returns="Int64", name="explicitly_named")
    def fn(x):
        return x

    assert fn.name == "explicitly_named"


def test_udf_handle_defaults_to_function_name():
    @udf(args=("Int64",), returns="Int64")
    def my_function(x):
        return x

    assert my_function.name == "my_function"


def test_unsupported_datatype_raises_at_decoration():
    import pytest

    with pytest.raises(ValueError, match="unsupported UDF DataType"):

        @udf(args=("Decimal128",), returns="Float64")  # noqa: F811
        def bad(x):
            return x
