"""Smoke + round-trip coverage for the Python @ematix.udaf
aggregate UDF.

The UDAF foundation: a Python *class* wraps as a DataFusion
ScalarUDF — sorry, ``AggregateUDF`` — that user-supplied SQL
inside ``transform_sql`` can invoke. These tests exercise the
full GIL → PyArrow → class → PyArrow → result round-trip via the
test-only ``apply_udaf_to_batch`` helper. The
``run_streaming_pipeline(aggregate_udfs=...)`` integration is
covered by the surface-only signature check at the bottom; full
end-to-end run-from-pytest is awkward (the runner blocks until
SIGTERM) so the Rust side carries the streaming wiring test
(``streaming_config_threads_aggregate_udfs_into_lazy_sql_transform``).
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from ematix_flow import apply_udaf_to_batch, udaf


def test_simple_sum_of_squares_aggregates_globally():
    """A trivial Int64 → Int64 aggregator. No GROUP BY → one row
    out, value = Σ x²."""

    @udaf(args=("Int64",), state=("Int64",), returns="Int64")
    class SumOfSquares:
        def __init__(self):
            self.s = 0

        def update_batch(self, xs):
            # Vectorise via pyarrow.compute. self.s += Σ xs².
            self.s += pc.sum(pc.multiply(xs, xs)).as_py()

        def merge_batch(self, partials):
            self.s += pc.sum(partials).as_py() or 0

        def evaluate(self):
            return pa.array([self.s], type=pa.int64())

        def state(self):
            return (pa.array([self.s], type=pa.int64()),)

    batch = pa.record_batch(
        [pa.array([1, 2, 99], type=pa.int64())],
        names=["x"],
    )
    out = apply_udaf_to_batch(SumOfSquares, batch, ["x"])
    # 1 + 4 + 9801 = 9806
    assert out.to_pylist() == [9806]
    assert out.type == pa.int64()


def test_realistic_volume_weighted_average_price():
    """The user-feedback shape: VWAP over options ticks. Two-arg
    Float64 → Float64 with two state fields (running num + den)."""

    @udaf(
        args=("Float64", "Float64"),
        state=("Float64", "Float64"),
        returns="Float64",
        name="vwap",
    )
    class Vwap:
        def __init__(self):
            self.num = 0.0
            self.den = 0.0

        def update_batch(self, prices, qtys):
            self.num += pc.sum(pc.multiply(prices, qtys)).as_py() or 0.0
            self.den += pc.sum(qtys).as_py() or 0.0

        def merge_batch(self, num_states, den_states):
            self.num += pc.sum(num_states).as_py() or 0.0
            self.den += pc.sum(den_states).as_py() or 0.0

        def evaluate(self):
            if self.den == 0:
                return pa.array([None], type=pa.float64())
            return pa.array([self.num / self.den], type=pa.float64())

        def state(self):
            return (
                pa.array([self.num], type=pa.float64()),
                pa.array([self.den], type=pa.float64()),
            )

    # Three trades: (price=100, qty=10), (price=110, qty=5), (price=105, qty=15).
    # VWAP = (100*10 + 110*5 + 105*15) / (10+5+15) = (1000 + 550 + 1575) / 30
    #      = 3125 / 30 = 104.1666...
    batch = pa.record_batch(
        [
            pa.array([100.0, 110.0, 105.0], type=pa.float64()),
            pa.array([10.0, 5.0, 15.0], type=pa.float64()),
        ],
        names=["price", "qty"],
    )
    out = apply_udaf_to_batch(Vwap, batch, ["price", "qty"])
    deltas = out.to_pylist()
    assert len(deltas) == 1
    assert abs(deltas[0] - 104.16666666666667) < 1e-9


def test_udaf_handle_carries_user_chosen_name():
    @udaf(
        args=("Int64",),
        state=("Int64",),
        returns="Int64",
        name="explicitly_named",
    )
    class Counter:
        def __init__(self):
            self.n = 0

        def update_batch(self, xs):
            self.n += len(xs)

        def merge_batch(self, ns):
            self.n += pc.sum(ns).as_py() or 0

        def evaluate(self):
            return pa.array([self.n], type=pa.int64())

        def state(self):
            return (pa.array([self.n], type=pa.int64()),)

    assert Counter.name == "explicitly_named"


def test_udaf_handle_defaults_to_class_name():
    @udaf(args=("Int64",), state=("Int64",), returns="Int64")
    class MyCounter:
        def __init__(self):
            self.n = 0

        def update_batch(self, xs):
            self.n += len(xs)

        def merge_batch(self, ns):
            self.n += pc.sum(ns).as_py() or 0

        def evaluate(self):
            return pa.array([self.n], type=pa.int64())

        def state(self):
            return (pa.array([self.n], type=pa.int64()),)

    assert MyCounter.name == "MyCounter"


def test_unsupported_state_datatype_raises_at_decoration():
    with pytest.raises(ValueError, match="unsupported UDF DataType"):

        @udaf(args=("Int64",), state=("Decimal128",), returns="Int64")
        class Bad:
            pass


def test_evaluate_must_return_pyarrow_array_of_declared_type():
    """If the user returns a length-1 array of the wrong dtype the
    Rust side surfaces a clear error pointing at the mismatch."""

    @udaf(args=("Int64",), state=("Int64",), returns="Int64")
    class WrongType:
        def __init__(self):
            self.n = 0

        def update_batch(self, xs):
            self.n += pc.sum(xs).as_py() or 0

        def merge_batch(self, ns):
            self.n += pc.sum(ns).as_py() or 0

        def evaluate(self):
            # Declared returns="Int64" but emitting Float64 — should
            # raise from the Rust wrapper at evaluate time.
            return pa.array([float(self.n)], type=pa.float64())

        def state(self):
            return (pa.array([self.n], type=pa.int64()),)

    batch = pa.record_batch([pa.array([1, 2, 3], type=pa.int64())], names=["x"])
    with pytest.raises(ValueError, match="evaluate.*dtype"):
        apply_udaf_to_batch(WrongType, batch, ["x"])


def test_run_streaming_pipeline_accepts_aggregate_udfs_kwarg():
    """Π.5 wiring: run_streaming_pipeline must accept
    ``aggregate_udfs=[...]``.

    Surface-only signature check; the Rust-side
    ``streaming_config_threads_aggregate_udfs_into_lazy_sql_transform``
    test in ``crates/ematix-flow-cli`` covers the actual
    threading end-to-end.
    """
    import inspect

    from ematix_flow import run_streaming_pipeline

    sig = inspect.signature(run_streaming_pipeline)
    assert "aggregate_udfs" in sig.parameters, (
        "run_streaming_pipeline must expose an `aggregate_udfs=` kwarg "
        "so users can register @ematix.udaf-decorated classes on the SQL pre-stage"
    )
    assert sig.parameters["aggregate_udfs"].default is None
