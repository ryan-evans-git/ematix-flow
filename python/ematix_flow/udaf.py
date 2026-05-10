"""Aggregate UDFs callable from ``transform_sql``.

Symmetric with :mod:`ematix_flow.udf` but the unit of dispatch is
an *Accumulator class* — DataFusion instantiates one per group and
threads `update_batch` / `merge_batch` / `evaluate` / `state` calls
through Python via PyO3.

Per-batch zero-copy through PyArrow on the inputs; per-group
length-1 PyArrow Arrays on the outputs (the contract that lets
Rust round-trip them back to ``ScalarValue`` without per-type
glue code).

Vectorise inside the methods — the GIL acquisition + PyArrow
round-trip amortises across the batch but the Python-level method
runs once per call. Reach for ``numpy`` / ``pyarrow.compute`` to
avoid per-row Python loops.

Example — volume-weighted average price (the canonical aggregate
that DataFusion's stdlib doesn't ship)::

    from ematix_flow import udaf, run_streaming_pipeline
    import pyarrow as pa
    import pyarrow.compute as pc


    @udaf(
        args=("Float64", "Float64"),
        state=("Float64", "Float64"),  # running num + den
        returns="Float64",
        name="vwap",
    )
    class Vwap:
        def __init__(self):
            self.num = 0.0
            self.den = 0.0

        def update_batch(self, prices, qtys):
            # PyArrow Float64 arrays of the batch's rows
            self.num += pc.sum(pc.multiply(prices, qtys)).as_py() or 0.0
            self.den += pc.sum(qtys).as_py() or 0.0

        def merge_batch(self, num_states, den_states):
            # K partial-state arrays, one per side of the merge
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


    run_streaming_pipeline(
        ...,
        transform_sql=(
            "SELECT minute, vwap(price, qty) AS vwap "
            "FROM source GROUP BY minute"
        ),
        aggregate_udfs=[Vwap],
    )
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from ematix_flow import _core

__all__ = ["PythonAggregateUdfHandle", "apply_udaf_to_batch", "udaf"]


# Re-export the PyO3 class so users can type-annotate against it.
PythonAggregateUdfHandle = _core.PythonAggregateUdfHandle


def udaf(
    *,
    args: Sequence[str],
    state: Sequence[str],
    returns: str,
    name: str | None = None,
) -> Callable[[type], PythonAggregateUdfHandle]:
    """Decorator that wraps a Python *class* as an aggregate UDF
    handle.

    ``args`` is the positional list of input ``DataType`` names —
    e.g. ``("Float64", "Float64")``. ``state`` is the list of
    intermediate-state field types DataFusion shuffles between
    parallel accumulators (most aggregates carry one or two state
    scalars; e.g. VWAP carries ``(num, den)``). ``returns`` is the
    output ``DataType``.

    The decorated class must implement four methods:

    - ``__init__(self)`` — initialise state to its identity. Called
      once per group.
    - ``update_batch(self, *pa_arrays)`` — fold an N-row batch into
      state. Receives one PyArrow Array per ``args`` entry.
    - ``merge_batch(self, *pa_state_arrays)`` — merge K partial
      states into this one. Receives one PyArrow Array per
      ``state`` field, each of length K.
    - ``evaluate(self) -> pa.Array`` — produce the final result as
      a length-1 PyArrow Array of the ``returns`` type.
    - ``state(self) -> tuple[pa.Array, ...]`` — emit intermediate
      state for shuffle. One length-1 PyArrow Array per ``state``
      field, in declaration order.

    The decorator returns a :class:`PythonAggregateUdfHandle` —
    pass it (or a list of them) to
    ``run_streaming_pipeline(aggregate_udfs=[...])``.
    """
    arg_types = list(args)
    state_types = list(state)
    return_type = returns

    def decorator(cls: type) -> PythonAggregateUdfHandle:
        udaf_name = name or cls.__name__
        return _core.make_python_udaf(udaf_name, cls, arg_types, state_types, return_type)

    return decorator


def apply_udaf_to_batch(
    handle: PythonAggregateUdfHandle,
    batch: Any,
    arg_columns: Sequence[str],
    *,
    output_column: str = "result",
) -> Any:
    """Apply a UDAF to a single PyArrow ``RecordBatch`` directly.

    Builds a one-shot DataFusion transform with the UDAF registered,
    runs ``SELECT <udaf>(<arg_cols>) AS result FROM source`` against
    the batch, and returns the result column as a PyArrow Array
    (one row per group; with no ``GROUP BY``, that's a single row
    representing the global aggregate).

    Test + exploration helper — production users register UDAFs on
    a streaming pipeline via
    ``run_streaming_pipeline(aggregate_udfs=...)``.
    """
    return _core._apply_python_udaf_to_batch(
        handle, batch, list(arg_columns), output_column
    )
