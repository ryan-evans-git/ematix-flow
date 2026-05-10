//! Python aggregate UDFs — wraps a Python class as a DataFusion
//! `AggregateUDF` so user-supplied SQL inside `transform_sql` can
//! call into Python for per-group reductions DataFusion's stdlib
//! doesn't ship (volume-weighted average price, custom percentiles,
//! distinct-by-cardinality with custom merge semantics, …).
//!
//! ## Architecture
//!
//! Symmetric with [`crate::udf`] but the unit of dispatch is an
//! `Accumulator`, not a single function call. On each `accumulator()`
//! invocation:
//!
//! 1. Acquire the GIL.
//! 2. Instantiate the Python class via `class.call0()` to get a
//!    fresh per-group accumulator instance.
//! 3. Wrap the instance in a [`PythonAccumulator`] that holds the
//!    `Py<PyAny>` reference and the declared state + return types.
//!
//! Per-batch the accumulator's `update_batch` / `merge_batch`
//! convert the input `&[ArrayRef]` into PyArrow Arrays (via the
//! C Data Interface, zero-copy) and dispatch into the Python
//! instance's matching method. `evaluate()` and `state()` ask the
//! instance to emit length-1 PyArrow Arrays which Rust converts
//! back to `ScalarValue`.
//!
//! ## Python contract
//!
//! The decorated class must implement four methods:
//!
//! - `update_batch(*pa_arrays)` — fold an N-row batch (N ≥ 1) into
//!   accumulator state. Receives one PyArrow Array per declared
//!   `args` entry, in declaration order.
//! - `merge_batch(*pa_state_arrays)` — merge K partial-state
//!   instances (from parallel-aggregate fan-in) into this one.
//!   Receives one PyArrow Array per declared `state` field, each
//!   of length K.
//! - `evaluate()` — produce the final result. Must return a
//!   length-1 PyArrow Array of the declared `returns` type.
//! - `state()` — emit intermediate state for shuffle / serde.
//!   Must return a tuple/list of length-1 PyArrow Arrays, one per
//!   declared `state` field.
//!
//! All four methods receive PyArrow Arrays (not numpy / pandas) —
//! convert via `arr.to_numpy(zero_copy_only=False)` inside the
//! method if numpy ergonomics are wanted.
//!
//! ## Limitations
//!
//! - Per-batch dispatch only — DataFusion's "groups accumulator"
//!   path (fast-path for low-cardinality grouping) isn't wired up;
//!   `groups_accumulator_supported` returns `false`. Users on
//!   high-cardinality data should profile vs. a pure-Rust
//!   `AggregateUDFImpl` if the GIL round-trip becomes a
//!   bottleneck.
//! - State fields are typed positionally via the decorator's
//!   `state=("Float64", ...)` argument. No support yet for
//!   `Struct<...>` state.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayData, make_array};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result as DfResult, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};
use pyo3::prelude::*;
use pyo3::types::PyTuple;

/// `AggregateUDFImpl` wrapper. Holds the Python *class* (not an
/// instance) so each call to [`AggregateUDFImpl::accumulator`] can
/// instantiate a fresh per-group accumulator. Identity is by
/// `name`; duplicate registrations are caught at the
/// `DataFusionTransform` construction site.
#[derive(Debug)]
pub(crate) struct PythonAggregateUdf {
    name: String,
    signature: Signature,
    return_type: DataType,
    state_types: Vec<DataType>,
    /// The Python class object — calling `class.call0()` produces
    /// a new per-group accumulator instance. Captured at
    /// decorator-time and held for the pipeline's lifetime.
    accumulator_class: Py<PyAny>,
}

impl PartialEq for PythonAggregateUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Eq for PythonAggregateUdf {}
impl std::hash::Hash for PythonAggregateUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl AggregateUDFImpl for PythonAggregateUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(self.return_type.clone())
    }
    fn state_fields(&self, args: StateFieldsArgs) -> DfResult<Vec<FieldRef>> {
        Ok(self
            .state_types
            .iter()
            .enumerate()
            .map(|(i, dt)| Arc::new(Field::new(format!("{}[s{i}]", args.name), dt.clone(), true)))
            .collect())
    }
    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DfResult<Box<dyn Accumulator>> {
        let instance = Python::attach(|py| -> PyResult<Py<PyAny>> {
            self.accumulator_class.bind(py).call0().map(|b| b.unbind())
        })
        .map_err(|e| {
            DataFusionError::External(
                format!(
                    "python udaf {}: instantiate accumulator class: {e}",
                    self.name
                )
                .into(),
            )
        })?;
        Ok(Box::new(PythonAccumulator {
            name: self.name.clone(),
            return_type: self.return_type.clone(),
            state_types: self.state_types.clone(),
            instance,
        }))
    }
}

/// Per-group `Accumulator`. Holds a Python instance + the declared
/// types so we know how to coerce `evaluate` / `state` results back
/// into Rust `ScalarValue`s.
#[derive(Debug)]
struct PythonAccumulator {
    name: String,
    return_type: DataType,
    state_types: Vec<DataType>,
    instance: Py<PyAny>,
}

impl Accumulator for PythonAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        Python::attach(|py| -> DfResult<()> {
            let py_args = arrays_to_pyarrow(py, values, &self.name, "update_batch")?;
            let tuple = PyTuple::new(py, py_args.iter()).map_err(|e| {
                DataFusionError::External(
                    format!(
                        "python udaf {}: build update_batch args tuple: {e}",
                        self.name
                    )
                    .into(),
                )
            })?;
            self.instance
                .bind(py)
                .call_method1("update_batch", tuple)
                .map_err(|e| {
                    DataFusionError::External(
                        format!("python udaf {}: update_batch raised: {e}", self.name).into(),
                    )
                })?;
            Ok(())
        })
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        Python::attach(|py| -> DfResult<()> {
            let py_args = arrays_to_pyarrow(py, states, &self.name, "merge_batch")?;
            let tuple = PyTuple::new(py, py_args.iter()).map_err(|e| {
                DataFusionError::External(
                    format!(
                        "python udaf {}: build merge_batch args tuple: {e}",
                        self.name
                    )
                    .into(),
                )
            })?;
            self.instance
                .bind(py)
                .call_method1("merge_batch", tuple)
                .map_err(|e| {
                    DataFusionError::External(
                        format!("python udaf {}: merge_batch raised: {e}", self.name).into(),
                    )
                })?;
            Ok(())
        })
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        Python::attach(|py| -> DfResult<ScalarValue> {
            let result = self
                .instance
                .bind(py)
                .call_method0("evaluate")
                .map_err(|e| {
                    DataFusionError::External(
                        format!("python udaf {}: evaluate raised: {e}", self.name).into(),
                    )
                })?;
            length_one_pyarray_to_scalar(&result, &self.return_type, &self.name, "evaluate")
        })
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Python::attach(|py| -> DfResult<Vec<ScalarValue>> {
            let result = self.instance.bind(py).call_method0("state").map_err(|e| {
                DataFusionError::External(
                    format!("python udaf {}: state raised: {e}", self.name).into(),
                )
            })?;
            // Expect a tuple/list of length-1 PyArrow Arrays, one
            // per declared state field. `extract::<Vec<_>>()`
            // works for both tuple and list — Python sequence
            // protocol handles both.
            let items: Vec<Bound<'_, PyAny>> = result.extract().map_err(|e| {
                DataFusionError::External(
                    format!(
                        "python udaf {}: state() must return a tuple/list of \
                         length-1 PyArrow Arrays — got: {e}",
                        self.name
                    )
                    .into(),
                )
            })?;
            if items.len() != self.state_types.len() {
                return Err(DataFusionError::External(
                    format!(
                        "python udaf {}: state() returned {} elements, declared {} state \
                         fields — fix the @udaf decorator's `state=(...)` or \
                         the class's state() method to match",
                        self.name,
                        items.len(),
                        self.state_types.len()
                    )
                    .into(),
                ));
            }
            items
                .iter()
                .zip(self.state_types.iter())
                .map(|(arr, dt)| length_one_pyarray_to_scalar(arr, dt, &self.name, "state"))
                .collect()
        })
    }

    fn size(&self) -> usize {
        // No reliable cross-language sizing for Python objects;
        // upstream uses this for a "should we spill?" heuristic.
        // Conservative fixed estimate keeps DataFusion happy
        // without false-positive spills on small accumulators.
        std::mem::size_of::<Self>() + 256
    }
}

/// Convert each `ArrayRef` to a PyArrow Array bound. Same path the
/// scalar UDF wrapper uses; lifted here as a helper so update_batch
/// and merge_batch share it.
fn arrays_to_pyarrow<'py>(
    py: Python<'py>,
    arrays: &[ArrayRef],
    name: &str,
    method: &str,
) -> DfResult<Vec<Bound<'py, PyAny>>> {
    arrays
        .iter()
        .map(|arr| {
            arr.to_data().to_pyarrow(py).map_err(|e| {
                DataFusionError::External(
                    format!("python udaf {name}: {method} arg → pyarrow: {e}").into(),
                )
            })
        })
        .collect()
}

/// Convert a length-1 PyArrow Array (the contract for `evaluate()`
/// and each element of `state()`) into a Rust `ScalarValue` matching
/// `expected_type`. Surfaces a clear error if the user returned a
/// non-Array, an Array of the wrong length, or a length-1 Array of
/// the wrong type.
fn length_one_pyarray_to_scalar(
    py_array: &Bound<'_, PyAny>,
    expected_type: &DataType,
    name: &str,
    method: &str,
) -> DfResult<ScalarValue> {
    let array_data = ArrayData::from_pyarrow_bound(py_array).map_err(|e| {
        DataFusionError::External(
            format!(
                "python udaf {name}: {method}() must return a PyArrow Array \
                 (got: {e}). Wrap your scalar in `pa.array([value], type=...)`."
            )
            .into(),
        )
    })?;
    let array = make_array(array_data);
    if array.len() != 1 {
        return Err(DataFusionError::External(
            format!(
                "python udaf {name}: {method}() returned a length-{} array; \
                 expected length-1 (one row per group result)",
                array.len()
            )
            .into(),
        ));
    }
    if array.data_type() != expected_type {
        return Err(DataFusionError::External(
            format!(
                "python udaf {name}: {method}() returned dtype {:?}, declared {:?} — \
                 fix the @udaf decorator or the array's `type=` argument",
                array.data_type(),
                expected_type
            )
            .into(),
        ));
    }
    ScalarValue::try_from_array(&array, 0).map_err(|e| {
        DataFusionError::External(
            format!("python udaf {name}: {method}() length-1 array → ScalarValue: {e}").into(),
        )
    })
}

/// Python-facing handle, mirrors [`crate::udf::PyUdfHandle`].
#[pyclass(name = "PythonAggregateUdfHandle", module = "ematix_flow._core")]
pub(crate) struct PyUdafHandle {
    pub(crate) udaf: Arc<AggregateUDF>,
}

#[pymethods]
impl PyUdafHandle {
    #[getter]
    fn name(&self) -> &str {
        self.udaf.name()
    }
}

/// Build a [`PyUdafHandle`] wrapping a Python class. Exported as
/// `ematix_flow._core.make_python_udaf`.
#[pyfunction]
#[pyo3(signature = (name, accumulator_class, arg_types, state_types, return_type))]
pub(crate) fn make_python_udaf(
    name: &str,
    accumulator_class: Py<PyAny>,
    arg_types: Vec<String>,
    state_types: Vec<String>,
    return_type: String,
) -> PyResult<PyUdafHandle> {
    let arg_dtypes: Vec<DataType> = arg_types
        .iter()
        .map(|s| crate::udf::parse_datatype(s))
        .collect::<PyResult<_>>()?;
    let state_dtypes: Vec<DataType> = state_types
        .iter()
        .map(|s| crate::udf::parse_datatype(s))
        .collect::<PyResult<_>>()?;
    let ret_dtype = crate::udf::parse_datatype(&return_type)?;
    let inner = PythonAggregateUdf {
        name: name.to_string(),
        signature: Signature::exact(arg_dtypes, Volatility::Volatile),
        return_type: ret_dtype,
        state_types: state_dtypes,
        accumulator_class,
    };
    Ok(PyUdafHandle {
        udaf: Arc::new(AggregateUDF::from(inner)),
    })
}

/// Apply a Python aggregate UDF to a single PyArrow `RecordBatch`
/// for testing — symmetric with `_apply_python_udf_to_batch`.
#[pyfunction]
#[pyo3(signature = (handle, batch, arg_columns, output_column="result"))]
pub(crate) fn _apply_python_udaf_to_batch<'py>(
    py: Python<'py>,
    handle: &PyUdafHandle,
    batch: &Bound<'_, PyAny>,
    arg_columns: Vec<String>,
    output_column: &str,
) -> PyResult<Bound<'py, PyAny>> {
    use arrow_array::RecordBatch;
    use ematix_flow_core::transform::{BatchContext, BatchTransform, DataFusionTransform};
    use pyo3::exceptions::PyValueError;

    let rb = RecordBatch::from_pyarrow_bound(batch)
        .map_err(|e| PyValueError::new_err(format!("input batch → arrow: {e}")))?;

    let arg_list = arg_columns
        .iter()
        .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");
    // DataFusion's SQL planner lowercases unquoted identifiers,
    // but the UDAF registry stores the literal registered name —
    // PascalCase class names (`SumOfSquares`) wouldn't resolve
    // against a bare `SumOfSquares(x)` call. Quote the function
    // name so the planner respects the case as-registered.
    let sql = format!(
        "SELECT \"{udaf}\"({arg_list}) AS {out} FROM source",
        udaf = handle.udaf.name().replace('"', "\"\""),
        out = output_column,
    );

    let udafs = vec![Arc::clone(&handle.udaf)];
    let schema = rb.schema();
    let result = py
        .detach(|| {
            crate::rt().block_on(async move {
                let t = DataFusionTransform::new_with_lookups_udfs_and_aggregate_udfs(
                    &sql,
                    schema,
                    Vec::new(),
                    Vec::new(),
                    udafs,
                )
                .await?;
                let out = t.transform(rb, &BatchContext::default()).await?;
                Ok::<_, ematix_flow_core::backend::BackendError>(out)
            })
        })
        .map_err(|e| PyValueError::new_err(format!("apply udaf: {e}")))?;

    if result.is_empty() {
        return Err(PyValueError::new_err(
            "udaf returned no batches (transform path emitted zero rows)",
        ));
    }
    let arr = result[0]
        .column_by_name(output_column)
        .ok_or_else(|| {
            PyValueError::new_err(format!(
                "output column {output_column:?} not present in transform result"
            ))
        })?
        .clone();
    arr.to_data()
        .to_pyarrow(py)
        .map_err(|e| PyValueError::new_err(format!("result → pyarrow: {e}")))
}
