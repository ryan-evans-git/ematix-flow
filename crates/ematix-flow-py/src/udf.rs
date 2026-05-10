//! Python scalar UDFs — wraps a Python callable as a DataFusion
//! `ScalarUDF` so user-supplied SQL inside `transform_sql` can call
//! into Python for math + domain functions DataFusion's stdlib
//! doesn't cover (cumulative-normal CDF for Black-Scholes, custom
//! hashing, financial day-count conventions, …).
//!
//! ## Architecture
//!
//! Per-batch dispatch through PyArrow zero-copy. On each invocation:
//!
//! 1. Acquire the GIL via `Python::with_gil`.
//! 2. Materialise each `ColumnarValue` arg to an `ArrayRef` and
//!    convert to a PyArrow `Array` via the C Data Interface
//!    (`ArrayData::to_pyarrow`).
//! 3. Call the Python callable with the PyArrow Arrays as positional
//!    args.
//! 4. Convert the returned PyArrow `Array` back to an `ArrayRef`
//!    (`ArrayData::from_pyarrow_bound`).
//! 5. Wrap as `ColumnarValue::Array` and return.
//!
//! Per-batch (not per-row) means N PyO3 calls per *batch*, not per
//! row — for a 10k-row batch, the GIL acquisition + PyArrow
//! conversion overhead is amortised across all rows. Users write
//! vectorised numpy / pandas inside the callable.
//!
//! ## Limitations (foundation cut)
//!
//! - The handle is constructable + invocable but **not yet wired
//!   through `run_streaming_pipeline`**. The follow-up PR threads
//!   the UDF dict from Python through `run_pipeline_from_toml_str`
//!   into `LazySqlTransform::new_with_lookups_and_udfs`.
//! - Argument and return types are declared via `DataType` strings
//!   matching DataFusion's parser (e.g. `"Float64"`, `"Utf8"`).
//!   Mismatches surface at plan-compile time as DataFusion type
//!   errors.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayData, make_array};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow_array::ArrayRef;
use arrow_schema::DataType;
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use pyo3::prelude::*;
use pyo3::types::PyTuple;

/// `ScalarUDFImpl` wrapper around a Python callable. Each instance
/// is uniquely identified by `name`; DataFusion will reject
/// duplicate registrations under the same name (the
/// `transform.rs::DataFusionTransform::new_with_lookups_and_udfs`
/// constructor surfaces this as a clean config-load error).
#[derive(Debug)]
pub(crate) struct PythonScalarUdf {
    name: String,
    signature: Signature,
    return_type: DataType,
    callable: Py<PyAny>,
}

impl PythonScalarUdf {
    /// Build from a Python callable + declared input + return types.
    /// `arg_types` is matched positionally against the SQL call site
    /// at plan time; `return_type` is what DataFusion's planner
    /// believes the function emits.
    pub(crate) fn new(
        name: String,
        callable: Py<PyAny>,
        arg_types: Vec<DataType>,
        return_type: DataType,
    ) -> Self {
        Self {
            name,
            signature: Signature::exact(arg_types, Volatility::Volatile),
            return_type,
            callable,
        }
    }
}

impl PartialEq for PythonScalarUdf {
    fn eq(&self, other: &Self) -> bool {
        // Identity by name — same name implies same registration.
        // Two UDFs with the same name but different callables would
        // have already been rejected at config-load time by the
        // duplicate-name check in `DataFusionTransform`.
        self.name == other.name
    }
}

impl Eq for PythonScalarUdf {}

impl std::hash::Hash for PythonScalarUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for PythonScalarUdf {
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arrays: Vec<ArrayRef> = args
            .args
            .iter()
            .map(|v| v.clone().into_array(args.number_rows))
            .collect::<DfResult<_>>()?;

        let result_array: ArrayRef = Python::attach(|py| -> DfResult<ArrayRef> {
            let py_args: Vec<Bound<'_, PyAny>> = arrays
                .iter()
                .map(|arr| {
                    arr.to_data().to_pyarrow(py).map_err(|e| {
                        DataFusionError::External(
                            format!("python udf {}: arg → pyarrow: {e}", self.name).into(),
                        )
                    })
                })
                .collect::<DfResult<_>>()?;
            let tuple = PyTuple::new(py, py_args.iter()).map_err(|e| {
                DataFusionError::External(
                    format!("python udf {}: build args tuple: {e}", self.name).into(),
                )
            })?;
            let result = self.callable.bind(py).call1(tuple).map_err(|e| {
                DataFusionError::External(
                    format!("python udf {}: callable raised: {e}", self.name).into(),
                )
            })?;
            let array_data = ArrayData::from_pyarrow_bound(&result).map_err(|e| {
                DataFusionError::External(
                    format!("python udf {}: result → pyarrow array: {e}", self.name).into(),
                )
            })?;
            Ok(make_array(array_data))
        })?;

        Ok(ColumnarValue::Array(result_array))
    }
}

/// Python-facing handle around an `Arc<ScalarUDF>` so callers can
/// pass the UDF back into Rust at pipeline-construction time without
/// re-marshalling the underlying callable.
#[pyclass(name = "PythonScalarUdfHandle", module = "ematix_flow._core")]
pub(crate) struct PyUdfHandle {
    pub(crate) udf: Arc<ScalarUDF>,
}

#[pymethods]
impl PyUdfHandle {
    /// Read access for diagnostics + tests.
    #[getter]
    fn name(&self) -> &str {
        self.udf.name()
    }
}

/// Build a [`PyUdfHandle`] wrapping a Python callable. Exported as
/// `ematix_flow._core.make_python_udf`.
///
/// `arg_types` and `return_type` are SQL-style type names parsed by
/// DataFusion's `arrow_schema::DataType::from_str`-equivalent parser
/// — `"Int64"`, `"Float64"`, `"Utf8"`, `"Boolean"`, etc. Unknown
/// types raise `ValueError`.
#[pyfunction]
#[pyo3(signature = (name, callable, arg_types, return_type))]
pub(crate) fn make_python_udf(
    name: &str,
    callable: Py<PyAny>,
    arg_types: Vec<String>,
    return_type: String,
) -> PyResult<PyUdfHandle> {
    let arg_dtypes: Vec<DataType> = arg_types
        .iter()
        .map(|s| parse_datatype(s))
        .collect::<PyResult<_>>()?;
    let ret_dtype = parse_datatype(&return_type)?;
    let inner = PythonScalarUdf::new(name.to_string(), callable, arg_dtypes, ret_dtype);
    Ok(PyUdfHandle {
        udf: Arc::new(ScalarUDF::from(inner)),
    })
}

/// Small, opinionated DataType-string parser — the surface a Python
/// `@ematix.udf` decorator needs without dragging in DataFusion's
/// full SQL parser. Add cases as concrete UDFs surface them.
pub(crate) fn parse_datatype(s: &str) -> PyResult<DataType> {
    use pyo3::exceptions::PyValueError;
    Ok(match s {
        "Int8" | "int8" => DataType::Int8,
        "Int16" | "int16" => DataType::Int16,
        "Int32" | "int32" => DataType::Int32,
        "Int64" | "int64" => DataType::Int64,
        "UInt8" | "uint8" => DataType::UInt8,
        "UInt16" | "uint16" => DataType::UInt16,
        "UInt32" | "uint32" => DataType::UInt32,
        "UInt64" | "uint64" => DataType::UInt64,
        "Float32" | "float32" => DataType::Float32,
        "Float64" | "float64" => DataType::Float64,
        "Boolean" | "boolean" | "bool" => DataType::Boolean,
        "Utf8" | "utf8" | "string" => DataType::Utf8,
        "Binary" | "binary" => DataType::Binary,
        other => {
            return Err(PyValueError::new_err(format!(
                "unsupported UDF DataType: {other:?} \
                 (supported: Int8/16/32/64, UInt8/16/32/64, Float32/64, \
                 Boolean, Utf8, Binary)"
            )));
        }
    })
}

/// Apply a Python UDF to a single PyArrow `RecordBatch` for testing.
/// Builds a one-shot `DataFusionTransform` with the UDF registered,
/// invokes `SELECT <udf>(<arg_cols>) AS result FROM source`, returns
/// the `result` column as a PyArrow Array.
///
/// **Foundation-cut helper.** Production users will register the
/// UDF on a streaming pipeline via `run_streaming_pipeline(udfs=...)`
/// once that wiring lands; this helper exists so the test suite can
/// exercise the wrapper end-to-end without standing up a full
/// pipeline.
#[pyfunction]
#[pyo3(signature = (handle, batch, arg_columns, output_column="result"))]
pub(crate) fn _apply_python_udf_to_batch<'py>(
    py: Python<'py>,
    handle: &PyUdfHandle,
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
    let sql = format!(
        "SELECT {udf}({arg_list}) AS {out} FROM source",
        udf = handle.udf.name(),
        out = output_column,
    );

    let udfs = vec![Arc::clone(&handle.udf)];
    let schema = rb.schema();
    let result = py
        .detach(|| {
            crate::rt().block_on(async move {
                let t =
                    DataFusionTransform::new_with_lookups_and_udfs(&sql, schema, Vec::new(), udfs)
                        .await?;
                let out = t.transform(rb, &BatchContext::default()).await?;
                Ok::<_, ematix_flow_core::backend::BackendError>(out)
            })
        })
        .map_err(|e| PyValueError::new_err(format!("apply udf: {e}")))?;

    if result.is_empty() {
        return Err(PyValueError::new_err(
            "udf returned no batches (transform path emitted zero rows)",
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
