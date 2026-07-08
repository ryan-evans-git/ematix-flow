//! Convert Arrow `RecordBatch`es to native Python objects for the web
//! SQL editor — deliberately WITHOUT pyarrow.
//!
//! `_core` installs mimalloc as Rust's `#[global_allocator]`, and
//! pyarrow bundles *its own* mimalloc. Two statically-linked mimalloc
//! runtimes in one process corrupt each other's arenas — segfaults both
//! mid-run (inside `pa.record_batch`) and at interpreter shutdown
//! (`mi_process_done` -> `mi_arena_try_purge_range`). Converting result
//! sets to plain Python lists here keeps pyarrow (and its transitive
//! pandas/numpy weight) out of the web-server process entirely, so the
//! whole crash class disappears and the API path stays lean.
//!
//! Common column types get a typed fast path (ints -> `int`, floats ->
//! `float`, utf8 -> `str`, bool -> `bool`); everything else (temporal,
//! decimal, nested, …) is rendered to a string via Arrow's
//! `ArrayFormatter`. Nulls become `None`.

use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use arrow_array::RecordBatch;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

/// Short, UI-friendly label for a column's Arrow type.
fn type_label(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "bool".into(),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => "int".into(),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => "int".into(),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => "double".into(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".into(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "binary".into(),
        DataType::Date32 | DataType::Date64 => "date".into(),
        DataType::Timestamp(_, _) => "timestamp".into(),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "decimal".into(),
        other => format!("{other}").to_lowercase(),
    }
}

fn err(msg: impl std::fmt::Display) -> PyErr {
    PyValueError::new_err(format!("arrow->py: {msg}"))
}

/// Render every value of `arr` to a string via Arrow's formatter,
/// null-aware. Used for types without a typed fast path.
fn cells_via_formatter<'py>(py: Python<'py>, arr: &ArrayRef) -> PyResult<Vec<Bound<'py, PyAny>>> {
    let opts = FormatOptions::default();
    let fmt = ArrayFormatter::try_new(arr.as_ref(), &opts).map_err(err)?;
    let mut out = Vec::with_capacity(arr.len());
    for i in 0..arr.len() {
        if arr.is_null(i) {
            out.push(py.None().into_bound(py));
        } else {
            out.push(fmt.value(i).to_string().into_bound_py_any(py)?);
        }
    }
    Ok(out)
}

/// Convert one column (whole array) into per-row Python values.
fn column_cells<'py>(py: Python<'py>, arr: &ArrayRef) -> PyResult<Vec<Bound<'py, PyAny>>> {
    let n = arr.len();
    match arr.data_type() {
        DataType::Boolean => {
            let a = arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| err("bool"))?;
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                out.push(if a.is_null(i) {
                    py.None().into_bound(py)
                } else {
                    a.value(i).into_bound_py_any(py)?
                });
            }
            Ok(out)
        }
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => {
            let casted = cast(arr.as_ref(), &DataType::Int64).map_err(err)?;
            let a = casted
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| err("int64"))?;
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                out.push(if a.is_null(i) {
                    py.None().into_bound(py)
                } else {
                    a.value(i).into_bound_py_any(py)?
                });
            }
            Ok(out)
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            let casted = cast(arr.as_ref(), &DataType::Float64).map_err(err)?;
            let a = casted
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| err("f64"))?;
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                out.push(if a.is_null(i) {
                    py.None().into_bound(py)
                } else {
                    a.value(i).into_bound_py_any(py)?
                });
            }
            Ok(out)
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let casted = cast(arr.as_ref(), &DataType::Utf8).map_err(err)?;
            let a = casted
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| err("utf8"))?;
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                out.push(if a.is_null(i) {
                    py.None().into_bound(py)
                } else {
                    a.value(i).into_bound_py_any(py)?
                });
            }
            Ok(out)
        }
        // Temporal / decimal / binary / nested: stringify losslessly.
        _ => cells_via_formatter(py, arr),
    }
}

/// Build `{columns: [{name, type}], rows: [[...]], truncated: bool}`
/// from `batches`, emitting at most `max_rows` rows.
pub fn batches_to_py_dict<'py>(
    py: Python<'py>,
    batches: &[RecordBatch],
    max_rows: usize,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);

    let columns = PyList::empty(py);
    if let Some(first) = batches.first() {
        for field in first.schema().fields() {
            let col = PyDict::new(py);
            col.set_item("name", field.name())?;
            col.set_item("type", type_label(field.data_type()))?;
            columns.append(col)?;
        }
    }
    dict.set_item("columns", &columns)?;

    let rows = PyList::empty(py);
    let mut emitted = 0usize;
    let mut truncated = false;
    'outer: for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        // Pre-convert this batch's columns once (column-major), then
        // transpose into rows.
        let cols: Vec<Vec<Bound<'py, PyAny>>> = batch
            .columns()
            .iter()
            .map(|c| column_cells(py, c))
            .collect::<PyResult<_>>()?;
        for i in 0..batch.num_rows() {
            if emitted >= max_rows {
                truncated = true;
                break 'outer;
            }
            let row = PyList::empty(py);
            for col in &cols {
                row.append(&col[i])?;
            }
            rows.append(row)?;
            emitted += 1;
        }
    }
    dict.set_item("rows", &rows)?;
    dict.set_item("truncated", truncated)?;
    Ok(dict)
}
