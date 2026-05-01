//! Phase Py.6: streaming iterator for `read_arrow_stream`.
//!
//! Wraps an [`ArrowBatchStream`] in a Python sync iterator so
//! callers can process batches one at a time without first
//! materializing the entire drain into a list. Works with
//! Python's `for batch in iter:` syntax — no asyncio required.
//!
//! ## Trade-off vs `read_arrow_stream`
//! `backend.read_arrow_stream(query)` materializes the whole
//! drain into a `list[pyarrow.RecordBatch]`. For small batches
//! that fits in memory comfortably; for large drains (megabytes
//! of records per call) the iterator avoids the materialization
//! cost.
//!
//! ## Termination
//! `__next__` raises `StopIteration` when the underlying stream
//! is exhausted (drain done, source idle, batch limits hit) and
//! `ValueError` on a stream-level error. Subsequent calls keep
//! raising `StopIteration` (the iterator is single-pass).
//!
//! ## Concurrency
//! The iterator holds the stream behind a `std::sync::Mutex`.
//! `__next__` briefly takes the stream out, awaits the next
//! batch (without holding the lock across the await), and puts
//! it back unless exhausted. Multiple Python threads calling
//! `__next__` concurrently serialize through the mutex. The GIL
//! is released during the tokio block_on.

use std::sync::{Arc, Mutex};

use arrow::pyarrow::ToPyArrow;
use arrow_array::RecordBatch;
use ematix_flow_core::backend::{ArrowBatchStream, BackendError};
use futures_util::TryStreamExt;
use pyo3::exceptions::{PyStopIteration, PyValueError};
use pyo3::prelude::*;

use crate::rt;

/// Python sync iterator over a Rust `ArrowBatchStream`.
///
/// Constructed via the `iter_arrow_stream(...)` method on the
/// streaming-backend pyclasses (KafkaBackend, RabbitMQBackend,
/// PubSubBackend, KinesisBackend).
#[pyclass(name = "ArrowBatchIter")]
pub struct PyArrowBatchIter {
    inner: Arc<Mutex<Option<ArrowBatchStream>>>,
}

impl PyArrowBatchIter {
    /// Wrap an `ArrowBatchStream` into a Python iterator. Used by
    /// each backend's `iter_arrow_stream` method.
    pub(crate) fn new(stream: ArrowBatchStream) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(stream))),
        }
    }
}

#[pymethods]
impl PyArrowBatchIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let stream_arc = self.inner.clone();
        let next: Result<Option<RecordBatch>, BackendError> = py.detach(|| {
            rt().block_on(async move {
                // Take the stream out of the mutex briefly so we
                // don't hold the lock across the await. Single-
                // threaded by Python convention; the mutex is
                // belt-and-braces against concurrent __next__
                // calls from multiple Python threads.
                let mut stream = match stream_arc.lock().unwrap().take() {
                    Some(s) => s,
                    None => return Ok(None),
                };
                let item = stream.try_next().await;
                // Put the stream back unless exhausted or errored.
                match &item {
                    Ok(None) | Err(_) => {
                        // Drop the stream — exhausted or broken.
                    }
                    Ok(Some(_)) => {
                        *stream_arc.lock().unwrap() = Some(stream);
                    }
                }
                item
            })
        });
        match next {
            Ok(Some(batch)) => batch.to_pyarrow(py),
            Ok(None) => Err(PyStopIteration::new_err(())),
            Err(e) => Err(PyValueError::new_err(e.to_string())),
        }
    }
}
