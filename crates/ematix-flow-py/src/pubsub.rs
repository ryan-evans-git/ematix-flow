//! Phase Py.4: PubSubBackend pyclass + PyArrow integration.
//!
//! Mirrors the Phase Py.2 / Py.3 templates against the GCP
//! Pub/Sub Rust surface.

use std::sync::Arc;

use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow_array::RecordBatch;
use ematix_flow_core::PubSubBackend as RsPubSubBackend;
use ematix_flow_core::backend::{Backend, BackendError, TargetTable, WriteMode};
use ematix_flow_core::pubsub_backend::PubSubBatchConfig;
use futures_util::TryStreamExt;
use futures_util::stream;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::rt;

/// Python-facing GCP Pub/Sub backend handle.
#[pyclass(name = "PubSubBackend")]
pub struct PyPubSubBackend {
    inner: Arc<RsPubSubBackend>,
}

#[pymethods]
impl PyPubSubBackend {
    /// Construct a PubSubBackend.
    ///
    /// `project_id` is the only required argument. All other
    /// configuration is keyword-only.
    ///
    /// `endpoint` overrides the default
    /// `https://pubsub.googleapis.com`. Set to
    /// `http://localhost:8085` for the gcloud emulator.
    ///
    /// `anonymous_auth=True` opts into anonymous credentials
    /// (required for the emulator path; production uses ADC).
    #[staticmethod]
    #[pyo3(signature = (
        project_id,
        *,
        endpoint = None,
        anonymous_auth = false,
        batch_size = None,
        batch_bytes = None,
        idle_timeout_ms = None,
    ))]
    fn open(
        project_id: &str,
        endpoint: Option<&str>,
        anonymous_auth: bool,
        batch_size: Option<usize>,
        batch_bytes: Option<usize>,
        idle_timeout_ms: Option<u64>,
    ) -> PyResult<Self> {
        let mut backend =
            RsPubSubBackend::open(project_id).map_err(|e| PyValueError::new_err(e.to_string()))?;
        if let Some(ep) = endpoint {
            backend = backend.with_endpoint(ep);
        }
        if anonymous_auth {
            backend = backend.with_anonymous_auth();
        }
        if batch_size.is_some() || batch_bytes.is_some() || idle_timeout_ms.is_some() {
            let mut cfg = PubSubBatchConfig::default();
            if let Some(v) = batch_size {
                cfg.batch_size = v;
            }
            if let Some(v) = batch_bytes {
                cfg.batch_bytes = v;
            }
            if let Some(v) = idle_timeout_ms {
                cfg.idle_timeout_ms = v;
            }
            backend = backend.with_batch_config(cfg);
        }
        Ok(Self {
            inner: Arc::new(backend),
        })
    }

    /// Validate connectivity (TopicAdmin.list_topics).
    fn ping(&self, py: Python<'_>) -> PyResult<()> {
        let backend = self.inner.clone();
        py.detach(|| rt().block_on(async move { backend.ping().await }))
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Drain `query` (subscription name; bare or fully-qualified)
    /// into a list of PyArrow RecordBatches. Auto-ack is **off**;
    /// call :py:meth:`commit_offsets` after the downstream write
    /// lands for at-least-once semantics, or
    /// :py:meth:`nack_pending` to drop the deliveries.
    fn read_arrow_stream<'py>(&self, py: Python<'py>, query: &str) -> PyResult<Bound<'py, PyList>> {
        let backend = self.inner.clone();
        let query = query.to_string();
        let batches: Vec<RecordBatch> = py
            .detach(|| {
                rt().block_on(async move {
                    let stream = backend.read_arrow_stream(&query).await?;
                    let collected: Vec<RecordBatch> = stream.try_collect().await?;
                    Ok::<_, BackendError>(collected)
                })
            })
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let py_batches: Vec<Bound<'py, PyAny>> = batches
            .iter()
            .map(|b| b.to_pyarrow(py))
            .collect::<PyResult<Vec<_>>>()?;
        PyList::new(py, py_batches)
    }

    /// Produce each row of every PyArrow batch to `topic`. Bare
    /// topic names are auto-qualified with the backend's
    /// `project_id`.
    fn write_arrow_stream(
        &self,
        py: Python<'_>,
        topic: &str,
        batches: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<u64> {
        let mut rs_batches: Vec<RecordBatch> = Vec::with_capacity(batches.len());
        for b in &batches {
            let rb = RecordBatch::from_pyarrow_bound(b)
                .map_err(|e| PyValueError::new_err(format!("pyarrow batch: {e}")))?;
            rs_batches.push(rb);
        }
        let backend = self.inner.clone();
        let topic = topic.to_string();
        py.detach(|| {
            rt().block_on(async move {
                let target = TargetTable {
                    schema: String::new(),
                    name: topic,
                };
                let arrow_stream = stream::iter(rs_batches.into_iter().map(Ok::<_, BackendError>));
                backend
                    .write_arrow_stream(&target, Box::pin(arrow_stream), WriteMode::Append)
                    .await
            })
        })
        .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Ack every retained delivery handler.
    fn commit_offsets(&self, py: Python<'_>) -> PyResult<()> {
        let backend = self.inner.clone();
        py.detach(|| rt().block_on(async move { backend.commit_offsets().await }))
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Drain pending handlers and drop them. Combined with a
    /// subscription-side `dead_letter_policy`, broker-side DLT
    /// routing applies after `max_delivery_attempts` nacks.
    fn nack_pending(&self, py: Python<'_>) -> PyResult<()> {
        let backend = self.inner.clone();
        py.detach(|| rt().block_on(async move { backend.nack_pending().await }))
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Observability: number of unacked deliveries retained on
    /// the consumer session.
    fn pending_handler_count(&self, py: Python<'_>) -> PyResult<usize> {
        let backend = self.inner.clone();
        Ok(py.detach(|| rt().block_on(async move { backend.pending_handler_count().await })))
    }

    fn __repr__(&self) -> String {
        format!("PubSubBackend(project_id={:?})", self.inner.project_id())
    }
}
