//! Phase Py.3: RabbitMQBackend pyclass + PyArrow integration.
//!
//! Mirrors the Phase Py.2 Kafka template against the RabbitMQ
//! Rust surface. Kwarg-style `open()` builds the full backend in
//! one call; instance methods cover the read/write/ack lifecycle
//! plus the broker-level DLQ primitive (`nack_pending`).

use std::sync::Arc;

use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow_array::RecordBatch;
use ematix_flow_core::RabbitMQBackend as RsRabbitMQBackend;
use ematix_flow_core::backend::{Backend, BackendError, TargetTable, WriteMode};
use ematix_flow_core::rabbitmq_backend::RabbitBatchConfig;
use futures_util::TryStreamExt;
use futures_util::stream;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::rt;

/// Python-facing RabbitMQ backend handle.
#[pyclass(name = "RabbitMQBackend")]
pub struct PyRabbitMQBackend {
    inner: Arc<RsRabbitMQBackend>,
}

#[pymethods]
impl PyRabbitMQBackend {
    /// Construct a RabbitMQBackend.
    ///
    /// `amqp_url` is the only required argument
    /// (`amqp://user:pass@host:port/vhost` or `amqps://...`).
    /// All other configuration is keyword-only.
    ///
    /// `consumer_tag` overrides the default "ematix-flow-consumer"
    /// consumer-tag prefix used by `basic_consume`.
    ///
    /// `batch_size` / `batch_bytes` / `idle_timeout_ms` tune the
    /// `read_arrow_stream` drain limits — first trigger flushes.
    #[staticmethod]
    #[pyo3(signature = (
        amqp_url,
        *,
        consumer_tag = None,
        batch_size = None,
        batch_bytes = None,
        idle_timeout_ms = None,
    ))]
    fn open(
        amqp_url: &str,
        consumer_tag: Option<&str>,
        batch_size: Option<usize>,
        batch_bytes: Option<usize>,
        idle_timeout_ms: Option<u64>,
    ) -> PyResult<Self> {
        let mut backend =
            RsRabbitMQBackend::open(amqp_url).map_err(|e| PyValueError::new_err(e.to_string()))?;

        if let Some(tag) = consumer_tag {
            backend = backend.with_consumer_tag(tag);
        }
        if batch_size.is_some() || batch_bytes.is_some() || idle_timeout_ms.is_some() {
            let mut cfg = RabbitBatchConfig::default();
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

    /// Validate connectivity (open connection + channel + close).
    fn ping(&self, py: Python<'_>) -> PyResult<()> {
        let backend = self.inner.clone();
        py.detach(|| rt().block_on(async move { backend.ping().await }))
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Drain `query` (the queue name) into a list of PyArrow
    /// RecordBatches. Auto-ack is **off**; call
    /// :py:meth:`commit_offsets` after the downstream write lands
    /// for at-least-once semantics, or :py:meth:`nack_pending` to
    /// drop the deliveries.
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

    /// Produce each row of every PyArrow batch to `queue` via the
    /// default exchange (routing_key = queue name).
    fn write_arrow_stream(
        &self,
        py: Python<'_>,
        queue: &str,
        batches: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<u64> {
        let mut rs_batches: Vec<RecordBatch> = Vec::with_capacity(batches.len());
        for b in &batches {
            let rb = RecordBatch::from_pyarrow_bound(b)
                .map_err(|e| PyValueError::new_err(format!("pyarrow batch: {e}")))?;
            rs_batches.push(rb);
        }
        let backend = self.inner.clone();
        let queue = queue.to_string();
        py.detach(|| {
            rt().block_on(async move {
                let target = TargetTable {
                    schema: String::new(),
                    name: queue,
                };
                let arrow_stream = stream::iter(rs_batches.into_iter().map(Ok::<_, BackendError>));
                backend
                    .write_arrow_stream(&target, Box::pin(arrow_stream), WriteMode::Append)
                    .await
            })
        })
        .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Ack every accumulated delivery in one round-trip
    /// (basic_ack with multiple=true).
    fn commit_offsets(&self, py: Python<'_>) -> PyResult<()> {
        let backend = self.inner.clone();
        py.detach(|| rt().block_on(async move { backend.commit_offsets().await }))
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Phase 37a.4: batch-nack every accumulated delivery via
    /// `basic_nack(multiple=true, requeue=...)`. With
    /// `requeue=False`, broker-side DLX routing applies if the
    /// queue was declared with `x-dead-letter-exchange`.
    #[pyo3(signature = (requeue))]
    fn nack_pending(&self, py: Python<'_>, requeue: bool) -> PyResult<()> {
        let backend = self.inner.clone();
        py.detach(|| rt().block_on(async move { backend.nack_pending(requeue).await }))
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Observability: highest pending delivery tag — coarse
    /// upper bound on outstanding deliveries since the last
    /// commit.
    fn pending_delivery_count(&self, py: Python<'_>) -> PyResult<u64> {
        let backend = self.inner.clone();
        Ok(py.detach(|| rt().block_on(async move { backend.pending_delivery_count().await })))
    }

    fn __repr__(&self) -> String {
        format!("RabbitMQBackend(amqp_url={:?})", self.inner.amqp_url())
    }
}
