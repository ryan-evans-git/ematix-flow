//! Phase Py.5: KinesisBackend pyclass + PyArrow integration.
//!
//! Mirrors the Phase Py.2 / Py.3 / Py.4 templates against the AWS
//! Kinesis Rust surface. Kwarg-style `open()` covers region /
//! endpoint / static credentials / batch config; methods cover
//! the multi-shard read, batched produce, manual checkpoint, and
//! the reset-to-committed primitive.

use std::sync::Arc;

use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow_array::RecordBatch;
use ematix_flow_core::KinesisBackend as RsKinesisBackend;
use ematix_flow_core::backend::{Backend, BackendError, TargetTable, WriteMode};
use ematix_flow_core::kinesis_backend::KinesisBatchConfig;
use futures_util::TryStreamExt;
use futures_util::stream;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::arrow_iter::PyArrowBatchIter;
use crate::rt;

/// Python-facing AWS Kinesis backend handle.
#[pyclass(name = "KinesisBackend")]
pub struct PyKinesisBackend {
    inner: Arc<RsKinesisBackend>,
}

#[pymethods]
impl PyKinesisBackend {
    /// Construct a KinesisBackend bound to `stream_name`.
    ///
    /// `stream_name` is the only required argument. All other
    /// configuration is keyword-only.
    ///
    /// `region` pins the AWS region; otherwise resolves via the
    /// AWS credential chain.
    ///
    /// `endpoint` overrides the default regional endpoint
    /// (`http://localhost:4566` for LocalStack).
    ///
    /// `access_key_id` + `secret_access_key` opt into static
    /// credentials, bypassing the AWS credential chain. Required
    /// for LocalStack tests; production code leaves these off.
    #[staticmethod]
    #[pyo3(signature = (
        stream_name,
        *,
        region = None,
        endpoint = None,
        access_key_id = None,
        secret_access_key = None,
        batch_size = None,
        batch_bytes = None,
        max_empty_polls = None,
        idle_poll_ms = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn open(
        stream_name: &str,
        region: Option<&str>,
        endpoint: Option<&str>,
        access_key_id: Option<&str>,
        secret_access_key: Option<&str>,
        batch_size: Option<usize>,
        batch_bytes: Option<usize>,
        max_empty_polls: Option<u32>,
        idle_poll_ms: Option<u64>,
    ) -> PyResult<Self> {
        let mut backend = RsKinesisBackend::open(stream_name)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        if let Some(r) = region {
            backend = backend.with_region(r);
        }
        if let Some(ep) = endpoint {
            backend = backend.with_endpoint(ep);
        }
        if let (Some(ak), Some(sk)) = (access_key_id, secret_access_key) {
            backend = backend.with_static_credentials(ak, sk);
        }
        if batch_size.is_some()
            || batch_bytes.is_some()
            || max_empty_polls.is_some()
            || idle_poll_ms.is_some()
        {
            let mut cfg = KinesisBatchConfig::default();
            if let Some(v) = batch_size {
                cfg.batch_size = v;
            }
            if let Some(v) = batch_bytes {
                cfg.batch_bytes = v;
            }
            if let Some(v) = max_empty_polls {
                cfg.max_empty_polls = v;
            }
            if let Some(v) = idle_poll_ms {
                cfg.idle_poll_ms = v;
            }
            backend = backend.with_batch_config(cfg);
        }
        Ok(Self {
            inner: Arc::new(backend),
        })
    }

    /// Validate connectivity (`list_streams`).
    fn ping(&self, py: Python<'_>) -> PyResult<()> {
        let backend = self.inner.clone();
        py.detach(|| rt().block_on(async move { backend.ping().await }))
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Drain every shard into a list of PyArrow RecordBatches.
    /// `query` is required to be non-empty but otherwise ignored —
    /// the bound `stream_name` is the consumption surface.
    /// Auto-advance is **off** in the manual-checkpoint sense:
    /// the in-memory iterator advances per-call, but
    /// `commit_offsets` is what advances the durable
    /// `committed_sequence_number`.
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

    /// Produce each row of every PyArrow batch to the bound
    /// stream. `partition_key_prefix` is used as the prefix for
    /// per-row partition keys (rows fan across shards).
    fn write_arrow_stream(
        &self,
        py: Python<'_>,
        partition_key_prefix: &str,
        batches: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<u64> {
        let mut rs_batches: Vec<RecordBatch> = Vec::with_capacity(batches.len());
        for b in &batches {
            let rb = RecordBatch::from_pyarrow_bound(b)
                .map_err(|e| PyValueError::new_err(format!("pyarrow batch: {e}")))?;
            rs_batches.push(rb);
        }
        let backend = self.inner.clone();
        let prefix = partition_key_prefix.to_string();
        py.detach(|| {
            rt().block_on(async move {
                let target = TargetTable {
                    schema: String::new(),
                    name: prefix,
                };
                let arrow_stream = stream::iter(rs_batches.into_iter().map(Ok::<_, BackendError>));
                backend
                    .write_arrow_stream(&target, Box::pin(arrow_stream), WriteMode::Append)
                    .await
            })
        })
        .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Streaming variant of :py:meth:`read_arrow_stream` —
    /// returns a Python iterator yielding one PyArrow
    /// RecordBatch at a time across the multi-shard fanout.
    fn iter_arrow_stream(&self, py: Python<'_>, query: &str) -> PyResult<PyArrowBatchIter> {
        let backend = self.inner.clone();
        let query = query.to_string();
        let stream = py
            .detach(|| rt().block_on(async move { backend.read_arrow_stream(&query).await }))
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyArrowBatchIter::new(stream))
    }

    /// Advance each shard's `committed_sequence_number = pending`.
    /// Mirrors Kafka 36e / RabbitMQ 37a.3 / Pub/Sub 37b.3 manual
    /// ack — the in-process at-least-once primitive.
    fn commit_offsets(&self, py: Python<'_>) -> PyResult<()> {
        let backend = self.inner.clone();
        py.detach(|| rt().block_on(async move { backend.commit_offsets().await }))
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Phase 37c.3: invalidate every shard's in-memory iterator
    /// and drop pending sequence numbers. Next read rebuilds
    /// iterators from the committed checkpoint (or
    /// `TRIM_HORIZON` for shards that have never been
    /// committed). Use after a target write fails to retry from
    /// the last commit within a single backend lifetime.
    fn reset_to_committed_offsets(&self, py: Python<'_>) -> PyResult<()> {
        let backend = self.inner.clone();
        py.detach(|| rt().block_on(async move { backend.reset_to_committed_offsets().await }))
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Observability: number of shards with pending (uncommitted)
    /// sequence numbers.
    fn pending_sequence_count(&self, py: Python<'_>) -> PyResult<usize> {
        let backend = self.inner.clone();
        Ok(py.detach(|| rt().block_on(async move { backend.pending_sequence_count().await })))
    }

    fn __repr__(&self) -> String {
        format!("KinesisBackend(stream_name={:?})", self.inner.stream_name())
    }
}
