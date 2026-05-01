//! Phase Py.2: KafkaBackend pyclass + PyArrow integration.
//!
//! Exposes the streaming Kafka backend's reader/writer methods to
//! Python with PyArrow record-batch conversion at the FFI
//! boundary. Driven from a single kwarg-style `open()` static
//! method — avoids fighting Rust's consume-self builder pattern
//! across the Python boundary, and reads naturally as Python.
//!
//! ## Surface
//!   - `KafkaBackend.open(bootstrap_servers, **kwargs)` →
//!     `KafkaBackend`. All builder-method config (auth, batch
//!     limits, payload format, schema-registry URL, delivery
//!     semantics) is supplied as keyword arguments.
//!   - `.ping()` — connectivity check; raises `ValueError` on fail.
//!   - `.read_arrow_stream(query)` → `list[pyarrow.RecordBatch]`.
//!     Drives the same drain-with-idle-timeout loop the Rust API
//!     uses; returns the accumulated batches when the source goes
//!     idle (or the size/byte limits fire).
//!   - `.write_arrow_stream(topic, batches)` → `int`. Accepts
//!     `list[pyarrow.RecordBatch]` and returns the row count
//!     produced.
//!   - `.commit_offsets()` — manual ack via Kafka's offset commit.
//!
//! ## What's deferred
//!   - Builder-style chained configuration. Python users get
//!     kwargs-on-open instead.
//!   - Streaming iterator return type. `read_arrow_stream`
//!     returns the materialized list of batches rather than a
//!     Python async iterator. The Rust pipeline runner
//!     (`ematix_flow.run_pipeline`) is the recommended path for
//!     long-running consumers; this surface is for ad-hoc
//!     reads/writes from notebooks + scripts.

use std::sync::Arc;

use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow_array::RecordBatch;
use ematix_flow_core::KafkaBackend as RsKafkaBackend;
use ematix_flow_core::backend::{Backend, BackendError, TargetTable, WriteMode};
use ematix_flow_core::kafka_backend::{
    KafkaBatchConfig, KafkaDeliverySemantics, KafkaPayloadFormat, ScramMechanism, TlsAuth,
};
use futures_util::TryStreamExt;
use futures_util::stream;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::arrow_iter::PyArrowBatchIter;
use crate::rt;

/// Python-facing Kafka backend handle.
///
/// Wraps `Arc<ematix_flow_core::KafkaBackend>` so multiple Python
/// references to the same backend share the persistent consumer
/// session (manual ack, Phase 36e). Methods that mutate session
/// state (`read_arrow_stream`, `commit_offsets`) coordinate via
/// the underlying Rust mutex.
#[pyclass(name = "KafkaBackend")]
pub struct PyKafkaBackend {
    inner: Arc<RsKafkaBackend>,
}

#[pymethods]
impl PyKafkaBackend {
    /// Construct a KafkaBackend.
    ///
    /// `bootstrap_servers` is the only required argument. All
    /// other configuration is keyword-only.
    ///
    /// `payload_format`: `"json"` (default), `"raw_bytes"`,
    /// `"avro"`, or `"protobuf"`. Avro/Protobuf require
    /// `schema_registry_url`.
    ///
    /// `delivery_semantics`: `"at_least_once"` (default) or
    /// `"exactly_once"`. Exactly-once requires
    /// `transactional_id` to be set.
    ///
    /// Auth providers (mutually exclusive — pass at most one):
    /// `sasl_plain_username` + `sasl_plain_password`;
    /// `sasl_scram_username` + `sasl_scram_password` +
    /// `sasl_scram_mechanism` (`"sha-256"` or `"sha-512"`);
    /// `tls_cert_path` + `tls_key_path` + `tls_ca_path`;
    /// `msk_iam_region`.
    #[staticmethod]
    #[pyo3(signature = (
        bootstrap_servers,
        *,
        group_id = None,
        payload_format = None,
        schema_registry_url = None,
        delivery_semantics = None,
        transactional_id = None,
        sasl_plain_username = None,
        sasl_plain_password = None,
        sasl_scram_username = None,
        sasl_scram_password = None,
        sasl_scram_mechanism = None,
        tls_cert_path = None,
        tls_key_path = None,
        tls_ca_path = None,
        msk_iam_region = None,
        batch_size = None,
        batch_bytes = None,
        batch_window_ms = None,
        idle_timeout_ms = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn open(
        bootstrap_servers: &str,
        group_id: Option<&str>,
        payload_format: Option<&str>,
        schema_registry_url: Option<String>,
        delivery_semantics: Option<&str>,
        transactional_id: Option<String>,
        sasl_plain_username: Option<String>,
        sasl_plain_password: Option<String>,
        sasl_scram_username: Option<String>,
        sasl_scram_password: Option<String>,
        sasl_scram_mechanism: Option<&str>,
        tls_cert_path: Option<String>,
        tls_key_path: Option<String>,
        tls_ca_path: Option<String>,
        msk_iam_region: Option<String>,
        batch_size: Option<usize>,
        batch_bytes: Option<usize>,
        batch_window_ms: Option<u64>,
        idle_timeout_ms: Option<u64>,
    ) -> PyResult<Self> {
        let mut backend = RsKafkaBackend::open(bootstrap_servers, group_id)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        if let Some(fmt) = payload_format {
            let fmt = match fmt.to_ascii_lowercase().as_str() {
                "json" => KafkaPayloadFormat::Json,
                "raw_bytes" | "raw" | "bytes" => KafkaPayloadFormat::RawBytes,
                "avro" => KafkaPayloadFormat::Avro,
                "protobuf" | "proto" => KafkaPayloadFormat::Protobuf,
                other => {
                    return Err(PyValueError::new_err(format!(
                        "unknown payload_format `{other}`; expected one of \
                         json, raw_bytes, avro, protobuf"
                    )));
                }
            };
            backend = backend.with_payload_format(fmt);
        }
        if let Some(url) = schema_registry_url {
            backend = backend.with_schema_registry_url(url);
        }
        if let Some(sem) = delivery_semantics {
            let sem = match sem.to_ascii_lowercase().as_str() {
                "at_least_once" => KafkaDeliverySemantics::AtLeastOnce,
                "exactly_once" => {
                    let id = transactional_id.ok_or_else(|| {
                        PyValueError::new_err(
                            "delivery_semantics='exactly_once' requires transactional_id",
                        )
                    })?;
                    KafkaDeliverySemantics::ExactlyOnce {
                        transactional_id: id,
                    }
                }
                other => {
                    return Err(PyValueError::new_err(format!(
                        "unknown delivery_semantics `{other}`; expected at_least_once \
                         or exactly_once"
                    )));
                }
            };
            backend = backend.with_delivery_semantics(sem);
        }
        // Auth providers — at most one. We don't enforce
        // mutual exclusion strictly; the last one wins, but
        // the docstring documents the contract.
        if let (Some(u), Some(p)) = (sasl_plain_username, sasl_plain_password) {
            backend = backend.with_sasl_plain(u, p);
        }
        if let (Some(u), Some(p), Some(mech)) = (
            sasl_scram_username,
            sasl_scram_password,
            sasl_scram_mechanism,
        ) {
            let mech = match mech.to_ascii_lowercase().as_str() {
                "sha-256" | "sha256" => ScramMechanism::Sha256,
                "sha-512" | "sha512" => ScramMechanism::Sha512,
                other => {
                    return Err(PyValueError::new_err(format!(
                        "unknown sasl_scram_mechanism `{other}`; expected sha-256 or sha-512"
                    )));
                }
            };
            backend = backend.with_sasl_scram(mech, u, p);
        }
        if let (Some(cert), Some(key), Some(ca)) = (tls_cert_path, tls_key_path, tls_ca_path) {
            backend = backend.with_tls(TlsAuth {
                cert_location: cert,
                key_location: key,
                ca_location: ca,
                key_password: None,
            });
        }
        if let Some(region) = msk_iam_region {
            backend = backend.with_msk_iam(region);
        }

        // Batch config — apply only if any field is set.
        if batch_size.is_some()
            || batch_bytes.is_some()
            || batch_window_ms.is_some()
            || idle_timeout_ms.is_some()
        {
            let mut cfg = KafkaBatchConfig::default();
            if let Some(v) = batch_size {
                cfg.batch_size = v;
            }
            if let Some(v) = batch_bytes {
                cfg.batch_bytes = v;
            }
            if let Some(v) = batch_window_ms {
                cfg.batch_window_ms = v;
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

    /// Validate connectivity. Raises `ValueError` on failure.
    fn ping(&self, py: Python<'_>) -> PyResult<()> {
        let backend = self.inner.clone();
        py.detach(|| rt().block_on(async move { backend.ping().await }))
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Drain `query` (the topic name) into a list of PyArrow
    /// RecordBatches. Honors the backend's batch / idle limits.
    /// Auto-commit is off; call `commit_offsets()` after a
    /// durable downstream write.
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

    /// Produce each row of every PyArrow RecordBatch to `topic`.
    /// Returns the total number of rows produced.
    fn write_arrow_stream(
        &self,
        py: Python<'_>,
        topic: &str,
        batches: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<u64> {
        // Convert PyArrow → arrow-rs RecordBatch up front (with the
        // GIL held). After conversion we drop the GIL for the IO.
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

    /// Same as :py:meth:`read_arrow_stream` but returns a Python
    /// iterator yielding one PyArrow RecordBatch at a time
    /// instead of materializing the full drain into a list.
    /// Useful when each batch is large and the user wants to
    /// process them streaming-style.
    fn iter_arrow_stream(&self, py: Python<'_>, query: &str) -> PyResult<PyArrowBatchIter> {
        let backend = self.inner.clone();
        let query = query.to_string();
        let stream = py
            .detach(|| rt().block_on(async move { backend.read_arrow_stream(&query).await }))
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyArrowBatchIter::new(stream))
    }

    /// Commit the consumer's pending offsets. No-op for
    /// producer-only backends. Call after the downstream write
    /// has durably landed for at-least-once semantics.
    fn commit_offsets(&self, py: Python<'_>) -> PyResult<()> {
        let backend = self.inner.clone();
        py.detach(|| rt().block_on(async move { backend.commit_offsets().await }))
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    fn __repr__(&self) -> String {
        format!(
            "KafkaBackend(bootstrap={:?})",
            self.inner.bootstrap_servers()
        )
    }
}
