use std::sync::{Arc, OnceLock};

use ematix_flow_core::ddl::{self, DriftResult};
use ematix_flow_core::meta::{DeleteHandling, WatermarkConfig};
use ematix_flow_core::pg::{self, EnsureOutcome, MergeRunResult, PgPool, Scd2RunResult};
use ematix_flow_core::strategy::append::augment_with_metadata;
use ematix_flow_core::strategy::scd2::augment_with_scd2;
use ematix_flow_core::types::TableSpec;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use tokio::runtime::Runtime;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn rt() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("ematix-flow")
            .build()
            .expect("failed to build tokio runtime")
    })
}

#[pyfunction]
fn core_version() -> &'static str {
    ematix_flow_core::VERSION
}

#[pyfunction]
fn parse_spec(json: &str) -> PyResult<String> {
    ematix_flow_core::normalize_json(json).map_err(|e| PyValueError::new_err(e.to_string()))
}

#[pyfunction]
fn parse_table_spec(json: &str) -> PyResult<String> {
    ematix_flow_core::normalize_table_json(json).map_err(|e| PyValueError::new_err(e.to_string()))
}

#[pyfunction]
fn same_database(a: &str, b: &str) -> PyResult<bool> {
    pg::same_database(a, b).map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Add SCD2 metadata columns (valid_from/valid_to/is_current/row_hash)
/// plus `_loaded_at`/`_batch_id` if absent. valid_from joins the natural
/// keys to form the composite PK so multiple versions per key can coexist.
#[pyfunction]
fn augment_table_spec_scd2(spec_json: &str) -> PyResult<String> {
    let normalized = ematix_flow_core::normalize_table_json(spec_json)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let spec: TableSpec =
        serde_json::from_str(&normalized).map_err(|e| PyValueError::new_err(e.to_string()))?;
    let augmented = augment_with_scd2(&spec);
    let augmented_json =
        serde_json::to_string(&augmented).map_err(|e| PyValueError::new_err(e.to_string()))?;
    ematix_flow_core::normalize_table_json(&augmented_json)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Add `_loaded_at` and `_batch_id` columns to a TableSpec if absent.
/// Returns normalized JSON of the augmented spec (with fingerprint).
#[pyfunction]
fn augment_table_spec(spec_json: &str) -> PyResult<String> {
    let normalized = ematix_flow_core::normalize_table_json(spec_json)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let spec: TableSpec =
        serde_json::from_str(&normalized).map_err(|e| PyValueError::new_err(e.to_string()))?;
    let augmented = augment_with_metadata(&spec);
    let augmented_json =
        serde_json::to_string(&augmented).map_err(|e| PyValueError::new_err(e.to_string()))?;
    // Re-normalize to recompute fingerprint with the new columns.
    ematix_flow_core::normalize_table_json(&augmented_json)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

#[pyfunction]
fn create_table_sql(spec_json: &str) -> PyResult<String> {
    let normalized = ematix_flow_core::normalize_table_json(spec_json)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let spec: TableSpec =
        serde_json::from_str(&normalized).map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok(ddl::create_table_sql(&spec))
}

#[pyclass]
struct Connection {
    pool: Arc<PgPool>,
}

#[pymethods]
impl Connection {
    fn ping(&self, py: Python<'_>) -> PyResult<i32> {
        let pool = self.pool.clone();
        py.detach(|| rt().block_on(async move { pool.ping().await }))
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    fn execute(&self, py: Python<'_>, sql: String) -> PyResult<u64> {
        let pool = self.pool.clone();
        py.detach(|| rt().block_on(async move { pool.execute(&sql).await }))
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    fn fetch_scalar_int(&self, py: Python<'_>, sql: String) -> PyResult<i32> {
        let pool = self.pool.clone();
        py.detach(|| rt().block_on(async move { pool.fetch_scalar_int(&sql).await }))
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    fn execute_in_transaction(&self, py: Python<'_>, sqls: Vec<String>) -> PyResult<()> {
        let pool = self.pool.clone();
        py.detach(|| rt().block_on(async move { pool.execute_in_transaction(&sqls).await }))
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// (host, port, dbname, user) tuple. Used Python-side to detect the
    /// same-DB vs cross-DB code path before calling run_append.
    fn connection_info<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let info = self.pool.info();
        let dict = PyDict::new(py);
        dict.set_item("host", &info.host)?;
        dict.set_item("port", info.port)?;
        dict.set_item("dbname", &info.dbname)?;
        dict.set_item("user", &info.user)?;
        Ok(dict)
    }

    /// Run an SCD2 load. Returns rows_inserted (new versions) and
    /// rows_closed (previous versions closed out).
    /// `handle_deletes='soft'` adds a close-out post-step for keys
    /// missing from the source.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (target_spec_json, source_query, pipeline_name, keys, compare_columns, source=None, handle_deletes=None))]
    fn run_scd2<'py>(
        &self,
        py: Python<'py>,
        target_spec_json: String,
        source_query: String,
        pipeline_name: String,
        keys: Vec<String>,
        compare_columns: Vec<String>,
        source: Option<&Connection>,
        handle_deletes: Option<&str>,
    ) -> PyResult<Bound<'py, PyDict>> {
        if keys.is_empty() {
            return Err(PyValueError::new_err("scd2 requires at least one key"));
        }
        if compare_columns.is_empty() {
            return Err(PyValueError::new_err(
                "scd2 requires at least one compare column",
            ));
        }
        let delete_handling = match handle_deletes {
            None => None,
            Some("soft") => Some(DeleteHandling::Soft),
            Some(other) => {
                return Err(PyValueError::new_err(format!(
                    "scd2 supports handle_deletes='soft' only (got {other:?})"
                )));
            }
        };
        let normalized = ematix_flow_core::normalize_table_json(&target_spec_json)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let spec: TableSpec =
            serde_json::from_str(&normalized).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let target_pool = self.pool.clone();
        let source_pool = source.map(|s| s.pool.clone());

        let outcome: Scd2RunResult = py
            .detach(|| {
                rt().block_on(async move {
                    match source_pool {
                        None => {
                            target_pool
                                .run_scd2_same_db(
                                    &spec,
                                    &source_query,
                                    &keys,
                                    &compare_columns,
                                    &pipeline_name,
                                    delete_handling,
                                )
                                .await
                        }
                        Some(src) => {
                            target_pool
                                .run_scd2_cross_db(
                                    &src,
                                    &spec,
                                    &source_query,
                                    &keys,
                                    &compare_columns,
                                    &pipeline_name,
                                    delete_handling,
                                )
                                .await
                        }
                    }
                })
            })
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        let dict = PyDict::new(py);
        dict.set_item("run_id", outcome.run_id)?;
        dict.set_item("rows_inserted", outcome.rows_inserted)?;
        dict.set_item("rows_closed", outcome.rows_closed)?;
        dict.set_item("status", outcome.status)?;
        dict.set_item("path", outcome.path)?;
        Ok(dict)
    }

    /// Run a MergeUpsert / SCD1 load. Returns rows_inserted/rows_updated/
    /// rows_unchanged. `mode_label` is whatever the user passed ("merge"
    /// or "scd1") and is recorded in run_history.
    /// `handle_deletes='hard'` runs a DELETE post-step for keys missing
    /// from the source.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (target_spec_json, source_query, pipeline_name, keys, update_columns, mode_label, source=None, handle_deletes=None))]
    fn run_merge<'py>(
        &self,
        py: Python<'py>,
        target_spec_json: String,
        source_query: String,
        pipeline_name: String,
        keys: Vec<String>,
        update_columns: Vec<String>,
        mode_label: String,
        source: Option<&Connection>,
        handle_deletes: Option<&str>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let delete_handling = match handle_deletes {
            None => None,
            Some("hard") => Some(DeleteHandling::Hard),
            Some(other) => {
                return Err(PyValueError::new_err(format!(
                    "merge supports handle_deletes='hard' only (got {other:?})"
                )));
            }
        };
        if keys.is_empty() {
            return Err(PyValueError::new_err(
                "merge mode requires at least one key",
            ));
        }
        let normalized = ematix_flow_core::normalize_table_json(&target_spec_json)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let spec: TableSpec =
            serde_json::from_str(&normalized).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let target_pool = self.pool.clone();
        let source_pool = source.map(|s| s.pool.clone());

        let outcome: MergeRunResult = py
            .detach(|| {
                rt().block_on(async move {
                    match source_pool {
                        None => {
                            target_pool
                                .run_merge_same_db(
                                    &spec,
                                    &source_query,
                                    &keys,
                                    &update_columns,
                                    &pipeline_name,
                                    &mode_label,
                                    delete_handling,
                                )
                                .await
                        }
                        Some(src) => {
                            target_pool
                                .run_merge_cross_db(
                                    &src,
                                    &spec,
                                    &source_query,
                                    &keys,
                                    &update_columns,
                                    &pipeline_name,
                                    &mode_label,
                                    delete_handling,
                                )
                                .await
                        }
                    }
                })
            })
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        let dict = PyDict::new(py);
        dict.set_item("run_id", outcome.run_id)?;
        dict.set_item("rows_inserted", outcome.rows_inserted)?;
        dict.set_item("rows_updated", outcome.rows_updated)?;
        dict.set_item("rows_unchanged", outcome.rows_unchanged)?;
        dict.set_item("status", outcome.status)?;
        dict.set_item("path", outcome.path)?;
        Ok(dict)
    }

    /// Run a TruncateReplace load. If `source` is None, uses self as the
    /// source (same-DB); otherwise stages source rows through COPY (binary).
    /// TRUNCATE + INSERT happen in the same transaction so the target's
    /// pre-load contents survive any failure.
    #[pyo3(signature = (target_spec_json, source_query, pipeline_name, source=None))]
    fn run_truncate<'py>(
        &self,
        py: Python<'py>,
        target_spec_json: String,
        source_query: String,
        pipeline_name: String,
        source: Option<&Connection>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let normalized = ematix_flow_core::normalize_table_json(&target_spec_json)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let spec: TableSpec =
            serde_json::from_str(&normalized).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let target_pool = self.pool.clone();
        let source_pool = source.map(|s| s.pool.clone());

        let outcome = py
            .detach(|| {
                rt().block_on(async move {
                    match source_pool {
                        None => {
                            target_pool
                                .run_truncate_same_db(&spec, &source_query, &pipeline_name)
                                .await
                        }
                        Some(src) => {
                            target_pool
                                .run_truncate_cross_db(&src, &spec, &source_query, &pipeline_name)
                                .await
                        }
                    }
                })
            })
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        let dict = PyDict::new(py);
        dict.set_item("run_id", outcome.run_id)?;
        dict.set_item("rows_inserted", outcome.rows_inserted)?;
        dict.set_item("status", outcome.status)?;
        dict.set_item("path", outcome.path)?;
        Ok(dict)
    }

    /// Read the watermark row for `pipeline_name`, or None if absent.
    /// Returns `{column_name, last_value}` so Python can build a cast
    /// literal (`'value'::type`) on the next run.
    fn read_watermark<'py>(
        &self,
        py: Python<'py>,
        pipeline_name: String,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        let pool = self.pool.clone();
        let row = py
            .detach(|| rt().block_on(async move { pool.read_watermark(&pipeline_name).await }))
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        match row {
            None => Ok(None),
            Some(r) => {
                let dict = PyDict::new(py);
                dict.set_item("pipeline_name", r.pipeline_name)?;
                dict.set_item("column_name", r.column_name)?;
                dict.set_item("last_value", r.last_value)?;
                Ok(Some(dict))
            }
        }
    }

    /// Run an AppendOnly load. If `source` is None, uses self as the source
    /// (same-DB path); otherwise runs cross-DB COPY through self as target.
    /// `target_spec_json` is the augmented spec (metadata cols already added).
    /// `incremental_column` + `last_value_literal` (an already-cast SQL
    /// literal like `'2026-04-30T00:00Z'::timestamptz`) opt into watermarking.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (target_spec_json, source_query, pipeline_name, source=None, incremental_column=None, last_value_literal=None))]
    fn run_append<'py>(
        &self,
        py: Python<'py>,
        target_spec_json: String,
        source_query: String,
        pipeline_name: String,
        source: Option<&Connection>,
        incremental_column: Option<String>,
        last_value_literal: Option<String>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let normalized = ematix_flow_core::normalize_table_json(&target_spec_json)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let spec: TableSpec =
            serde_json::from_str(&normalized).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let target_pool = self.pool.clone();
        let source_pool = source.map(|s| s.pool.clone());
        let watermark = incremental_column.map(|column| WatermarkConfig {
            column,
            last_value_literal,
        });

        let outcome = py
            .detach(|| {
                rt().block_on(async move {
                    match source_pool {
                        None => {
                            target_pool
                                .run_append_same_db(
                                    &spec,
                                    &source_query,
                                    &pipeline_name,
                                    watermark.as_ref(),
                                )
                                .await
                        }
                        Some(src) => {
                            target_pool
                                .run_append_cross_db(
                                    &src,
                                    &spec,
                                    &source_query,
                                    &pipeline_name,
                                    watermark.as_ref(),
                                )
                                .await
                        }
                    }
                })
            })
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        let dict = PyDict::new(py);
        dict.set_item("run_id", outcome.run_id)?;
        dict.set_item("rows_inserted", outcome.rows_inserted)?;
        dict.set_item("status", outcome.status)?;
        dict.set_item("path", outcome.path)?;
        Ok(dict)
    }

    /// Ensure the target table exists and matches the declared spec.
    /// `on_drift` ∈ {"error", "warn"}. Returns
    /// `{"action": "created" | "matched" | "drift", "differences": [...]}`.
    #[pyo3(signature = (spec_json, on_drift="error"))]
    fn ensure_table<'py>(
        &self,
        py: Python<'py>,
        spec_json: String,
        on_drift: &str,
    ) -> PyResult<Bound<'py, PyDict>> {
        if on_drift != "error" && on_drift != "warn" {
            return Err(PyValueError::new_err(format!(
                "on_drift must be 'error' or 'warn' (got {on_drift:?})"
            )));
        }
        let normalized = ematix_flow_core::normalize_table_json(&spec_json)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let spec: TableSpec =
            serde_json::from_str(&normalized).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let pool = self.pool.clone();
        let outcome = py
            .detach(|| rt().block_on(async move { pool.ensure_table(&spec).await }))
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        let dict = PyDict::new(py);
        let diffs = PyList::empty(py);
        match outcome {
            EnsureOutcome::Created => {
                dict.set_item("action", "created")?;
            }
            EnsureOutcome::Matched => {
                dict.set_item("action", "matched")?;
            }
            EnsureOutcome::Drift(differences) => {
                if on_drift == "error" {
                    let messages: Vec<String> = differences.iter().map(|d| d.to_string()).collect();
                    return Err(PyValueError::new_err(format!(
                        "schema drift detected:\n  - {}",
                        messages.join("\n  - ")
                    )));
                }
                dict.set_item("action", "drift")?;
                for d in &differences {
                    diffs.append(d.to_string())?;
                }
            }
        }
        // Use the `DriftResult` discriminator for symmetry with the Rust enum
        // (helps Python tests pattern-match).
        let _ = DriftResult::Match;
        dict.set_item("differences", diffs)?;
        Ok(dict)
    }
}

#[pyfunction]
fn connect(py: Python<'_>, url: &str) -> PyResult<Connection> {
    let url = url.to_string();
    let pool = py
        .detach(|| rt().block_on(async move { PgPool::connect(&url).await }))
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok(Connection {
        pool: Arc::new(pool),
    })
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(core_version, m)?)?;
    m.add_function(wrap_pyfunction!(parse_spec, m)?)?;
    m.add_function(wrap_pyfunction!(parse_table_spec, m)?)?;
    m.add_function(wrap_pyfunction!(same_database, m)?)?;
    m.add_function(wrap_pyfunction!(create_table_sql, m)?)?;
    m.add_function(wrap_pyfunction!(augment_table_spec, m)?)?;
    m.add_function(wrap_pyfunction!(augment_table_spec_scd2, m)?)?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_class::<Connection>()?;
    Ok(())
}
