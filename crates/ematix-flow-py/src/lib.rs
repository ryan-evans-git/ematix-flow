mod arrow_iter;
mod kafka;
mod kinesis;
mod pubsub;
mod py_callbacks;
mod rabbitmq;
mod udaf;
mod udf;

use std::sync::{Arc, OnceLock};

use ematix_flow_core::backend::{Backend, Dialect, PostgresBackend, TargetTable, WriteMode};
use ematix_flow_core::ddl::{self, DriftResult};
use ematix_flow_core::meta::{DeleteHandling, WatermarkConfig};
use ematix_flow_core::pg::{self, EnsureOutcome, MergeRunResult, PgPool, Scd2RunResult};
use ematix_flow_core::strategy::append::{augment_with_metadata, plan_same_db_append};
use ematix_flow_core::strategy::merge::plan_merge_upsert;
use ematix_flow_core::strategy::scd2::{augment_with_scd2, plan_scd2};
use ematix_flow_core::strategy::truncate::plan_truncate_replace;
use ematix_flow_core::types::TableSpec;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use tokio::runtime::Runtime;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

pub(crate) fn rt() -> &'static Runtime {
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

/// Phase 25: pure-SQL planners exposed for `preview()`. These don't
/// touch the database; they synthesize the SQL `pipeline.sync` would
/// execute. Used by the preview/dry-run rendering path.
#[pyfunction]
fn plan_append_sql(spec_json: &str, source_query: &str) -> PyResult<String> {
    let normalized = ematix_flow_core::normalize_table_json(spec_json)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let spec: TableSpec =
        serde_json::from_str(&normalized).map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok(plan_same_db_append(&spec, source_query).sql)
}

#[pyfunction]
fn plan_truncate_sql(spec_json: &str, source_query: &str) -> PyResult<Vec<String>> {
    let normalized = ematix_flow_core::normalize_table_json(spec_json)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let spec: TableSpec =
        serde_json::from_str(&normalized).map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok(plan_truncate_replace(&spec, source_query).statements)
}

#[pyfunction]
fn plan_merge_sql(
    spec_json: &str,
    source_query: &str,
    keys: Vec<String>,
    update_columns: Vec<String>,
) -> PyResult<String> {
    let normalized = ematix_flow_core::normalize_table_json(spec_json)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let spec: TableSpec =
        serde_json::from_str(&normalized).map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok(plan_merge_upsert(&spec, source_query, &keys, &update_columns).sql)
}

#[pyfunction]
#[pyo3(signature = (spec_json, source_query, keys, compare_columns, run_token, event_ts_column=None))]
fn plan_scd2_sql(
    spec_json: &str,
    source_query: &str,
    keys: Vec<String>,
    compare_columns: Vec<String>,
    run_token: &str,
    event_ts_column: Option<&str>,
) -> PyResult<Vec<String>> {
    let normalized = ematix_flow_core::normalize_table_json(spec_json)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let spec: TableSpec =
        serde_json::from_str(&normalized).map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok(plan_scd2(
        &spec,
        source_query,
        &keys,
        &compare_columns,
        run_token,
        event_ts_column,
    )
    .statements)
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
    dsn: String,
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

    /// Phase 27a: record a transforms_post step in run_history. The
    /// step's SQL/callable is invoked from Python; this only records
    /// the outcome row.
    #[pyo3(signature = (parent_run_id, pipeline_name, step_name, status, target_schema, target_table, error_message=None, metrics_json=None))]
    #[allow(clippy::too_many_arguments)]
    fn record_transform_history(
        &self,
        py: Python<'_>,
        parent_run_id: String,
        pipeline_name: String,
        step_name: String,
        status: String,
        target_schema: String,
        target_table: String,
        error_message: Option<String>,
        metrics_json: Option<String>,
    ) -> PyResult<()> {
        let pool = self.pool.clone();
        py.detach(|| {
            rt().block_on(async move {
                pool.insert_transform_history(
                    &parent_run_id,
                    &pipeline_name,
                    &step_name,
                    &status,
                    &target_schema,
                    &target_table,
                    error_message.as_deref(),
                    metrics_json.as_deref(),
                )
                .await
            })
        })
        .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Phase 27e: return the original DSN. The Python-side df module
    /// uses this to construct a psycopg2 connection for read_df / write_df.
    /// Carries the password — keep callers within the same trust boundary
    /// as the user code that constructed the Connection.
    fn dsn(&self) -> String {
        self.dsn.clone()
    }

    /// Phase 30c: backend kind. Used by `pipeline.sync` to detect a
    /// cross-backend pair (source.dialect != target.dialect) and route
    /// through the Arrow streaming bridge instead of a Postgres-native
    /// fast path. Returns the lowercase backend name (`"postgres"`,
    /// `"mysql"`, ...).
    fn dialect(&self) -> &'static str {
        // For Phase 30c the only backend is Postgres. Phase 31+ extend
        // this to MySQL/SQLite/DuckDB/etc.
        "postgres"
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
    /// `event_timestamp_column='col'` switches to event-time SCD2:
    /// `valid_from` comes from that column; out-of-order arrivals are
    /// rejected with an error.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (target_spec_json, source_query, pipeline_name, keys, compare_columns, source=None, handle_deletes=None, event_timestamp_column=None, ttl_seconds=None, dry_run=false))]
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
        event_timestamp_column: Option<String>,
        ttl_seconds: Option<i64>,
        dry_run: bool,
    ) -> PyResult<Bound<'py, PyDict>> {
        if dry_run && source.is_some() {
            return Err(PyValueError::new_err(
                "dry_run is supported for same-DB only in v0.1; cross-DB dry_run lands later",
            ));
        }
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
                    let ets = event_timestamp_column.as_deref();
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
                                    ets,
                                    ttl_seconds,
                                    dry_run,
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
                                    ets,
                                    ttl_seconds,
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
    #[pyo3(signature = (target_spec_json, source_query, pipeline_name, keys, update_columns, mode_label, source=None, handle_deletes=None, dry_run=false))]
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
        dry_run: bool,
    ) -> PyResult<Bound<'py, PyDict>> {
        if dry_run && source.is_some() {
            return Err(PyValueError::new_err(
                "dry_run is supported for same-DB only in v0.1",
            ));
        }
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
                                    dry_run,
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
    #[pyo3(signature = (target_spec_json, source_query, pipeline_name, source=None, dry_run=false))]
    fn run_truncate<'py>(
        &self,
        py: Python<'py>,
        target_spec_json: String,
        source_query: String,
        pipeline_name: String,
        source: Option<&Connection>,
        dry_run: bool,
    ) -> PyResult<Bound<'py, PyDict>> {
        if dry_run && source.is_some() {
            return Err(PyValueError::new_err(
                "dry_run is supported for same-DB only in v0.1",
            ));
        }
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
                                .run_truncate_same_db(&spec, &source_query, &pipeline_name, dry_run)
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
    #[pyo3(signature = (target_spec_json, source_query, pipeline_name, source=None, incremental_column=None, last_value_literal=None, dry_run=false))]
    fn run_append<'py>(
        &self,
        py: Python<'py>,
        target_spec_json: String,
        source_query: String,
        pipeline_name: String,
        source: Option<&Connection>,
        incremental_column: Option<String>,
        last_value_literal: Option<String>,
        dry_run: bool,
    ) -> PyResult<Bound<'py, PyDict>> {
        if dry_run && source.is_some() {
            return Err(PyValueError::new_err(
                "dry_run is supported for same-DB only in v0.1",
            ));
        }
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
                                    dry_run,
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

/// Phase 30c: cross-backend Arrow streaming bridge.
///
/// `pipeline.sync` (Python) calls this when source.dialect() !=
/// target.dialect(). The function:
///   1. wraps both connections in their respective `Backend` impls,
///   2. opens a `read_arrow_stream` on the source,
///   3. pipes the resulting `RecordBatch` stream into the target's
///      `write_arrow_stream(target=…, mode=…)`.
///
/// Returns the row count written. Pure dispatch — no run_history,
/// no metadata-column augmentation. The strategy executors (Phase 30d)
/// remain the right layer for SCD2 / merge / row hashing; cross-backend
/// is currently scoped to append + truncate semantics (mirroring
/// Phase 27e write_df's inferred-path constraints).
#[pyfunction]
#[pyo3(signature = (source, target, source_query, target_schema, target_table, mode))]
fn cross_backend_arrow_sync(
    py: Python<'_>,
    source: &Connection,
    target: &Connection,
    source_query: String,
    target_schema: String,
    target_table: String,
    mode: &str,
) -> PyResult<u64> {
    let write_mode = match mode {
        "append" => WriteMode::Append,
        "truncate" => WriteMode::Truncate,
        other => {
            return Err(PyValueError::new_err(format!(
                "cross-backend Arrow sync supports mode='append'|'truncate', got {other:?}; \
                 use a same-backend pair for merge/scd1/scd2 (Phase 30d)"
            )));
        }
    };
    let source_backend: Arc<dyn Backend> = Arc::new(PostgresBackend::new(
        source.pool.clone(),
        source.dsn.clone(),
    ));
    let target_backend: Arc<dyn Backend> = Arc::new(PostgresBackend::new(
        target.pool.clone(),
        target.dsn.clone(),
    ));
    let target_ref = TargetTable {
        schema: target_schema,
        name: target_table,
    };
    let _ = source_backend.dialect(); // anchor for upcoming Phase 31 dispatch
    let _ = Dialect::Postgres; // keep enum reachable from PyO3 lib for future
    py.detach(|| {
        rt().block_on(async move {
            let stream = source_backend.read_arrow_stream(&source_query).await?;
            target_backend
                .write_arrow_stream(&target_ref, stream, write_mode)
                .await
        })
    })
    .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Phase Py.1: run a streaming pipeline from a TOML config
/// string. Blocks the calling Python thread until SIGTERM /
/// SIGINT is received or the pipeline exits cleanly.
///
/// Returns a dict with keys `total_rows`, `iterations`, and
/// `shutdown_triggered` mirroring `StreamingPipelineMetrics`.
/// Errors raise `ValueError` with the underlying error string.
///
/// `metrics_port`, when set, spawns the Prometheus `/metrics`
/// HTTP endpoint on `127.0.0.1:<port>` for the pipeline's
/// lifetime. Same shape as the `--metrics-port` flag on the
/// `flow consume` CLI.
///
/// Π.5: `udfs` accepts a Python list of `PythonScalarUdfHandle`
/// objects (built via the `@ematix.udf` decorator). Each handle
/// is unwrapped into the underlying `Arc<ScalarUDF>` and
/// registered on the pipeline's SQL pre-stage `SessionContext`,
/// callable from `transform_sql`.
#[pyfunction]
#[pyo3(signature = (toml_str, metrics_port=None, udfs=None, aggregate_udfs=None))]
fn run_pipeline_from_toml_str<'py>(
    py: Python<'py>,
    toml_str: &str,
    metrics_port: Option<u16>,
    udfs: Option<Vec<Py<udf::PyUdfHandle>>>,
    aggregate_udfs: Option<Vec<Py<udaf::PyUdafHandle>>>,
) -> PyResult<Bound<'py, PyDict>> {
    let cfg = ematix_flow_cli::PipelineCliConfig::from_toml_str(toml_str)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let udf_arcs = unwrap_udf_handles(py, udfs);
    let udaf_arcs = unwrap_udaf_handles(py, aggregate_udfs);
    let options = ematix_flow_cli::ConsumeOptions {
        metrics_port,
        // Python path: install_shutdown_handler manages SIGTERM /
        // SIGINT internally. Python's own signal handler (which
        // raises KeyboardInterrupt) gets temporarily shadowed by
        // tokio's; on Ctrl-C the pipeline drains + this fn
        // returns cleanly with shutdown_triggered=true.
        shutdown_signal: None,
        udfs: udf_arcs,
        aggregate_udfs: udaf_arcs,
    };
    let metrics = py
        .detach(|| {
            rt().block_on(async move { ematix_flow_cli::run_consume_with(cfg, options).await })
        })
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let dict = PyDict::new(py);
    dict.set_item("total_rows", metrics.total_rows)?;
    dict.set_item("iterations", metrics.iterations)?;
    dict.set_item("shutdown_triggered", metrics.shutdown_triggered)?;
    Ok(dict)
}

/// Phase Py.1: run a streaming pipeline from a TOML config file
/// path. Convenience wrapper around `run_pipeline_from_toml_str`
/// that reads the file. Same return shape + error semantics.
#[pyfunction]
#[pyo3(signature = (path, metrics_port=None, udfs=None, aggregate_udfs=None))]
fn run_pipeline_from_path<'py>(
    py: Python<'py>,
    path: &str,
    metrics_port: Option<u16>,
    udfs: Option<Vec<Py<udf::PyUdfHandle>>>,
    aggregate_udfs: Option<Vec<Py<udaf::PyUdafHandle>>>,
) -> PyResult<Bound<'py, PyDict>> {
    let cfg = ematix_flow_cli::PipelineCliConfig::from_path(path)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let udf_arcs = unwrap_udf_handles(py, udfs);
    let udaf_arcs = unwrap_udaf_handles(py, aggregate_udfs);
    let options = ematix_flow_cli::ConsumeOptions {
        metrics_port,
        shutdown_signal: None,
        udfs: udf_arcs,
        aggregate_udfs: udaf_arcs,
    };
    let metrics = py
        .detach(|| {
            rt().block_on(async move { ematix_flow_cli::run_consume_with(cfg, options).await })
        })
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let dict = PyDict::new(py);
    dict.set_item("total_rows", metrics.total_rows)?;
    dict.set_item("iterations", metrics.iterations)?;
    dict.set_item("shutdown_triggered", metrics.shutdown_triggered)?;
    Ok(dict)
}

/// Π.5: pull `Arc<ScalarUDF>` out of each Python-owned
/// `PythonScalarUdfHandle`. Returns an empty Vec when `udfs` is
/// `None` so callers don't need to special-case the "no UDFs"
/// path. Borrows each handle for the duration of the unwrap (no
/// clone of the underlying Python callable — the `Arc<ScalarUDF>`
/// already owns a `Py<PyAny>` reference).
fn unwrap_udf_handles(
    py: Python<'_>,
    udfs: Option<Vec<Py<udf::PyUdfHandle>>>,
) -> Vec<std::sync::Arc<ematix_flow_core::transform::ScalarUDF>> {
    udfs.unwrap_or_default()
        .into_iter()
        .map(|handle| std::sync::Arc::clone(&handle.borrow(py).udf))
        .collect()
}

/// Symmetric helper for `@udaf` handles — pulls each
/// `Arc<AggregateUDF>` out of a Python-owned `PythonAggregateUdfHandle`.
fn unwrap_udaf_handles(
    py: Python<'_>,
    udafs: Option<Vec<Py<udaf::PyUdafHandle>>>,
) -> Vec<std::sync::Arc<ematix_flow_core::transform::AggregateUDF>> {
    udafs
        .unwrap_or_default()
        .into_iter()
        .map(|handle| std::sync::Arc::clone(&handle.borrow(py).udaf))
        .collect()
}

#[pyfunction]
fn connect(py: Python<'_>, url: &str) -> PyResult<Connection> {
    let url_owned = url.to_string();
    let url_for_connect = url_owned.clone();
    let pool = py
        .detach(|| rt().block_on(async move { PgPool::connect(&url_for_connect).await }))
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok(Connection {
        pool: Arc::new(pool),
        dsn: url_owned,
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
    m.add_function(wrap_pyfunction!(plan_append_sql, m)?)?;
    m.add_function(wrap_pyfunction!(plan_truncate_sql, m)?)?;
    m.add_function(wrap_pyfunction!(plan_merge_sql, m)?)?;
    m.add_function(wrap_pyfunction!(plan_scd2_sql, m)?)?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(cross_backend_arrow_sync, m)?)?;
    m.add_function(wrap_pyfunction!(run_pipeline_from_toml_str, m)?)?;
    m.add_function(wrap_pyfunction!(run_pipeline_from_path, m)?)?;
    m.add_function(wrap_pyfunction!(udf::make_python_udf, m)?)?;
    m.add_function(wrap_pyfunction!(udf::_apply_python_udf_to_batch, m)?)?;
    m.add_function(wrap_pyfunction!(udaf::make_python_udaf, m)?)?;
    m.add_function(wrap_pyfunction!(udaf::_apply_python_udaf_to_batch, m)?)?;
    m.add_class::<Connection>()?;
    m.add_class::<kafka::PyKafkaBackend>()?;
    m.add_class::<rabbitmq::PyRabbitMQBackend>()?;
    m.add_class::<pubsub::PyPubSubBackend>()?;
    m.add_class::<kinesis::PyKinesisBackend>()?;
    m.add_class::<arrow_iter::PyArrowBatchIter>()?;
    m.add_class::<udf::PyUdfHandle>()?;
    m.add_class::<udaf::PyUdafHandle>()?;
    // Task #556 / #559 — Rust → Python callback registry.
    m.add_function(wrap_pyfunction!(py_callbacks::register_python_callback, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_callbacks::unregister_python_callback,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_callbacks::is_python_callback_registered,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_callbacks::invoke_python_callback, m)?)?;
    Ok(())
}
