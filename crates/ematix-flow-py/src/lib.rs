use std::sync::{Arc, OnceLock};

use ematix_flow_core::ddl::{self, DriftResult};
use ematix_flow_core::pg::{self, EnsureOutcome, PgPool};
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
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_class::<Connection>()?;
    Ok(())
}
