use std::sync::{Arc, OnceLock};

use ematix_flow_core::pg::{self, PgPool};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
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
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_class::<Connection>()?;
    Ok(())
}
