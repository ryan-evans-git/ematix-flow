use pyo3::prelude::*;

#[pyfunction]
fn core_version() -> &'static str {
    ematix_flow_core::VERSION
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(core_version, m)?)?;
    Ok(())
}
