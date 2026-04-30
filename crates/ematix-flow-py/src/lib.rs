use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[pyfunction]
fn core_version() -> &'static str {
    ematix_flow_core::VERSION
}

/// Parse a `PipelineSpec` from JSON, normalize and validate it, and return
/// the normalized JSON. Phase 1 bridge entry point — Python builds the spec
/// dict, ships it through here, and trusts the result for downstream phases.
#[pyfunction]
fn parse_spec(json: &str) -> PyResult<String> {
    ematix_flow_core::normalize_json(json).map_err(|e| PyValueError::new_err(e.to_string()))
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(core_version, m)?)?;
    m.add_function(wrap_pyfunction!(parse_spec, m)?)?;
    Ok(())
}
