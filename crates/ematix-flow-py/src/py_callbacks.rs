//! PyO3 bindings for the `ematix_flow_core::py_callbacks` registry.
//!
//! Exposes three Python-facing functions:
//!
//! * [`register_python_callback`] — register a Python callable under
//!   a string name. The callable receives a `bytes` JSON payload and
//!   must return `bytes` (JSON) or raise.
//! * [`unregister_python_callback`] — clear a registration.
//! * [`is_python_callback_registered`] — introspection for tests.
//!
//! The Rust-side registry lives in `ematix-flow-core` so Rust
//! backends (Kafka Glue dispatch, future warehouse-pipeline runner)
//! can invoke callbacks without taking a PyO3 dependency themselves.

use std::sync::Arc;

use ematix_flow_core::py_callbacks::{CallbackFn, global};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// Register a Python callable under `name`. The Rust side will invoke
/// it whenever code calls into the named callback (typically via
/// `ematix_flow_core::py_callbacks::global().invoke(name, args)`).
///
/// Contract:
///
/// * The callable is invoked with one positional argument of type
///   ``bytes`` — the request payload (JSON, typically) from the
///   Rust call site.
/// * It must return ``bytes`` — the response payload. Returning any
///   other type raises a clear ``TypeError`` so the bug is obvious.
/// * Raising propagates as a Rust ``CallbackError::CallbackFailed``
///   with the exception's ``str()`` form as the message.
///
/// Re-registering replaces the previous binding — the most recent
/// registration wins. This is useful for tests that swap in a stub.
#[pyfunction]
pub fn register_python_callback(name: &str, callable: Py<PyAny>) -> PyResult<()> {
    let callable: Arc<Py<PyAny>> = Arc::new(callable);
    let cb: CallbackFn = Arc::new(move |args: &[u8]| -> Result<Vec<u8>, String> {
        Python::attach(|py| -> Result<Vec<u8>, String> {
            let arg_bytes = PyBytes::new(py, args);
            let result = callable
                .call1(py, (arg_bytes,))
                .map_err(|e| format!("python callback raised: {e}"))?;
            let bound = result.bind(py);
            let bytes_obj = bound.cast::<PyBytes>().map_err(|_| {
                format!(
                    "python callback must return bytes, got {}",
                    bound.get_type().name().map(|s| s.to_string())
                        .unwrap_or_else(|_| "<unknown>".to_string()),
                )
            })?;
            Ok(bytes_obj.as_bytes().to_vec())
        })
    });
    global().register(name.to_string(), cb);
    Ok(())
}

/// Remove a previously registered callback. Returns ``True`` if the
/// callback was registered (and is now gone), ``False`` otherwise.
#[pyfunction]
pub fn unregister_python_callback(name: &str) -> bool {
    global().unregister(name)
}

/// Whether a callback is registered under ``name``. Useful for tests
/// that want to confirm an import-time registration actually fired.
#[pyfunction]
pub fn is_python_callback_registered(name: &str) -> bool {
    global().is_registered(name)
}

/// Invoke a registered callback with `args` bytes and return the
/// response bytes. Intended for tests / introspection; production
/// callers reach the callback through Rust code that calls into
/// `global().invoke(...)` directly.
#[pyfunction]
pub fn invoke_python_callback<'py>(
    py: Python<'py>,
    name: &str,
    args: &[u8],
) -> PyResult<Bound<'py, PyBytes>> {
    let result = global()
        .invoke(name, args)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok(PyBytes::new(py, &result))
}
