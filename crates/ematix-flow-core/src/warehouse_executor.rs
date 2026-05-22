//! Rust-side invoker for `@ematix.warehouse_pipeline` callbacks.
//!
//! Companion to [`crate::py_callbacks`]: the Python decorator
//! registers each wrapped warehouse-pipeline function as a Rust
//! callback under a deterministic name
//! (`ematix_flow.warehouse_pipeline:<pipeline_name>`); this module
//! is what a Rust scheduler calls to dispatch one of those pipelines
//! without subprocess overhead.
//!
//! The bridge is JSON-in / JSON-out (per [`crate::py_callbacks`]).
//! Today the request body is empty (`{}`) — the pipeline name comes
//! from the callback name itself — but the structure is in place for
//! future extension (e.g. passing a watermark hint or a retry
//! attempt counter).

use serde::{Deserialize, Serialize};

use crate::py_callbacks::{CallbackError, global};

/// The prefix the Python side uses for warehouse-pipeline callbacks.
/// Pinned in both languages; changing one without the other will
/// silently route to "callback not registered" errors.
pub const WAREHOUSE_PIPELINE_CALLBACK_PREFIX: &str = "ematix_flow.warehouse_pipeline:";

/// The run result a `@ematix.warehouse_pipeline` body returns. Same
/// shape as the dict the Python in-process scheduler builds; the
/// Rust executor parses it back so it can update its own observability
/// fields (metrics, alerters, run-log).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarehousePipelineRunResult {
    pub status: String,
    pub pipeline: String,
    pub rows_read: u64,
    pub rows_written: u64,
    pub duration_ms: u64,
    /// Only populated when the pipeline declares
    /// `incremental_column=`. ``None`` otherwise.
    #[serde(default)]
    pub watermark: Option<String>,
}

/// Errors the Rust executor surfaces when invoking a warehouse
/// pipeline. Wraps [`CallbackError`] so callers don't need to import
/// the registry module directly.
#[derive(Debug, thiserror::Error)]
pub enum WarehouseExecutorError {
    #[error(
        "warehouse pipeline {0:?} has no registered callback. Did the user's \
         module import run yet? Pipelines are registered at @ematix.warehouse_\
         pipeline decoration time, so importing the module is what makes the \
         callback available."
    )]
    NotRegistered(String),

    #[error("warehouse pipeline {pipeline:?} raised: {message}")]
    CallbackFailed { pipeline: String, message: String },

    #[error("warehouse pipeline {pipeline:?} returned malformed JSON: {message}")]
    BadResponseShape { pipeline: String, message: String },
}

/// Invoke a warehouse pipeline by name. Returns the parsed run
/// result; surfaces a clear error variant for the three common
/// failure modes (no callback registered, callback raised, response
/// not parseable as JSON-with-the-expected-shape).
pub fn invoke_warehouse_pipeline(
    pipeline_name: &str,
) -> Result<WarehousePipelineRunResult, WarehouseExecutorError> {
    let callback_name = format!("{WAREHOUSE_PIPELINE_CALLBACK_PREFIX}{pipeline_name}");

    // The Python adapter expects an empty JSON object today. Leaving
    // it as the explicit body so future extension (e.g. passing the
    // current retry attempt counter) only requires changing this
    // line + the Python adapter signature.
    let req_bytes: &[u8] = b"{}";

    let resp_bytes = global()
        .invoke(&callback_name, req_bytes)
        .map_err(|err| match err {
            CallbackError::NotRegistered(_) => {
                WarehouseExecutorError::NotRegistered(pipeline_name.to_string())
            }
            CallbackError::CallbackFailed { message, .. } => {
                WarehouseExecutorError::CallbackFailed {
                    pipeline: pipeline_name.to_string(),
                    message,
                }
            }
        })?;

    serde_json::from_slice::<WarehousePipelineRunResult>(&resp_bytes).map_err(|e| {
        WarehouseExecutorError::BadResponseShape {
            pipeline: pipeline_name.to_string(),
            message: format!("{e}"),
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::py_callbacks::CallbackFn;

    fn register_test_callback(
        suffix: &str,
        adapter: impl Fn(&[u8]) -> Result<Vec<u8>, String> + Send + Sync + 'static,
    ) -> String {
        let name = format!("{WAREHOUSE_PIPELINE_CALLBACK_PREFIX}{suffix}");
        let cb: CallbackFn = Arc::new(adapter);
        global().register(name.clone(), cb);
        name
    }

    #[test]
    fn invoke_returns_parsed_result() {
        register_test_callback("test_ok", |_req| {
            let resp = WarehousePipelineRunResult {
                status: "succeeded".into(),
                pipeline: "test_ok".into(),
                rows_read: 5,
                rows_written: 5,
                duration_ms: 42,
                watermark: None,
            };
            Ok(serde_json::to_vec(&resp).unwrap())
        });
        let result = invoke_warehouse_pipeline("test_ok").unwrap();
        assert_eq!(result.status, "succeeded");
        assert_eq!(result.rows_read, 5);
        assert_eq!(result.duration_ms, 42);
        global().unregister(&format!("{WAREHOUSE_PIPELINE_CALLBACK_PREFIX}test_ok"));
    }

    #[test]
    fn invoke_returns_not_registered_when_missing() {
        let err =
            invoke_warehouse_pipeline("definitely_not_registered_pipeline_12345").unwrap_err();
        match err {
            WarehouseExecutorError::NotRegistered(name) => {
                assert_eq!(name, "definitely_not_registered_pipeline_12345");
            }
            other => panic!("expected NotRegistered, got {other:?}"),
        }
    }

    #[test]
    fn invoke_propagates_callback_failure() {
        register_test_callback("test_fail", |_req| Err("synthetic pipeline error".into()));
        let err = invoke_warehouse_pipeline("test_fail").unwrap_err();
        match err {
            WarehouseExecutorError::CallbackFailed { pipeline, message } => {
                assert_eq!(pipeline, "test_fail");
                assert!(message.contains("synthetic pipeline error"));
            }
            other => panic!("expected CallbackFailed, got {other:?}"),
        }
        global().unregister(&format!("{WAREHOUSE_PIPELINE_CALLBACK_PREFIX}test_fail"));
    }

    #[test]
    fn invoke_surfaces_bad_response_shape() {
        register_test_callback("test_bad_shape", |_req| Ok(b"not-valid-json".to_vec()));
        let err = invoke_warehouse_pipeline("test_bad_shape").unwrap_err();
        assert!(matches!(
            err,
            WarehouseExecutorError::BadResponseShape { .. }
        ));
        global().unregister(&format!(
            "{WAREHOUSE_PIPELINE_CALLBACK_PREFIX}test_bad_shape"
        ));
    }

    #[test]
    fn watermark_field_parses_when_present() {
        register_test_callback("test_watermark", |_req| {
            let json = r#"{
                "status": "succeeded",
                "pipeline": "test_watermark",
                "rows_read": 1,
                "rows_written": 1,
                "duration_ms": 10,
                "watermark": "2026-05-21T12:00:00Z"
            }"#;
            Ok(json.as_bytes().to_vec())
        });
        let result = invoke_warehouse_pipeline("test_watermark").unwrap();
        assert_eq!(result.watermark.as_deref(), Some("2026-05-21T12:00:00Z"),);
        global().unregister(&format!(
            "{WAREHOUSE_PIPELINE_CALLBACK_PREFIX}test_watermark"
        ));
    }

    #[test]
    fn watermark_field_optional() {
        register_test_callback("test_no_watermark", |_req| {
            let json = r#"{
                "status": "succeeded",
                "pipeline": "test_no_watermark",
                "rows_read": 0,
                "rows_written": 0,
                "duration_ms": 1
            }"#;
            Ok(json.as_bytes().to_vec())
        });
        let result = invoke_warehouse_pipeline("test_no_watermark").unwrap();
        assert_eq!(result.watermark, None);
        global().unregister(&format!(
            "{WAREHOUSE_PIPELINE_CALLBACK_PREFIX}test_no_watermark"
        ));
    }
}
