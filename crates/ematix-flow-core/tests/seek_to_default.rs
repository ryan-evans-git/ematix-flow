//! Phase 39.5a PR 1 slice 1.5: backends that haven't opted in to
//! `seek_to` must report `false` from `supports_seek_to()` and
//! return a clear error from `seek_to()`. The session/join config
//! validator will reject pipelines whose source backend isn't
//! seek-capable.

use ematix_flow_core::SQLiteBackend;
use ematix_flow_core::backend::Backend;

#[tokio::test]
async fn sqlite_backend_does_not_support_seek_to() {
    let backend = SQLiteBackend::open(":memory:").unwrap();
    assert!(
        !backend.supports_seek_to(),
        "SQLite is not a streaming source — must report false"
    );
    let err = backend.seek_to(b"any-bytes").await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("seek_to") && msg.contains("does not support"),
        "default error must mention seek_to + lack of support; got: {msg}"
    );
}
