//! DLQ Phase 1: `DeadLetterStore` contract suite.
//!
//! Bodies live in `tests/dlq_helpers/mod.rs` so the same suite can be
//! re-run against `TableDlq` on Postgres and the applicable subset
//! against `KafkaTopicDlq`.
//!
//! TDD: this commit lands the suite RED against `StubDlq` — a
//! deliberately inert store — before any real implementation exists.
//! The next commit replaces `make_store` with the SQLite `TableDlq`.

mod dlq_helpers;

use std::time::Duration;

use dlq_helpers as h;
use ematix_flow_core::dlq::{
    DeadLetterStore, DlqDepth, DlqError, DlqRecord, DlqRecordId, DlqRecordStatus, DlqSelection,
};

/// Inert placeholder implementing the trait shape only. Every
/// contract test below must FAIL against it — proving the suite
/// actually asserts semantics, not just compilation.
#[derive(Debug, Default)]
struct StubDlq;

#[async_trait::async_trait]
impl DeadLetterStore for StubDlq {
    async fn append(&self, _records: Vec<DlqRecord>) -> Result<(), DlqError> {
        Ok(())
    }
    async fn depth(&self, _pipeline: &str) -> Result<DlqDepth, DlqError> {
        Ok(DlqDepth::default())
    }
    async fn browse(
        &self,
        _pipeline: &str,
        _page: u64,
        _page_size: u64,
        _status_filter: Option<DlqRecordStatus>,
    ) -> Result<Vec<DlqRecord>, DlqError> {
        Ok(Vec::new())
    }
    async fn take_for_replay(
        &self,
        _pipeline: &str,
        _selection: DlqSelection,
        _lease: Duration,
        _now_ms: i64,
    ) -> Result<Vec<DlqRecord>, DlqError> {
        Ok(Vec::new())
    }
    async fn ack_replayed(&self, _pipeline: &str, _ids: &[DlqRecordId]) -> Result<(), DlqError> {
        Ok(())
    }
    async fn park(&self, _pipeline: &str, _ids: &[DlqRecordId]) -> Result<(), DlqError> {
        Ok(())
    }
    async fn purge(&self, _pipeline: &str, _selection: DlqSelection) -> Result<u64, DlqError> {
        Ok(0)
    }
}

async fn make_store() -> StubDlq {
    StubDlq
}

#[tokio::test]
async fn empty_store_edges() {
    let store = make_store().await;
    h::run_empty_store_edges(&store, "p").await;
}

#[tokio::test]
async fn append_then_depth() {
    let store = make_store().await;
    h::run_append_then_depth(&store, "p").await;
}

#[tokio::test]
async fn browse_pages_oldest_first() {
    let store = make_store().await;
    h::run_browse_pages_oldest_first(&store, "p").await;
}

#[tokio::test]
async fn meta_round_trips() {
    let store = make_store().await;
    h::run_meta_round_trips(&store, "p").await;
}

#[tokio::test]
async fn meta_none_fields_round_trip() {
    let store = make_store().await;
    h::run_meta_none_fields_round_trip(&store, "p").await;
}

#[tokio::test]
async fn take_leases_exclusively() {
    let store = make_store().await;
    h::run_take_leases_exclusively(&store, "p").await;
}

#[tokio::test]
async fn lease_expiry_releases() {
    let store = make_store().await;
    h::run_lease_expiry_releases(&store, "p").await;
}

#[tokio::test]
async fn take_by_ids() {
    let store = make_store().await;
    h::run_take_by_ids(&store, "p").await;
}

#[tokio::test]
async fn ack_replayed_removes() {
    let store = make_store().await;
    h::run_ack_replayed_removes(&store, "p").await;
}

#[tokio::test]
async fn park_excludes_from_take() {
    let store = make_store().await;
    h::run_park_excludes_from_take(&store, "p").await;
}

#[tokio::test]
async fn park_leased_record() {
    let store = make_store().await;
    h::run_park_leased_record(&store, "p").await;
}

#[tokio::test]
async fn purge_ids() {
    let store = make_store().await;
    h::run_purge_ids(&store, "p").await;
}

#[tokio::test]
async fn purge_first_n() {
    let store = make_store().await;
    h::run_purge_first_n(&store, "p").await;
}

#[tokio::test]
async fn purge_all_includes_parked() {
    let store = make_store().await;
    h::run_purge_all_includes_parked(&store, "p").await;
}

#[tokio::test]
async fn pipelines_isolated() {
    let store = make_store().await;
    h::run_pipelines_isolated(&store, "p1", "p2").await;
}

#[tokio::test]
async fn error_truncated_to_8kb() {
    let store = make_store().await;
    h::run_error_truncated_to_8kb(&store, "p").await;
}

// `DeadLetterStore` must stay object-safe — the pipeline holds it
// as `Arc<dyn DeadLetterStore>`.
#[tokio::test]
async fn trait_is_object_safe() {
    let store: std::sync::Arc<dyn DeadLetterStore> = std::sync::Arc::new(make_store().await);
    h::run_append_then_depth(store.as_ref(), "p").await;
}
