//! DLQ Phase 1: `DeadLetterStore` contract suite.
//!
//! Bodies live in `tests/dlq_helpers/mod.rs` so the same suite is
//! re-run against `TableDlq` on Postgres
//! (`tests/integration_dlq_pg.rs`) and the applicable subset against
//! `KafkaTopicDlq` (`tests/integration_pg.rs`).
//!
//! TDD note: this suite was committed FIRST, red, against
//! `StubDlq` below — a deliberately wrong do-nothing store — before
//! any real implementation existed. Once `TableDlq` landed, the
//! `make_store` constructor switched to the SQLite (`:memory:`)
//! table store and the suite went green. The stub stays deleted
//! from history's tip; see the commit sequence.

mod dlq_helpers;

use dlq_helpers as h;
use ematix_flow_core::dlq::{DeadLetterStore, TableDlq};

/// Fresh, isolated store per test. SQLite in-memory rides the same
/// `TableDlq` code path production uses for local dev.
async fn make_store() -> TableDlq {
    TableDlq::open_sqlite(":memory:").await.unwrap()
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

/// A file-backed SQLite store persists across handles — the
/// local-dev durability story (`:memory:` is per-connection).
#[tokio::test]
async fn sqlite_file_store_persists_across_handles() {
    use std::sync::atomic::{AtomicU64, Ordering};
    // House rule: per-call atomic counter for tmp fixture paths.
    static FIXTURE_SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(format!(
        "dlq-{}.sqlite",
        FIXTURE_SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    let path = path.to_str().unwrap().to_string();

    {
        let store = TableDlq::open_sqlite(&path).await.unwrap();
        store
            .append(vec![h::mk_record("durable", 1)])
            .await
            .unwrap();
    }
    let reopened = TableDlq::open_sqlite(&path).await.unwrap();
    assert_eq!(reopened.depth("durable").await.unwrap().pending, 1);
    let got = reopened.browse("durable", 0, 10, None).await.unwrap();
    assert_eq!(got[0], h::mk_record("durable", 1));
}

// `DeadLetterStore` must stay object-safe — the pipeline holds it
// as `Arc<dyn DeadLetterStore>`.
#[tokio::test]
async fn trait_is_object_safe() {
    let store: std::sync::Arc<dyn DeadLetterStore> = std::sync::Arc::new(make_store().await);
    h::run_append_then_depth(store.as_ref(), "p").await;
}
