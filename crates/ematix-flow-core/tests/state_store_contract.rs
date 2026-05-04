//! Phase 39.5a PR 1: in-memory contract tests for `StateStore`.
//!
//! Bodies live in `tests/state_store_helpers/mod.rs` so the same suite
//! can be re-run against `PostgresStateStore`
//! (`tests/integration_state_store_pg.rs`).

mod state_store_helpers;

use ematix_flow_core::state_store::InMemoryStateStore;
use state_store_helpers as h;

#[tokio::test]
async fn unknown_pipeline_returns_empty() {
    let store = InMemoryStateStore::new();
    h::run_unknown_pipeline_returns_empty(&store).await;
}

#[tokio::test]
async fn commit_then_load_roundtrips_state() {
    let store = InMemoryStateStore::new();
    h::run_commit_then_load_roundtrips_state(&store, "p").await;
}

#[tokio::test]
async fn upsert_overwrites_existing_blob() {
    let store = InMemoryStateStore::new();
    h::run_upsert_overwrites_existing_blob(&store, "p").await;
}

#[tokio::test]
async fn delete_removes_key() {
    let store = InMemoryStateStore::new();
    h::run_delete_removes_key(&store, "p").await;
}

#[tokio::test]
async fn delete_of_unknown_key_is_ok() {
    let store = InMemoryStateStore::new();
    h::run_delete_of_unknown_key_is_ok(&store, "p").await;
}

#[tokio::test]
async fn offsets_are_replaced_per_source() {
    let store = InMemoryStateStore::new();
    h::run_offsets_are_replaced_per_source(&store, "p").await;
}

#[tokio::test]
async fn pipelines_are_isolated() {
    let store = InMemoryStateStore::new();
    h::run_pipelines_are_isolated(&store, "p1", "p2").await;
}

#[tokio::test]
async fn state_version_persists_latest() {
    let store = InMemoryStateStore::new();
    h::run_state_version_persists_latest(&store, "p").await;
}

#[tokio::test]
async fn empty_commit_is_noop() {
    let store = InMemoryStateStore::new();
    h::run_empty_commit_is_noop(&store, "p").await;
}
