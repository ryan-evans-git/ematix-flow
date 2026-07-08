//! Phase 39.5a PR 1: end-to-end Postgres `StateStore` tests.
//!
//! Same contract suite as `state_store_contract.rs`, executed against a
//! real Postgres started via testcontainers. Marked `#[ignore]` so
//! `cargo test` stays fast by default. Run with:
//!
//! ```bash
//! cargo test -p ematix-flow-core --test integration_state_store_pg -- --ignored
//! ```
//!
//! Each test gets its own pipeline name so the tests can share a
//! container (one container start / suite is much faster than nine).
//! We use a fresh container per test for now to keep isolation simple;
//! revisit with a shared container if startup becomes a bottleneck.

mod state_store_helpers;

use ematix_flow_core::state_store::PostgresStateStore;
use state_store_helpers as h;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

async fn fresh_store() -> (testcontainers::ContainerAsync<Postgres>, PostgresStateStore) {
    let container = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("failed to start postgres testcontainer");
    let host = container.get_host().await.unwrap().to_string();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let store = PostgresStateStore::connect(&url, "public")
        .await
        .expect("PostgresStateStore::connect failed");
    store.ensure_schema().await.expect("ensure_schema failed");
    (container, store)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_unknown_pipeline_returns_empty() {
    let (_c, store) = fresh_store().await;
    h::run_unknown_pipeline_returns_empty(&store).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_commit_then_load_roundtrips_state() {
    let (_c, store) = fresh_store().await;
    h::run_commit_then_load_roundtrips_state(&store, "p").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_upsert_overwrites_existing_blob() {
    let (_c, store) = fresh_store().await;
    h::run_upsert_overwrites_existing_blob(&store, "p").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_delete_removes_key() {
    let (_c, store) = fresh_store().await;
    h::run_delete_removes_key(&store, "p").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_delete_of_unknown_key_is_ok() {
    let (_c, store) = fresh_store().await;
    h::run_delete_of_unknown_key_is_ok(&store, "p").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_offsets_are_replaced_per_source() {
    let (_c, store) = fresh_store().await;
    h::run_offsets_are_replaced_per_source(&store, "p").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_pipelines_are_isolated() {
    let (_c, store) = fresh_store().await;
    h::run_pipelines_are_isolated(&store, "p1", "p2").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_state_version_persists_latest() {
    let (_c, store) = fresh_store().await;
    h::run_state_version_persists_latest(&store, "p").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_empty_commit_is_noop() {
    let (_c, store) = fresh_store().await;
    h::run_empty_commit_is_noop(&store, "p").await;
}

/// Postgres-specific: the schema bootstrap is idempotent. A second
/// `ensure_schema` call on a populated store must not wipe state.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_ensure_schema_is_idempotent() {
    use ematix_flow_core::state_store::{CommitSnapshot, StateStore};

    let (_c, store) = fresh_store().await;
    store
        .commit(
            "p",
            CommitSnapshot {
                state_upserts: vec![(b"k".to_vec(), b"v".to_vec())],
                state_version: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    store.ensure_schema().await.unwrap();
    store.ensure_schema().await.unwrap();

    let r = store.load("p").await.unwrap();
    assert_eq!(
        r.state_by_key.get(b"k".as_slice()).map(|v| v.as_slice()),
        Some(&b"v"[..]),
        "ensure_schema must be idempotent — uses CREATE TABLE IF NOT EXISTS"
    );
}

/// Postgres-specific: a `commit` that fails part-way through (e.g.
/// constraint violation injected via raw SQL) must not leave partial
/// state visible to a subsequent `load`. This is the atomicity
/// guarantee the in-memory test can't exercise meaningfully.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_commit_is_atomic_under_failure() {
    use ematix_flow_core::state_store::{CommitSnapshot, StateStore};

    let (_c, store) = fresh_store().await;

    // Seed a known-good state.
    store
        .commit(
            "p",
            CommitSnapshot {
                state_upserts: vec![(b"keep".to_vec(), b"original".to_vec())],
                state_version: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Force a failure: inject a row mid-commit by exploiting the
    // duplicate-PK invariant on `state_upserts`. We pass the same
    // key twice with different blobs and a constraint inside the
    // commit transaction must catch it (or the upsert must collapse
    // them deterministically — either way the second `load` shouldn't
    // see a half-applied state).
    //
    // The store's chosen behavior is `ON CONFLICT DO UPDATE` which
    // collapses duplicates → the *last* upsert in the snapshot wins.
    // So we test the commit's transaction boundary by aiming an
    // *offsets* row at a deliberately broken table state instead:
    // drop the offsets table, then call commit; the in-transaction
    // CREATE TABLE in `ensure_schema` is not part of `commit`, so the
    // failure must roll back the state upsert too.
    store
        .raw_execute_for_test("DROP TABLE ematix_streaming_offsets")
        .await
        .unwrap();

    let bad_snap = CommitSnapshot {
        state_upserts: vec![(b"keep".to_vec(), b"BAD".to_vec())],
        offsets: h::map([("s", b"1".to_vec())]),
        state_version: 99,
        ..Default::default()
    };
    let result = store.commit("p", bad_snap).await;
    assert!(
        result.is_err(),
        "commit must fail when offsets table is missing"
    );

    // Restore the offsets table so `load` works again, then verify
    // that the failed commit did not corrupt the prior state.
    store.ensure_schema().await.unwrap();
    let r = store.load("p").await.unwrap();
    assert_eq!(
        r.state_by_key.get(b"keep".as_slice()).map(|v| v.as_slice()),
        Some(&b"original"[..]),
        "failed commit must roll back: state_upsert must NOT be visible"
    );
    assert_eq!(
        r.state_version, 1,
        "failed commit must roll back: state_version must NOT advance"
    );
    assert!(
        r.offsets.is_empty(),
        "failed commit must roll back: offsets must NOT be visible"
    );
}

// ----- DLQ Phase 3: reset (rewind primitive) --------------------------

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_reset_clears_state_and_replaces_offsets() {
    let (_c, store) = fresh_store().await;
    h::run_reset_clears_state_and_replaces_offsets(&store, "p").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_reset_leaves_other_pipelines_alone() {
    let (_c, store) = fresh_store().await;
    h::run_reset_leaves_other_pipelines_alone(&store, "p1", "p2").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_reset_of_unknown_pipeline_writes_offsets() {
    let (_c, store) = fresh_store().await;
    h::run_reset_of_unknown_pipeline_writes_offsets(&store, "p").await;
}
