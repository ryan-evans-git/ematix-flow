//! DLQ Phase 1: end-to-end Postgres `TableDlq` tests.
//!
//! Same contract suite as `dlq_contract.rs` (bodies in
//! `tests/dlq_helpers/mod.rs`), executed against a real Postgres via
//! testcontainers. Marked `#[ignore]` so `cargo test` stays fast by
//! default. Run with:
//!
//! ```bash
//! cargo test -p ematix-flow-core --test integration_dlq_pg -- --ignored
//! ```

mod dlq_helpers;

use dlq_helpers as h;
use ematix_flow_core::dlq::{DeadLetterStore, TableDlq};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

async fn fresh_store() -> (testcontainers::ContainerAsync<Postgres>, TableDlq) {
    let container = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("failed to start postgres testcontainer");
    let host = container.get_host().await.unwrap().to_string();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let store = TableDlq::connect_postgres(&url, "public")
        .await
        .expect("TableDlq::connect_postgres failed");
    (container, store)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_empty_store_edges() {
    let (_c, store) = fresh_store().await;
    h::run_empty_store_edges(&store, "p").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_append_then_depth() {
    let (_c, store) = fresh_store().await;
    h::run_append_then_depth(&store, "p").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_browse_pages_oldest_first() {
    let (_c, store) = fresh_store().await;
    h::run_browse_pages_oldest_first(&store, "p").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_meta_round_trips() {
    let (_c, store) = fresh_store().await;
    h::run_meta_round_trips(&store, "p").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_meta_none_fields_round_trip() {
    let (_c, store) = fresh_store().await;
    h::run_meta_none_fields_round_trip(&store, "p").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_take_leases_exclusively() {
    let (_c, store) = fresh_store().await;
    h::run_take_leases_exclusively(&store, "p").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_lease_expiry_releases() {
    let (_c, store) = fresh_store().await;
    h::run_lease_expiry_releases(&store, "p").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_take_by_ids() {
    let (_c, store) = fresh_store().await;
    h::run_take_by_ids(&store, "p").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_ack_replayed_removes() {
    let (_c, store) = fresh_store().await;
    h::run_ack_replayed_removes(&store, "p").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_park_excludes_from_take() {
    let (_c, store) = fresh_store().await;
    h::run_park_excludes_from_take(&store, "p").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_park_leased_record() {
    let (_c, store) = fresh_store().await;
    h::run_park_leased_record(&store, "p").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_purge_ids() {
    let (_c, store) = fresh_store().await;
    h::run_purge_ids(&store, "p").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_purge_first_n() {
    let (_c, store) = fresh_store().await;
    h::run_purge_first_n(&store, "p").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_purge_all_includes_parked() {
    let (_c, store) = fresh_store().await;
    h::run_purge_all_includes_parked(&store, "p").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_pipelines_isolated() {
    let (_c, store) = fresh_store().await;
    h::run_pipelines_isolated(&store, "p1", "p2").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_error_truncated_to_8kb() {
    let (_c, store) = fresh_store().await;
    h::run_error_truncated_to_8kb(&store, "p").await;
}

/// Postgres-specific: two `TableDlq` handles over the same database
/// see each other's records — the multi-process operator story (UI
/// browsing while the pipeline appends).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_two_handles_share_state() {
    let container = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("failed to start postgres testcontainer");
    let host = container.get_host().await.unwrap().to_string();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let writer = TableDlq::connect_postgres(&url, "public").await.unwrap();
    let reader = TableDlq::connect_postgres(&url, "public").await.unwrap();

    writer
        .append(vec![h::mk_record("shared", 1)])
        .await
        .unwrap();
    assert_eq!(reader.depth("shared").await.unwrap().pending, 1);
    let got = reader.browse("shared", 0, 10, None).await.unwrap();
    assert_eq!(got[0], h::mk_record("shared", 1));
}

/// Postgres-specific: `PostgresStateStore::dead_letter_store()`
/// hands back a TableDlq riding the SAME pool — the "configured
/// state store family" resolution path the streaming pipeline uses.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_state_store_provides_dlq_on_same_family() {
    use ematix_flow_core::state_store::{PostgresStateStore, StateStore};

    let container = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("failed to start postgres testcontainer");
    let host = container.get_host().await.unwrap().to_string();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let state_store = PostgresStateStore::connect(&url, "public").await.unwrap();
    state_store.ensure_schema().await.unwrap();

    let dlq = state_store
        .dead_letter_store()
        .await
        .unwrap()
        .expect("PostgresStateStore must provide a table DLQ");
    h::run_append_then_depth(dlq.as_ref(), "family-p").await;

    // And an independent handle over the same DB sees the rows —
    // proving the DLQ landed in the shared `ematix_dlq_records`
    // table, not some pool-local state.
    let direct = TableDlq::connect_postgres(&url, "public").await.unwrap();
    assert_eq!(direct.depth("family-p").await.unwrap().pending, 3);
}
