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

// ----- DLQ Phase 2: replay engine over the Postgres table store ----------
//
// TDD note: committed red against the `run_dlq_replay` stub before
// the engine existed (same discipline as the Phase 1 contract
// suite).

use std::sync::Arc;

use ematix_flow_core::backend::{Backend, TargetTable};
use ematix_flow_core::dlq::{DlqSelection, ReplayOptions};
use ematix_flow_core::streaming::{StreamingPipeline, StreamingPipelineConfig};
use futures_util::TryStreamExt;

/// A transform that always fails — drives the poison-park path.
#[derive(Debug)]
struct AlwaysFailTransform;

#[async_trait::async_trait]
impl ematix_flow_core::transform::BatchTransform for AlwaysFailTransform {
    fn input_schema(&self) -> arrow_schema::SchemaRef {
        Arc::new(arrow_schema::Schema::empty())
    }
    fn output_schema(&self) -> arrow_schema::SchemaRef {
        self.input_schema()
    }
    async fn transform(
        &self,
        _input: arrow_array::RecordBatch,
        _ctx: &ematix_flow_core::transform::BatchContext,
    ) -> Result<Vec<arrow_array::RecordBatch>, ematix_flow_core::backend::BackendError> {
        Err(ematix_flow_core::backend::BackendError::Other(
            "replay poison: scripted transform failure".into(),
        ))
    }
}

/// A pipeline whose DLQ is the given store and whose (single)
/// target is a fresh in-proc SQLite database. The source backend is
/// never read during a replay (the DLQ selection IS the source).
fn mk_replay_pipeline(
    pipeline: &str,
    store: Arc<dyn DeadLetterStore>,
    transform: Option<Arc<dyn ematix_flow_core::transform::BatchTransform>>,
) -> (Arc<dyn Backend>, StreamingPipeline) {
    let source: Arc<dyn Backend> =
        Arc::new(ematix_flow_core::SQLiteBackend::open(":memory:").unwrap());
    let target: Arc<dyn Backend> =
        Arc::new(ematix_flow_core::SQLiteBackend::open(":memory:").unwrap());
    let table = TargetTable {
        schema: "main".into(),
        name: "events".into(),
    };
    let mut cfg = StreamingPipelineConfig::new("seed-src", table.clone(), pipeline)
        .with_dead_letter_store(store);
    if let Some(t) = transform {
        cfg = cfg.with_transform(t);
    }
    let p = StreamingPipeline::new(source, vec![(Arc::clone(&target), table)], cfg);
    (target, p)
}

async fn count_events(backend: &Arc<dyn Backend>) -> i64 {
    let stream = backend
        .read_arrow_stream("SELECT count(*) FROM events")
        .await
        .unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0)
}

/// PRD round trip on the Postgres store family: sink broken →
/// replay re-dead-letters with attempt+1 → fix sink → replay All →
/// rows present, DLQ drained, reports exact.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_replay_round_trip_after_sink_fix() {
    let (_c, store) = fresh_store().await;
    let store: Arc<dyn DeadLetterStore> = Arc::new(store);
    store
        .append(vec![
            h::mk_record("pgrt", 1),
            h::mk_record("pgrt", 2),
            h::mk_record("pgrt", 3),
        ])
        .await
        .unwrap();

    let (target, pipeline) = mk_replay_pipeline("pgrt", Arc::clone(&store), None);

    // Sink broken (`events` doesn't exist): everything fails again
    // and returns to the DLQ at attempt 2.
    let r1 = pipeline
        .run_dlq_replay(DlqSelection::All, ReplayOptions::default())
        .await
        .unwrap();
    assert_eq!(
        (r1.taken, r1.succeeded, r1.redeadlettered, r1.parked),
        (3, 0, 3, 0)
    );
    assert_eq!(store.depth("pgrt").await.unwrap().pending, 3);
    let pending = store.browse("pgrt", 0, 10, None).await.unwrap();
    assert!(
        pending.iter().all(|r| r.meta.attempt == 2),
        "redriven records carry attempt 2"
    );

    // Fix the sink; replay again.
    target
        .execute("CREATE TABLE events (id INTEGER, name TEXT)")
        .await
        .unwrap();
    let r2 = pipeline
        .run_dlq_replay(DlqSelection::All, ReplayOptions::default())
        .await
        .unwrap();
    assert_eq!(
        (r2.taken, r2.succeeded, r2.redeadlettered, r2.parked),
        (3, 3, 0, 0)
    );

    assert_eq!(count_events(&target).await, 3, "rows present at the target");
    assert_eq!(
        store.depth("pgrt").await.unwrap(),
        ematix_flow_core::dlq::DlqDepth::default(),
        "DLQ drained — replays acked"
    );
}

/// Poison-park on Postgres: attempt increments on every redrive and
/// the record parks (in place — table stores CAN express park) at
/// max_attempts; parked records are excluded from later takes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_replay_poison_parks_at_max_attempts() {
    let (_c, store) = fresh_store().await;
    let store: Arc<dyn DeadLetterStore> = Arc::new(store);
    store
        .append(vec![h::mk_record("pgpoison", 1)])
        .await
        .unwrap();

    let (target, pipeline) = mk_replay_pipeline(
        "pgpoison",
        Arc::clone(&store),
        Some(Arc::new(AlwaysFailTransform)),
    );
    target
        .execute("CREATE TABLE events (id INTEGER, name TEXT)")
        .await
        .unwrap();

    let r1 = pipeline
        .run_dlq_replay(DlqSelection::All, ReplayOptions::default())
        .await
        .unwrap();
    assert_eq!((r1.taken, r1.redeadlettered, r1.parked), (1, 1, 0));
    let after1 = store.browse("pgpoison", 0, 10, None).await.unwrap();
    assert_eq!(after1[0].meta.attempt, 2);

    let r2 = pipeline
        .run_dlq_replay(DlqSelection::All, ReplayOptions::default())
        .await
        .unwrap();
    assert_eq!((r2.taken, r2.redeadlettered, r2.parked), (1, 1, 0));
    let after2 = store.browse("pgpoison", 0, 10, None).await.unwrap();
    assert_eq!(after2[0].meta.attempt, 3);

    let r3 = pipeline
        .run_dlq_replay(DlqSelection::All, ReplayOptions::default())
        .await
        .unwrap();
    assert_eq!((r3.taken, r3.redeadlettered, r3.parked), (1, 0, 1));

    let depth = store.depth("pgpoison").await.unwrap();
    assert_eq!((depth.pending, depth.parked), (0, 1));
    let parked = store
        .browse(
            "pgpoison",
            0,
            10,
            Some(ematix_flow_core::dlq::DlqRecordStatus::Parked),
        )
        .await
        .unwrap();
    assert_eq!(parked.len(), 1);
    assert_eq!(parked[0].meta.attempt, 3, "parked at max_attempts");

    let r4 = pipeline
        .run_dlq_replay(DlqSelection::All, ReplayOptions::default())
        .await
        .unwrap();
    assert_eq!(r4.taken, 0, "parked records excluded from take");
    assert_eq!(
        count_events(&target).await,
        0,
        "poison never reached the sink"
    );
}

/// Two overlapping replays over the SAME Postgres DLQ (separate
/// handles → separate pools → real `FOR UPDATE SKIP LOCKED`
/// contention) must not double-process any record.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn pg_concurrent_replays_do_not_double_process() {
    let container = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("failed to start postgres testcontainer");
    let host = container.get_host().await.unwrap().to_string();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let store_a: Arc<dyn DeadLetterStore> =
        Arc::new(TableDlq::connect_postgres(&url, "public").await.unwrap());
    let store_b: Arc<dyn DeadLetterStore> =
        Arc::new(TableDlq::connect_postgres(&url, "public").await.unwrap());

    let records: Vec<_> = (1..=20).map(|n| h::mk_record("pgconc", n)).collect();
    store_a.append(records).await.unwrap();

    let (target_a, pa) = mk_replay_pipeline("pgconc", Arc::clone(&store_a), None);
    let (target_b, pb) = mk_replay_pipeline("pgconc", Arc::clone(&store_b), None);
    target_a
        .execute("CREATE TABLE events (id INTEGER, name TEXT)")
        .await
        .unwrap();
    target_b
        .execute("CREATE TABLE events (id INTEGER, name TEXT)")
        .await
        .unwrap();

    let (ra, rb) = tokio::join!(
        pa.run_dlq_replay(DlqSelection::All, ReplayOptions::default()),
        pb.run_dlq_replay(DlqSelection::All, ReplayOptions::default()),
    );
    let (ra, rb) = (ra.unwrap(), rb.unwrap());

    assert_eq!(
        ra.taken + rb.taken,
        20,
        "SKIP LOCKED leases are exclusive — no double-take"
    );
    assert_eq!(ra.succeeded + rb.succeeded, 20);
    let total = count_events(&target_a).await + count_events(&target_b).await;
    assert_eq!(total, 20, "every record written exactly once");
    assert_eq!(
        store_a.depth("pgconc").await.unwrap(),
        ematix_flow_core::dlq::DlqDepth::default()
    );
}
