//! DLQ Phase 4: ops-layer tests over a live (SQLite-family) store.
//!
//! Builds an operations-only pipeline from TOML with
//! `dlq_store = "table"`, seeds records through the SAME store
//! `resolve_dlq_store` resolves, and drives the ops surface the
//! HTTP API layer calls through pyo3. No Docker needed — the
//! sqlite family covers the live-store exit criterion; the topic
//! store's semantics are pinned by the Phase 1/2 broker-gated
//! suites.
//!
//! TDD note: written FIRST, red, against `dlq_ops` stubs that
//! return "unimplemented" errors — same discipline as the earlier
//! phases.

use ematix_flow_cli::{PipelineCliConfig, dlq_ops};
use ematix_flow_core::SQLiteBackend;
use ematix_flow_core::backend::Backend;
use ematix_flow_core::dlq::{DlqMeta, DlqRecord, DlqRecordId, DlqSelection, DlqStage};
use ematix_flow_core::streaming::{RewindTarget, StreamingPipeline};
use futures_util::TryStreamExt;

fn ops_toml(sqlite_path: &str) -> String {
    // Kafka source (constructing a KafkaBackend never contacts the
    // broker — pinned by the Phase 1 resolution tests) + sqlite
    // target. `dlq_store = "table"` with no state store resolves
    // the in-process sqlite fallback, which is exactly what these
    // live-store tests want: seed + ops hit the SAME store handle.
    format!(
        r#"
        pipeline_name = "dlq-ops-p"
        source_query = "events"
        dlq_store = "table"
        dlq_max_attempts = 2

        [source]
        kind = "kafka"
        bootstrap_servers = "localhost:9092"
        group_id = "dlq-ops-g"

        [target]
        kind = "sqlite"
        path = "{sqlite_path}"

        [target.table]
        schema = "main"
        name = "events"
    "#
    )
}

async fn ops_pipeline(sqlite_path: &str) -> StreamingPipeline {
    let cfg = PipelineCliConfig::from_toml_str(&ops_toml(sqlite_path)).expect("parse toml");
    dlq_ops::build_ops_pipeline(&cfg)
        .await
        .expect("build ops pipeline")
}

/// A record shaped like the emission path writes for a non-Kafka
/// source: JSONL payload, `payload_format = "json"`.
fn seeded_record(n: u32, stage: DlqStage, failed_at: i64, attempt: u32) -> DlqRecord {
    DlqRecord {
        id: DlqRecordId(format!("ops-seed-{n:04}")),
        meta: DlqMeta {
            pipeline: "dlq-ops-p".into(),
            stage,
            error: format!("seeded failure {n}"),
            source_id: "events".into(),
            offset_bytes: None,
            event_ts: None,
            failed_at,
            attempt,
            payload_format: "json".into(),
        },
        payload: format!("{{\"v\": {n}}}").into_bytes(),
    }
}

async fn seed(pipeline: &StreamingPipeline, records: Vec<DlqRecord>) {
    pipeline
        .resolve_dlq_store()
        .await
        .expect("resolve store")
        .append(records)
        .await
        .expect("seed append");
}

#[tokio::test(flavor = "multi_thread")]
async fn ops_pipeline_resolves_table_store() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ops.sqlite");
    let pipeline = ops_pipeline(path.to_str().unwrap()).await;
    let store = pipeline.resolve_dlq_store().await.unwrap();
    let debug = format!("{store:?}");
    assert!(debug.contains("TableDlq"), "resolved: {debug}");
    let stats = dlq_ops::dlq_stats(&pipeline, 1_000).await.unwrap();
    assert_eq!((stats.pending, stats.parked), (0, 0));
}

#[tokio::test(flavor = "multi_thread")]
async fn stats_report_stage_breakdown_and_arrivals() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ops.sqlite");
    let pipeline = ops_pipeline(path.to_str().unwrap()).await;
    let now_ms: i64 = 3_600_000; // t = 1h, so buckets are easy to place
    seed(
        &pipeline,
        vec![
            // 2 transform failures inside the last minute.
            seeded_record(1, DlqStage::Transform, now_ms - 10_000, 1),
            seeded_record(2, DlqStage::Transform, now_ms - 50_000, 1),
            // 1 write failure ~4 minutes ago (5m bucket, not 1m).
            seeded_record(3, DlqStage::Write, now_ms - 240_000, 1),
            // 1 late-data failure ~30 minutes ago (60m bucket).
            seeded_record(4, DlqStage::LateData, now_ms - 1_800_000, 1),
        ],
    )
    .await;

    let stats = dlq_ops::dlq_stats(&pipeline, now_ms).await.unwrap();
    assert_eq!(stats.pending, 4);
    assert_eq!(stats.parked, 0);
    assert_eq!(stats.by_stage.get("transform"), Some(&2));
    assert_eq!(stats.by_stage.get("write"), Some(&1));
    assert_eq!(stats.by_stage.get("late_data"), Some(&1));
    assert_eq!(stats.arrivals_1m, 2);
    assert_eq!(stats.arrivals_5m, 3);
    assert_eq!(stats.arrivals_15m, 3);
    assert_eq!(stats.arrivals_60m, 4);
    assert_eq!(stats.scanned, 4);
    assert!(!stats.truncated);
}

#[tokio::test(flavor = "multi_thread")]
async fn records_page_oldest_first_with_status_filter() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ops.sqlite");
    let pipeline = ops_pipeline(path.to_str().unwrap()).await;
    seed(
        &pipeline,
        (1..=5)
            .map(|n| seeded_record(n, DlqStage::Write, 1_000 + n as i64, 1))
            .collect(),
    )
    .await;

    let p0 = dlq_ops::dlq_records(&pipeline, None, 0, 2).await.unwrap();
    let p1 = dlq_ops::dlq_records(&pipeline, None, 1, 2).await.unwrap();
    let p2 = dlq_ops::dlq_records(&pipeline, None, 2, 2).await.unwrap();
    let p3 = dlq_ops::dlq_records(&pipeline, None, 3, 2).await.unwrap();
    assert_eq!(
        (p0.len(), p1.len(), p2.len(), p3.len()),
        (2, 2, 1, 0),
        "pages split 2/2/1 then empty"
    );
    assert_eq!(p0[0].id.0, "ops-seed-0001", "oldest first");

    // Park two, then filter by status.
    dlq_ops::dlq_park(&pipeline, DlqSelection::FirstN(2))
        .await
        .unwrap();
    let parked = dlq_ops::dlq_records(&pipeline, Some("parked"), 0, 10)
        .await
        .unwrap();
    assert_eq!(parked.len(), 2);
    let pending = dlq_ops::dlq_records(&pipeline, Some("pending"), 0, 10)
        .await
        .unwrap();
    assert_eq!(pending.len(), 3);

    // Unknown status strings are typed config errors, not empty
    // pages.
    let err = dlq_ops::dlq_records(&pipeline, Some("bogus"), 0, 10)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("status"), "typed error: {err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn record_by_id_finds_and_misses() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ops.sqlite");
    let pipeline = ops_pipeline(path.to_str().unwrap()).await;
    seed(
        &pipeline,
        vec![seeded_record(1, DlqStage::Transform, 1_000, 1)],
    )
    .await;

    let hit = dlq_ops::dlq_record_by_id(&pipeline, "ops-seed-0001")
        .await
        .unwrap()
        .expect("record found");
    assert_eq!(hit.payload, b"{\"v\": 1}");
    let miss = dlq_ops::dlq_record_by_id(&pipeline, "nope").await.unwrap();
    assert!(miss.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn park_and_purge_by_selection() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ops.sqlite");
    let pipeline = ops_pipeline(path.to_str().unwrap()).await;
    seed(
        &pipeline,
        (1..=4)
            .map(|n| seeded_record(n, DlqStage::Write, 1_000 + n as i64, 1))
            .collect(),
    )
    .await;

    let parked = dlq_ops::dlq_park(&pipeline, DlqSelection::FirstN(2))
        .await
        .unwrap();
    assert_eq!(parked, 2);
    let stats = dlq_ops::dlq_stats(&pipeline, 10_000).await.unwrap();
    assert_eq!((stats.pending, stats.parked), (2, 2));

    // Purge by explicit id, then everything.
    let purged = dlq_ops::dlq_purge(
        &pipeline,
        DlqSelection::Ids(vec![DlqRecordId("ops-seed-0003".into())]),
    )
    .await
    .unwrap();
    assert_eq!(purged, 1);
    let purged = dlq_ops::dlq_purge(&pipeline, DlqSelection::All)
        .await
        .unwrap();
    assert_eq!(purged, 3, "parked records purge too");
    let stats = dlq_ops::dlq_stats(&pipeline, 10_000).await.unwrap();
    assert_eq!((stats.pending, stats.parked), (0, 0));
}

/// Full replay through the ops layer: the seeded JSON record
/// redrives through the pipeline's own target and lands in the
/// sqlite table; the DLQ drains.
#[tokio::test(flavor = "multi_thread")]
async fn replay_redrives_into_live_sqlite_target() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ops.sqlite");
    let path_str = path.to_str().unwrap();
    // Pre-create the target table so the redrive write succeeds.
    let setup = SQLiteBackend::open(path_str).unwrap();
    setup
        .execute("CREATE TABLE events (v BIGINT)")
        .await
        .unwrap();

    let pipeline = ops_pipeline(path_str).await;
    seed(&pipeline, vec![seeded_record(7, DlqStage::Write, 1_000, 1)]).await;

    let report = dlq_ops::dlq_replay(&pipeline, DlqSelection::All, None)
        .await
        .unwrap();
    assert_eq!(
        (
            report.taken,
            report.succeeded,
            report.redeadlettered,
            report.parked
        ),
        (1, 1, 0, 0)
    );

    let probe = SQLiteBackend::open(path_str).unwrap();
    let stream = probe
        .read_arrow_stream("SELECT count(*) AS n FROM events")
        .await
        .unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 1, "replayed row present at the target");

    let stats = dlq_ops::dlq_stats(&pipeline, 10_000).await.unwrap();
    assert_eq!((stats.pending, stats.parked), (0, 0), "DLQ drained");
}

/// `max_attempts = None` takes the TOML `dlq_max_attempts` (2
/// here): a record already at attempt 2 that fails again parks
/// instead of re-dead-lettering.
#[tokio::test(flavor = "multi_thread")]
async fn replay_defaults_max_attempts_from_config() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ops.sqlite");
    // NO events table → the redrive write fails deterministically.
    let pipeline = ops_pipeline(path.to_str().unwrap()).await;
    seed(&pipeline, vec![seeded_record(9, DlqStage::Write, 1_000, 2)]).await;

    let report = dlq_ops::dlq_replay(&pipeline, DlqSelection::All, None)
        .await
        .unwrap();
    assert_eq!(
        (report.taken, report.parked),
        (1, 1),
        "attempt 2 + failure exceeds the configured budget of 2 → parked"
    );

    let stats = dlq_ops::dlq_stats(&pipeline, 10_000).await.unwrap();
    assert_eq!((stats.pending, stats.parked), (0, 1));
}

/// The rewind plumbing propagates typed core errors: garbage
/// offset bytes fail the Kafka offset decode (the seek_to /
/// confirm_state_reset gates themselves are pinned by the core
/// Phase 3 suite).
#[tokio::test(flavor = "multi_thread")]
async fn rewind_surfaces_typed_offset_decode_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ops.sqlite");
    let pipeline = ops_pipeline(path.to_str().unwrap()).await;
    let err = dlq_ops::rewind(&pipeline, RewindTarget::Offset(vec![0]), false)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("offset decode"),
        "typed error names the decode failure: {err}"
    );
}

#[test]
fn selection_json_round_trips() {
    assert_eq!(
        dlq_ops::parse_selection(r#"{"kind":"all"}"#).unwrap(),
        DlqSelection::All
    );
    assert_eq!(
        dlq_ops::parse_selection(r#"{"kind":"first_n","n":5}"#).unwrap(),
        DlqSelection::FirstN(5)
    );
    assert_eq!(
        dlq_ops::parse_selection(r#"{"kind":"ids","ids":["a","b"]}"#).unwrap(),
        DlqSelection::Ids(vec![DlqRecordId("a".into()), DlqRecordId("b".into())])
    );
    let err = dlq_ops::parse_selection(r#"{"kind":"everything"}"#).unwrap_err();
    assert!(err.to_string().contains("selection"), "typed: {err}");
    // FirstN without n / Ids without ids are malformed, not
    // defaulted.
    assert!(dlq_ops::parse_selection(r#"{"kind":"first_n"}"#).is_err());
    assert!(dlq_ops::parse_selection(r#"{"kind":"ids"}"#).is_err());
}

#[test]
fn rewind_target_json_round_trips() {
    assert_eq!(
        dlq_ops::parse_rewind_target(r#"{"kind":"timestamp","ms":1700000000000}"#).unwrap(),
        RewindTarget::Timestamp(1_700_000_000_000)
    );
    assert_eq!(
        dlq_ops::parse_rewind_target(r#"{"kind":"offset","bytes":[1,2,3]}"#).unwrap(),
        RewindTarget::Offset(vec![1, 2, 3])
    );
    let err = dlq_ops::parse_rewind_target(r#"{"kind":"yesterday"}"#).unwrap_err();
    assert!(err.to_string().contains("rewind"), "typed: {err}");
    assert!(dlq_ops::parse_rewind_target(r#"{"kind":"timestamp"}"#).is_err());
}
