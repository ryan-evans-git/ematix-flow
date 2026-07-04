//! Shared `DeadLetterStore` contract test bodies (DLQ Phase 1).
//!
//! Each `run_*` function takes a `&S: DeadLetterStore` and asserts
//! one semantic property. The same bodies are exercised against:
//!
//! - `TableDlq` (SQLite) — `tests/dlq_contract.rs`, always runs.
//! - `TableDlq` (Postgres) — `tests/integration_dlq_pg.rs`,
//!   testcontainers, `#[ignore]`.
//! - `KafkaTopicDlq` — the applicable subset in
//!   `tests/integration_pg.rs` (topic semantics can't express park /
//!   by-id purge; those return typed `Unsupported` errors asserted
//!   separately there).
//!
//! Marked `#![allow(dead_code)]` for the same reason as
//! `state_store_helpers`: each integration-test crate compiles this
//! module independently and only calls a subset.

#![allow(dead_code)]

use std::time::Duration;

use ematix_flow_core::dlq::{
    DeadLetterStore, DlqDepth, DlqMeta, DlqRecord, DlqRecordId, DlqRecordStatus, DlqSelection,
    DlqStage,
};

/// Fixed "clock" the suite passes in as `now_ms`. Nothing here
/// calls `SystemTime::now()` — lease expiry is exercised by moving
/// this value, per the house testability convention.
pub const T0_MS: i64 = 1_700_000_000_000;

/// Default lease used where the test isn't about expiry.
pub const LEASE: Duration = Duration::from_secs(60);

/// Deterministic record fixture. `n` orders records within a
/// pipeline (ids sort in append order, payloads differ per record).
pub fn mk_record(pipeline: &str, n: u32) -> DlqRecord {
    DlqRecord {
        id: DlqRecordId(format!("{pipeline}-rec-{n:04}")),
        meta: DlqMeta {
            pipeline: pipeline.to_string(),
            stage: DlqStage::Transform,
            error: format!("transform error {n}: division by zero"),
            source_id: "kafka-orders".to_string(),
            offset_bytes: Some(vec![1, n as u8]),
            event_ts: Some(1_690_000_000_000_000 + n as i64),
            failed_at: T0_MS - 1_000 + n as i64,
            attempt: 1,
            payload_format: "json".to_string(),
        },
        payload: format!("{{\"id\": {n}, \"name\": \"row-{n}\"}}").into_bytes(),
    }
}

pub fn ids(records: &[DlqRecord]) -> Vec<DlqRecordId> {
    records.iter().map(|r| r.id.clone()).collect()
}

async fn append_n<S: DeadLetterStore + ?Sized>(
    store: &S,
    pipeline: &str,
    n: u32,
) -> Vec<DlqRecord> {
    let records: Vec<DlqRecord> = (1..=n).map(|i| mk_record(pipeline, i)).collect();
    store.append(records.clone()).await.unwrap();
    records
}

// ---------------------------------------------------------------
// Empty-store edges
// ---------------------------------------------------------------

pub async fn run_empty_store_edges<S: DeadLetterStore + ?Sized>(store: &S, pipeline: &str) {
    assert_eq!(
        store.depth(pipeline).await.unwrap(),
        DlqDepth::default(),
        "empty store must report zero depth, not an error"
    );
    assert!(
        store
            .browse(pipeline, 0, 10, None)
            .await
            .unwrap()
            .is_empty(),
        "browse on an empty store returns an empty page"
    );
    assert!(
        store
            .take_for_replay(pipeline, DlqSelection::All, LEASE, T0_MS)
            .await
            .unwrap()
            .is_empty(),
        "take on an empty store returns nothing"
    );
    // Unknown-id ack/park are idempotent no-ops, not errors.
    let ghost = [DlqRecordId("ghost".into())];
    store.ack_replayed(pipeline, &ghost).await.unwrap();
    store.park(pipeline, &ghost).await.unwrap();
    assert_eq!(
        store.purge(pipeline, DlqSelection::All).await.unwrap(),
        0,
        "purge-all on an empty store deletes nothing"
    );
}

// ---------------------------------------------------------------
// append → depth
// ---------------------------------------------------------------

pub async fn run_append_then_depth<S: DeadLetterStore + ?Sized>(store: &S, pipeline: &str) {
    append_n(store, pipeline, 3).await;
    assert_eq!(
        store.depth(pipeline).await.unwrap(),
        DlqDepth {
            pending: 3,
            parked: 0
        },
        "3 appended records must show as pending depth"
    );
}

// ---------------------------------------------------------------
// browse: paging + ordering
// ---------------------------------------------------------------

pub async fn run_browse_pages_oldest_first<S: DeadLetterStore + ?Sized>(store: &S, pipeline: &str) {
    let records = append_n(store, pipeline, 5).await;

    let p0 = store.browse(pipeline, 0, 2, None).await.unwrap();
    let p1 = store.browse(pipeline, 1, 2, None).await.unwrap();
    let p2 = store.browse(pipeline, 2, 2, None).await.unwrap();
    let p3 = store.browse(pipeline, 3, 2, None).await.unwrap();

    assert_eq!(ids(&p0), ids(&records[0..2]), "page 0 = oldest two");
    assert_eq!(ids(&p1), ids(&records[2..4]), "page 1 = next two");
    assert_eq!(ids(&p2), ids(&records[4..5]), "page 2 = final partial");
    assert!(p3.is_empty(), "page past the end is empty, not an error");
}

// ---------------------------------------------------------------
// meta round-trip fidelity
// ---------------------------------------------------------------

pub async fn run_meta_round_trips<S: DeadLetterStore + ?Sized>(store: &S, pipeline: &str) {
    let mut record = mk_record(pipeline, 1);
    // Populate every optional field with distinctive values.
    record.meta.stage = DlqStage::Write;
    record.meta.offset_bytes = Some(vec![0xde, 0xad, 0xbe, 0xef]);
    record.meta.event_ts = Some(1_699_999_999_123_456);
    record.meta.attempt = 3;
    record.meta.payload_format = "raw_bytes".to_string();
    record.payload = vec![0x00, 0xff, 0x10, 0x7f]; // non-UTF8 payload
    store.append(vec![record.clone()]).await.unwrap();

    let got = store.browse(pipeline, 0, 10, None).await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0], record, "record must round-trip byte-identically");
}

pub async fn run_meta_none_fields_round_trip<S: DeadLetterStore + ?Sized>(
    store: &S,
    pipeline: &str,
) {
    let mut record = mk_record(pipeline, 1);
    record.meta.offset_bytes = None;
    record.meta.event_ts = None;
    store.append(vec![record.clone()]).await.unwrap();

    let got = store.browse(pipeline, 0, 10, None).await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(
        got[0], record,
        "None offset_bytes/event_ts must stay None, not become empty values"
    );
}

// ---------------------------------------------------------------
// take_for_replay: lease exclusivity + expiry + selections
// ---------------------------------------------------------------

pub async fn run_take_leases_exclusively<S: DeadLetterStore + ?Sized>(store: &S, pipeline: &str) {
    let records = append_n(store, pipeline, 3).await;

    let first = store
        .take_for_replay(pipeline, DlqSelection::FirstN(2), LEASE, T0_MS)
        .await
        .unwrap();
    assert_eq!(
        ids(&first),
        ids(&records[0..2]),
        "FirstN takes the oldest records in append order"
    );

    // A concurrent take within the lease window must NOT see the
    // two leased records.
    let second = store
        .take_for_replay(pipeline, DlqSelection::All, LEASE, T0_MS)
        .await
        .unwrap();
    assert_eq!(
        ids(&second),
        ids(&records[2..3]),
        "leased records are invisible to a second take"
    );

    let third = store
        .take_for_replay(pipeline, DlqSelection::All, LEASE, T0_MS + 1)
        .await
        .unwrap();
    assert!(third.is_empty(), "everything is leased — nothing to take");

    // Leased records still count toward pending depth (custody is
    // not resolution) and remain browsable.
    assert_eq!(
        store.depth(pipeline).await.unwrap(),
        DlqDepth {
            pending: 3,
            parked: 0
        }
    );
}

pub async fn run_lease_expiry_releases<S: DeadLetterStore + ?Sized>(store: &S, pipeline: &str) {
    let records = append_n(store, pipeline, 2).await;
    let lease = Duration::from_secs(10);

    let taken = store
        .take_for_replay(pipeline, DlqSelection::All, lease, T0_MS)
        .await
        .unwrap();
    assert_eq!(taken.len(), 2);

    // Before expiry (lease ends at T0 + 10_000).
    let mid = store
        .take_for_replay(pipeline, DlqSelection::All, lease, T0_MS + 9_999)
        .await
        .unwrap();
    assert!(mid.is_empty(), "lease still held at expiry - 1ms");

    // At/after expiry the records are re-takeable.
    let after = store
        .take_for_replay(pipeline, DlqSelection::All, lease, T0_MS + 10_000)
        .await
        .unwrap();
    assert_eq!(
        ids(&after),
        ids(&records),
        "expired leases release the records for re-take (crash-safe replay)"
    );
}

pub async fn run_take_by_ids<S: DeadLetterStore + ?Sized>(store: &S, pipeline: &str) {
    let records = append_n(store, pipeline, 3).await;

    let taken = store
        .take_for_replay(
            pipeline,
            DlqSelection::Ids(vec![records[1].id.clone(), DlqRecordId("ghost".into())]),
            LEASE,
            T0_MS,
        )
        .await
        .unwrap();
    assert_eq!(
        ids(&taken),
        vec![records[1].id.clone()],
        "Ids selection takes exactly the named (known) records; unknown ids skip silently"
    );

    let rest = store
        .take_for_replay(pipeline, DlqSelection::All, LEASE, T0_MS)
        .await
        .unwrap();
    assert_eq!(
        ids(&rest),
        vec![records[0].id.clone(), records[2].id.clone()],
        "non-selected records stay takeable"
    );
}

// ---------------------------------------------------------------
// ack_replayed removes
// ---------------------------------------------------------------

pub async fn run_ack_replayed_removes<S: DeadLetterStore + ?Sized>(store: &S, pipeline: &str) {
    let records = append_n(store, pipeline, 2).await;
    let taken = store
        .take_for_replay(pipeline, DlqSelection::All, LEASE, T0_MS)
        .await
        .unwrap();
    assert_eq!(taken.len(), 2);

    store.ack_replayed(pipeline, &ids(&taken)).await.unwrap();

    assert_eq!(
        store.depth(pipeline).await.unwrap(),
        DlqDepth::default(),
        "acked records leave the store entirely"
    );
    assert!(
        store
            .browse(pipeline, 0, 10, None)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .take_for_replay(pipeline, DlqSelection::All, LEASE, T0_MS + 999_999)
            .await
            .unwrap()
            .is_empty(),
        "acked records never come back, even after the old lease horizon"
    );
    let _ = records;
}

// ---------------------------------------------------------------
// park transitions
// ---------------------------------------------------------------

pub async fn run_park_excludes_from_take<S: DeadLetterStore + ?Sized>(store: &S, pipeline: &str) {
    let records = append_n(store, pipeline, 3).await;

    store
        .park(pipeline, &[records[0].id.clone()])
        .await
        .unwrap();

    assert_eq!(
        store.depth(pipeline).await.unwrap(),
        DlqDepth {
            pending: 2,
            parked: 1
        },
        "parked records move from pending to parked depth"
    );

    let taken = store
        .take_for_replay(pipeline, DlqSelection::All, LEASE, T0_MS)
        .await
        .unwrap();
    assert_eq!(
        ids(&taken),
        ids(&records[1..3]),
        "parked records are excluded from take_for_replay"
    );

    // Parked records remain visible via the status filter.
    let parked = store
        .browse(pipeline, 0, 10, Some(DlqRecordStatus::Parked))
        .await
        .unwrap();
    assert_eq!(ids(&parked), vec![records[0].id.clone()]);

    // Explicitly taking a parked record by id is also a no-op —
    // parked is terminal until purge.
    let by_id = store
        .take_for_replay(
            pipeline,
            DlqSelection::Ids(vec![records[0].id.clone()]),
            LEASE,
            T0_MS + 999_999,
        )
        .await
        .unwrap();
    assert!(by_id.is_empty(), "parked records can't be taken by id");
}

pub async fn run_park_leased_record<S: DeadLetterStore + ?Sized>(store: &S, pipeline: &str) {
    // The Phase 2 poison-guard path: take → replay fails at
    // max_attempts → park while the lease is held.
    let records = append_n(store, pipeline, 1).await;
    let taken = store
        .take_for_replay(pipeline, DlqSelection::All, LEASE, T0_MS)
        .await
        .unwrap();
    assert_eq!(taken.len(), 1);

    store.park(pipeline, &ids(&taken)).await.unwrap();
    assert_eq!(
        store.depth(pipeline).await.unwrap(),
        DlqDepth {
            pending: 0,
            parked: 1
        }
    );
    // Even after the lease would have expired, parked wins.
    let after = store
        .take_for_replay(pipeline, DlqSelection::All, LEASE, T0_MS + 999_999)
        .await
        .unwrap();
    assert!(after.is_empty(), "parked outlives lease expiry");
    let _ = records;
}

// ---------------------------------------------------------------
// purge
// ---------------------------------------------------------------

pub async fn run_purge_ids<S: DeadLetterStore + ?Sized>(store: &S, pipeline: &str) {
    let records = append_n(store, pipeline, 3).await;
    let n = store
        .purge(
            pipeline,
            DlqSelection::Ids(vec![records[1].id.clone(), DlqRecordId("ghost".into())]),
        )
        .await
        .unwrap();
    assert_eq!(n, 1, "one known id purged; ghost id skipped");
    let left = store.browse(pipeline, 0, 10, None).await.unwrap();
    assert_eq!(
        ids(&left),
        vec![records[0].id.clone(), records[2].id.clone()]
    );
}

pub async fn run_purge_first_n<S: DeadLetterStore + ?Sized>(store: &S, pipeline: &str) {
    let records = append_n(store, pipeline, 3).await;
    let n = store
        .purge(pipeline, DlqSelection::FirstN(2))
        .await
        .unwrap();
    assert_eq!(n, 2);
    let left = store.browse(pipeline, 0, 10, None).await.unwrap();
    assert_eq!(
        ids(&left),
        vec![records[2].id.clone()],
        "FirstN purges the oldest records"
    );
}

pub async fn run_purge_all_includes_parked<S: DeadLetterStore + ?Sized>(store: &S, pipeline: &str) {
    let records = append_n(store, pipeline, 3).await;
    store
        .park(pipeline, &[records[0].id.clone()])
        .await
        .unwrap();

    let n = store.purge(pipeline, DlqSelection::All).await.unwrap();
    assert_eq!(n, 3, "purge-all deletes pending AND parked records");
    assert_eq!(store.depth(pipeline).await.unwrap(), DlqDepth::default());
}

// ---------------------------------------------------------------
// pipeline isolation (table stores only — a KafkaTopicDlq is bound
// to a single pipeline's topic by construction)
// ---------------------------------------------------------------

pub async fn run_pipelines_isolated<S: DeadLetterStore + ?Sized>(
    store: &S,
    pipeline_a: &str,
    pipeline_b: &str,
) {
    store.append(vec![mk_record(pipeline_a, 1)]).await.unwrap();
    store
        .append(vec![mk_record(pipeline_b, 1), mk_record(pipeline_b, 2)])
        .await
        .unwrap();

    assert_eq!(store.depth(pipeline_a).await.unwrap().pending, 1);
    assert_eq!(store.depth(pipeline_b).await.unwrap().pending, 2);

    // Taking + purging pipeline B must not disturb pipeline A.
    let taken = store
        .take_for_replay(pipeline_b, DlqSelection::All, LEASE, T0_MS)
        .await
        .unwrap();
    assert_eq!(taken.len(), 2);
    store.purge(pipeline_b, DlqSelection::All).await.unwrap();

    assert_eq!(store.depth(pipeline_a).await.unwrap().pending, 1);
    let a = store.browse(pipeline_a, 0, 10, None).await.unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].meta.pipeline, pipeline_a);
}

// ---------------------------------------------------------------
// error truncation is enforced at the store boundary too
// ---------------------------------------------------------------

pub async fn run_error_truncated_to_8kb<S: DeadLetterStore + ?Sized>(store: &S, pipeline: &str) {
    let mut record = mk_record(pipeline, 1);
    record.meta.error = "e".repeat(ematix_flow_core::dlq::MAX_ERROR_BYTES + 4096);
    store.append(vec![record]).await.unwrap();

    let got = store.browse(pipeline, 0, 10, None).await.unwrap();
    assert_eq!(got.len(), 1);
    assert!(
        got[0].meta.error.len() <= ematix_flow_core::dlq::MAX_ERROR_BYTES,
        "stores defensively truncate error strings to MAX_ERROR_BYTES (got {})",
        got[0].meta.error.len()
    );
}
