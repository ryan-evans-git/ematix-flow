//! Shared `StateStore` contract test bodies.
//!
//! Each `run_*` function takes a `&S: StateStore` and asserts one
//! semantic property. The same bodies are exercised against
//! `InMemoryStateStore` (`tests/state_store_contract.rs`) and
//! `PostgresStateStore` via testcontainers
//! (`tests/integration_state_store_pg.rs`).
//!
//! Marked `#![allow(dead_code)]` because each integration-test crate
//! that includes this module via `mod state_store_helpers;` only uses
//! a subset of the helpers from its own perspective — Rust treats
//! each integration test as its own compilation unit.

#![allow(dead_code)]

use std::collections::HashMap;

use ematix_flow_core::state_store::{CommitSnapshot, RecoveredState, StateStore};

pub fn k(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

pub fn map<const N: usize>(pairs: [(&str, Vec<u8>); N]) -> HashMap<String, Vec<u8>> {
    pairs.into_iter().map(|(s, v)| (s.to_string(), v)).collect()
}

pub async fn run_unknown_pipeline_returns_empty<S: StateStore>(store: &S) {
    let recovered = store.load("never-seen").await.unwrap();
    assert_eq!(
        recovered,
        RecoveredState::default(),
        "load for an unseen pipeline must return an empty RecoveredState, \
         not an error and not stale data from another pipeline"
    );
}

pub async fn run_commit_then_load_roundtrips_state<S: StateStore>(store: &S, pipeline: &str) {
    let snap = CommitSnapshot {
        state_upserts: vec![
            (k("user-1"), b"blob-1".to_vec()),
            (k("user-2"), b"blob-2".to_vec()),
        ],
        state_deletes: Vec::new(),
        offsets: map([("kafka-orders", b"\x00\x01".to_vec())]),
        state_version: 1,
    };
    store.commit(pipeline, snap).await.unwrap();

    let r = store.load(pipeline).await.unwrap();
    assert_eq!(r.state_by_key.len(), 2);
    assert_eq!(
        r.state_by_key.get(&k("user-1")).map(|v| v.as_slice()),
        Some(&b"blob-1"[..])
    );
    assert_eq!(
        r.state_by_key.get(&k("user-2")).map(|v| v.as_slice()),
        Some(&b"blob-2"[..])
    );
    assert_eq!(
        r.offsets.get("kafka-orders").map(|v| v.as_slice()),
        Some(&[0, 1][..])
    );
    assert_eq!(r.state_version, 1);
}

pub async fn run_upsert_overwrites_existing_blob<S: StateStore>(store: &S, pipeline: &str) {
    store
        .commit(
            pipeline,
            CommitSnapshot {
                state_upserts: vec![(k("u"), b"v1".to_vec())],
                state_version: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    store
        .commit(
            pipeline,
            CommitSnapshot {
                state_upserts: vec![(k("u"), b"v2".to_vec())],
                state_version: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let r = store.load(pipeline).await.unwrap();
    assert_eq!(
        r.state_by_key.get(&k("u")).map(|v| v.as_slice()),
        Some(&b"v2"[..])
    );
}

pub async fn run_delete_removes_key<S: StateStore>(store: &S, pipeline: &str) {
    store
        .commit(
            pipeline,
            CommitSnapshot {
                state_upserts: vec![(k("a"), b"A".to_vec()), (k("b"), b"B".to_vec())],
                state_version: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    store
        .commit(
            pipeline,
            CommitSnapshot {
                state_deletes: vec![k("a")],
                state_version: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let r = store.load(pipeline).await.unwrap();
    assert!(
        !r.state_by_key.contains_key(&k("a")),
        "deleted key must be gone"
    );
    assert_eq!(
        r.state_by_key.get(&k("b")).map(|v| v.as_slice()),
        Some(&b"B"[..])
    );
}

pub async fn run_delete_of_unknown_key_is_ok<S: StateStore>(store: &S, pipeline: &str) {
    // Deleting a key that was never present is not an error — the
    // session machine may compute an evict-list that races with the
    // first emit. The store should absorb it idempotently.
    store
        .commit(
            pipeline,
            CommitSnapshot {
                state_deletes: vec![k("ghost")],
                state_version: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let r = store.load(pipeline).await.unwrap();
    assert!(r.state_by_key.is_empty());
}

pub async fn run_offsets_are_replaced_per_source<S: StateStore>(store: &S, pipeline: &str) {
    store
        .commit(
            pipeline,
            CommitSnapshot {
                offsets: map([("kafka-a", b"1".to_vec()), ("kafka-b", b"10".to_vec())]),
                state_version: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    store
        .commit(
            pipeline,
            CommitSnapshot {
                offsets: map([("kafka-a", b"2".to_vec())]),
                state_version: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let r = store.load(pipeline).await.unwrap();
    // kafka-a updated, kafka-b retained — absent source ids are not
    // wiped, so a per-source ticker can commit just its own offset.
    assert_eq!(
        r.offsets.get("kafka-a").map(|v| v.as_slice()),
        Some(&b"2"[..])
    );
    assert_eq!(
        r.offsets.get("kafka-b").map(|v| v.as_slice()),
        Some(&b"10"[..])
    );
}

pub async fn run_pipelines_are_isolated<S: StateStore>(
    store: &S,
    pipeline_a: &str,
    pipeline_b: &str,
) {
    store
        .commit(
            pipeline_a,
            CommitSnapshot {
                state_upserts: vec![(k("u"), b"P1".to_vec())],
                offsets: map([("s", b"1".to_vec())]),
                state_version: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    store
        .commit(
            pipeline_b,
            CommitSnapshot {
                state_upserts: vec![(k("u"), b"P2".to_vec())],
                offsets: map([("s", b"2".to_vec())]),
                state_version: 7,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let r1 = store.load(pipeline_a).await.unwrap();
    let r2 = store.load(pipeline_b).await.unwrap();
    assert_eq!(
        r1.state_by_key.get(&k("u")).map(|v| v.as_slice()),
        Some(&b"P1"[..])
    );
    assert_eq!(
        r2.state_by_key.get(&k("u")).map(|v| v.as_slice()),
        Some(&b"P2"[..])
    );
    assert_eq!(r1.offsets.get("s").map(|v| v.as_slice()), Some(&b"1"[..]));
    assert_eq!(r2.offsets.get("s").map(|v| v.as_slice()), Some(&b"2"[..]));
    assert_eq!(r1.state_version, 1);
    assert_eq!(r2.state_version, 7);
}

pub async fn run_state_version_persists_latest<S: StateStore>(store: &S, pipeline: &str) {
    store
        .commit(
            pipeline,
            CommitSnapshot {
                state_upserts: vec![(k("u"), b"x".to_vec())],
                state_version: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    store
        .commit(
            pipeline,
            CommitSnapshot {
                state_upserts: vec![(k("u"), b"y".to_vec())],
                state_version: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let r = store.load(pipeline).await.unwrap();
    assert_eq!(
        r.state_version, 2,
        "state_version must reflect the latest commit so the migration \
         chain on the next load knows where to start from"
    );
}

pub async fn run_empty_commit_is_noop<S: StateStore>(store: &S, pipeline: &str) {
    store
        .commit(
            pipeline,
            CommitSnapshot {
                state_upserts: vec![(k("u"), b"v".to_vec())],
                state_version: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    // Empty snapshot — no upserts, no deletes, no offsets. Should
    // succeed cleanly without wiping prior state.
    store
        .commit(
            pipeline,
            CommitSnapshot {
                state_version: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let r = store.load(pipeline).await.unwrap();
    assert_eq!(
        r.state_by_key.get(&k("u")).map(|v| v.as_slice()),
        Some(&b"v"[..])
    );
}
