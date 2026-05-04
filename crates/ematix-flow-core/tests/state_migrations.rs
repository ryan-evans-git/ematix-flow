//! Phase 39.5a PR 1: forward-only state-blob migration chain.
//!
//! The session/join layer (PR 2 / 39.5b) registers a migrator per
//! version step; on `load`, blobs at older versions walk the chain
//! up to the current version. PR 1 ships only the infrastructure.

use ematix_flow_core::state_store::migrations::{MigrationChain, MigrationError};

#[test]
fn migrate_with_equal_versions_is_identity() {
    let chain: MigrationChain = MigrationChain::new();
    let out = chain.migrate(b"untouched", 3, 3).unwrap();
    assert_eq!(out, b"untouched");
}

#[test]
fn migrate_walks_chain_in_order() {
    let chain = MigrationChain::new()
        .add(1, |blob| Ok([blob, b"->v2"].concat()))
        .add(2, |blob| Ok([blob, b"->v3"].concat()));
    let out = chain.migrate(b"v1", 1, 3).unwrap();
    assert_eq!(out, b"v1->v2->v3");
}

#[test]
fn migrate_partial_walks_only_required_steps() {
    let chain = MigrationChain::new()
        .add(1, |blob| Ok([blob, b"->v2"].concat()))
        .add(2, |blob| Ok([blob, b"->v3"].concat()));
    // 2 -> 3 should run only the second step.
    let out = chain.migrate(b"v2", 2, 3).unwrap();
    assert_eq!(out, b"v2->v3");
}

#[test]
fn migrate_rejects_backward() {
    let chain = MigrationChain::new();
    let err = chain.migrate(b"x", 5, 3).unwrap_err();
    assert!(
        matches!(err, MigrationError::Backward { from: 5, to: 3 }),
        "got {err:?}"
    );
}

#[test]
fn migrate_returns_unsupported_when_step_missing() {
    let chain = MigrationChain::new().add(1, |blob| Ok(blob.to_vec()));
    // No migrator registered for 2 -> 3, so going 1 -> 3 must fail
    // at the second hop.
    let err = chain.migrate(b"x", 1, 3).unwrap_err();
    assert!(
        matches!(err, MigrationError::Unsupported { at: 2 }),
        "got {err:?}"
    );
}

#[test]
fn migrator_failure_propagates_with_step_index() {
    let chain = MigrationChain::new()
        .add(1, |blob| Ok(blob.to_vec()))
        .add(2, |_blob| Err("v2 schema cannot be derived from v1".into()));
    let err = chain.migrate(b"x", 1, 3).unwrap_err();
    match err {
        MigrationError::Failed { at, reason } => {
            assert_eq!(at, 2);
            assert!(reason.contains("v2 schema"), "reason: {reason}");
        }
        other => panic!("expected Failed, got {other:?}"),
    }
}

#[test]
fn duplicate_registration_panics() {
    // Two migrators for the same `from` version is a programming
    // error — registering the chain is a one-time setup; let it
    // fail loudly so the conflict surfaces in tests rather than
    // shadow-shipping a silently-overwritten step.
    let result = std::panic::catch_unwind(|| {
        MigrationChain::new()
            .add(1, |blob| Ok(blob.to_vec()))
            .add(1, |blob| Ok(blob.to_vec()))
    });
    assert!(result.is_err(), "duplicate registration must panic");
}
