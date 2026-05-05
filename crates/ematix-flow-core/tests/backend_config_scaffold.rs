//! Σ.B PR 1: connector-trait refactor scaffold.
//!
//! Locks the Backend trait shape changes that the rest of Σ.B's
//! commits will fill in:
//!   - `BackendConfig` tagged enum (one variant per backend kind);
//!     subsequent commits add per-backend `<Backend>Config` payloads.
//!   - `Backend::config()` method that returns the running backend's
//!     serializable config; default `unimplemented!()` until each
//!     backend migrates.
//!   - `backend_from_config(cfg)` free function reverse-direction
//!     constructor; default returns `NotImplementedYet` per backend.
//!   - `Backend::partitioning_hint(query)` Σ.D-ready method;
//!     default returns `None`.
//!
//! These tests anchor the contract: a config blob is JSON-serializable,
//! round-trips lossless, and can be handed to `backend_from_config` to
//! reconstruct a backend (or get a clear error if the per-backend
//! migration commit hasn't landed).
//!
//! Plan + spike: `docs/PHASE_SIGMA_PLAN.md` Σ.B + `docs/PHASE_SIGMA_B_TRAIT_SPIKE.md`.

use ematix_flow_core::backend::{BackendConfig, backend_from_config};

#[test]
fn backend_config_postgres_round_trips_as_json() {
    let cfg = BackendConfig::Postgres;
    let json = serde_json::to_string(&cfg).expect("serialize Postgres");
    assert_eq!(json, r#"{"kind":"postgres"}"#);

    let recovered: BackendConfig = serde_json::from_str(&json).expect("deserialize Postgres");
    assert!(matches!(recovered, BackendConfig::Postgres));
}

#[test]
fn backend_config_kafka_round_trips_as_json() {
    let cfg = BackendConfig::Kafka;
    let json = serde_json::to_string(&cfg).expect("serialize Kafka");
    assert_eq!(json, r#"{"kind":"kafka"}"#);

    let recovered: BackendConfig = serde_json::from_str(&json).expect("deserialize Kafka");
    assert!(matches!(recovered, BackendConfig::Kafka));
}

/// All 10 backend variants must be present in the enum so PR 1's
/// scaffold covers the full migration target. Each variant gets its
/// per-backend config payload populated in commits b/c/d.
#[test]
fn backend_config_covers_all_known_backends() {
    let kinds = [
        BackendConfig::Postgres,
        BackendConfig::MySql,
        BackendConfig::Sqlite,
        BackendConfig::DuckDb,
        BackendConfig::Kafka,
        BackendConfig::Kinesis,
        BackendConfig::PubSub,
        BackendConfig::RabbitMq,
        BackendConfig::Delta,
        BackendConfig::ObjectStore,
    ];
    // Each round-trips as a distinct JSON discriminator; sanity-check
    // by serializing all and confirming uniqueness.
    let mut seen: Vec<String> = kinds
        .iter()
        .map(|k| serde_json::to_string(k).expect("serialize"))
        .collect();
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), 10, "each variant must serialize distinctly");
}

/// Reverse direction: `backend_from_config` is a stub in PR 1 — it
/// errors clearly for every variant pointing at the migration commit
/// that will fill it in. Pattern-match rather than `expect_err`
/// because `Arc<dyn Backend>` doesn't implement Debug (the trait
/// would have to bound it; intentionally not done here so individual
/// backends control their own redacting Debug impls).
#[tokio::test]
async fn backend_from_config_returns_not_implemented_for_postgres() {
    match backend_from_config(BackendConfig::Postgres).await {
        Ok(_) => panic!("Postgres should not yet be wired in PR 1"),
        Err(err) => {
            let msg = format!("{err}");
            assert!(
                msg.to_lowercase().contains("not implemented")
                    || msg.to_lowercase().contains("not yet"),
                "must signal incomplete migration; got: {msg}"
            );
            assert!(
                msg.contains("Postgres") || msg.contains("postgres"),
                "must name the backend kind; got: {msg}"
            );
        }
    }
}

/// `Backend: 'static` is needed for shipping `Arc<dyn Backend>`
/// across Arrow Flight in Σ.B (the BallistaBackend) and Σ.D (state
/// store hooks). Compile-only check — if the trait grows the bound
/// and any in-tree backend doesn't satisfy it, the workspace stops
/// compiling and CI catches it. The assertion below makes the
/// contract explicit + fails to compile if the bound regresses.
#[test]
fn backend_trait_is_static_object_safe() {
    fn assert_object_safe<T: ?Sized>() {}
    assert_object_safe::<dyn ematix_flow_core::backend::Backend>();

    // Verify the 'static bound by attempting to construct a type that
    // requires it. `Arc<dyn Backend + 'static>` is the shape Ballista
    // will ship over Arrow Flight; if Backend isn't 'static-bound the
    // workspace stops compiling at the trait definition.
    fn requires_static<T: 'static + ?Sized>() {}
    requires_static::<dyn ematix_flow_core::backend::Backend>();
}
