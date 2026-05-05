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

use ematix_flow_core::backend::{
    BackendConfig, DuckDbConfig, MySqlConfig, PostgresConfig, SqliteConfig, backend_from_config,
};

// --- JSON round-trip ----------------------------------------------

#[test]
fn backend_config_postgres_round_trips_with_dsn() {
    let cfg = BackendConfig::Postgres(PostgresConfig {
        dsn: "postgres://user:pw@localhost:5432/db".into(),
    });
    let json = serde_json::to_string(&cfg).expect("serialize");
    // serde-tagged enums flatten the inner struct's fields next to
    // the discriminator: `{"kind":"postgres","dsn":"..."}`.
    assert_eq!(
        json,
        r#"{"kind":"postgres","dsn":"postgres://user:pw@localhost:5432/db"}"#
    );

    let recovered: BackendConfig = serde_json::from_str(&json).expect("deserialize");
    match recovered {
        BackendConfig::Postgres(c) => {
            assert_eq!(c.dsn, "postgres://user:pw@localhost:5432/db");
        }
        other => panic!("expected Postgres, got {other:?}"),
    }
}

#[test]
fn backend_config_sqlite_round_trips_with_location() {
    let cfg = BackendConfig::Sqlite(SqliteConfig {
        location: ":memory:".into(),
    });
    let json = serde_json::to_string(&cfg).expect("serialize");
    assert_eq!(json, r#"{"kind":"sqlite","location":":memory:"}"#);
    let recovered: BackendConfig = serde_json::from_str(&json).expect("deserialize");
    assert!(matches!(
        recovered,
        BackendConfig::Sqlite(SqliteConfig { location }) if location == ":memory:"
    ));
}

#[test]
fn backend_config_kafka_unit_variant_still_serializes() {
    // Streaming variants (Kafka / Kinesis / Pub/Sub / RabbitMq) +
    // ObjectStore + Delta stay as unit variants until commits c/d
    // populate them. JSON-tagged unit variants emit just the
    // discriminator.
    let cfg = BackendConfig::Kafka;
    let json = serde_json::to_string(&cfg).expect("serialize Kafka");
    assert_eq!(json, r#"{"kind":"kafka"}"#);
}

// --- backend_from_config dispatch ---------------------------------

#[tokio::test]
async fn backend_from_config_constructs_sqlite_in_memory() {
    let cfg = BackendConfig::Sqlite(SqliteConfig {
        location: ":memory:".into(),
    });
    let backend = backend_from_config(cfg).await.expect("construct sqlite");
    backend.ping().await.expect("ping");
    // Round-trip: the constructed backend's `config()` should match
    // the input we handed in (modulo any normalization).
    match backend.config() {
        BackendConfig::Sqlite(c) => assert_eq!(c.location, ":memory:"),
        other => panic!("expected Sqlite config, got {other:?}"),
    }
}

#[tokio::test]
async fn backend_from_config_constructs_duckdb_in_memory() {
    let cfg = BackendConfig::DuckDb(DuckDbConfig {
        location: ":memory:".into(),
    });
    let backend = backend_from_config(cfg).await.expect("construct duckdb");
    backend.ping().await.expect("ping");
    match backend.config() {
        BackendConfig::DuckDb(c) => assert_eq!(c.location, ":memory:"),
        other => panic!("expected DuckDb config, got {other:?}"),
    }
}

#[tokio::test]
async fn backend_from_config_returns_not_implemented_for_kafka() {
    // Streaming variants still error out — commit d will wire them.
    match backend_from_config(BackendConfig::Kafka).await {
        Ok(_) => panic!("Kafka should not yet be wired"),
        Err(err) => {
            let msg = format!("{err}");
            assert!(
                msg.to_lowercase().contains("not yet"),
                "must signal incomplete migration; got: {msg}"
            );
        }
    }
}

#[tokio::test]
async fn backend_from_config_postgres_invalid_dsn_propagates_error() {
    // Constructing a Postgres backend from a bad DSN should error
    // cleanly via `PgPool::connect`, not panic. Captures the connect-
    // failure path that Σ.B's executors will hit in production when
    // an executor receives a config blob with a stale DSN.
    let cfg = BackendConfig::Postgres(PostgresConfig {
        dsn: "postgres://invalid:bad@localhost:1/nope".into(),
    });
    assert!(
        backend_from_config(cfg).await.is_err(),
        "invalid DSN should not connect — any BackendError variant is acceptable"
    );
}

/// `Backend: 'static` is needed for `Arc<dyn Backend>` to ship over
/// Arrow Flight in Σ.B's BallistaBackend. Compile-only assertion —
/// regresses CI loudly if the bound is removed.
#[test]
fn backend_trait_is_static_object_safe() {
    fn assert_object_safe<T: ?Sized>() {}
    assert_object_safe::<dyn ematix_flow_core::backend::Backend>();
    fn requires_static<T: 'static + ?Sized>() {}
    requires_static::<dyn ematix_flow_core::backend::Backend>();
}

#[test]
fn mysql_config_payload_round_trips() {
    // No async constructor needed — exercises just the tagged enum.
    let cfg = BackendConfig::MySql(MySqlConfig {
        dsn: "mysql://app:s3cret@db.local:3306/orders".into(),
    });
    let json = serde_json::to_string(&cfg).unwrap();
    let recovered: BackendConfig = serde_json::from_str(&json).unwrap();
    assert!(matches!(
        recovered,
        BackendConfig::MySql(MySqlConfig { dsn }) if dsn == "mysql://app:s3cret@db.local:3306/orders"
    ));
}
