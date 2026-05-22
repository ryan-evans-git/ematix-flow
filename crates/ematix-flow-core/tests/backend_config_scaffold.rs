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
    BackendConfig, DeltaConfig, DeltaLocation, DistributedConfig, DuckDbConfig, KafkaConfig,
    KinesisConfig, MySqlConfig, ObjectFormat, ObjectStoreConfig, ObjectStoreLocation,
    ObjectWriteOptions, ParquetCompression, PostgresConfig, PubSubConfig, RabbitMqConfig,
    SqliteConfig, backend_from_config,
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

/// Helper: construct a KafkaConfig with everything-default builder
/// state. The Σ.B follow-up extended the struct to seven optional
/// builder-state fields; tests that don't care about them use this
/// helper to keep the noise down.
fn kafka_config_minimal(bootstrap: &str, group_id: Option<&str>) -> KafkaConfig {
    KafkaConfig {
        bootstrap_servers: bootstrap.into(),
        group_id: group_id.map(|s| s.into()),
        auth: None,
        payload_format: None,
        delivery_semantics: None,
        schema_registry_url: None,
        schema_registry_basic_auth: None,
        schema_registry_kind: None,
        message_key_column: None,
        batch_config: None,
    }
}

#[test]
fn kafka_config_round_trips_with_constructor_args() {
    let cfg = BackendConfig::Kafka(kafka_config_minimal(
        "broker1:9092,broker2:9092",
        Some("ematix-readers"),
    ));
    let json = serde_json::to_string(&cfg).expect("serialize");
    let recovered: BackendConfig = serde_json::from_str(&json).expect("deserialize");
    match recovered {
        BackendConfig::Kafka(c) => {
            assert_eq!(c.bootstrap_servers, "broker1:9092,broker2:9092");
            assert_eq!(c.group_id.as_deref(), Some("ematix-readers"));
        }
        other => panic!("expected Kafka, got {other:?}"),
    }
}

#[test]
fn kafka_config_producer_only_has_no_group_id() {
    let cfg = BackendConfig::Kafka(kafka_config_minimal("broker:9092", None));
    let json = serde_json::to_string(&cfg).unwrap();
    let recovered: BackendConfig = serde_json::from_str(&json).unwrap();
    assert!(matches!(
        recovered,
        BackendConfig::Kafka(KafkaConfig { ref group_id, .. }) if group_id.is_none()
    ));
}

/// Σ.B follow-up: full Kafka builder-state round-trip. Exercises
/// the auth + payload format + SR config + delivery semantics +
/// message-key column + batch config fields end-to-end through
/// serde JSON. backend_from_config() construction is exercised in
/// `kafka_backend_from_config_applies_sasl_plain_auth` below.
#[test]
fn kafka_config_full_builder_state_round_trips_through_serde() {
    use ematix_flow_core::backend::KafkaAuthConfig;
    use ematix_flow_core::kafka_backend::{
        KafkaBatchConfig, KafkaDeliverySemantics, KafkaPayloadFormat, ScramMechanism, SrBasicAuth,
    };

    let original = BackendConfig::Kafka(KafkaConfig {
        bootstrap_servers: "broker1:9092".into(),
        group_id: Some("readers".into()),
        auth: Some(KafkaAuthConfig::SaslScram {
            mechanism: ScramMechanism::Sha512,
            username: "app".into(),
            password: "s3cret".into(),
        }),
        payload_format: Some(KafkaPayloadFormat::Avro),
        delivery_semantics: Some(KafkaDeliverySemantics::ExactlyOnce {
            transactional_id: "ematix-flow-tx-01".into(),
        }),
        schema_registry_url: Some("https://sr.confluent.cloud".into()),
        schema_registry_basic_auth: Some(SrBasicAuth {
            username: "sr-user".into(),
            password: "sr-pass".into(),
        }),
        schema_registry_kind: None,
        message_key_column: Some("user_id".into()),
        batch_config: Some(KafkaBatchConfig {
            batch_size: 50_000,
            batch_bytes: 8 * 1024 * 1024,
            batch_window_ms: 1_000,
            idle_timeout_ms: 2_000,
        }),
    });

    let json = serde_json::to_string(&original).unwrap();
    let recovered: BackendConfig = serde_json::from_str(&json).unwrap();
    let inner = match recovered {
        BackendConfig::Kafka(c) => c,
        other => panic!("expected Kafka, got {other:?}"),
    };

    // Spot-check each field round-tripped.
    match inner.auth.as_ref().unwrap() {
        KafkaAuthConfig::SaslScram {
            mechanism,
            username,
            password,
        } => {
            assert!(matches!(mechanism, ScramMechanism::Sha512));
            assert_eq!(username, "app");
            assert_eq!(password, "s3cret");
        }
        other => panic!("expected SaslScram, got {other:?}"),
    }
    assert!(matches!(
        inner.payload_format,
        Some(KafkaPayloadFormat::Avro)
    ));
    match inner.delivery_semantics.as_ref().unwrap() {
        KafkaDeliverySemantics::ExactlyOnce { transactional_id } => {
            assert_eq!(transactional_id, "ematix-flow-tx-01");
        }
        other => panic!("expected ExactlyOnce, got {other:?}"),
    }
    assert_eq!(
        inner.schema_registry_url.as_deref(),
        Some("https://sr.confluent.cloud")
    );
    let sra = inner.schema_registry_basic_auth.as_ref().unwrap();
    assert_eq!(sra.username, "sr-user");
    assert_eq!(sra.password, "sr-pass");
    assert_eq!(inner.message_key_column.as_deref(), Some("user_id"));
    let batch = inner.batch_config.as_ref().unwrap();
    assert_eq!(batch.batch_size, 50_000);
    assert_eq!(batch.batch_window_ms, 1_000);
}

/// Σ.B follow-up: backend_from_config(Kafka) applies the auth +
/// other builder-state fields, and backend.config() round-trips
/// them back. SASL/PLAIN is the simplest auth variant to verify
/// without spinning up a real broker — librdkafka builds clients
/// lazily so we can construct + inspect without a network round.
#[tokio::test]
async fn kafka_backend_from_config_applies_sasl_plain_auth() {
    use ematix_flow_core::backend::KafkaAuthConfig;

    let cfg = BackendConfig::Kafka(KafkaConfig {
        bootstrap_servers: "broker:9092".into(),
        group_id: Some("g".into()),
        auth: Some(KafkaAuthConfig::SaslPlain {
            username: "app".into(),
            password: "pw".into(),
        }),
        payload_format: None,
        delivery_semantics: None,
        schema_registry_url: Some("http://sr:8081".into()),
        schema_registry_basic_auth: None,
        schema_registry_kind: None,
        message_key_column: Some("user_id".into()),
        batch_config: None,
    });
    let backend = backend_from_config(cfg).await.expect("construct");
    let recovered = backend.config();
    let c = match recovered {
        BackendConfig::Kafka(c) => c,
        other => panic!("expected Kafka, got {other:?}"),
    };
    assert_eq!(c.bootstrap_servers, "broker:9092");
    match c.auth.as_ref().unwrap() {
        KafkaAuthConfig::SaslPlain { username, password } => {
            assert_eq!(username, "app");
            assert_eq!(password, "pw");
        }
        other => panic!("expected SaslPlain, got {other:?}"),
    }
    assert_eq!(c.schema_registry_url.as_deref(), Some("http://sr:8081"));
    assert_eq!(c.message_key_column.as_deref(), Some("user_id"));
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

/// Σ.B follow-up: lock down the `as_postgres()` escape-hatch
/// contract. The trait method is `#[doc(hidden)]` and intended for
/// PostgresBackend's strategy executors only. Every non-PG backend
/// must inherit the default `None` impl — anything else means a new
/// override slipped past review and has effectively re-leaked the
/// abstraction.
///
/// Doesn't try to assert the PostgresBackend `Some(...)` half: that
/// requires a live Postgres pool (covered by integration tests in
/// `tests/integration_pg.rs`), and the failure mode for the override
/// going missing is a different test.
#[tokio::test]
async fn as_postgres_returns_none_for_every_non_postgres_backend() {
    // SQLite, DuckDB are the simplest in-memory backends to spin up
    // without containers. Together they cover both the "C-extension
    // wrapped" and "pure-rust embedded" branches; anything that
    // overrides as_postgres in either of them is the regression we
    // want to catch.
    let sqlite = backend_from_config(BackendConfig::Sqlite(SqliteConfig {
        location: ":memory:".into(),
    }))
    .await
    .expect("sqlite");
    assert!(
        sqlite.as_postgres().is_none(),
        "SqliteBackend must inherit the trait-default None impl"
    );

    let duckdb = backend_from_config(BackendConfig::DuckDb(DuckDbConfig {
        location: ":memory:".into(),
    }))
    .await
    .expect("duckdb");
    assert!(
        duckdb.as_postgres().is_none(),
        "DuckDbBackend must inherit the trait-default None impl"
    );
}

/// Streaming-backend constructors that don't reach a remote service
/// can be exercised without TestContainers. Kinesis / PubSub /
/// RabbitMQ all defer the real connection to `ping()` / first IO,
/// so building from config is synchronous + offline. Kafka's
/// `open()` likewise builds librdkafka clients lazily.
#[tokio::test]
async fn backend_from_config_constructs_kinesis_offline() {
    let cfg = BackendConfig::Kinesis(KinesisConfig {
        stream_name: "events".into(),
        region: None,
        endpoint: None,
        static_credentials: None,
        batch_config: None,
    });
    let backend = backend_from_config(cfg).await.expect("construct");
    match backend.config() {
        BackendConfig::Kinesis(c) => assert_eq!(c.stream_name, "events"),
        other => panic!("expected Kinesis, got {other:?}"),
    }
}

/// Σ.B follow-up: full builder-state round-trip for Kinesis.
/// region / endpoint / static_credentials / batch_config all
/// preserved through serde JSON + reconstruction.
#[tokio::test]
async fn backend_from_config_kinesis_round_trips_builder_state() {
    use ematix_flow_core::backend::KinesisStaticCredentials;
    use ematix_flow_core::kinesis_backend::KinesisBatchConfig;

    let original = BackendConfig::Kinesis(KinesisConfig {
        stream_name: "events".into(),
        region: Some("us-west-2".into()),
        endpoint: Some("http://localhost:4566".into()),
        static_credentials: Some(KinesisStaticCredentials {
            access_key_id: "test-key".into(),
            secret_access_key: "test-secret".into(),
        }),
        batch_config: Some(KinesisBatchConfig {
            batch_size: 500,
            batch_bytes: 4 * 1024 * 1024,
            max_empty_polls: 3,
            idle_poll_ms: 100,
        }),
    });

    // 1) Serde round-trip lossless.
    let json = serde_json::to_string(&original).unwrap();
    let recovered: BackendConfig = serde_json::from_str(&json).unwrap();
    let inner = match recovered {
        BackendConfig::Kinesis(c) => c,
        other => panic!("expected Kinesis, got {other:?}"),
    };
    assert_eq!(inner.region.as_deref(), Some("us-west-2"));
    assert_eq!(inner.endpoint.as_deref(), Some("http://localhost:4566"));
    let creds = inner.static_credentials.as_ref().unwrap();
    assert_eq!(creds.access_key_id, "test-key");
    assert_eq!(creds.secret_access_key, "test-secret");
    let batch = inner.batch_config.as_ref().unwrap();
    assert_eq!(batch.batch_size, 500);
    assert_eq!(batch.max_empty_polls, 3);

    // 2) backend_from_config applies all four builder knobs.
    let backend = backend_from_config(BackendConfig::Kinesis(inner))
        .await
        .expect("construct");

    // 3) backend.config() round-trips back to the same surface.
    match backend.config() {
        BackendConfig::Kinesis(c) => {
            assert_eq!(c.stream_name, "events");
            assert_eq!(c.region.as_deref(), Some("us-west-2"));
            assert_eq!(c.endpoint.as_deref(), Some("http://localhost:4566"));
            let creds = c.static_credentials.as_ref().unwrap();
            assert_eq!(creds.access_key_id, "test-key");
            // backend.config() always emits a batch_config (no longer
            // None-when-default); verify the values round-tripped.
            let bc = c.batch_config.as_ref().unwrap();
            assert_eq!(bc.batch_size, 500);
            assert_eq!(bc.idle_poll_ms, 100);
        }
        other => panic!("expected Kinesis config, got {other:?}"),
    }
}

#[tokio::test]
async fn backend_from_config_constructs_pubsub_offline() {
    let cfg = BackendConfig::PubSub(PubSubConfig {
        project_id: "demo-project".into(),
        endpoint: None,
        anonymous_auth: false,
        batch_config: None,
    });
    let backend = backend_from_config(cfg).await.expect("construct");
    match backend.config() {
        BackendConfig::PubSub(c) => assert_eq!(c.project_id, "demo-project"),
        other => panic!("expected PubSub, got {other:?}"),
    }
}

/// Σ.B follow-up: full PubSub builder-state round-trip. emulator
/// path = endpoint + anonymous_auth, plus a tweaked batch_config.
#[tokio::test]
async fn backend_from_config_pubsub_round_trips_emulator_state() {
    use ematix_flow_core::pubsub_backend::PubSubBatchConfig;

    let cfg = BackendConfig::PubSub(PubSubConfig {
        project_id: "demo-project".into(),
        endpoint: Some("http://localhost:8085".into()),
        anonymous_auth: true,
        batch_config: Some(PubSubBatchConfig {
            batch_size: 200,
            batch_bytes: 1024 * 1024,
            idle_timeout_ms: 50,
        }),
    });
    let json = serde_json::to_string(&cfg).unwrap();
    let recovered: BackendConfig = serde_json::from_str(&json).unwrap();
    let backend = backend_from_config(recovered).await.expect("construct");
    match backend.config() {
        BackendConfig::PubSub(c) => {
            assert_eq!(c.project_id, "demo-project");
            assert_eq!(c.endpoint.as_deref(), Some("http://localhost:8085"));
            assert!(c.anonymous_auth);
            let bc = c.batch_config.as_ref().unwrap();
            assert_eq!(bc.batch_size, 200);
            assert_eq!(bc.idle_timeout_ms, 50);
        }
        other => panic!("expected PubSub, got {other:?}"),
    }
}

// --- Σ.B PR 2: distributed-execution config -----------------------

#[test]
fn distributed_config_round_trips_with_peer_urls() {
    let cfg = BackendConfig::Distributed(DistributedConfig {
        peers: vec![
            "http://flow-01.cluster.local:50051".into(),
            "http://flow-02.cluster.local:50051".into(),
        ],
        tls: None,
    });
    let json = serde_json::to_string(&cfg).expect("serialize");
    assert!(json.contains(r#""kind":"distributed""#));
    assert!(json.contains("flow-01.cluster.local"));
    // `skip_serializing_if` keeps the JSON minimal when TLS is off.
    assert!(
        !json.contains(r#""tls""#),
        "tls=None must serialize cleanly: {json}"
    );

    let recovered: BackendConfig = serde_json::from_str(&json).expect("deserialize");
    match recovered {
        BackendConfig::Distributed(c) => {
            assert_eq!(c.peers.len(), 2);
            assert_eq!(c.peers[0], "http://flow-01.cluster.local:50051");
            assert!(c.tls.is_none());
        }
        other => panic!("expected Distributed, got {other:?}"),
    }
}

#[test]
fn distributed_config_default_has_empty_peers() {
    let cfg = DistributedConfig::default();
    assert!(cfg.peers.is_empty());
    let wrapped = BackendConfig::Distributed(cfg);
    let json = serde_json::to_string(&wrapped).unwrap();
    let recovered: BackendConfig = serde_json::from_str(&json).unwrap();
    assert!(matches!(
        recovered,
        BackendConfig::Distributed(DistributedConfig { ref peers, .. }) if peers.is_empty()
    ));
}

/// `backend_from_config(Distributed)` is intentionally a no-op in
/// core to avoid a circular dep with `ematix-flow-distributed`.
/// The error must point at the right constructor so users don't
/// hunt for the cause.
#[tokio::test]
async fn backend_from_config_distributed_points_at_distributed_crate() {
    let cfg = BackendConfig::Distributed(DistributedConfig {
        peers: vec!["http://localhost:50051".into()],
        tls: None,
    });
    match backend_from_config(cfg).await {
        Ok(_) => panic!("Distributed should not construct via core"),
        Err(err) => {
            let msg = format!("{err}");
            assert!(
                msg.contains("DistributedBackend::open"),
                "must point at the right constructor; got: {msg}"
            );
            assert!(
                msg.contains("ematix_flow_distributed") || msg.contains("ematix-flow-distributed"),
                "must name the crate; got: {msg}"
            );
        }
    }
}

/// Σ.B follow-up: TLS config (CA bundle + optional mTLS identity +
/// optional SNI override) round-trips through serde JSON when set.
/// The carrier is paths-only; the distributed crate loads the PEM
/// files lazily when building the channel resolver.
#[test]
fn distributed_config_round_trips_with_tls() {
    use ematix_flow_core::backend::{DistributedClientIdentityConfig, DistributedTlsConfig};

    let cfg = BackendConfig::Distributed(DistributedConfig {
        peers: vec!["https://flow-01.cluster.local:50051".into()],
        tls: Some(DistributedTlsConfig {
            ca_cert_pem_path: "/etc/ematix-flow/tls/ca.pem".into(),
            client_identity: Some(DistributedClientIdentityConfig {
                cert_pem_path: "/etc/ematix-flow/tls/coordinator.pem".into(),
                key_pem_path: "/etc/ematix-flow/tls/coordinator.key".into(),
            }),
            domain_name_override: Some("flow.cluster.local".into()),
        }),
    });
    let json = serde_json::to_string(&cfg).expect("serialize");
    assert!(json.contains(r#""tls""#), "tls field must surface: {json}");
    assert!(json.contains("ca.pem"));
    assert!(json.contains("coordinator.key"));

    let recovered: BackendConfig = serde_json::from_str(&json).unwrap();
    match recovered {
        BackendConfig::Distributed(c) => {
            let tls = c.tls.expect("tls present");
            assert_eq!(tls.ca_cert_pem_path, "/etc/ematix-flow/tls/ca.pem");
            let id = tls.client_identity.expect("client identity");
            assert_eq!(id.cert_pem_path, "/etc/ematix-flow/tls/coordinator.pem");
            assert_eq!(id.key_pem_path, "/etc/ematix-flow/tls/coordinator.key");
            assert_eq!(
                tls.domain_name_override.as_deref(),
                Some("flow.cluster.local")
            );
        }
        other => panic!("expected Distributed, got {other:?}"),
    }
}

#[tokio::test]
async fn backend_from_config_constructs_rabbitmq_offline() {
    let cfg = BackendConfig::RabbitMq(RabbitMqConfig {
        amqp_url: "amqp://guest:guest@localhost:5672/%2f".into(),
        consumer_tag: None,
        batch_config: None,
    });
    let backend = backend_from_config(cfg).await.expect("construct");
    match backend.config() {
        BackendConfig::RabbitMq(c) => {
            assert_eq!(c.amqp_url, "amqp://guest:guest@localhost:5672/%2f");
            // Default consumer_tag rides through as None — keeps the
            // JSON minimal for the common case.
            assert!(c.consumer_tag.is_none());
        }
        other => panic!("expected RabbitMq, got {other:?}"),
    }
}

/// Σ.B follow-up: full RabbitMQ builder-state round-trip. Custom
/// consumer_tag + tweaked batch_config preserved end-to-end.
#[tokio::test]
async fn backend_from_config_rabbitmq_round_trips_builder_state() {
    use ematix_flow_core::rabbitmq_backend::RabbitBatchConfig;

    let cfg = BackendConfig::RabbitMq(RabbitMqConfig {
        amqp_url: "amqp://guest:guest@localhost:5672/%2f".into(),
        consumer_tag: Some("ematix-prod-east".into()),
        batch_config: Some(RabbitBatchConfig {
            batch_size: 4_096,
            batch_bytes: 4 * 1024 * 1024,
            idle_timeout_ms: 1_000,
        }),
    });
    let json = serde_json::to_string(&cfg).unwrap();
    let recovered: BackendConfig = serde_json::from_str(&json).unwrap();
    let backend = backend_from_config(recovered).await.expect("construct");
    match backend.config() {
        BackendConfig::RabbitMq(c) => {
            assert_eq!(c.consumer_tag.as_deref(), Some("ematix-prod-east"));
            let bc = c.batch_config.as_ref().unwrap();
            assert_eq!(bc.batch_size, 4_096);
            assert_eq!(bc.idle_timeout_ms, 1_000);
        }
        other => panic!("expected RabbitMq, got {other:?}"),
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

// --- Σ.B PR 1 commit c: object-store + Delta -----------------------

#[test]
fn object_store_local_config_round_trips_with_format() {
    let cfg = BackendConfig::ObjectStore(ObjectStoreConfig {
        location: ObjectStoreLocation::Local {
            root_dir: "/tmp/exports".into(),
        },
        format: ObjectFormat::Parquet,
        write_options: ObjectWriteOptions {
            parquet_compression: Some(ParquetCompression::Snappy),
            ..Default::default()
        },
        read_options: Default::default(),
    });
    let json = serde_json::to_string(&cfg).expect("serialize");
    let recovered: BackendConfig = serde_json::from_str(&json).expect("deserialize");
    match recovered {
        BackendConfig::ObjectStore(c) => {
            assert!(matches!(
                c.location,
                ObjectStoreLocation::Local { ref root_dir } if root_dir == "/tmp/exports"
            ));
            assert!(matches!(c.format, ObjectFormat::Parquet));
            assert_eq!(
                c.write_options.parquet_compression,
                Some(ParquetCompression::Snappy)
            );
        }
        other => panic!("expected ObjectStore, got {other:?}"),
    }
}

#[test]
fn object_store_s3_config_round_trips() {
    let cfg = BackendConfig::ObjectStore(ObjectStoreConfig {
        location: ObjectStoreLocation::S3 {
            endpoint: "http://localhost:9000".into(),
            bucket: "warehouse".into(),
            region: "us-east-1".into(),
            access_key: "minio".into(),
            secret_key: "minio123".into(),
        },
        format: ObjectFormat::Csv,
        write_options: ObjectWriteOptions::default(),
        read_options: Default::default(),
    });
    let json = serde_json::to_string(&cfg).expect("serialize");
    let recovered: BackendConfig = serde_json::from_str(&json).expect("deserialize");
    if let BackendConfig::ObjectStore(c) = recovered
        && let ObjectStoreLocation::S3 { bucket, region, .. } = c.location
    {
        assert_eq!(bucket, "warehouse");
        assert_eq!(region, "us-east-1");
    } else {
        panic!("expected ObjectStore::S3");
    }
}

#[tokio::test]
async fn backend_from_config_constructs_local_object_store() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = BackendConfig::ObjectStore(ObjectStoreConfig {
        location: ObjectStoreLocation::Local {
            root_dir: tmp.path().to_string_lossy().into_owned(),
        },
        format: ObjectFormat::Parquet,
        write_options: ObjectWriteOptions::default(),
        read_options: Default::default(),
    });
    let backend = backend_from_config(cfg).await.expect("construct");
    backend.ping().await.expect("ping");
    // Round-trip the config through the live backend.
    match backend.config() {
        BackendConfig::ObjectStore(c) => {
            assert!(matches!(c.location, ObjectStoreLocation::Local { .. }));
            assert!(matches!(c.format, ObjectFormat::Parquet));
        }
        other => panic!("expected ObjectStore config, got {other:?}"),
    }
}

#[test]
fn delta_local_config_round_trips_with_partition_columns() {
    let cfg = BackendConfig::Delta(DeltaConfig {
        location: DeltaLocation::Local {
            root_dir: "/tmp/delta".into(),
        },
        partition_columns: vec!["year".into(), "month".into()],
    });
    let json = serde_json::to_string(&cfg).expect("serialize");
    let recovered: BackendConfig = serde_json::from_str(&json).expect("deserialize");
    match recovered {
        BackendConfig::Delta(c) => {
            assert!(matches!(
                c.location,
                DeltaLocation::Local { ref root_dir } if root_dir == "/tmp/delta"
            ));
            assert_eq!(c.partition_columns, vec!["year", "month"]);
        }
        other => panic!("expected Delta, got {other:?}"),
    }
}

#[tokio::test]
async fn backend_from_config_constructs_local_delta() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = BackendConfig::Delta(DeltaConfig {
        location: DeltaLocation::Local {
            root_dir: tmp.path().to_string_lossy().into_owned(),
        },
        partition_columns: Vec::new(),
    });
    let backend = backend_from_config(cfg).await.expect("construct");
    backend.ping().await.expect("ping");
    match backend.config() {
        BackendConfig::Delta(c) => {
            assert!(matches!(c.location, DeltaLocation::Local { .. }));
            assert!(c.partition_columns.is_empty());
        }
        other => panic!("expected Delta config, got {other:?}"),
    }
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
