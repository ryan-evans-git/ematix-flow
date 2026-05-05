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

#[test]
fn kafka_config_round_trips_with_constructor_args() {
    let cfg = BackendConfig::Kafka(KafkaConfig {
        bootstrap_servers: "broker1:9092,broker2:9092".into(),
        group_id: Some("ematix-readers".into()),
    });
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
    let cfg = BackendConfig::Kafka(KafkaConfig {
        bootstrap_servers: "broker:9092".into(),
        group_id: None,
    });
    let json = serde_json::to_string(&cfg).unwrap();
    let recovered: BackendConfig = serde_json::from_str(&json).unwrap();
    assert!(matches!(
        recovered,
        BackendConfig::Kafka(KafkaConfig { ref group_id, .. }) if group_id.is_none()
    ));
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
    });
    let backend = backend_from_config(cfg).await.expect("construct");
    match backend.config() {
        BackendConfig::PubSub(c) => assert_eq!(c.project_id, "demo-project"),
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
    });
    let json = serde_json::to_string(&cfg).expect("serialize");
    assert!(json.contains(r#""kind":"distributed""#));
    assert!(json.contains("flow-01.cluster.local"));

    let recovered: BackendConfig = serde_json::from_str(&json).expect("deserialize");
    match recovered {
        BackendConfig::Distributed(c) => {
            assert_eq!(c.peers.len(), 2);
            assert_eq!(c.peers[0], "http://flow-01.cluster.local:50051");
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
        BackendConfig::Distributed(DistributedConfig { ref peers }) if peers.is_empty()
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

#[tokio::test]
async fn backend_from_config_constructs_rabbitmq_offline() {
    let cfg = BackendConfig::RabbitMq(RabbitMqConfig {
        amqp_url: "amqp://guest:guest@localhost:5672/%2f".into(),
    });
    let backend = backend_from_config(cfg).await.expect("construct");
    match backend.config() {
        BackendConfig::RabbitMq(c) => {
            assert_eq!(c.amqp_url, "amqp://guest:guest@localhost:5672/%2f");
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
