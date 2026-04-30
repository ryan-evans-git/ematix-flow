//! Phase 13: Rust-side end-to-end integration tests against a real Postgres
//! container. These complement the Python integration tests by exercising
//! the Rust APIs directly — which is what the runtime will actually use.
//!
//! Marked `#[ignore]` so `cargo test` stays fast by default. Run with
//! `cargo test -p ematix-flow-core -- --ignored` (Docker required).

use std::sync::Arc;

use ematix_flow_core::backend::{Backend, Dialect, PostgresBackend, TargetTable, WriteMode};
use ematix_flow_core::pg::PgPool;
use ematix_flow_core::strategy::append::augment_with_metadata;
use ematix_flow_core::types::{ColumnSpec, ColumnType, TableSpec};
use futures_util::TryStreamExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

async fn start_postgres() -> (testcontainers::ContainerAsync<Postgres>, String) {
    let container = Postgres::default()
        .start()
        .await
        .expect("failed to start postgres testcontainer");
    let host = container
        .get_host()
        .await
        .expect("failed to read host")
        .to_string();
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to read port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (container, url)
}

fn target_spec() -> TableSpec {
    augment_with_metadata(&TableSpec {
        schema: "warehouse".into(),
        name: "event_log".into(),
        columns: vec![
            ColumnSpec {
                name: "event_id".into(),
                ty: ColumnType::BigInt,
                nullable: false,
                primary_key: true,
            },
            ColumnSpec {
                name: "name".into(),
                ty: ColumnType::Text,
                nullable: true,
                primary_key: false,
            },
        ],
        unique_constraints: Vec::new(),
        fingerprint: String::new(),
    })
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn append_same_db_inserts_and_records_history() {
    let (_container, url) = start_postgres().await;
    let pool = PgPool::connect(&url).await.unwrap();

    pool.execute("CREATE SCHEMA src").await.unwrap();
    pool.execute("CREATE TABLE src.events (event_id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    pool.execute("INSERT INTO src.events VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();

    let target = target_spec();
    pool.ensure_table(&target).await.unwrap();

    let result = pool
        .run_append_same_db(
            &target,
            "SELECT event_id, name FROM src.events",
            "rust_integration_append",
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(result.rows_inserted, 3);
    assert_eq!(result.path, "same_db");

    let row_count = pool
        .fetch_scalar_int("SELECT count(*)::int FROM warehouse.event_log")
        .await
        .unwrap();
    assert_eq!(row_count, 3);

    let history_count = pool
        .fetch_scalar_int(
            "SELECT count(*)::int FROM ematix_flow.run_history \
             WHERE pipeline_name = 'rust_integration_append' AND status = 'success'",
        )
        .await
        .unwrap();
    assert_eq!(history_count, 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn ensure_table_round_trips_columns() {
    let (_container, url) = start_postgres().await;
    let pool = PgPool::connect(&url).await.unwrap();

    let target = target_spec();
    pool.ensure_table(&target).await.unwrap();
    let reflected = pool
        .read_existing_columns(&target.schema, &target.name)
        .await
        .unwrap();

    let names: Vec<&str> = reflected.iter().map(|c| c.name.as_str()).collect();
    assert!(names.contains(&"event_id"));
    assert!(names.contains(&"name"));
    assert!(names.contains(&"_loaded_at"));
    assert!(names.contains(&"_batch_id"));

    // ensure is idempotent: a second call sees Matched, not Drift.
    pool.ensure_table(&target).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn arrow_round_trip_via_backend_trait() {
    // Phase 30b: read_arrow_stream + write_arrow_stream form a
    // backend-agnostic IO contract. Round-trip a small table to prove
    // the contract works on PostgresBackend before generalizing.
    let (_container, url) = start_postgres().await;
    let pool = Arc::new(PgPool::connect(&url).await.unwrap());
    let backend: Arc<dyn Backend> = Arc::new(PostgresBackend::new(pool.clone(), url.clone()));

    backend.execute("CREATE SCHEMA arrow_test").await.unwrap();
    backend
        .execute("CREATE TABLE arrow_test.src (id BIGINT, name TEXT, flag BOOLEAN)")
        .await
        .unwrap();
    backend
        .execute(
            "INSERT INTO arrow_test.src VALUES \
             (1, 'a', true), (2, 'b', false), (3, NULL, true)",
        )
        .await
        .unwrap();
    backend
        .execute("CREATE TABLE arrow_test.dst (id BIGINT, name TEXT, flag BOOLEAN)")
        .await
        .unwrap();

    let stream = backend
        .read_arrow_stream("SELECT id, name, flag FROM arrow_test.src ORDER BY id")
        .await
        .unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    assert!(!batches.is_empty());
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3);
    assert_eq!(batches[0].num_columns(), 3);
    assert_eq!(batches[0].schema().field(0).name(), "id");

    // Round-trip: write the batches back to the destination table.
    let stream2 = backend
        .read_arrow_stream("SELECT id, name, flag FROM arrow_test.src ORDER BY id")
        .await
        .unwrap();
    let target = TargetTable {
        schema: "arrow_test".into(),
        name: "dst".into(),
    };
    let written = backend
        .write_arrow_stream(&target, stream2, WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(written, 3);

    let dst_count = pool
        .fetch_scalar_int("SELECT count(*)::int FROM arrow_test.dst")
        .await
        .unwrap();
    assert_eq!(dst_count, 3);
    let null_count = pool
        .fetch_scalar_int(
            "SELECT count(*)::int FROM arrow_test.dst WHERE name IS NULL",
        )
        .await
        .unwrap();
    assert_eq!(null_count, 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn arrow_write_stream_truncate_replaces_existing() {
    let (_container, url) = start_postgres().await;
    let pool = Arc::new(PgPool::connect(&url).await.unwrap());
    let backend: Arc<dyn Backend> = Arc::new(PostgresBackend::new(pool.clone(), url.clone()));

    backend.execute("CREATE SCHEMA arrow_trunc").await.unwrap();
    backend
        .execute("CREATE TABLE arrow_trunc.t (id INTEGER, label TEXT)")
        .await
        .unwrap();
    backend
        .execute("INSERT INTO arrow_trunc.t VALUES (99, 'old1'), (100, 'old2')")
        .await
        .unwrap();
    backend
        .execute("CREATE TABLE arrow_trunc.src (id INTEGER, label TEXT)")
        .await
        .unwrap();
    backend
        .execute("INSERT INTO arrow_trunc.src VALUES (1, 'new')")
        .await
        .unwrap();

    let stream = backend
        .read_arrow_stream("SELECT id, label FROM arrow_trunc.src")
        .await
        .unwrap();
    let target = TargetTable {
        schema: "arrow_trunc".into(),
        name: "t".into(),
    };
    backend
        .write_arrow_stream(&target, stream, WriteMode::Truncate)
        .await
        .unwrap();
    let count = pool
        .fetch_scalar_int("SELECT count(*)::int FROM arrow_trunc.t")
        .await
        .unwrap();
    assert_eq!(count, 1);
    let new_present = pool
        .fetch_scalar_int(
            "SELECT count(*)::int FROM arrow_trunc.t WHERE id = 1 AND label = 'new'",
        )
        .await
        .unwrap();
    assert_eq!(new_present, 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn postgres_backend_trait_dispatches_ping_and_execute() {
    // Phase 30a: confirm the new Backend trait dispatches over a real
    // Postgres connection identically to the existing PgPool surface.
    let (_container, url) = start_postgres().await;
    let pool = Arc::new(PgPool::connect(&url).await.unwrap());
    let backend: Arc<dyn Backend> = Arc::new(PostgresBackend::new(pool.clone(), url.clone()));

    assert_eq!(backend.dialect(), Dialect::Postgres);
    assert_eq!(backend.dsn().as_deref(), Some(url.as_str()));

    backend.ping().await.expect("ping via Backend trait");

    backend
        .execute("CREATE SCHEMA backend_trait_test")
        .await
        .expect("execute via Backend trait");
    let exists: i32 = pool
        .fetch_scalar_int(
            "SELECT count(*)::int FROM information_schema.schemata \
             WHERE schema_name = 'backend_trait_test'",
        )
        .await
        .unwrap();
    assert_eq!(exists, 1);

    let info = backend.connection_info();
    assert_eq!(info.dbname, "postgres");
}
