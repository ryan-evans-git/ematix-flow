//! Phase 13: Rust-side end-to-end integration tests against a real Postgres
//! container. These complement the Python integration tests by exercising
//! the Rust APIs directly — which is what the runtime will actually use.
//!
//! Marked `#[ignore]` so `cargo test` stays fast by default. Run with
//! `cargo test -p ematix-flow-core -- --ignored` (Docker required).

use std::sync::Arc;

use ematix_flow_core::DuckDBBackend;
use ematix_flow_core::backend::{
    Backend, Dialect, PostgresBackend, StrategyRunResult, TargetTable, WriteMode,
};
use ematix_flow_core::pg::PgPool;
use ematix_flow_core::strategy::append::augment_with_metadata;
use ematix_flow_core::strategy::scd2::augment_with_scd2;
use ematix_flow_core::types::{ColumnSpec, ColumnType, TableSpec};
use futures_util::TryStreamExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

async fn start_postgres() -> (testcontainers::ContainerAsync<Postgres>, String) {
    use testcontainers::ImageExt;
    // Use a Postgres version that matches the Python integration tests
    // (postgres:16-alpine). The testcontainers-modules default image is
    // postgres:11, which lacks features like `WITH ... AS MATERIALIZED`
    // CTEs that the merge / scd2 strategies emit.
    let container = Postgres::default()
        .with_tag("16-alpine")
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
async fn run_append_via_backend_trait_same_db() {
    // Phase 30d: confirm the strategy executors dispatch through the
    // Backend trait identically to direct PgPool usage. This is the
    // surface DuckDB / MySQL / etc. will implement.
    let (_container, url) = start_postgres().await;
    let pool = Arc::new(PgPool::connect(&url).await.unwrap());
    let backend: Arc<dyn Backend> = Arc::new(PostgresBackend::new(pool.clone(), url.clone()));

    pool.execute("CREATE SCHEMA src").await.unwrap();
    pool.execute("CREATE TABLE src.events (event_id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    pool.execute("INSERT INTO src.events VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();

    let target = target_spec();
    pool.ensure_table(&target).await.unwrap();

    let result: StrategyRunResult = backend
        .run_append(
            &target,
            "SELECT event_id, name FROM src.events",
            "trait_append_test",
            None,
            None,
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(result.rows_inserted, 3);
    assert_eq!(result.path, "same_db");
    assert_eq!(result.status, "success");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn run_truncate_via_backend_trait() {
    let (_container, url) = start_postgres().await;
    let pool = Arc::new(PgPool::connect(&url).await.unwrap());
    let backend: Arc<dyn Backend> = Arc::new(PostgresBackend::new(pool.clone(), url.clone()));

    pool.execute("CREATE SCHEMA src").await.unwrap();
    pool.execute("CREATE TABLE src.events (event_id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    pool.execute("INSERT INTO src.events VALUES (1, 'a'), (2, 'b')")
        .await
        .unwrap();

    let target = target_spec();
    pool.ensure_table(&target).await.unwrap();
    // Pre-seed the target so truncate has something to clear.
    backend
        .run_append(
            &target,
            "SELECT 99::bigint AS event_id, 'old'::text AS name",
            "trait_truncate_seed",
            None,
            None,
            None,
            false,
        )
        .await
        .unwrap();

    let result = backend
        .run_truncate(
            &target,
            "SELECT event_id, name FROM src.events",
            "trait_truncate_test",
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(result.rows_inserted, 2);
    let count = pool
        .fetch_scalar_int(&format!(
            "SELECT count(*)::int FROM {}.{}",
            target.schema, target.name
        ))
        .await
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn run_merge_via_backend_trait() {
    let (_container, url) = start_postgres().await;
    let pool = Arc::new(PgPool::connect(&url).await.unwrap());
    let backend: Arc<dyn Backend> = Arc::new(PostgresBackend::new(pool.clone(), url.clone()));

    pool.execute("CREATE SCHEMA src").await.unwrap();
    pool.execute("CREATE TABLE src.events (event_id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    pool.execute("INSERT INTO src.events VALUES (1, 'a'), (2, 'b')")
        .await
        .unwrap();

    let target = target_spec();
    pool.ensure_table(&target).await.unwrap();

    // First run: insert.
    let result = backend
        .run_merge(
            &target,
            "SELECT event_id, name FROM src.events",
            &["event_id".to_string()],
            &["name".to_string()],
            "trait_merge_test",
            "merge",
            None,
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(result.rows_inserted, 2);

    // Update: change name for event_id=1.
    pool.execute("UPDATE src.events SET name = 'a-new' WHERE event_id = 1")
        .await
        .unwrap();
    let result2 = backend
        .run_merge(
            &target,
            "SELECT event_id, name FROM src.events",
            &["event_id".to_string()],
            &["name".to_string()],
            "trait_merge_test",
            "merge",
            None,
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(result2.rows_updated, Some(1));
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
        .fetch_scalar_int("SELECT count(*)::int FROM arrow_test.dst WHERE name IS NULL")
        .await
        .unwrap();
    assert_eq!(null_count, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn duckdb_run_append_same_backend() {
    // Phase 31b: DuckDB run_append against an in-memory DB.
    let backend: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());
    backend.execute("CREATE SCHEMA src").await.unwrap();
    backend.execute("CREATE SCHEMA wh").await.unwrap();
    backend
        .execute("CREATE TABLE src.events (event_id BIGINT, name VARCHAR)")
        .await
        .unwrap();
    backend
        .execute("INSERT INTO src.events VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();

    let target = augment_with_metadata(&TableSpec {
        schema: "wh".into(),
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
    });

    backend
        .execute(
            "CREATE TABLE wh.event_log (\
              event_id BIGINT, name VARCHAR, _loaded_at TIMESTAMPTZ, _batch_id UUID\
            )",
        )
        .await
        .unwrap();

    let result = backend
        .run_append(
            &target,
            "SELECT event_id, name FROM src.events",
            "duckdb_append_test",
            None,
            None,
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(result.rows_inserted, 3);
    assert_eq!(result.path, "same_db");
    assert_eq!(result.status, "success");
}

#[tokio::test(flavor = "multi_thread")]
async fn duckdb_run_truncate_same_backend() {
    let backend: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());
    backend.execute("CREATE SCHEMA src").await.unwrap();
    backend.execute("CREATE SCHEMA wh").await.unwrap();
    backend
        .execute("CREATE TABLE src.events (event_id BIGINT, name VARCHAR)")
        .await
        .unwrap();
    backend
        .execute("INSERT INTO src.events VALUES (1, 'a')")
        .await
        .unwrap();
    backend
        .execute(
            "CREATE TABLE wh.event_log (\
              event_id BIGINT, name VARCHAR, _loaded_at TIMESTAMPTZ, _batch_id UUID\
            )",
        )
        .await
        .unwrap();
    backend
        .execute(
            "INSERT INTO wh.event_log VALUES \
             (99, 'old', now(), gen_random_uuid())",
        )
        .await
        .unwrap();

    let target = augment_with_metadata(&TableSpec {
        schema: "wh".into(),
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
    });

    let result = backend
        .run_truncate(
            &target,
            "SELECT event_id, name FROM src.events",
            "duckdb_truncate_test",
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(result.rows_inserted, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn duckdb_run_merge_same_backend() {
    let backend: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());
    backend.execute("CREATE SCHEMA src").await.unwrap();
    backend.execute("CREATE SCHEMA wh").await.unwrap();
    backend
        .execute("CREATE TABLE src.events (event_id BIGINT, name VARCHAR)")
        .await
        .unwrap();
    backend
        .execute("INSERT INTO src.events VALUES (1, 'a'), (2, 'b')")
        .await
        .unwrap();
    backend
        .execute(
            "CREATE TABLE wh.event_log (\
              event_id BIGINT PRIMARY KEY, name VARCHAR, \
              _loaded_at TIMESTAMPTZ, _batch_id UUID\
            )",
        )
        .await
        .unwrap();

    let target = augment_with_metadata(&TableSpec {
        schema: "wh".into(),
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
    });

    // First merge: insert.
    let r1 = backend
        .run_merge(
            &target,
            "SELECT event_id, name FROM src.events",
            &["event_id".to_string()],
            &["name".to_string()],
            "duckdb_merge_test",
            "merge",
            None,
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(r1.rows_inserted, 2);

    // Update one row in source, re-merge: upsert.
    backend
        .execute("UPDATE src.events SET name = 'a-new' WHERE event_id = 1")
        .await
        .unwrap();
    backend
        .run_merge(
            &target,
            "SELECT event_id, name FROM src.events",
            &["event_id".to_string()],
            &["name".to_string()],
            "duckdb_merge_test",
            "merge",
            None,
            None,
            false,
        )
        .await
        .unwrap();
    // Verify the upsert.
    let stream = backend
        .read_arrow_stream("SELECT name FROM wh.event_log WHERE event_id = 1")
        .await
        .unwrap();
    use arrow_array::StringArray;
    use futures_util::TryStreamExt;
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    let name_col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(name_col.value(0), "a-new");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn cross_backend_pg_to_duckdb_arrow() {
    // Phase 31a's headline test: drive an Arrow stream from a real
    // Postgres source into an in-memory DuckDB target. This is the
    // first cross-dialect dispatch in the framework.
    use futures_util::TryStreamExt;

    let (_container, url) = start_postgres().await;
    let pg_pool = Arc::new(PgPool::connect(&url).await.unwrap());
    let pg: Arc<dyn Backend> = Arc::new(PostgresBackend::new(pg_pool.clone(), url.clone()));
    let duck: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());

    // Seed the PG side.
    pg.execute("CREATE SCHEMA cb_src").await.unwrap();
    pg.execute("CREATE TABLE cb_src.events (id BIGINT, name TEXT, score DOUBLE PRECISION)")
        .await
        .unwrap();
    pg.execute(
        "INSERT INTO cb_src.events VALUES \
         (1, 'alice', 1.5), (2, 'bob', 2.5), (3, NULL, 3.5)",
    )
    .await
    .unwrap();

    // Set up the DuckDB target.
    duck.execute("CREATE SCHEMA duck_dst").await.unwrap();
    duck.execute("CREATE TABLE duck_dst.events (id BIGINT, name VARCHAR, score DOUBLE)")
        .await
        .unwrap();

    // Cross-dialect: PG dialect != DuckDB dialect → must go through
    // Arrow streaming.
    assert_ne!(pg.dialect(), duck.dialect());

    // Read from PG, write to DuckDB.
    let stream = pg
        .read_arrow_stream("SELECT id, name, score FROM cb_src.events ORDER BY id")
        .await
        .unwrap();
    let target = TargetTable {
        schema: "duck_dst".into(),
        name: "events".into(),
    };
    let written = duck
        .write_arrow_stream(&target, stream, WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(written, 3);

    // Verify by reading back from DuckDB.
    let verify_stream = duck
        .read_arrow_stream("SELECT id, name, score FROM duck_dst.events ORDER BY id")
        .await
        .unwrap();
    let verify_batches: Vec<_> = verify_stream.try_collect().await.unwrap();
    let total: usize = verify_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn cross_backend_duckdb_to_pg_arrow() {
    // Reverse direction: DuckDB source → PG target.
    let (_container, url) = start_postgres().await;
    let pg_pool = Arc::new(PgPool::connect(&url).await.unwrap());
    let pg: Arc<dyn Backend> = Arc::new(PostgresBackend::new(pg_pool.clone(), url.clone()));
    let duck: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());

    duck.execute("CREATE SCHEMA src").await.unwrap();
    duck.execute("CREATE TABLE src.t (id BIGINT, label VARCHAR)")
        .await
        .unwrap();
    duck.execute("INSERT INTO src.t VALUES (10, 'x'), (20, 'y')")
        .await
        .unwrap();

    pg.execute("CREATE SCHEMA dst").await.unwrap();
    pg.execute("CREATE TABLE dst.t (id BIGINT, label TEXT)")
        .await
        .unwrap();

    let stream = duck
        .read_arrow_stream("SELECT id, label FROM src.t ORDER BY id")
        .await
        .unwrap();
    let target = TargetTable {
        schema: "dst".into(),
        name: "t".into(),
    };
    let written = pg
        .write_arrow_stream(&target, stream, WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(written, 2);
    let count = pg_pool
        .fetch_scalar_int("SELECT count(*)::int FROM dst.t")
        .await
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn arrow_round_trip_wide_type_matrix() {
    // Phase 30d.2: round-trip a wide table covering every type the
    // Arrow path supports. Coverage gaps surface here as type-mapping
    // errors with a clear message; passing all types means cross-
    // backend syncs (Phase 31+) can carry these columns end-to-end.
    let (_container, url) = start_postgres().await;
    let pool = Arc::new(PgPool::connect(&url).await.unwrap());
    let backend: Arc<dyn Backend> = Arc::new(PostgresBackend::new(pool.clone(), url.clone()));

    backend.execute("CREATE SCHEMA wide").await.unwrap();
    backend
        .execute(
            "CREATE TABLE wide.src (
                a_int2 SMALLINT,
                a_int4 INTEGER,
                a_int8 BIGINT,
                a_float4 REAL,
                a_float8 DOUBLE PRECISION,
                a_bool BOOLEAN,
                a_text TEXT,
                a_bytea BYTEA,
                a_uuid UUID,
                a_json JSONB,
                a_ts TIMESTAMPTZ
            )",
        )
        .await
        .unwrap();
    backend
        .execute(
            "INSERT INTO wide.src VALUES (
                42::smallint,
                1000000::int,
                9000000000::bigint,
                3.14::real,
                2.718281828::double precision,
                true,
                'hello',
                E'\\\\xDEADBEEF'::bytea,
                'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'::uuid,
                '{\"k\": \"v\", \"n\": 1}'::jsonb,
                '2026-01-01T00:00:00Z'::timestamptz
            )",
        )
        .await
        .unwrap();
    backend
        .execute(
            "CREATE TABLE wide.dst (
                a_int2 SMALLINT,
                a_int4 INTEGER,
                a_int8 BIGINT,
                a_float4 REAL,
                a_float8 DOUBLE PRECISION,
                a_bool BOOLEAN,
                a_text TEXT,
                a_bytea BYTEA,
                a_uuid UUID,
                a_json JSONB,
                a_ts TIMESTAMPTZ
            )",
        )
        .await
        .unwrap();

    let stream = backend
        .read_arrow_stream("SELECT * FROM wide.src")
        .await
        .unwrap();
    let target = TargetTable {
        schema: "wide".into(),
        name: "dst".into(),
    };
    backend
        .write_arrow_stream(&target, stream, WriteMode::Append)
        .await
        .unwrap();

    // Round-trip integrity: every column matches.
    let count = pool
        .fetch_scalar_int(
            "SELECT count(*)::int FROM wide.dst d JOIN wide.src s ON \
             d.a_int2 = s.a_int2 AND d.a_int4 = s.a_int4 AND d.a_int8 = s.a_int8 \
             AND d.a_float4 = s.a_float4 AND d.a_float8 = s.a_float8 \
             AND d.a_bool = s.a_bool AND d.a_text = s.a_text \
             AND d.a_bytea = s.a_bytea AND d.a_uuid = s.a_uuid \
             AND d.a_json = s.a_json AND d.a_ts = s.a_ts",
        )
        .await
        .unwrap();
    assert_eq!(count, 1, "all 11 columns must round-trip identically");
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
        .fetch_scalar_int("SELECT count(*)::int FROM arrow_trunc.t WHERE id = 1 AND label = 'new'")
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

// --- Phase 31c: DuckDB SCD2 (no-Docker, in-memory) -----------------------

fn duckdb_scd2_dim_spec() -> TableSpec {
    augment_with_scd2(&TableSpec {
        schema: "wh".into(),
        name: "customer_dim".into(),
        columns: vec![
            ColumnSpec {
                name: "customer_id".into(),
                ty: ColumnType::BigInt,
                nullable: false,
                primary_key: true,
            },
            ColumnSpec {
                name: "email".into(),
                ty: ColumnType::Text,
                nullable: false,
                primary_key: false,
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

async fn create_duckdb_scd2_target(backend: &Arc<dyn Backend>) {
    backend
        .execute(
            "CREATE TABLE wh.customer_dim (\
              customer_id BIGINT, \
              email VARCHAR, \
              name VARCHAR, \
              valid_from TIMESTAMPTZ NOT NULL, \
              valid_to TIMESTAMPTZ, \
              is_current BOOLEAN NOT NULL, \
              row_hash BLOB NOT NULL, \
              _loaded_at TIMESTAMPTZ NOT NULL, \
              _batch_id UUID NOT NULL, \
              PRIMARY KEY (customer_id, valid_from)\
            )",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn duckdb_run_scd2_first_load_inserts_all_current() {
    let backend: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());
    backend.execute("CREATE SCHEMA src").await.unwrap();
    backend.execute("CREATE SCHEMA wh").await.unwrap();
    backend
        .execute("CREATE TABLE src.customers (customer_id BIGINT, email VARCHAR, name VARCHAR)")
        .await
        .unwrap();
    backend
        .execute(
            "INSERT INTO src.customers VALUES \
             (1, 'a@x.com', 'alice'), \
             (2, 'b@x.com', 'bob'), \
             (3, 'c@x.com', NULL)",
        )
        .await
        .unwrap();
    create_duckdb_scd2_target(&backend).await;

    let target = duckdb_scd2_dim_spec();
    let result = backend
        .run_scd2(
            &target,
            "SELECT customer_id, email, name FROM src.customers",
            &["customer_id".to_string()],
            &["email".to_string(), "name".to_string()],
            "duckdb_scd2_first",
            None,
            None,
            None,
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(result.status, "success");
    assert_eq!(result.path, "same_db");

    use arrow_array::{BooleanArray, Int64Array};
    let stream = backend
        .read_arrow_stream(
            "SELECT count(*)::BIGINT, sum(CASE WHEN is_current THEN 1 ELSE 0 END)::BIGINT, \
             sum(CASE WHEN valid_to IS NULL THEN 1 ELSE 0 END)::BIGINT FROM wh.customer_dim",
        )
        .await
        .unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let currents = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let null_valid_to = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 3);
    assert_eq!(currents, 3);
    assert_eq!(null_valid_to, 3);
    let _: BooleanArray; // silence unused-import lint when only Int64Array is used
}

#[tokio::test(flavor = "multi_thread")]
async fn duckdb_run_scd2_second_load_closes_changed_row_and_inserts_new_version() {
    let backend: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());
    backend.execute("CREATE SCHEMA src").await.unwrap();
    backend.execute("CREATE SCHEMA wh").await.unwrap();
    backend
        .execute("CREATE TABLE src.customers (customer_id BIGINT, email VARCHAR, name VARCHAR)")
        .await
        .unwrap();
    backend
        .execute(
            "INSERT INTO src.customers VALUES \
             (1, 'a@x.com', 'alice'), \
             (2, 'b@x.com', 'bob')",
        )
        .await
        .unwrap();
    create_duckdb_scd2_target(&backend).await;

    let target = duckdb_scd2_dim_spec();
    backend
        .run_scd2(
            &target,
            "SELECT customer_id, email, name FROM src.customers",
            &["customer_id".to_string()],
            &["email".to_string(), "name".to_string()],
            "duckdb_scd2_2_first",
            None,
            None,
            None,
            None,
            false,
        )
        .await
        .unwrap();

    // Bob updates his email; alice unchanged.
    backend
        .execute("UPDATE src.customers SET email = 'b2@x.com' WHERE customer_id = 2")
        .await
        .unwrap();

    backend
        .run_scd2(
            &target,
            "SELECT customer_id, email, name FROM src.customers",
            &["customer_id".to_string()],
            &["email".to_string(), "name".to_string()],
            "duckdb_scd2_2_second",
            None,
            None,
            None,
            None,
            false,
        )
        .await
        .unwrap();

    use arrow_array::Int64Array;
    let stream = backend
        .read_arrow_stream(
            "SELECT \
              count(*)::BIGINT AS total, \
              sum(CASE WHEN is_current THEN 1 ELSE 0 END)::BIGINT AS currents, \
              sum(CASE WHEN customer_id = 2 AND is_current THEN 1 ELSE 0 END)::BIGINT AS bob_current, \
              sum(CASE WHEN customer_id = 2 AND NOT is_current THEN 1 ELSE 0 END)::BIGINT AS bob_closed \
             FROM wh.customer_dim",
        )
        .await
        .unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let col = |i: usize| {
        batches[0]
            .column(i)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    };
    // 2 rows alice (current) + 2 rows bob (closed + current) = 3 alive + 1 closed = 4 total when alice idempotent (1) + bob old (1) + bob new (1) = 3.
    // Actually: alice has 1 row, bob has 2 rows (closed old + new current). Total = 3.
    assert_eq!(col(0), 3, "total versions");
    assert_eq!(col(1), 2, "current versions: alice + bob_new");
    assert_eq!(col(2), 1, "exactly one current bob");
    assert_eq!(col(3), 1, "exactly one closed bob");

    // Closed bob's valid_to is set, current bob's is NULL.
    let stream = backend
        .read_arrow_stream(
            "SELECT \
              sum(CASE WHEN customer_id = 2 AND NOT is_current AND valid_to IS NOT NULL \
                       THEN 1 ELSE 0 END)::BIGINT \
             FROM wh.customer_dim",
        )
        .await
        .unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let closed_with_valid_to = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(closed_with_valid_to, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn duckdb_run_scd2_idempotent_when_no_changes() {
    let backend: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());
    backend.execute("CREATE SCHEMA src").await.unwrap();
    backend.execute("CREATE SCHEMA wh").await.unwrap();
    backend
        .execute("CREATE TABLE src.customers (customer_id BIGINT, email VARCHAR, name VARCHAR)")
        .await
        .unwrap();
    backend
        .execute("INSERT INTO src.customers VALUES (1, 'a@x.com', 'alice')")
        .await
        .unwrap();
    create_duckdb_scd2_target(&backend).await;

    let target = duckdb_scd2_dim_spec();
    let args = (
        "SELECT customer_id, email, name FROM src.customers",
        vec!["customer_id".to_string()],
        vec!["email".to_string(), "name".to_string()],
    );

    backend
        .run_scd2(
            &target,
            args.0,
            &args.1,
            &args.2,
            "duckdb_scd2_idem_1",
            None,
            None,
            None,
            None,
            false,
        )
        .await
        .unwrap();
    backend
        .run_scd2(
            &target,
            args.0,
            &args.1,
            &args.2,
            "duckdb_scd2_idem_2",
            None,
            None,
            None,
            None,
            false,
        )
        .await
        .unwrap();

    use arrow_array::Int64Array;
    let stream = backend
        .read_arrow_stream("SELECT count(*)::BIGINT FROM wh.customer_dim")
        .await
        .unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        total, 1,
        "idempotent: no second version when nothing changed"
    );
}

// --- Phase 31d.1: DuckDB run_history -------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn duckdb_run_history_records_successful_append() {
    let backend: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());
    backend.execute("CREATE SCHEMA src").await.unwrap();
    backend.execute("CREATE SCHEMA wh").await.unwrap();
    backend
        .execute("CREATE TABLE src.events (event_id BIGINT, name VARCHAR)")
        .await
        .unwrap();
    backend
        .execute("INSERT INTO src.events VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();
    let target = augment_with_metadata(&TableSpec {
        schema: "wh".into(),
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
    });
    backend
        .execute(
            "CREATE TABLE wh.event_log (\
              event_id BIGINT, name VARCHAR, _loaded_at TIMESTAMPTZ, _batch_id UUID\
            )",
        )
        .await
        .unwrap();

    backend
        .run_append(
            &target,
            "SELECT event_id, name FROM src.events",
            "duckdb_history_append",
            None,
            None,
            None,
            false,
        )
        .await
        .unwrap();

    use arrow_array::{Int64Array, StringArray};
    let stream = backend
        .read_arrow_stream(
            "SELECT count(*)::BIGINT, \
                    sum(CASE WHEN status='success' THEN 1 ELSE 0 END)::BIGINT, \
                    max(rows_inserted) \
             FROM ematix_flow.run_history \
             WHERE pipeline_name='duckdb_history_append'",
        )
        .await
        .unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let success = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let rows_inserted = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 1, "exactly one history row for the run");
    assert_eq!(success, 1, "status is 'success'");
    assert_eq!(rows_inserted, 3);

    // Mode + path + target identifiers correctly recorded.
    let stream = backend
        .read_arrow_stream(
            "SELECT mode, path, target_schema, target_table FROM ematix_flow.run_history \
             WHERE pipeline_name='duckdb_history_append'",
        )
        .await
        .unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let s = |i: usize| {
        batches[0]
            .column(i)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string()
    };
    assert_eq!(s(0), "append");
    assert_eq!(s(1), "same_db");
    assert_eq!(s(2), "wh");
    assert_eq!(s(3), "event_log");
}

#[tokio::test(flavor = "multi_thread")]
async fn duckdb_run_history_records_failure_when_target_missing() {
    let backend: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());
    backend.execute("CREATE SCHEMA src").await.unwrap();
    backend
        .execute("CREATE TABLE src.events (event_id BIGINT, name VARCHAR)")
        .await
        .unwrap();
    backend
        .execute("INSERT INTO src.events VALUES (1, 'a')")
        .await
        .unwrap();
    let target = augment_with_metadata(&TableSpec {
        schema: "wh".into(),
        name: "missing_table".into(),
        columns: vec![ColumnSpec {
            name: "event_id".into(),
            ty: ColumnType::BigInt,
            nullable: false,
            primary_key: true,
        }],
        unique_constraints: Vec::new(),
        fingerprint: String::new(),
    });
    // Don't create wh.missing_table — the INSERT should fail.
    let result = backend
        .run_append(
            &target,
            "SELECT event_id FROM src.events",
            "duckdb_history_failure",
            None,
            None,
            None,
            false,
        )
        .await;
    assert!(result.is_err(), "run_append should fail on missing target");

    use arrow_array::{Array, Int64Array, StringArray};
    let stream = backend
        .read_arrow_stream(
            "SELECT count(*)::BIGINT, \
                    sum(CASE WHEN status='failed' THEN 1 ELSE 0 END)::BIGINT \
             FROM ematix_flow.run_history \
             WHERE pipeline_name='duckdb_history_failure'",
        )
        .await
        .unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let failed = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 1);
    assert_eq!(failed, 1, "status is 'failed' even when strategy errors");

    // error_message is populated.
    let stream = backend
        .read_arrow_stream(
            "SELECT error_message FROM ematix_flow.run_history \
             WHERE pipeline_name='duckdb_history_failure'",
        )
        .await
        .unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let msg = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(!msg.is_null(0), "error_message should be set on failure");
    assert!(
        !msg.value(0).is_empty(),
        "error_message should be non-empty"
    );
}

// --- Phase 31d.2: incremental_column on DuckDB run_append ----------------

#[tokio::test(flavor = "multi_thread")]
async fn duckdb_run_append_incremental_filters_and_advances_watermark() {
    let backend: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());
    backend.execute("CREATE SCHEMA src").await.unwrap();
    backend.execute("CREATE SCHEMA wh").await.unwrap();
    backend
        .execute("CREATE TABLE src.events (event_id BIGINT, ts BIGINT)")
        .await
        .unwrap();
    backend
        .execute(
            "INSERT INTO src.events VALUES \
             (1, 100), (2, 200), (3, 300)",
        )
        .await
        .unwrap();
    let target = augment_with_metadata(&TableSpec {
        schema: "wh".into(),
        name: "event_log".into(),
        columns: vec![
            ColumnSpec {
                name: "event_id".into(),
                ty: ColumnType::BigInt,
                nullable: false,
                primary_key: true,
            },
            ColumnSpec {
                name: "ts".into(),
                ty: ColumnType::BigInt,
                nullable: false,
                primary_key: false,
            },
        ],
        unique_constraints: Vec::new(),
        fingerprint: String::new(),
    });
    backend
        .execute(
            "CREATE TABLE wh.event_log (\
              event_id BIGINT, ts BIGINT, _loaded_at TIMESTAMPTZ, _batch_id UUID\
            )",
        )
        .await
        .unwrap();

    // First load with watermark column but no prior literal (cold start).
    let r1 = backend
        .run_append(
            &target,
            "SELECT event_id, ts FROM src.events",
            "duckdb_incr_test",
            None,
            Some("ts"),
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(r1.rows_inserted, 3);

    // Watermark advanced to max(ts) = 300.
    use arrow_array::{Int64Array, StringArray};
    let stream = backend
        .read_arrow_stream(
            "SELECT last_value FROM ematix_flow.watermarks \
             WHERE pipeline_name='duckdb_incr_test'",
        )
        .await
        .unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let last = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
        .to_string();
    assert_eq!(last, "300");

    // Add a couple of newer rows (ts=400, 500) plus a stale one (ts=150).
    backend
        .execute(
            "INSERT INTO src.events VALUES \
             (4, 400), (5, 500), (6, 150)",
        )
        .await
        .unwrap();

    // Second load: filter using last literal.
    let r2 = backend
        .run_append(
            &target,
            "SELECT event_id, ts FROM src.events",
            "duckdb_incr_test",
            None,
            Some("ts"),
            Some("300"),
            false,
        )
        .await
        .unwrap();
    assert_eq!(r2.rows_inserted, 2, "only ts > 300 should pass");

    // Total in target: 3 (first load) + 2 (second) = 5.
    let stream = backend
        .read_arrow_stream("SELECT count(*)::BIGINT FROM wh.event_log")
        .await
        .unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 5);

    // Watermark advanced to 500.
    let stream = backend
        .read_arrow_stream(
            "SELECT last_value FROM ematix_flow.watermarks \
             WHERE pipeline_name='duckdb_incr_test'",
        )
        .await
        .unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let last = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
        .to_string();
    assert_eq!(last, "500");
}

// --- Phase 31d.3: DuckDB merge insert/update split + handle_deletes ------

fn duckdb_merge_target_spec() -> TableSpec {
    augment_with_metadata(&TableSpec {
        schema: "wh".into(),
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

async fn duckdb_merge_setup(backend: &Arc<dyn Backend>) {
    backend.execute("CREATE SCHEMA src").await.unwrap();
    backend.execute("CREATE SCHEMA wh").await.unwrap();
    backend
        .execute("CREATE TABLE src.events (event_id BIGINT, name VARCHAR)")
        .await
        .unwrap();
    backend
        .execute(
            "CREATE TABLE wh.event_log (\
              event_id BIGINT PRIMARY KEY, name VARCHAR, \
              _loaded_at TIMESTAMPTZ, _batch_id UUID\
            )",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn duckdb_run_merge_splits_inserts_from_updates() {
    let backend: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());
    duckdb_merge_setup(&backend).await;
    let target = duckdb_merge_target_spec();
    backend
        .execute("INSERT INTO src.events VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();

    // First merge: 3 inserts, 0 updates.
    let r1 = backend
        .run_merge(
            &target,
            "SELECT event_id, name FROM src.events",
            &["event_id".into()],
            &["name".into()],
            "duckdb_split_test",
            "merge",
            None,
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(r1.rows_inserted, 3, "all 3 are new");
    assert_eq!(r1.rows_updated, Some(0));

    // Second merge: change 1, leave 2 alone, add 4. Drop 3 from source
    // (but no handle_deletes → target should still have 3).
    backend.execute("DELETE FROM src.events").await.unwrap();
    backend
        .execute("INSERT INTO src.events VALUES (1, 'a-new'), (2, 'b'), (4, 'd')")
        .await
        .unwrap();
    let r2 = backend
        .run_merge(
            &target,
            "SELECT event_id, name FROM src.events",
            &["event_id".into()],
            &["name".into()],
            "duckdb_split_test",
            "merge",
            None,
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(r2.rows_inserted, 1, "only event_id=4 is new");
    // 1 + 2 are updates (2's values are unchanged but DuckDB cannot
    // distinguish without a row-hash; 31d treats them as updates).
    assert_eq!(r2.rows_updated, Some(2));

    // Target retains row 3 because no handle_deletes.
    use arrow_array::Int64Array;
    let stream = backend
        .read_arrow_stream("SELECT count(*)::BIGINT FROM wh.event_log")
        .await
        .unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 4, "target = {{1,2,3,4}}");
}

#[tokio::test(flavor = "multi_thread")]
async fn duckdb_run_merge_handle_deletes_removes_missing_keys() {
    use ematix_flow_core::meta::DeleteHandling;
    let backend: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());
    duckdb_merge_setup(&backend).await;
    let target = duckdb_merge_target_spec();
    backend
        .execute("INSERT INTO src.events VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();
    backend
        .run_merge(
            &target,
            "SELECT event_id, name FROM src.events",
            &["event_id".into()],
            &["name".into()],
            "duckdb_delete_test",
            "merge",
            None,
            None,
            false,
        )
        .await
        .unwrap();

    // Drop event_id=3 from source; merge with handle_deletes=Hard.
    backend.execute("DELETE FROM src.events").await.unwrap();
    backend
        .execute("INSERT INTO src.events VALUES (1, 'a'), (2, 'b-new')")
        .await
        .unwrap();
    backend
        .run_merge(
            &target,
            "SELECT event_id, name FROM src.events",
            &["event_id".into()],
            &["name".into()],
            "duckdb_delete_test",
            "merge",
            None,
            Some(DeleteHandling::Hard),
            false,
        )
        .await
        .unwrap();

    use arrow_array::Int64Array;
    let stream = backend
        .read_arrow_stream(
            "SELECT count(*)::BIGINT, \
                    sum(CASE WHEN event_id = 3 THEN 1 ELSE 0 END)::BIGINT \
             FROM wh.event_log",
        )
        .await
        .unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let row3 = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 2, "row 3 deleted; only 1 + 2 remain");
    assert_eq!(row3, 0, "row 3 specifically gone");
}

// --- Phase 31d.4: DuckDB SCD2 handle_deletes + ttl_seconds ---------------

#[tokio::test(flavor = "multi_thread")]
async fn duckdb_run_scd2_soft_delete_closes_missing_keys() {
    use ematix_flow_core::meta::DeleteHandling;
    let backend: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());
    backend.execute("CREATE SCHEMA src").await.unwrap();
    backend.execute("CREATE SCHEMA wh").await.unwrap();
    backend
        .execute("CREATE TABLE src.customers (customer_id BIGINT, email VARCHAR, name VARCHAR)")
        .await
        .unwrap();
    backend
        .execute(
            "INSERT INTO src.customers VALUES \
             (1, 'a@x.com', 'alice'), (2, 'b@x.com', 'bob')",
        )
        .await
        .unwrap();
    create_duckdb_scd2_target(&backend).await;
    let target = duckdb_scd2_dim_spec();

    // First load — both rows current.
    backend
        .run_scd2(
            &target,
            "SELECT customer_id, email, name FROM src.customers",
            &["customer_id".into()],
            &["email".into(), "name".into()],
            "duckdb_soft_delete_test",
            None,
            None,
            None,
            None,
            false,
        )
        .await
        .unwrap();

    // Drop bob from source.
    backend
        .execute("DELETE FROM src.customers WHERE customer_id = 2")
        .await
        .unwrap();
    // Re-run with soft-delete.
    backend
        .run_scd2(
            &target,
            "SELECT customer_id, email, name FROM src.customers",
            &["customer_id".into()],
            &["email".into(), "name".into()],
            "duckdb_soft_delete_test_2",
            None,
            Some(DeleteHandling::Soft),
            None,
            None,
            false,
        )
        .await
        .unwrap();

    // Bob's current row got closed: is_current=false, valid_to set.
    use arrow_array::Int64Array;
    let stream = backend
        .read_arrow_stream(
            "SELECT \
              sum(CASE WHEN customer_id = 2 AND is_current THEN 1 ELSE 0 END)::BIGINT, \
              sum(CASE WHEN customer_id = 2 AND NOT is_current AND valid_to IS NOT NULL THEN 1 ELSE 0 END)::BIGINT, \
              sum(CASE WHEN customer_id = 1 AND is_current THEN 1 ELSE 0 END)::BIGINT \
             FROM wh.customer_dim",
        )
        .await
        .unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let bob_current = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let bob_closed = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let alice_current = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(bob_current, 0, "bob's current row was closed");
    assert_eq!(bob_closed, 1, "bob has exactly one closed row");
    assert_eq!(alice_current, 1, "alice still current");
}

#[tokio::test(flavor = "multi_thread")]
async fn duckdb_run_scd2_ttl_expires_stale_current_rows() {
    let backend: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());
    backend.execute("CREATE SCHEMA src").await.unwrap();
    backend.execute("CREATE SCHEMA wh").await.unwrap();
    backend
        .execute("CREATE TABLE src.customers (customer_id BIGINT, email VARCHAR, name VARCHAR)")
        .await
        .unwrap();
    backend
        .execute("INSERT INTO src.customers VALUES (1, 'a@x.com', 'alice')")
        .await
        .unwrap();
    create_duckdb_scd2_target(&backend).await;
    let target = duckdb_scd2_dim_spec();

    // First load with ttl=600s — alice's valid_from = now(), so she's
    // not stale yet.
    backend
        .run_scd2(
            &target,
            "SELECT customer_id, email, name FROM src.customers",
            &["customer_id".into()],
            &["email".into(), "name".into()],
            "duckdb_ttl_test_1",
            None,
            None,
            None,
            Some(600),
            false,
        )
        .await
        .unwrap();

    // Force-age alice's valid_from beyond the TTL window.
    backend
        .execute(
            "UPDATE wh.customer_dim SET valid_from = now() - INTERVAL '2 hours' \
             WHERE customer_id = 1",
        )
        .await
        .unwrap();

    // Re-run with same source: TTL expiry should close alice's current
    // version even though her data hasn't changed (no new version
    // inserted, just tombstone).
    backend
        .run_scd2(
            &target,
            "SELECT customer_id, email, name FROM src.customers",
            &["customer_id".into()],
            &["email".into(), "name".into()],
            "duckdb_ttl_test_2",
            None,
            None,
            None,
            Some(600),
            false,
        )
        .await
        .unwrap();

    use arrow_array::Int64Array;
    let stream = backend
        .read_arrow_stream(
            "SELECT \
              sum(CASE WHEN is_current THEN 1 ELSE 0 END)::BIGINT, \
              sum(CASE WHEN NOT is_current AND valid_to IS NOT NULL THEN 1 ELSE 0 END)::BIGINT \
             FROM wh.customer_dim",
        )
        .await
        .unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let currents = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let closed = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(currents, 0, "alice's current row was TTL-expired");
    assert_eq!(closed, 1, "exactly one closed row from the TTL sweep");
}

// --- Phase 32e: cross-backend Arrow with SQLite --------------------------

use ematix_flow_core::SQLiteBackend;

#[tokio::test(flavor = "multi_thread")]
async fn cross_backend_duckdb_to_sqlite_arrow() {
    use arrow_array::{Array, Float64Array, Int64Array, StringArray};
    let duck: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());
    let sqlite: Arc<dyn Backend> = Arc::new(SQLiteBackend::open(":memory:").unwrap());
    duck.execute("CREATE SCHEMA s").await.unwrap();
    duck.execute("CREATE TABLE s.events (id BIGINT, name VARCHAR, score DOUBLE)")
        .await
        .unwrap();
    duck.execute("INSERT INTO s.events VALUES (1, 'alice', 1.5), (2, 'bob', 2.5), (3, NULL, 3.5)")
        .await
        .unwrap();
    sqlite
        .execute("CREATE TABLE events (id INTEGER, name TEXT, score REAL)")
        .await
        .unwrap();

    assert_ne!(duck.dialect(), sqlite.dialect());
    let stream = duck
        .read_arrow_stream("SELECT id, name, score FROM s.events ORDER BY id")
        .await
        .unwrap();
    let target = TargetTable {
        schema: "main".into(),
        name: "events".into(),
    };
    let n = sqlite
        .write_arrow_stream(&target, stream, WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(n, 3);

    let s = sqlite
        .read_arrow_stream("SELECT id, name, score FROM events ORDER BY id")
        .await
        .unwrap();
    let batches: Vec<_> = s.try_collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 3);
    let id = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let name = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let score = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(id.value(0), 1);
    assert_eq!(name.value(0), "alice");
    assert!((score.value(0) - 1.5).abs() < 1e-9);
    assert!(name.is_null(2), "third row had NULL name");
}

#[tokio::test(flavor = "multi_thread")]
async fn cross_backend_sqlite_to_duckdb_arrow() {
    use arrow_array::Int64Array;
    let sqlite: Arc<dyn Backend> = Arc::new(SQLiteBackend::open(":memory:").unwrap());
    let duck: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());
    sqlite
        .execute("CREATE TABLE events (id INTEGER, name TEXT)")
        .await
        .unwrap();
    sqlite
        .execute("INSERT INTO events VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();
    duck.execute("CREATE SCHEMA s").await.unwrap();
    duck.execute("CREATE TABLE s.events (id BIGINT, name VARCHAR)")
        .await
        .unwrap();

    let stream = sqlite
        .read_arrow_stream("SELECT id, name FROM events ORDER BY id")
        .await
        .unwrap();
    let target = TargetTable {
        schema: "s".into(),
        name: "events".into(),
    };
    let n = duck
        .write_arrow_stream(&target, stream, WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(n, 3);

    let s = duck
        .read_arrow_stream("SELECT count(*)::BIGINT FROM s.events")
        .await
        .unwrap();
    let batches: Vec<_> = s.try_collect().await.unwrap();
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 3);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn cross_backend_pg_to_sqlite_arrow() {
    use arrow_array::Int64Array;
    let (_container, url) = start_postgres().await;
    let pg_pool = Arc::new(PgPool::connect(&url).await.unwrap());
    let pg: Arc<dyn Backend> = Arc::new(PostgresBackend::new(pg_pool.clone(), url.clone()));
    let sqlite: Arc<dyn Backend> = Arc::new(SQLiteBackend::open(":memory:").unwrap());

    pg.execute("CREATE SCHEMA cb_src").await.unwrap();
    pg.execute("CREATE TABLE cb_src.events (id BIGINT, name TEXT, score DOUBLE PRECISION)")
        .await
        .unwrap();
    pg.execute(
        "INSERT INTO cb_src.events VALUES (1, 'alice', 1.5), (2, 'bob', 2.5), (3, NULL, 3.5)",
    )
    .await
    .unwrap();

    sqlite
        .execute("CREATE TABLE events (id INTEGER, name TEXT, score REAL)")
        .await
        .unwrap();
    let stream = pg
        .read_arrow_stream("SELECT id, name, score FROM cb_src.events ORDER BY id")
        .await
        .unwrap();
    let target = TargetTable {
        schema: "main".into(),
        name: "events".into(),
    };
    let n = sqlite
        .write_arrow_stream(&target, stream, WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(n, 3);

    let s = sqlite
        .read_arrow_stream("SELECT count(*) FROM events")
        .await
        .unwrap();
    let batches: Vec<_> = s.try_collect().await.unwrap();
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 3);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn cross_backend_sqlite_to_pg_arrow() {
    let (_container, url) = start_postgres().await;
    let pg_pool = Arc::new(PgPool::connect(&url).await.unwrap());
    let pg: Arc<dyn Backend> = Arc::new(PostgresBackend::new(pg_pool.clone(), url.clone()));
    let sqlite: Arc<dyn Backend> = Arc::new(SQLiteBackend::open(":memory:").unwrap());
    sqlite
        .execute("CREATE TABLE events (id INTEGER, name TEXT)")
        .await
        .unwrap();
    sqlite
        .execute("INSERT INTO events VALUES (1, 'a'), (2, 'b')")
        .await
        .unwrap();
    pg.execute("CREATE SCHEMA cb_dst").await.unwrap();
    pg.execute("CREATE TABLE cb_dst.events (id BIGINT, name TEXT)")
        .await
        .unwrap();
    let stream = sqlite
        .read_arrow_stream("SELECT id, name FROM events ORDER BY id")
        .await
        .unwrap();
    let target = TargetTable {
        schema: "cb_dst".into(),
        name: "events".into(),
    };
    let n = pg
        .write_arrow_stream(&target, stream, WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(n, 2);
    let count = pg_pool
        .fetch_scalar_int("SELECT count(*)::int FROM cb_dst.events")
        .await
        .unwrap();
    assert_eq!(count, 2);
}

// ----- Phase 33a: MySQL integration tests (Docker-only) -----------------

use ematix_flow_core::MySQLBackend;
use testcontainers_modules::mysql::Mysql;

async fn start_mysql() -> (testcontainers::ContainerAsync<Mysql>, String) {
    let container = Mysql::default()
        .start()
        .await
        .expect("failed to start mysql testcontainer");
    let host = container
        .get_host()
        .await
        .expect("failed to read host")
        .to_string();
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("failed to read port");
    // testcontainers-modules' Mysql defaults: root user, empty password,
    // initial database "test".
    let url = format!("mysql://root@{host}:{port}/test");
    (container, url)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn mysql_backend_ping() {
    let (_container, url) = start_mysql().await;
    let backend = MySQLBackend::open(&url).unwrap();
    backend.ping().await.unwrap();
    assert_eq!(backend.dialect(), Dialect::MySQL);
    let info = backend.connection_info();
    assert_eq!(info.user, "root");
    assert_eq!(info.dbname, "test");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn mysql_backend_execute_and_read_arrow_stream() {
    use arrow_array::Array;
    use arrow_array::{BooleanArray, Float64Array, Int64Array, StringArray};

    let (_container, url) = start_mysql().await;
    let backend = MySQLBackend::open(&url).unwrap();
    backend
        .execute(
            "CREATE TABLE items (\
                id BIGINT, \
                name VARCHAR(64), \
                price DOUBLE, \
                in_stock TINYINT(1)\
            )",
        )
        .await
        .unwrap();
    let inserted = backend
        .execute(
            "INSERT INTO items VALUES \
             (1, 'apple', 1.5, 1), \
             (2, 'banana', 0.75, 0), \
             (3, NULL, NULL, 1)",
        )
        .await
        .unwrap();
    assert_eq!(inserted, 3);

    let stream = backend
        .read_arrow_stream("SELECT id, name, price, in_stock FROM items ORDER BY id")
        .await
        .unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let b = &batches[0];
    assert_eq!(b.num_rows(), 3);

    let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(ids.values(), &[1, 2, 3]);

    let names = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(names.value(0), "apple");
    assert_eq!(names.value(1), "banana");
    assert!(names.is_null(2));

    let prices = b.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((prices.value(0) - 1.5).abs() < 1e-9);
    assert!((prices.value(1) - 0.75).abs() < 1e-9);
    assert!(prices.is_null(2));

    let stocks = b.column(3).as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(stocks.value(0));
    assert!(!stocks.value(1));
    assert!(stocks.value(2));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn mysql_backend_write_arrow_stream() {
    use arrow_array::{Float64Array, Int64Array, RecordBatch as RB, StringArray};
    use arrow_schema::{DataType as Dt, Field as F, Schema as S};

    let (_container, url) = start_mysql().await;
    let backend = MySQLBackend::open(&url).unwrap();
    backend
        .execute(
            "CREATE TABLE writes (\
                id BIGINT, \
                name VARCHAR(64), \
                price DOUBLE\
            )",
        )
        .await
        .unwrap();

    let schema = std::sync::Arc::new(S::new(vec![
        F::new("id", Dt::Int64, true),
        F::new("name", Dt::Utf8, true),
        F::new("price", Dt::Float64, true),
    ]));
    let batch = RB::try_new(
        schema,
        vec![
            std::sync::Arc::new(Int64Array::from(vec![10, 20, 30])),
            std::sync::Arc::new(StringArray::from(vec!["a", "b", "c"])),
            std::sync::Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
        ],
    )
    .unwrap();
    let stream = futures_util::stream::once(async move { Ok::<_, _>(batch) });
    let stream: ematix_flow_core::backend::ArrowBatchStream = Box::pin(stream);

    let target = TargetTable {
        schema: "test".into(),
        name: "writes".into(),
    };
    let n = backend
        .write_arrow_stream(&target, stream, WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(n, 3);

    // Round-trip via a count(*) read.
    use arrow_array::Int64Array as I64;
    let s2 = backend
        .read_arrow_stream("SELECT count(*) AS n FROM writes")
        .await
        .unwrap();
    let bs: Vec<_> = s2.try_collect().await.unwrap();
    let total = bs[0]
        .column(0)
        .as_any()
        .downcast_ref::<I64>()
        .unwrap()
        .value(0);
    assert_eq!(total, 3);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn mysql_backend_write_arrow_stream_truncate() {
    use arrow_array::{Int64Array, RecordBatch as RB};
    use arrow_schema::{DataType as Dt, Field as F, Schema as S};

    let (_container, url) = start_mysql().await;
    let backend = MySQLBackend::open(&url).unwrap();
    backend
        .execute("CREATE TABLE trunc_t (id BIGINT)")
        .await
        .unwrap();
    backend
        .execute("INSERT INTO trunc_t VALUES (99), (100)")
        .await
        .unwrap();
    let schema = std::sync::Arc::new(S::new(vec![F::new("id", Dt::Int64, true)]));
    let batch = RB::try_new(
        schema,
        vec![std::sync::Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();
    let stream = futures_util::stream::once(async move { Ok::<_, _>(batch) });
    let stream: ematix_flow_core::backend::ArrowBatchStream = Box::pin(stream);
    let target = TargetTable {
        schema: "test".into(),
        name: "trunc_t".into(),
    };
    let n = backend
        .write_arrow_stream(&target, stream, WriteMode::Truncate)
        .await
        .unwrap();
    assert_eq!(n, 2);

    let s2 = backend
        .read_arrow_stream("SELECT id FROM trunc_t ORDER BY id")
        .await
        .unwrap();
    let bs: Vec<_> = s2.try_collect().await.unwrap();
    let ids = bs[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    // Old rows (99, 100) removed by TRUNCATE; only the new ones remain.
    assert_eq!(ids.values(), &[1, 2]);
}

// ----- Phase 33b: MySQL run_append + run_truncate + run_history ---------

fn mysql_event_log_spec() -> TableSpec {
    augment_with_metadata(&TableSpec {
        schema: "test".into(),
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

async fn mysql_make_backend_with_event_log()
-> (testcontainers::ContainerAsync<Mysql>, Arc<dyn Backend>) {
    let (container, url) = start_mysql().await;
    let b: Arc<dyn Backend> = Arc::new(MySQLBackend::open(&url).unwrap());
    b.execute("CREATE TABLE src_events (event_id BIGINT, name VARCHAR(64))")
        .await
        .unwrap();
    b.execute(
        "CREATE TABLE event_log (\
            event_id BIGINT, \
            name VARCHAR(64), \
            _loaded_at DATETIME(6) NOT NULL, \
            _batch_id CHAR(36) NOT NULL\
        )",
    )
    .await
    .unwrap();
    (container, b)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn mysql_run_append_inserts_rows_and_records_history() {
    use arrow_array::Int64Array;

    let (_c, b) = mysql_make_backend_with_event_log().await;
    b.execute("INSERT INTO src_events VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();
    let target = mysql_event_log_spec();
    let result = b
        .run_append(
            &target,
            "SELECT event_id, name FROM src_events",
            "mysql_append_test",
            None,
            None,
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(result.rows_inserted, 3);
    assert_eq!(result.path, "same_db");
    assert_eq!(result.status, "success");

    let s = b
        .read_arrow_stream(
            "SELECT count(*), \
                    CAST(sum(status='success') AS SIGNED), \
                    max(rows_inserted) \
             FROM ematix_flow.run_history \
             WHERE pipeline_name='mysql_append_test'",
        )
        .await
        .unwrap();
    let batches: Vec<_> = s.try_collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let success = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let rows_inserted = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 1);
    assert_eq!(success, 1);
    assert_eq!(rows_inserted, 3);

    let s = b
        .read_arrow_stream("SELECT count(*) FROM event_log")
        .await
        .unwrap();
    let batches: Vec<_> = s.try_collect().await.unwrap();
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 3);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn mysql_run_truncate_replaces_target() {
    use arrow_array::Int64Array;

    let (_c, b) = mysql_make_backend_with_event_log().await;
    b.execute(
        "INSERT INTO event_log VALUES \
            (99, 'old', '2024-01-01 00:00:00.000000', \
             '00000000-0000-0000-0000-000000000000')",
    )
    .await
    .unwrap();
    b.execute("INSERT INTO src_events VALUES (1, 'a')")
        .await
        .unwrap();
    let target = mysql_event_log_spec();
    let r = b
        .run_truncate(
            &target,
            "SELECT event_id, name FROM src_events",
            "mysql_truncate_test",
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(r.rows_inserted, 1);
    assert_eq!(r.status, "success");

    let s = b
        .read_arrow_stream("SELECT count(*), max(event_id) FROM event_log")
        .await
        .unwrap();
    let batches: Vec<_> = s.try_collect().await.unwrap();
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let max_id = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 1, "old row 99 replaced");
    assert_eq!(max_id, 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn mysql_run_append_records_failure_when_target_missing() {
    let (_c, url) = start_mysql().await;
    let b: Arc<dyn Backend> = Arc::new(MySQLBackend::open(&url).unwrap());
    b.execute("CREATE TABLE src_events (event_id BIGINT)")
        .await
        .unwrap();
    b.execute("INSERT INTO src_events VALUES (1)")
        .await
        .unwrap();
    let target = augment_with_metadata(&TableSpec {
        schema: "test".into(),
        name: "missing_table".into(),
        columns: vec![ColumnSpec {
            name: "event_id".into(),
            ty: ColumnType::BigInt,
            nullable: false,
            primary_key: true,
        }],
        unique_constraints: Vec::new(),
        fingerprint: String::new(),
    });
    let result = b
        .run_append(
            &target,
            "SELECT event_id FROM src_events",
            "mysql_failure_test",
            None,
            None,
            None,
            false,
        )
        .await;
    assert!(result.is_err(), "missing target should error");

    use arrow_array::{Int64Array, StringArray};
    let s = b
        .read_arrow_stream(
            "SELECT count(*), max(status), max(error_message) \
             FROM ematix_flow.run_history \
             WHERE pipeline_name='mysql_failure_test'",
        )
        .await
        .unwrap();
    let batches: Vec<_> = s.try_collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let status = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    assert_eq!(count, 1);
    assert_eq!(status, "failed");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn mysql_run_append_advances_watermark_and_filters_next_run() {
    use arrow_array::{Int64Array, StringArray};

    let (_c, b) = mysql_make_backend_with_event_log().await;
    b.execute("INSERT INTO src_events VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();
    let target = mysql_event_log_spec();
    let r = b
        .run_append(
            &target,
            "SELECT event_id, name FROM src_events",
            "mysql_wm_test",
            None,
            Some("event_id"),
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(r.rows_inserted, 3);

    let s = b
        .read_arrow_stream(
            "SELECT column_name, `last_value` FROM ematix_flow.watermarks \
             WHERE pipeline_name='mysql_wm_test'",
        )
        .await
        .unwrap();
    let batches: Vec<_> = s.try_collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    let col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let last = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(col.value(0), "event_id");
    assert_eq!(last.value(0), "3");

    // Second run: feed the watermark literal back so only newer rows
    // come through. Insert (2, 'b2') and (4, 'd'); only 4 should land.
    b.execute("INSERT INTO src_events VALUES (4, 'd')")
        .await
        .unwrap();
    let r2 = b
        .run_append(
            &target,
            "SELECT event_id, name FROM src_events",
            "mysql_wm_test",
            None,
            Some("event_id"),
            Some("3"),
            false,
        )
        .await
        .unwrap();
    assert_eq!(r2.rows_inserted, 1);
    let s = b
        .read_arrow_stream("SELECT count(*) FROM event_log")
        .await
        .unwrap();
    let batches: Vec<_> = s.try_collect().await.unwrap();
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 4);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn mysql_run_append_dry_run_rolls_back() {
    use arrow_array::Int64Array;

    let (_c, b) = mysql_make_backend_with_event_log().await;
    b.execute("INSERT INTO src_events VALUES (1, 'a'), (2, 'b')")
        .await
        .unwrap();
    let target = mysql_event_log_spec();
    let r = b
        .run_append(
            &target,
            "SELECT event_id, name FROM src_events",
            "mysql_dry_test",
            None,
            None,
            None,
            true,
        )
        .await
        .unwrap();
    assert_eq!(r.status, "dry_run");

    let s = b
        .read_arrow_stream("SELECT count(*) FROM event_log")
        .await
        .unwrap();
    let batches: Vec<_> = s.try_collect().await.unwrap();
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 0, "dry_run rolled back");
}

// ----- Phase 33c: MySQL run_merge ---------------------------------------

fn mysql_merge_target_spec() -> TableSpec {
    augment_with_metadata(&TableSpec {
        schema: "test".into(),
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

async fn mysql_merge_setup() -> (testcontainers::ContainerAsync<Mysql>, Arc<dyn Backend>) {
    let (container, url) = start_mysql().await;
    let b: Arc<dyn Backend> = Arc::new(MySQLBackend::open(&url).unwrap());
    b.execute("CREATE TABLE src_events (event_id BIGINT, name VARCHAR(64))")
        .await
        .unwrap();
    // event_log needs a PRIMARY KEY on event_id for ON DUPLICATE KEY
    // UPDATE to fire.
    b.execute(
        "CREATE TABLE event_log (\
            event_id BIGINT NOT NULL PRIMARY KEY, \
            name VARCHAR(64), \
            _loaded_at DATETIME(6) NOT NULL, \
            _batch_id CHAR(36) NOT NULL\
        )",
    )
    .await
    .unwrap();
    (container, b)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn mysql_run_merge_splits_inserts_from_updates() {
    use arrow_array::Int64Array;

    let (_c, b) = mysql_merge_setup().await;
    let target = mysql_merge_target_spec();
    b.execute("INSERT INTO src_events VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();

    let r1 = b
        .run_merge(
            &target,
            "SELECT event_id, name FROM src_events",
            &["event_id".into()],
            &["name".into()],
            "mysql_merge_split",
            "merge",
            None,
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(r1.rows_inserted, 3);
    assert_eq!(r1.rows_updated, Some(0));

    b.execute("DELETE FROM src_events").await.unwrap();
    b.execute("INSERT INTO src_events VALUES (1, 'a-new'), (2, 'b'), (4, 'd')")
        .await
        .unwrap();
    let r2 = b
        .run_merge(
            &target,
            "SELECT event_id, name FROM src_events",
            &["event_id".into()],
            &["name".into()],
            "mysql_merge_split",
            "merge",
            None,
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(r2.rows_inserted, 1, "only event_id=4 is new");
    assert_eq!(r2.rows_updated, Some(2));

    let s = b
        .read_arrow_stream("SELECT count(*) FROM event_log")
        .await
        .unwrap();
    let batches: Vec<_> = s.try_collect().await.unwrap();
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 4, "target = {{1,2,3,4}}");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn mysql_run_merge_handle_deletes_removes_missing_keys() {
    use arrow_array::Int64Array;
    use ematix_flow_core::meta::DeleteHandling;

    let (_c, b) = mysql_merge_setup().await;
    let target = mysql_merge_target_spec();
    b.execute("INSERT INTO src_events VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();
    b.run_merge(
        &target,
        "SELECT event_id, name FROM src_events",
        &["event_id".into()],
        &["name".into()],
        "mysql_merge_delete",
        "merge",
        None,
        None,
        false,
    )
    .await
    .unwrap();

    b.execute("DELETE FROM src_events").await.unwrap();
    b.execute("INSERT INTO src_events VALUES (1, 'a'), (2, 'b-new')")
        .await
        .unwrap();
    b.run_merge(
        &target,
        "SELECT event_id, name FROM src_events",
        &["event_id".into()],
        &["name".into()],
        "mysql_merge_delete",
        "merge",
        None,
        Some(DeleteHandling::Hard),
        false,
    )
    .await
    .unwrap();

    let s = b
        .read_arrow_stream(
            "SELECT count(*), \
                    CAST(sum(CASE WHEN event_id=3 THEN 1 ELSE 0 END) AS SIGNED) \
             FROM event_log",
        )
        .await
        .unwrap();
    let batches: Vec<_> = s.try_collect().await.unwrap();
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let row3 = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 2);
    assert_eq!(row3, 0, "row 3 hard-deleted");
}

// ----- Phase 33d: MySQL run_scd2 -----------------------------------------

fn mysql_scd2_dim_spec() -> TableSpec {
    augment_with_scd2(&TableSpec {
        schema: "test".into(),
        name: "customer_dim".into(),
        columns: vec![
            ColumnSpec {
                name: "customer_id".into(),
                ty: ColumnType::BigInt,
                nullable: false,
                primary_key: true,
            },
            ColumnSpec {
                name: "email".into(),
                ty: ColumnType::Text,
                nullable: false,
                primary_key: false,
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

async fn mysql_scd2_setup() -> (testcontainers::ContainerAsync<Mysql>, Arc<dyn Backend>) {
    let (container, url) = start_mysql().await;
    let b: Arc<dyn Backend> = Arc::new(MySQLBackend::open(&url).unwrap());
    b.execute(
        "CREATE TABLE src_customers (customer_id BIGINT, email VARCHAR(255), name VARCHAR(255))",
    )
    .await
    .unwrap();
    b.execute(
        "CREATE TABLE customer_dim (\
            customer_id BIGINT NOT NULL, \
            email VARCHAR(255), \
            name VARCHAR(255), \
            valid_from DATETIME(6) NOT NULL, \
            valid_to DATETIME(6) NULL, \
            is_current TINYINT(1) NOT NULL, \
            row_hash VARBINARY(32) NOT NULL, \
            _loaded_at DATETIME(6) NOT NULL, \
            _batch_id CHAR(36) NOT NULL, \
            PRIMARY KEY (customer_id, valid_from)\
        ) ENGINE=InnoDB",
    )
    .await
    .unwrap();
    (container, b)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn mysql_run_scd2_first_load_inserts_all_current() {
    use arrow_array::Int64Array;

    let (_c, b) = mysql_scd2_setup().await;
    b.execute(
        "INSERT INTO src_customers VALUES \
         (1, 'a@x.com', 'alice'), (2, 'b@x.com', 'bob'), (3, 'c@x.com', NULL)",
    )
    .await
    .unwrap();
    let target = mysql_scd2_dim_spec();
    b.run_scd2(
        &target,
        "SELECT customer_id, email, name FROM src_customers",
        &["customer_id".into()],
        &["email".into(), "name".into()],
        "mysql_scd2_first",
        None,
        None,
        None,
        None,
        false,
    )
    .await
    .unwrap();

    let s = b
        .read_arrow_stream(
            "SELECT count(*), \
                    CAST(sum(CASE WHEN is_current=1 THEN 1 ELSE 0 END) AS SIGNED), \
                    CAST(sum(CASE WHEN valid_to IS NULL THEN 1 ELSE 0 END) AS SIGNED) \
             FROM customer_dim",
        )
        .await
        .unwrap();
    let batches: Vec<_> = s.try_collect().await.unwrap();
    let col = |i: usize| {
        batches[0]
            .column(i)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    };
    assert_eq!(col(0), 3);
    assert_eq!(col(1), 3);
    assert_eq!(col(2), 3);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn mysql_run_scd2_second_load_closes_changed_row() {
    use arrow_array::Int64Array;

    let (_c, b) = mysql_scd2_setup().await;
    b.execute("INSERT INTO src_customers VALUES (1, 'a@x.com', 'alice'), (2, 'b@x.com', 'bob')")
        .await
        .unwrap();
    let target = mysql_scd2_dim_spec();
    b.run_scd2(
        &target,
        "SELECT customer_id, email, name FROM src_customers",
        &["customer_id".into()],
        &["email".into(), "name".into()],
        "mysql_scd2_a",
        None,
        None,
        None,
        None,
        false,
    )
    .await
    .unwrap();

    b.execute("UPDATE src_customers SET email = 'b2@x.com' WHERE customer_id = 2")
        .await
        .unwrap();
    b.run_scd2(
        &target,
        "SELECT customer_id, email, name FROM src_customers",
        &["customer_id".into()],
        &["email".into(), "name".into()],
        "mysql_scd2_b",
        None,
        None,
        None,
        None,
        false,
    )
    .await
    .unwrap();

    let s = b
        .read_arrow_stream(
            "SELECT count(*), \
                    CAST(sum(CASE WHEN is_current=1 THEN 1 ELSE 0 END) AS SIGNED), \
                    CAST(sum(CASE WHEN customer_id=2 AND is_current=1 THEN 1 ELSE 0 END) AS SIGNED), \
                    CAST(sum(CASE WHEN customer_id=2 AND is_current=0 THEN 1 ELSE 0 END) AS SIGNED) \
             FROM customer_dim",
        )
        .await
        .unwrap();
    let batches: Vec<_> = s.try_collect().await.unwrap();
    let col = |i: usize| {
        batches[0]
            .column(i)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    };
    assert_eq!(col(0), 3);
    assert_eq!(col(1), 2);
    assert_eq!(col(2), 1, "exactly one current bob");
    assert_eq!(col(3), 1, "exactly one closed bob");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn mysql_run_scd2_idempotent_when_no_changes() {
    use arrow_array::Int64Array;

    let (_c, b) = mysql_scd2_setup().await;
    b.execute("INSERT INTO src_customers VALUES (1, 'a@x.com', 'alice')")
        .await
        .unwrap();
    let target = mysql_scd2_dim_spec();
    for tag in ["mysql_scd2_idem_1", "mysql_scd2_idem_2"] {
        b.run_scd2(
            &target,
            "SELECT customer_id, email, name FROM src_customers",
            &["customer_id".into()],
            &["email".into(), "name".into()],
            tag,
            None,
            None,
            None,
            None,
            false,
        )
        .await
        .unwrap();
    }
    let s = b
        .read_arrow_stream("SELECT count(*) FROM customer_dim")
        .await
        .unwrap();
    let batches: Vec<_> = s.try_collect().await.unwrap();
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 1, "second run inserts nothing");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn mysql_run_scd2_soft_delete_closes_missing_keys() {
    use arrow_array::Int64Array;
    use ematix_flow_core::meta::DeleteHandling;

    let (_c, b) = mysql_scd2_setup().await;
    b.execute("INSERT INTO src_customers VALUES (1, 'a@x.com', 'alice'), (2, 'b@x.com', 'bob'), (3, 'c@x.com', 'cat')")
        .await
        .unwrap();
    let target = mysql_scd2_dim_spec();
    b.run_scd2(
        &target,
        "SELECT customer_id, email, name FROM src_customers",
        &["customer_id".into()],
        &["email".into(), "name".into()],
        "mysql_scd2_soft_a",
        None,
        None,
        None,
        None,
        false,
    )
    .await
    .unwrap();

    // Drop customer_id=3 from source, expect soft-delete.
    b.execute("DELETE FROM src_customers WHERE customer_id = 3")
        .await
        .unwrap();
    b.run_scd2(
        &target,
        "SELECT customer_id, email, name FROM src_customers",
        &["customer_id".into()],
        &["email".into(), "name".into()],
        "mysql_scd2_soft_b",
        None,
        Some(DeleteHandling::Soft),
        None,
        None,
        false,
    )
    .await
    .unwrap();

    let s = b
        .read_arrow_stream(
            "SELECT \
                CAST(sum(CASE WHEN customer_id=3 AND is_current=1 THEN 1 ELSE 0 END) AS SIGNED), \
                CAST(sum(CASE WHEN customer_id=3 AND is_current=0 THEN 1 ELSE 0 END) AS SIGNED), \
                CAST(sum(CASE WHEN customer_id IN (1,2) AND is_current=1 THEN 1 ELSE 0 END) AS SIGNED) \
             FROM customer_dim",
        )
        .await
        .unwrap();
    let batches: Vec<_> = s.try_collect().await.unwrap();
    let col = |i: usize| {
        batches[0]
            .column(i)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    };
    assert_eq!(col(0), 0, "row 3 no longer current");
    assert_eq!(col(1), 1, "row 3 has one closed version");
    assert_eq!(col(2), 2, "rows 1+2 still current");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn mysql_run_scd2_ttl_expires_stale_current() {
    use arrow_array::Int64Array;

    let (_c, b) = mysql_scd2_setup().await;
    b.execute("INSERT INTO src_customers VALUES (1, 'a@x.com', 'alice')")
        .await
        .unwrap();
    let target = mysql_scd2_dim_spec();
    // First load → 1 current row, valid_from = NOW(6).
    b.run_scd2(
        &target,
        "SELECT customer_id, email, name FROM src_customers",
        &["customer_id".into()],
        &["email".into(), "name".into()],
        "mysql_scd2_ttl",
        None,
        None,
        None,
        None,
        false,
    )
    .await
    .unwrap();
    // Backdate the valid_from to 2 hours ago, then run a no-change SCD2
    // with ttl_seconds=3600. The TTL pass should tombstone the current
    // row even though nothing in the source changed.
    b.execute(
        "UPDATE customer_dim SET valid_from = NOW(6) - INTERVAL 2 HOUR WHERE customer_id = 1",
    )
    .await
    .unwrap();
    b.run_scd2(
        &target,
        "SELECT customer_id, email, name FROM src_customers",
        &["customer_id".into()],
        &["email".into(), "name".into()],
        "mysql_scd2_ttl_2",
        None,
        None,
        None,
        Some(3600),
        false,
    )
    .await
    .unwrap();

    let s = b
        .read_arrow_stream(
            "SELECT CAST(sum(CASE WHEN is_current=1 THEN 1 ELSE 0 END) AS SIGNED), \
                    CAST(sum(CASE WHEN is_current=0 THEN 1 ELSE 0 END) AS SIGNED) \
             FROM customer_dim",
        )
        .await
        .unwrap();
    let batches: Vec<_> = s.try_collect().await.unwrap();
    let cur = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let closed = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(cur, 0, "stale row tombstoned");
    assert_eq!(closed, 1);
}

// ----- Phase 33e: cross-backend Arrow tests for MySQL ↔ {DuckDB, SQLite, PG}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn cross_backend_duckdb_to_mysql_arrow() {
    use arrow_array::Int64Array;
    let (_container, url) = start_mysql().await;
    let duck: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());
    let mysql: Arc<dyn Backend> = Arc::new(MySQLBackend::open(&url).unwrap());

    duck.execute("CREATE SCHEMA s").await.unwrap();
    duck.execute("CREATE TABLE s.events (id BIGINT, name VARCHAR, score DOUBLE)")
        .await
        .unwrap();
    duck.execute(
        "INSERT INTO s.events VALUES \
         (1, 'alice', 1.5), (2, 'bob', 2.5), (3, NULL, 3.5)",
    )
    .await
    .unwrap();
    mysql
        .execute("CREATE TABLE events (id BIGINT, name VARCHAR(64), score DOUBLE)")
        .await
        .unwrap();

    let stream = duck
        .read_arrow_stream("SELECT id, name, score FROM s.events ORDER BY id")
        .await
        .unwrap();
    let target = TargetTable {
        schema: "test".into(),
        name: "events".into(),
    };
    let n = mysql
        .write_arrow_stream(&target, stream, WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(n, 3);

    let s = mysql
        .read_arrow_stream("SELECT count(*) FROM events")
        .await
        .unwrap();
    let batches: Vec<_> = s.try_collect().await.unwrap();
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 3);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn cross_backend_mysql_to_duckdb_arrow() {
    use arrow_array::Int64Array;
    let (_container, url) = start_mysql().await;
    let mysql: Arc<dyn Backend> = Arc::new(MySQLBackend::open(&url).unwrap());
    let duck: Arc<dyn Backend> = Arc::new(DuckDBBackend::open(":memory:").unwrap());

    mysql
        .execute("CREATE TABLE events (id BIGINT, name VARCHAR(64))")
        .await
        .unwrap();
    mysql
        .execute("INSERT INTO events VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();
    duck.execute("CREATE SCHEMA s").await.unwrap();
    duck.execute("CREATE TABLE s.events (id BIGINT, name VARCHAR)")
        .await
        .unwrap();

    let stream = mysql
        .read_arrow_stream("SELECT id, name FROM events ORDER BY id")
        .await
        .unwrap();
    let target = TargetTable {
        schema: "s".into(),
        name: "events".into(),
    };
    let n = duck
        .write_arrow_stream(&target, stream, WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(n, 3);

    let s = duck
        .read_arrow_stream("SELECT count(*)::BIGINT FROM s.events")
        .await
        .unwrap();
    let batches: Vec<_> = s.try_collect().await.unwrap();
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 3);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn cross_backend_sqlite_to_mysql_arrow() {
    use arrow_array::Int64Array;
    let (_container, url) = start_mysql().await;
    let sqlite: Arc<dyn Backend> = Arc::new(SQLiteBackend::open(":memory:").unwrap());
    let mysql: Arc<dyn Backend> = Arc::new(MySQLBackend::open(&url).unwrap());

    sqlite
        .execute("CREATE TABLE events (id INTEGER, name TEXT)")
        .await
        .unwrap();
    sqlite
        .execute("INSERT INTO events VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();
    mysql
        .execute("CREATE TABLE events (id BIGINT, name VARCHAR(64))")
        .await
        .unwrap();

    let stream = sqlite
        .read_arrow_stream("SELECT id, name FROM events ORDER BY id")
        .await
        .unwrap();
    let target = TargetTable {
        schema: "test".into(),
        name: "events".into(),
    };
    let n = mysql
        .write_arrow_stream(&target, stream, WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(n, 3);

    let s = mysql
        .read_arrow_stream("SELECT count(*) FROM events")
        .await
        .unwrap();
    let batches: Vec<_> = s.try_collect().await.unwrap();
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 3);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn cross_backend_mysql_to_sqlite_arrow() {
    use arrow_array::Int64Array;
    let (_container, url) = start_mysql().await;
    let mysql: Arc<dyn Backend> = Arc::new(MySQLBackend::open(&url).unwrap());
    let sqlite: Arc<dyn Backend> = Arc::new(SQLiteBackend::open(":memory:").unwrap());

    mysql
        .execute("CREATE TABLE events (id BIGINT, name VARCHAR(64))")
        .await
        .unwrap();
    mysql
        .execute("INSERT INTO events VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();
    sqlite
        .execute("CREATE TABLE events (id INTEGER, name TEXT)")
        .await
        .unwrap();

    let stream = mysql
        .read_arrow_stream("SELECT id, name FROM events ORDER BY id")
        .await
        .unwrap();
    let target = TargetTable {
        schema: "main".into(),
        name: "events".into(),
    };
    let n = sqlite
        .write_arrow_stream(&target, stream, WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(n, 3);

    let s = sqlite
        .read_arrow_stream("SELECT count(*) FROM events")
        .await
        .unwrap();
    let batches: Vec<_> = s.try_collect().await.unwrap();
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 3);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn cross_backend_pg_to_mysql_arrow() {
    use arrow_array::Int64Array;
    let (_pg_c, pg_url) = start_postgres().await;
    let (_my_c, my_url) = start_mysql().await;
    let pg_pool = Arc::new(PgPool::connect(&pg_url).await.unwrap());
    let pg: Arc<dyn Backend> = Arc::new(PostgresBackend::new(pg_pool, pg_url.clone()));
    let mysql: Arc<dyn Backend> = Arc::new(MySQLBackend::open(&my_url).unwrap());

    pg.execute("CREATE SCHEMA cb_src").await.unwrap();
    pg.execute("CREATE TABLE cb_src.events (id BIGINT, name TEXT, score DOUBLE PRECISION)")
        .await
        .unwrap();
    pg.execute(
        "INSERT INTO cb_src.events VALUES (1, 'alice', 1.5), (2, 'bob', 2.5), (3, NULL, 3.5)",
    )
    .await
    .unwrap();
    mysql
        .execute("CREATE TABLE events (id BIGINT, name VARCHAR(64), score DOUBLE)")
        .await
        .unwrap();

    let stream = pg
        .read_arrow_stream("SELECT id, name, score FROM cb_src.events ORDER BY id")
        .await
        .unwrap();
    let target = TargetTable {
        schema: "test".into(),
        name: "events".into(),
    };
    let n = mysql
        .write_arrow_stream(&target, stream, WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(n, 3);

    let s = mysql
        .read_arrow_stream("SELECT count(*) FROM events")
        .await
        .unwrap();
    let batches: Vec<_> = s.try_collect().await.unwrap();
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 3);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn cross_backend_mysql_to_pg_arrow() {
    let (_pg_c, pg_url) = start_postgres().await;
    let (_my_c, my_url) = start_mysql().await;
    let pg_pool = Arc::new(PgPool::connect(&pg_url).await.unwrap());
    let pg: Arc<dyn Backend> = Arc::new(PostgresBackend::new(pg_pool.clone(), pg_url.clone()));
    let mysql: Arc<dyn Backend> = Arc::new(MySQLBackend::open(&my_url).unwrap());

    mysql
        .execute("CREATE TABLE events (id BIGINT, name VARCHAR(64))")
        .await
        .unwrap();
    mysql
        .execute("INSERT INTO events VALUES (1, 'a'), (2, 'b')")
        .await
        .unwrap();
    pg.execute("CREATE SCHEMA cb_dst").await.unwrap();
    pg.execute("CREATE TABLE cb_dst.events (id BIGINT, name TEXT)")
        .await
        .unwrap();

    let stream = mysql
        .read_arrow_stream("SELECT id, name FROM events ORDER BY id")
        .await
        .unwrap();
    let target = TargetTable {
        schema: "cb_dst".into(),
        name: "events".into(),
    };
    let n = pg
        .write_arrow_stream(&target, stream, WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(n, 2);
    let count = pg_pool
        .fetch_scalar_int("SELECT count(*)::int FROM cb_dst.events")
        .await
        .unwrap();
    assert_eq!(count, 2);
}

// ----- Phase 34e: ObjectStore on S3 / MinIO -----------------------------

use ematix_flow_core::ObjectStoreBackend;
use ematix_flow_core::backend::ObjectFormat;
use testcontainers::core::ExecCommand;
use testcontainers_modules::minio::MinIO;

const MINIO_ACCESS_KEY: &str = "minioadmin";
const MINIO_SECRET_KEY: &str = "minioadmin";
const MINIO_REGION: &str = "us-east-1";

/// Start a MinIO container with a pre-created bucket. Returns the
/// container guard (drop = stop), endpoint URL, and bucket name. The
/// bucket is created by `mkdir -p /data/<bucket>` inside the container
/// — minio's filesystem layout maps directories under `/data` to
/// buckets, so this is the cheapest "CreateBucket" we can do without
/// pulling in `aws-sdk-s3` for tests.
async fn start_minio_with_bucket(bucket: &str) -> (testcontainers::ContainerAsync<MinIO>, String) {
    let container = MinIO::default()
        .start()
        .await
        .expect("failed to start minio testcontainer");
    container
        .exec(ExecCommand::new([
            "mkdir",
            "-p",
            &format!("/data/{bucket}"),
        ]))
        .await
        .expect("failed to mkdir bucket inside minio container");
    let host = container
        .get_host()
        .await
        .expect("failed to read host")
        .to_string();
    let port = container
        .get_host_port_ipv4(9000)
        .await
        .expect("failed to read port");
    let endpoint = format!("http://{host}:{port}");
    (container, endpoint)
}

async fn minio_round_trip(format: ObjectFormat, bucket: &str) {
    use arrow_array::{Int64Array, RecordBatch as RB, StringArray};
    use arrow_schema::{DataType as Dt, Field as F, Schema as S};
    use std::sync::Arc as A;

    let (_container, endpoint) = start_minio_with_bucket(bucket).await;
    let backend = ObjectStoreBackend::open_s3(
        &endpoint,
        bucket,
        MINIO_REGION,
        MINIO_ACCESS_KEY,
        MINIO_SECRET_KEY,
        format,
    )
    .unwrap();
    backend.ping().await.unwrap();

    let schema = A::new(S::new(vec![
        F::new("id", Dt::Int64, true),
        F::new("name", Dt::Utf8, true),
    ]));
    let batch = RB::try_new(
        schema,
        vec![
            A::new(Int64Array::from(vec![1, 2, 3])),
            A::new(StringArray::from(vec!["alice", "bob", "carol"])),
        ],
    )
    .unwrap();
    let stream = futures_util::stream::once(async move { Ok::<_, _>(batch) });
    let stream: ematix_flow_core::backend::ArrowBatchStream = Box::pin(stream);

    let target = TargetTable {
        schema: "raw".into(),
        name: "events".into(),
    };
    let n = backend
        .write_arrow_stream(&target, stream, WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(n, 3, "wrote 3 rows on {format:?}");

    let stream = backend.read_arrow_stream("raw/events").await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3, "round-tripped 3 rows on {format:?}");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn minio_parquet_round_trip() {
    minio_round_trip(ObjectFormat::Parquet, "test-parquet").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn minio_csv_round_trip() {
    minio_round_trip(ObjectFormat::Csv, "test-csv").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn minio_jsonl_round_trip() {
    minio_round_trip(ObjectFormat::JsonLines, "test-jsonl").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn minio_orc_round_trip() {
    minio_round_trip(ObjectFormat::Orc, "test-orc").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn minio_truncate_clears_prefix() {
    use arrow_array::{Int64Array, RecordBatch as RB, StringArray};
    use arrow_schema::{DataType as Dt, Field as F, Schema as S};
    use std::sync::Arc as A;

    let (_c, endpoint) = start_minio_with_bucket("test-trunc").await;
    let backend = ObjectStoreBackend::open_s3(
        &endpoint,
        "test-trunc",
        MINIO_REGION,
        MINIO_ACCESS_KEY,
        MINIO_SECRET_KEY,
        ObjectFormat::Parquet,
    )
    .unwrap();
    let target = TargetTable {
        schema: "raw".into(),
        name: "events".into(),
    };
    let make_stream = || {
        let schema = A::new(S::new(vec![
            F::new("id", Dt::Int64, true),
            F::new("name", Dt::Utf8, true),
        ]));
        let batch = RB::try_new(
            schema,
            vec![
                A::new(Int64Array::from(vec![1, 2, 3])),
                A::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        let s = futures_util::stream::once(async move { Ok::<_, _>(batch) });
        let s: ematix_flow_core::backend::ArrowBatchStream = Box::pin(s);
        s
    };
    backend
        .write_arrow_stream(&target, make_stream(), WriteMode::Append)
        .await
        .unwrap();
    backend
        .write_arrow_stream(&target, make_stream(), WriteMode::Append)
        .await
        .unwrap();
    backend
        .write_arrow_stream(&target, make_stream(), WriteMode::Truncate)
        .await
        .unwrap();
    let stream = backend.read_arrow_stream("raw/events").await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3, "old files removed by truncate on MinIO");
}

// ----- Phase 35f: Delta on MinIO ----------------------------------------

use ematix_flow_core::DeltaBackend;
use ematix_flow_core::types::{ColumnSpec as Cs2, ColumnType as Ct2};

const DELTA_S3_REGION: &str = "us-east-1";

fn delta_test_spec() -> TableSpec {
    TableSpec {
        schema: "raw".into(),
        name: "events".into(),
        columns: vec![
            Cs2 {
                name: "id".into(),
                ty: Ct2::BigInt,
                nullable: false,
                primary_key: false,
            },
            Cs2 {
                name: "name".into(),
                ty: Ct2::Text,
                nullable: true,
                primary_key: false,
            },
        ],
        unique_constraints: vec![],
        fingerprint: String::new(),
    }
}

async fn duckdb_with_simple_events() -> Arc<dyn Backend> {
    let duck: Arc<dyn Backend> =
        Arc::new(ematix_flow_core::DuckDBBackend::open(":memory:").unwrap());
    duck.execute("CREATE SCHEMA s").await.unwrap();
    duck.execute("CREATE TABLE s.events (id BIGINT, name VARCHAR)")
        .await
        .unwrap();
    duck.execute("INSERT INTO s.events VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();
    duck
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn delta_minio_run_append_round_trip() {
    use arrow_array::Int64Array;

    let bucket = "delta-append";
    let (_container, endpoint) = start_minio_with_bucket(bucket).await;
    let target = DeltaBackend::open_s3(
        &endpoint,
        bucket,
        "",
        DELTA_S3_REGION,
        MINIO_ACCESS_KEY,
        MINIO_SECRET_KEY,
    )
    .unwrap();
    target.ping().await.unwrap();
    let source = duckdb_with_simple_events().await;
    let spec = delta_test_spec();

    let r = target
        .run_append(
            &spec,
            "SELECT id, name FROM s.events ORDER BY id",
            "minio_delta_append",
            Some(source.as_ref()),
            None,
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(r.rows_inserted, 3);
    assert_eq!(r.status, "success");

    let stream = target.read_arrow_stream("raw/events").await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3);

    // Spot-check row contents — the bigint column round-tripped.
    let id = batches[0]
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut ids: Vec<i64> = (0..id.len()).map(|i| id.value(i)).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3]);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn delta_minio_run_truncate_replaces() {
    let bucket = "delta-trunc";
    let (_container, endpoint) = start_minio_with_bucket(bucket).await;
    let target = DeltaBackend::open_s3(
        &endpoint,
        bucket,
        "",
        DELTA_S3_REGION,
        MINIO_ACCESS_KEY,
        MINIO_SECRET_KEY,
    )
    .unwrap();
    let source = duckdb_with_simple_events().await;
    let spec = delta_test_spec();

    // Two appends → 6 rows in two commits.
    for tag in ["s3_a", "s3_b"] {
        target
            .run_append(
                &spec,
                "SELECT id, name FROM s.events ORDER BY id",
                tag,
                Some(source.as_ref()),
                None,
                None,
                false,
            )
            .await
            .unwrap();
    }
    let r = target
        .run_truncate(
            &spec,
            "SELECT id, name FROM s.events ORDER BY id",
            "s3_trunc",
            Some(source.as_ref()),
            false,
        )
        .await
        .unwrap();
    assert_eq!(r.rows_inserted, 3);

    let stream = target.read_arrow_stream("raw/events").await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3, "Overwrite replaced both seed commits on S3");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn delta_minio_run_merge_inserts_and_updates() {
    let bucket = "delta-merge";
    let (_container, endpoint) = start_minio_with_bucket(bucket).await;
    let target = DeltaBackend::open_s3(
        &endpoint,
        bucket,
        "",
        DELTA_S3_REGION,
        MINIO_ACCESS_KEY,
        MINIO_SECRET_KEY,
    )
    .unwrap();
    let source = duckdb_with_simple_events().await;
    let spec = delta_test_spec();
    // Seed.
    target
        .run_append(
            &spec,
            "SELECT id, name FROM s.events ORDER BY id",
            "seed",
            Some(source.as_ref()),
            None,
            None,
            false,
        )
        .await
        .unwrap();
    // Mutate source: id=2 changes, id=4 added.
    source
        .execute("UPDATE s.events SET name = 'b-updated' WHERE id = 2")
        .await
        .unwrap();
    source
        .execute("INSERT INTO s.events VALUES (4, 'd')")
        .await
        .unwrap();
    let r = target
        .run_merge(
            &spec,
            "SELECT id, name FROM s.events ORDER BY id",
            &["id".into()],
            &["name".into()],
            "s3_merge",
            "merge",
            Some(source.as_ref()),
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(r.rows_inserted, 1, "id=4 inserted");
    assert_eq!(r.rows_updated, Some(1), "id=2 updated");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn delta_minio_run_scd2_first_load() {
    let bucket = "delta-scd2";
    let (_container, endpoint) = start_minio_with_bucket(bucket).await;
    let target = DeltaBackend::open_s3(
        &endpoint,
        bucket,
        "",
        DELTA_S3_REGION,
        MINIO_ACCESS_KEY,
        MINIO_SECRET_KEY,
    )
    .unwrap();
    let source = duckdb_with_simple_events().await;
    let spec = delta_test_spec();
    let r = target
        .run_scd2(
            &spec,
            "SELECT id, name FROM s.events ORDER BY id",
            &["id".into()],
            &["name".into()],
            "s3_scd2_first",
            Some(source.as_ref()),
            None,
            None,
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(r.rows_inserted, 3);
    assert_eq!(r.rows_closed, Some(0));

    // Read back: 3 rows, all current.
    let stream = target.read_arrow_stream("raw/events").await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3);
}

// ----- Phase 36a: Kafka skeleton + ping ---------------------------------

use ematix_flow_core::KafkaBackend;
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};

async fn start_kafka() -> (testcontainers::ContainerAsync<Kafka>, String) {
    use testcontainers::ImageExt;
    // The default apache:3.8 container only sets
    // KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1; the
    // transaction-state-log topic still defaults to RF=3 which is
    // unsatisfiable on a single-broker setup, so any
    // init_transactions() call hangs until timeout. Override here
    // so Phase 36j tests pass alongside the rest.
    let container = Kafka::default()
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .start()
        .await
        .expect("failed to start kafka testcontainer");
    let host = container
        .get_host()
        .await
        .expect("failed to read kafka host")
        .to_string();
    let port = container
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("failed to read kafka port");
    let bootstrap = format!("{host}:{port}");
    (container, bootstrap)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn kafka_backend_ping_against_apache() {
    let (_container, bootstrap) = start_kafka().await;
    let backend = KafkaBackend::open(&bootstrap, None).unwrap();
    backend.ping().await.unwrap();
    assert!(matches!(
        backend.dialect(),
        ematix_flow_core::backend::Dialect::Streaming { .. }
    ));
    let info = backend.connection_info();
    assert_eq!(info.user, "producer", "no group_id → producer label");
    assert_eq!(info.dbname, bootstrap);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn kafka_backend_ping_consumer_group() {
    let (_container, bootstrap) = start_kafka().await;
    let backend = KafkaBackend::open(&bootstrap, Some("test-group")).unwrap();
    backend.ping().await.unwrap();
    assert_eq!(backend.connection_info().user, "test-group");
}

// ----- Phase 36b: Kafka consume read_arrow_stream -----------------------

use rdkafka::ClientConfig as KafkaClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration as StdDuration;

async fn produce_json_messages(bootstrap: &str, topic: &str, payloads: &[&str]) {
    let producer: FutureProducer = KafkaClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("kafka producer create");
    for payload in payloads {
        producer
            .send(
                FutureRecord::<(), str>::to(topic).payload(*payload),
                StdDuration::from_secs(5),
            )
            .await
            .expect("kafka produce");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn kafka_read_arrow_stream_consumes_json_messages() {
    use arrow_array::{Int64Array, StringArray};

    let (_container, bootstrap) = start_kafka().await;
    let topic = "test-events";
    produce_json_messages(
        &bootstrap,
        topic,
        &[
            r#"{"id": 1, "name": "alice"}"#,
            r#"{"id": 2, "name": "bob"}"#,
            r#"{"id": 3, "name": "carol"}"#,
        ],
    )
    .await;

    let backend = KafkaBackend::open(&bootstrap, Some("test-consume")).unwrap();
    let stream = backend.read_arrow_stream(topic).await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3);

    // arrow-json infers schema from message bodies; numeric fields
    // come back as Int64, string fields as Utf8.
    let id = batches[0]
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let name = batches[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut pairs: Vec<(i64, String)> = (0..id.len())
        .map(|i| (id.value(i), name.value(i).to_string()))
        .collect();
    pairs.sort_by_key(|(k, _)| *k);
    assert_eq!(
        pairs,
        vec![(1, "alice".into()), (2, "bob".into()), (3, "carol".into())]
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn kafka_read_arrow_stream_empty_topic() {
    let (_container, bootstrap) = start_kafka().await;
    // Topic doesn't exist yet; subscription auto-creates it (broker
    // default), no messages → empty stream after the idle timeout.
    let backend = KafkaBackend::open(&bootstrap, Some("test-empty")).unwrap();
    let stream = backend.read_arrow_stream("never-produced").await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_read_arrow_stream_requires_group_id() {
    let backend = KafkaBackend::open("localhost:9092", None).unwrap();
    let err = match backend.read_arrow_stream("any-topic").await {
        Ok(_) => panic!("expected group_id rejection"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(msg.contains("group_id is required"), "got: {msg}");
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_read_arrow_stream_rejects_empty_topic() {
    let backend = KafkaBackend::open("localhost:9092", Some("g")).unwrap();
    let err = match backend.read_arrow_stream("   ").await {
        Ok(_) => panic!("expected empty-topic rejection"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(msg.contains("non-empty topic"), "got: {msg}");
}

// ----- Phase 36c: Kafka produce write_arrow_stream / run_append ----------

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn kafka_write_arrow_stream_produces_json_messages() {
    use arrow_array::{Int64Array, RecordBatch as RB, StringArray};
    use arrow_schema::{DataType as Dt, Field as F, Schema as S};

    let (_container, bootstrap) = start_kafka().await;
    let producer = KafkaBackend::open(&bootstrap, None).unwrap();

    let schema = std::sync::Arc::new(S::new(vec![
        F::new("id", Dt::Int64, true),
        F::new("name", Dt::Utf8, true),
    ]));
    let batch = RB::try_new(
        schema,
        vec![
            std::sync::Arc::new(Int64Array::from(vec![1, 2, 3])),
            std::sync::Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
        ],
    )
    .unwrap();
    let stream = futures_util::stream::once(async move { Ok::<_, _>(batch) });
    let stream: ematix_flow_core::backend::ArrowBatchStream = Box::pin(stream);

    let target = TargetTable {
        schema: "".into(),
        name: "produce-test".into(),
    };
    let n = producer
        .write_arrow_stream(&target, stream, WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(n, 3);

    // Round-trip: read them back via the consumer (36b).
    let consumer = KafkaBackend::open(&bootstrap, Some("produce-test-consume")).unwrap();
    let stream = consumer.read_arrow_stream("produce-test").await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn kafka_run_append_from_duckdb_to_topic() {
    use arrow_array::Int64Array;

    let (_container, bootstrap) = start_kafka().await;
    let producer: Arc<dyn Backend> = Arc::new(KafkaBackend::open(&bootstrap, None).unwrap());
    let source: Arc<dyn Backend> =
        Arc::new(ematix_flow_core::DuckDBBackend::open(":memory:").unwrap());
    source.execute("CREATE SCHEMA s").await.unwrap();
    source
        .execute("CREATE TABLE s.events (id BIGINT, name VARCHAR)")
        .await
        .unwrap();
    source
        .execute("INSERT INTO s.events VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();

    let spec = TableSpec {
        schema: "".into(),
        name: "appended-test".into(),
        columns: vec![
            ColumnSpec {
                name: "id".into(),
                ty: ColumnType::BigInt,
                nullable: false,
                primary_key: false,
            },
            ColumnSpec {
                name: "name".into(),
                ty: ColumnType::Text,
                nullable: true,
                primary_key: false,
            },
        ],
        unique_constraints: vec![],
        fingerprint: String::new(),
    };
    let r = producer
        .run_append(
            &spec,
            "SELECT id, name FROM s.events ORDER BY id",
            "k36c_run_append",
            Some(source.as_ref()),
            None,
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(r.rows_inserted, 3);
    assert!(r.path.contains("appended-test"), "got path: {}", r.path);
    assert_eq!(r.status, "success");

    // Verify on the consumer side.
    let consumer = KafkaBackend::open(&bootstrap, Some("appended-test-consume")).unwrap();
    let stream = consumer.read_arrow_stream("appended-test").await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3);

    // Spot check a value.
    let id = batches[0]
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut ids: Vec<i64> = (0..id.len()).map(|i| id.value(i)).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3]);
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_write_arrow_stream_rejects_truncate() {
    use arrow_array::Int64Array;
    use arrow_schema::{DataType as Dt, Field as F, Schema as S};

    let backend = KafkaBackend::open("localhost:9092", None).unwrap();
    let schema = std::sync::Arc::new(S::new(vec![F::new("x", Dt::Int64, true)]));
    let batch = arrow_array::RecordBatch::try_new(
        schema,
        vec![std::sync::Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let stream = futures_util::stream::once(async move { Ok::<_, _>(batch) });
    let stream: ematix_flow_core::backend::ArrowBatchStream = Box::pin(stream);
    let target = TargetTable {
        schema: "".into(),
        name: "any".into(),
    };
    let err = backend
        .write_arrow_stream(&target, stream, WriteMode::Truncate)
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("Truncate is not supported"), "got: {msg}");
}

// ----- Phase 36d: Consumer batching ------------------------------------

use ematix_flow_core::kafka_backend::KafkaBatchConfig;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn kafka_read_arrow_stream_honors_batch_size() {
    let (_container, bootstrap) = start_kafka().await;
    let topic = "batch-size-test";

    // Produce 10 messages.
    let payloads: Vec<String> = (0..10)
        .map(|i| format!(r#"{{"id": {i}, "name": "n{i}"}}"#))
        .collect();
    let payload_refs: Vec<&str> = payloads.iter().map(|s| s.as_str()).collect();
    produce_json_messages(&bootstrap, topic, &payload_refs).await;

    // Configure a batch_size of 4 — read_arrow_stream should return
    // 4 rows even though 10 are available.
    let backend = KafkaBackend::open(&bootstrap, Some("batch-size-grp"))
        .unwrap()
        .with_batch_config(KafkaBatchConfig {
            batch_size: 4,
            ..Default::default()
        });
    let stream = backend.read_arrow_stream(topic).await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 4, "batch_size cap must fire before drain idle");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn kafka_read_arrow_stream_honors_batch_window_ms() {
    let (_container, bootstrap) = start_kafka().await;
    let topic = "batch-window-test";

    // Produce 2 messages once, wait, then produce more after the
    // batch window closes — this verifies the window-since-first-msg
    // clock fires before we accumulate more.
    produce_json_messages(&bootstrap, topic, &[r#"{"id": 1}"#, r#"{"id": 2}"#]).await;

    // batch_window_ms=200 → after 200ms from the first message, even
    // if more arrive, we flush. We immediately produce nothing for
    // the rest of the window so the batch flushes at 200ms with 2
    // rows. idle_timeout_ms=10s would otherwise wait far longer.
    let backend = KafkaBackend::open(&bootstrap, Some("batch-window-grp"))
        .unwrap()
        .with_batch_config(KafkaBatchConfig {
            batch_size: 1_000,
            batch_window_ms: 200,
            idle_timeout_ms: 10_000,
            ..Default::default()
        });
    let started = std::time::Instant::now();
    let stream = backend.read_arrow_stream(topic).await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let elapsed = started.elapsed();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2, "got 2 rows from initial produce");
    // Window elapsed at 200ms after the first message; first-message
    // wait can take up to 15s, but the window is the dominant cap
    // once a message arrives. Generous upper bound to avoid flake.
    assert!(
        elapsed < std::time::Duration::from_secs(20),
        "elapsed too long ({elapsed:?}); idle_timeout dominated instead of window"
    );
}

// ----- Phase 36e: Manual offset commits + at-least-once ----------------

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn kafka_at_least_once_no_commit_redelivers() {
    let (_container, bootstrap) = start_kafka().await;
    let topic = "atleastonce-redeliver";
    let group = "atleastonce-redeliver-grp";

    produce_json_messages(
        &bootstrap,
        topic,
        &[r#"{"id": 1}"#, r#"{"id": 2}"#, r#"{"id": 3}"#],
    )
    .await;

    // Session 1: read 3, do NOT commit, drop the backend.
    {
        let backend = KafkaBackend::open(&bootstrap, Some(group)).unwrap();
        let stream = backend.read_arrow_stream(topic).await.unwrap();
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
        assert!(backend.pending_offset_count() > 0, "offsets pending");
        // Drop without commit_offsets() — uncommitted reads should
        // be re-delivered on the next session.
    }

    // Session 2: same group_id, commit this time.
    let backend = KafkaBackend::open(&bootstrap, Some(group)).unwrap();
    let stream = backend.read_arrow_stream(topic).await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3, "messages re-delivered because no prior commit");
    backend.commit_offsets().await.unwrap();
    assert_eq!(
        backend.pending_offset_count(),
        0,
        "pending offsets cleared after commit"
    );
    drop(backend);

    // Session 3: same group_id, no new produces — should see 0
    // because session 2 committed.
    let backend = KafkaBackend::open(&bootstrap, Some(group)).unwrap();
    let stream = backend.read_arrow_stream(topic).await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 0, "committed offsets advance the consumer");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn kafka_commit_offsets_no_op_with_no_consumer() {
    let (_container, bootstrap) = start_kafka().await;
    // Backend with group_id but never read — commit should be a no-op.
    let backend = KafkaBackend::open(&bootstrap, Some("commit-noop")).unwrap();
    backend.commit_offsets().await.unwrap();
}

// ----- Phase 36g: StreamingPipeline (Kafka → SQLite end-to-end) ----------

use ematix_flow_core::streaming::{ShutdownSignal, StreamingPipeline, StreamingPipelineConfig};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn streaming_pipeline_kafka_to_sqlite_end_to_end() {
    use arrow_array::Int64Array;

    let (_container, bootstrap) = start_kafka().await;
    let topic = "stream-pipeline-test";

    // Produce 7 messages with id field.
    let payloads: Vec<String> = (1..=7).map(|i| format!(r#"{{"id": {i}}}"#)).collect();
    let payload_refs: Vec<&str> = payloads.iter().map(|s| s.as_str()).collect();
    produce_json_messages(&bootstrap, topic, &payload_refs).await;

    // Source: Kafka with a tight batch config so the loop iterates
    // a few times before draining. Tight idle_pause so empty-batch
    // sleeps don't dominate the test wall time.
    let source: Arc<dyn Backend> = Arc::new(
        ematix_flow_core::KafkaBackend::open(&bootstrap, Some("stream-pipeline-grp"))
            .unwrap()
            .with_batch_config(ematix_flow_core::kafka_backend::KafkaBatchConfig {
                batch_size: 3,
                idle_timeout_ms: 1_500,
                batch_window_ms: 10_000,
                ..Default::default()
            }),
    );

    // Target: in-memory SQLite. Schema must match the JSON payload —
    // arrow-json infers `id` as Int64.
    let target_backend: Arc<dyn Backend> =
        Arc::new(ematix_flow_core::SQLiteBackend::open(":memory:").unwrap());
    target_backend
        .execute("CREATE TABLE events (id BIGINT)")
        .await
        .unwrap();

    let config = StreamingPipelineConfig::new(
        topic,
        TargetTable {
            schema: "main".into(),
            name: "events".into(),
        },
        "stream-test",
    );

    let pipeline = StreamingPipeline::new(source, Arc::clone(&target_backend), config);

    // Drive the pipeline; trigger shutdown after a short delay so it
    // gets a chance to drain the topic. 5 seconds covers Kafka
    // rebalance (15s first-message timeout) — but we expect the
    // first read to hit before that.
    let (sig, trigger) = ShutdownSignal::new();
    let pipeline_handle = tokio::spawn(async move { pipeline.run(sig).await });

    // Poll the target until we see all 7 rows or hit a timeout.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        if std::time::Instant::now() >= deadline {
            break;
        }
        let stream = target_backend
            .read_arrow_stream("SELECT count(*) FROM events")
            .await
            .unwrap();
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        if n >= 7 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    trigger.trigger();
    let metrics = pipeline_handle.await.unwrap().unwrap();

    assert!(metrics.shutdown_triggered);
    assert_eq!(
        metrics.total_rows, 7,
        "all 7 produced rows landed in target"
    );
    assert!(
        metrics.iterations >= 1,
        "at least one read→write cycle ran (got {})",
        metrics.iterations
    );

    // Sanity: target has exactly 7 rows.
    let stream = target_backend
        .read_arrow_stream("SELECT count(*) FROM events")
        .await
        .unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 7);
}

// ----- Phase 36h: payload format (RawBytes round-trip) ------------------

use ematix_flow_core::kafka_backend::KafkaPayloadFormat;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn kafka_raw_bytes_round_trip() {
    use arrow_array::Array;
    use arrow_array::BinaryArray;
    use arrow_schema::{DataType as Dt, Field as F, Schema as S};

    let (_container, bootstrap) = start_kafka().await;
    let topic = "raw-bytes-test";

    // Producer side: a 1-column Binary RecordBatch with three rows.
    let producer = KafkaBackend::open(&bootstrap, None)
        .unwrap()
        .with_payload_format(KafkaPayloadFormat::RawBytes);
    let schema = std::sync::Arc::new(S::new(vec![F::new("payload", Dt::Binary, false)]));
    let batch = arrow_array::RecordBatch::try_new(
        schema,
        vec![std::sync::Arc::new(BinaryArray::from_vec(vec![
            b"\x01\x02\x03",
            b"hello world",
            b"\xff",
        ]))],
    )
    .unwrap();
    let stream = futures_util::stream::once(async move { Ok::<_, _>(batch) });
    let stream: ematix_flow_core::backend::ArrowBatchStream = Box::pin(stream);
    let target = TargetTable {
        schema: "".into(),
        name: topic.into(),
    };
    let n = producer
        .write_arrow_stream(&target, stream, WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(n, 3);

    // Consumer side: same format yields one row per message with a
    // single Binary column.
    let consumer = KafkaBackend::open(&bootstrap, Some("raw-bytes-grp"))
        .unwrap()
        .with_payload_format(KafkaPayloadFormat::RawBytes);
    let stream = consumer.read_arrow_stream(topic).await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3);

    // Byte-level round trip — bytes flow through unchanged.
    let arr = batches[0]
        .column_by_name("payload")
        .unwrap()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let mut got: Vec<&[u8]> = (0..arr.len()).map(|i| arr.value(i)).collect();
    // Order across partitions isn't guaranteed; sort for stable
    // comparison.
    got.sort();
    let mut expected: Vec<&[u8]> = vec![b"\x01\x02\x03", b"hello world", b"\xff"];
    expected.sort();
    assert_eq!(got, expected);
}

// ----- Phase 36i: DLQ end-to-end ---------------------------------------

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn streaming_pipeline_routes_failed_batch_to_dlq() {
    let (_container, bootstrap) = start_kafka().await;
    let primary_topic = "dlq-primary";
    let dlq_topic = "dlq-dead-letters";

    // Produce 3 JSON messages to the primary topic.
    produce_json_messages(
        &bootstrap,
        primary_topic,
        &[
            r#"{"id": 1, "name": "alice"}"#,
            r#"{"id": 2, "name": "bob"}"#,
            r#"{"id": 3, "name": "carol"}"#,
        ],
    )
    .await;

    // Source: Kafka, JSON-formatted, tight batch. Same handle is the
    // DLQ producer.
    let source: Arc<dyn Backend> = Arc::new(
        ematix_flow_core::KafkaBackend::open(&bootstrap, Some("dlq-test-grp"))
            .unwrap()
            .with_batch_config(ematix_flow_core::kafka_backend::KafkaBatchConfig {
                batch_size: 100,
                idle_timeout_ms: 1_500,
                batch_window_ms: 5_000,
                ..Default::default()
            }),
    );

    // Target: SQLite with a schema mismatch — only allows column `x`,
    // so any insert from `{id, name}` data fails.
    let target_backend: Arc<dyn Backend> =
        Arc::new(ematix_flow_core::SQLiteBackend::open(":memory:").unwrap());
    target_backend
        .execute("CREATE TABLE wrong_schema (x INTEGER)")
        .await
        .unwrap();

    let cfg = StreamingPipelineConfig::new(
        primary_topic,
        TargetTable {
            schema: "main".into(),
            name: "wrong_schema".into(),
        },
        "dlq-test",
    )
    .with_dead_letter_topic(dlq_topic);

    let pipeline = StreamingPipeline::new(source, target_backend, cfg);

    let (sig, trigger) = ShutdownSignal::new();
    let pipeline_handle = tokio::spawn(async move { pipeline.run(sig).await });

    // Wait long enough for the pipeline to consume + fail + DLQ +
    // commit + reach the next idle iteration where it'll find
    // nothing left.
    tokio::time::sleep(std::time::Duration::from_secs(8)).await;
    trigger.trigger();
    let metrics = pipeline_handle.await.unwrap().unwrap();

    assert!(metrics.shutdown_triggered);
    assert!(
        metrics.total_rows >= 3,
        "got {} total_rows; expected at least 3",
        metrics.total_rows
    );

    // Read back the DLQ topic via a separate consumer. The DLQ rows
    // are JSON (the source's payload format), one per original row.
    let dlq_consumer =
        ematix_flow_core::KafkaBackend::open(&bootstrap, Some("dlq-verify-grp")).unwrap();
    let stream = dlq_consumer.read_arrow_stream(dlq_topic).await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let dlq_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        dlq_rows, 3,
        "all 3 failed rows should have been routed to the DLQ"
    );
}

// ----- Phase 36j: Exactly-once produce ----------------------------------

use ematix_flow_core::kafka_backend::KafkaDeliverySemantics;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn kafka_exactly_once_produce_round_trip() {
    use arrow_array::{Int64Array, RecordBatch as RB, StringArray};
    use arrow_schema::{DataType as Dt, Field as F, Schema as S};

    let (_container, bootstrap) = start_kafka().await;
    let topic = "eos-produce-test";

    // Random transactional id so re-running this test against the
    // same broker doesn't fence by colliding with a prior id.
    let transactional_id = format!(
        "eos-test-{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let producer = KafkaBackend::open(&bootstrap, None)
        .unwrap()
        .with_delivery_semantics(KafkaDeliverySemantics::ExactlyOnce { transactional_id });

    // Produce two batches in one write_arrow_stream call. Each
    // batch is wrapped in its own Kafka transaction.
    let schema = std::sync::Arc::new(S::new(vec![
        F::new("id", Dt::Int64, true),
        F::new("name", Dt::Utf8, true),
    ]));
    let batch1 = RB::try_new(
        schema.clone(),
        vec![
            std::sync::Arc::new(Int64Array::from(vec![1, 2])),
            std::sync::Arc::new(StringArray::from(vec!["alice", "bob"])),
        ],
    )
    .unwrap();
    let batch2 = RB::try_new(
        schema,
        vec![
            std::sync::Arc::new(Int64Array::from(vec![3])),
            std::sync::Arc::new(StringArray::from(vec!["carol"])),
        ],
    )
    .unwrap();
    let batches = vec![batch1, batch2];
    let stream = futures_util::stream::iter(batches.into_iter().map(Ok));
    let stream: ematix_flow_core::backend::ArrowBatchStream = Box::pin(stream);

    let target = TargetTable {
        schema: "".into(),
        name: topic.into(),
    };
    let n = producer
        .write_arrow_stream(&target, stream, WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(n, 3);

    // Verify the rows landed and are committed (read.committed ON
    // by default for our consumer).
    let consumer = KafkaBackend::open(&bootstrap, Some("eos-verify-grp")).unwrap();
    let stream = consumer.read_arrow_stream(topic).await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3);
}

// ----- Phase 36j.2: Kafka→Kafka EOS pipeline ----------------------------

use ematix_flow_core::streaming::KafkaToKafkaEosPipeline;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn eos_pipeline_kafka_to_kafka_round_trip() {
    use arrow_array::Int64Array;

    let (_container, bootstrap) = start_kafka().await;
    let input_topic = "eos-pipeline-in";
    let output_topic = "eos-pipeline-out";

    // Seed input with 5 messages.
    let payloads: Vec<String> = (1..=5).map(|i| format!(r#"{{"id": {i}}}"#)).collect();
    let payload_refs: Vec<&str> = payloads.iter().map(|s| s.as_str()).collect();
    produce_json_messages(&bootstrap, input_topic, &payload_refs).await;

    let source = Arc::new(
        ematix_flow_core::KafkaBackend::open(&bootstrap, Some("eos-pipeline-grp"))
            .unwrap()
            .with_batch_config(ematix_flow_core::kafka_backend::KafkaBatchConfig {
                batch_size: 100,
                batch_window_ms: 1_000,
                idle_timeout_ms: 1_500,
                ..Default::default()
            }),
    );

    let transactional_id = format!(
        "eos-pipeline-{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let target = Arc::new(
        ematix_flow_core::KafkaBackend::open(&bootstrap, None)
            .unwrap()
            .with_delivery_semantics(KafkaDeliverySemantics::ExactlyOnce { transactional_id }),
    );

    let config = StreamingPipelineConfig::new(
        input_topic,
        TargetTable {
            schema: "".into(),
            name: output_topic.into(),
        },
        "eos-test",
    );
    let pipeline = KafkaToKafkaEosPipeline::new(source.clone(), target, config).unwrap();

    let (sig, trigger) = ShutdownSignal::new();
    let pipeline_handle = tokio::spawn(async move { pipeline.run(sig).await });

    // Give the pipeline time to consume + produce + commit.
    tokio::time::sleep(std::time::Duration::from_secs(8)).await;
    trigger.trigger();
    let metrics = pipeline_handle.await.unwrap().unwrap();

    assert!(metrics.shutdown_triggered);
    assert_eq!(metrics.total_rows, 5);

    // Verify output topic has exactly 5 messages (no duplicates).
    let verifier =
        ematix_flow_core::KafkaBackend::open(&bootstrap, Some("eos-verify-grp")).unwrap();
    let stream = verifier.read_arrow_stream(output_topic).await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let n_out: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(n_out, 5, "output topic must have exactly 5 rows");

    // Spot check: ids 1..=5 in the output.
    let mut ids: Vec<i64> = Vec::new();
    for batch in &batches {
        let arr = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..arr.len() {
            ids.push(arr.value(i));
        }
    }
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3, 4, 5]);

    // Verify source-side offsets advanced (the EOS commit did
    // send_offsets_to_transaction). A new consumer in the same group
    // should see no messages.
    let recheck =
        ematix_flow_core::KafkaBackend::open(&bootstrap, Some("eos-pipeline-grp")).unwrap();
    let stream = recheck.read_arrow_stream(input_topic).await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let n_remaining: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        n_remaining, 0,
        "source consumer's offsets should have advanced via send_offsets_to_transaction"
    );
}
