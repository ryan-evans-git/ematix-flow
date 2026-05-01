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
