//! Σ.A2 PR 5: end-to-end correctness gate for the DuckDB translator.
//!
//! Mirrors `dialect_spark_e2e.rs`: pipe DuckDB SQL through
//! `dialect::translate(_, DuckDb)`, hand the output to a real
//! DataFusion `SessionContext`, verify row counts + scalar values
//! against the canonical TPC-H reference. Confirms that the (much
//! narrower) DuckDB translator + DataFusion's planner together
//! handle the same TPC-H representative set Σ.A1 ran.
//!
//! Panics with a generate-from-here pointer if SF=1 Parquet is
//! missing — same shape as the Spark e2e harness.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::prelude::SessionContext;
use ematix_flow_core::dialect::{Dialect, translate};

fn data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(|root| root.join("examples/tpch/data/sf1"))
        .expect("workspace root")
}

/// Returns `None` when the SF=1 Parquet directory is missing.
/// CI doesn't generate the data (it's `.gitignore`d, ~320 MB of
/// regeneratable Parquet); forcing a panic when the optional
/// artifact is absent breaks the release workflow's `cargo test
/// --workspace --all-targets` step. Local devs who *have* run
/// `tpch_generate` still get the full assertion path; CI emits a
/// `skip:` line and the tests pass.
async fn build_tpch_session() -> Option<SessionContext> {
    let dir = data_dir();
    let ctx = SessionContext::new();
    let tables = [
        "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
    ];
    for table in tables {
        let path = dir.join(format!("{table}.parquet"));
        if !path.exists() {
            eprintln!(
                "skip: TPC-H Parquet missing at {}; generate via\n  \
                 cargo run --release -p ematix-flow-core --example tpch_generate -- \
                 --sf 1 --out {}",
                path.display(),
                dir.display()
            );
            return None;
        }
        ctx.register_parquet(table, path.to_str().unwrap(), Default::default())
            .await
            .unwrap_or_else(|e| panic!("register {table}: {e}"));
    }
    Some(ctx)
}

async fn run_duckdb(ctx: &SessionContext, duckdb_sql: &str) -> Vec<RecordBatch> {
    let translated = translate(duckdb_sql, Dialect::DuckDb).expect("DuckDB translate");
    let df = ctx
        .sql(&translated)
        .await
        .unwrap_or_else(|e| panic!("DataFusion plan failed for {translated:?}: {e}"));
    df.collect()
        .await
        .unwrap_or_else(|e| panic!("DataFusion execute failed for {translated:?}: {e}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_tpch_q1_executes_correctly() {
    let Some(ctx) = build_tpch_session().await else {
        return;
    };
    let q1 = include_str!("../../../examples/tpch/queries/q01.sql");
    let q1 = q1.trim().trim_end_matches(';');
    let rows = run_duckdb(&ctx, q1).await;
    let total: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 4, "Q1 returns 4 group-by rows at SF=1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_tpch_q3_executes_correctly() {
    let Some(ctx) = build_tpch_session().await else {
        return;
    };
    let q3 = include_str!("../../../examples/tpch/queries/q03.sql");
    let q3 = q3.trim().trim_end_matches(';');
    let rows = run_duckdb(&ctx, q3).await;
    let total: usize = rows.iter().map(|b| b.num_rows()).sum();
    // Canonical TPC-H Q3 has no LIMIT clause; SF=1 returns 11620
    // unshipped-orders rows. (Hand-curated variants in the wild
    // sometimes add LIMIT 10 — the previous test expectation was
    // written against one of those. The Σ.C extension's
    // tpch_extract_queries example pulls the spec-canonical form
    // from tpchgen, which matches the audit reference.)
    assert_eq!(total, 11620, "Q3 SF=1 returns 11620 rows");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_tpch_q6_executes_correctly() {
    let Some(ctx) = build_tpch_session().await else {
        return;
    };
    let q6 = include_str!("../../../examples/tpch/queries/q06.sql");
    let q6 = q6.trim().trim_end_matches(';');
    let rows = run_duckdb(&ctx, q6).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].num_rows(), 1);

    let revenue = extract_scalar_f64(&rows[0]);
    let expected = 123_141_078.23;
    assert!(
        (revenue - expected).abs() < 0.01,
        "Q6 revenue {revenue} != reference {expected}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_tpch_q19_executes_correctly() {
    let Some(ctx) = build_tpch_session().await else {
        return;
    };
    let q19 = include_str!("../../../examples/tpch/queries/q19.sql");
    let q19 = q19.trim().trim_end_matches(';');
    let rows = run_duckdb(&ctx, q19).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].num_rows(), 1);
    let _revenue = extract_scalar_f64(&rows[0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_list_value_executes() {
    let ctx = SessionContext::new();
    let rows = run_duckdb(&ctx, "SELECT list_value(1, 2, 3) AS a").await;
    assert_eq!(rows[0].num_rows(), 1);
}

fn extract_scalar_f64(batch: &RecordBatch) -> f64 {
    let column: &Arc<dyn Array> = batch.column(0);
    if let Some(arr) = column
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Float64Array>()
    {
        arr.value(0)
    } else if let Some(arr) = column
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Decimal128Array>()
    {
        let raw = arr.value(0) as f64;
        raw / 10f64.powi(arr.scale() as i32)
    } else {
        panic!("scalar column had unexpected type {:?}", column.data_type())
    }
}
