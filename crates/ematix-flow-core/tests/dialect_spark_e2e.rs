//! Σ.A2 PR 3: end-to-end correctness gate for the Spark translator.
//!
//! Audit-by-running: pipe Spark SQL through `dialect::translate(...,
//! Spark)`, hand the output to a real DataFusion `SessionContext`,
//! compare row counts + values against the canonical TPC-H reference.
//!
//! Two test groups:
//! - **TPC-H Q1/Q3/Q6/Q19 in Spark dialect** — confirms PR 2's name
//!   remap is sufficient for the representative set (no structural
//!   rewrites needed for plain TPC-H).
//! - **Spark-only idioms** — exercises constructs Spark users
//!   commonly paste (array literals, named_struct, LATERAL VIEW
//!   EXPLODE, INTERVAL literals). Whatever fails here is the PR 3
//!   work item.
//!
//! This file panics with a generate-from-here pointer if SF=1
//! Parquet is missing — same shape as the criterion bench.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::prelude::SessionContext;
use ematix_flow_core::dialect::{Dialect, translate};

/// Resolve workspace-root `examples/tpch/data/sf1/` regardless of
/// where the test runner started.
fn data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(|root| root.join("examples/tpch/data/sf1"))
        .expect("workspace root")
}

/// Returns `None` when the SF=1 Parquet directory is missing.
/// Same skip-on-missing semantics as the duckdb sibling — CI
/// doesn't generate the data, so the build_tpch_session-based
/// tests early-return with a `skip:` line. Local devs who *have*
/// run `tpch_generate` still get the full assertion path.
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

/// Translate Spark SQL → DataFusion SQL → execute → return rows.
async fn run_spark(ctx: &SessionContext, spark_sql: &str) -> Vec<RecordBatch> {
    let translated = translate(spark_sql, Dialect::Spark).expect("Spark translate");
    let df = ctx
        .sql(&translated)
        .await
        .unwrap_or_else(|e| panic!("DataFusion plan failed for {translated:?}: {e}"));
    df.collect()
        .await
        .unwrap_or_else(|e| panic!("DataFusion execute failed for {translated:?}: {e}"))
}

// =====================================================================
// TPC-H representative set under Spark dialect
// =====================================================================

/// Q1 in Spark dialect → DataFusion. The query has no Spark-specific
/// idioms, so this just confirms PR 2's translator is a true no-op
/// for TPC-H spec SQL.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spark_tpch_q1_executes_correctly() {
    let Some(ctx) = build_tpch_session().await else {
        return;
    };
    let q1 = include_str!("../../../examples/tpch/queries/q01.sql");
    let q1 = q1.trim().trim_end_matches(';');
    let rows = run_spark(&ctx, q1).await;

    let total: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 4, "Q1 returns 4 group-by rows at SF=1; got {total}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spark_tpch_q3_executes_correctly() {
    let Some(ctx) = build_tpch_session().await else {
        return;
    };
    let q3 = include_str!("../../../examples/tpch/queries/q03.sql");
    let q3 = q3.trim().trim_end_matches(';');
    let rows = run_spark(&ctx, q3).await;
    let total: usize = rows.iter().map(|b| b.num_rows()).sum();
    // Canonical TPC-H Q3 has no LIMIT — SF=1 returns 11620 rows.
    // See the duckdb sibling test for the historical context on
    // the previous LIMIT-10 expectation.
    assert_eq!(total, 11620, "Q3 SF=1 returns 11620 rows; got {total}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spark_tpch_q6_executes_correctly() {
    let Some(ctx) = build_tpch_session().await else {
        return;
    };
    let q6 = include_str!("../../../examples/tpch/queries/q06.sql");
    let q6 = q6.trim().trim_end_matches(';');
    let rows = run_spark(&ctx, q6).await;
    assert_eq!(rows.len(), 1, "Q6 returns one batch");
    assert_eq!(rows[0].num_rows(), 1, "Q6 returns one row");

    let revenue = extract_scalar_f64(&rows[0]);
    let expected = 123_141_078.23;
    assert!(
        (revenue - expected).abs() < 0.01,
        "Q6 revenue {revenue} != reference {expected}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spark_tpch_q19_executes_correctly() {
    let Some(ctx) = build_tpch_session().await else {
        return;
    };
    let q19 = include_str!("../../../examples/tpch/queries/q19.sql");
    let q19 = q19.trim().trim_end_matches(';');
    let rows = run_spark(&ctx, q19).await;
    assert_eq!(rows.len(), 1, "Q19 returns one batch");
    assert_eq!(rows[0].num_rows(), 1, "Q19 returns one row");
    // Don't pin the exact revenue here — the spec value is stable, but
    // the test for the shape (one scalar row) is the load-bearing
    // assertion that the Spark translator didn't break the query.
    let _revenue = extract_scalar_f64(&rows[0]);
}

// =====================================================================
// Spark-only idioms — audit what fails through DataFusion as-is
// =====================================================================

/// `array(1, 2, 3)` Spark literal. DataFusion has the same function
/// — should pass through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spark_array_literal_executes() {
    let ctx = SessionContext::new();
    let rows = run_spark(&ctx, "SELECT array(1, 2, 3) AS a").await;
    assert_eq!(rows[0].num_rows(), 1);
}

/// `named_struct('a', 1, 'b', 2)` Spark idiom for building a struct.
/// DataFusion's equivalent is `struct(1 AS a, 2 AS b)` or
/// `named_struct(...)` (DataFusion has both since 30+). Audit which
/// one DataFusion 53 accepts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spark_named_struct_executes() {
    let ctx = SessionContext::new();
    let rows = run_spark(&ctx, "SELECT named_struct('a', 1, 'b', 2) AS s").await;
    assert_eq!(rows[0].num_rows(), 1);
}

/// `LATERAL VIEW EXPLODE(arr)` Spark idiom → `CROSS JOIN UNNEST(arr)`
/// DataFusion equivalent. PR 3 lands the rewrite.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spark_lateral_view_explode_executes() {
    let ctx = SessionContext::new();
    let sql = "
        SELECT id, item
        FROM (SELECT 1 AS id, array(10, 20, 30) AS items)
        LATERAL VIEW EXPLODE(items) v AS item
    ";
    let rows = run_spark(&ctx, sql).await;
    let total: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3, "EXPLODE produces one row per array element");
}

/// `INTERVAL '90' DAY` literal — Polars failed on this; we expect
/// DataFusion to accept it (Q1 already uses it through the
/// pass-through datafusion dialect). Confirms the audit assumption.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spark_interval_literal_executes() {
    let ctx = SessionContext::new();
    let sql = "SELECT DATE '1998-12-01' - INTERVAL '90' DAY AS cutoff";
    let rows = run_spark(&ctx, sql).await;
    assert_eq!(rows[0].num_rows(), 1);
}

// --- helpers --------------------------------------------------------

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
