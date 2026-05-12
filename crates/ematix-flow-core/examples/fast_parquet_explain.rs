//! Σ.E2 diagnostic: side-by-side `EXPLAIN ANALYZE` of DataFusion's
//! default parquet path vs `FastParquetTableProvider` for the loss
//! queries (Q10, Q12, Q18 as of 2026-05-12). Tells us *where* in the
//! plan the gap comes from — usually a DataFusion-default optimization
//! (DynamicFilter, page-index pruning) that the custom provider
//! bypasses.
//!
//! Usage:
//!   cargo run --release -p ematix-flow-core --example fast_parquet_explain
//!   cargo run --release -p ematix-flow-core --example fast_parquet_explain -- 10 12
//!
//! With no args, runs on the current known-loss set. Args are
//! TPC-H query numbers (1-22).

use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::prelude::SessionContext;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

const DEFAULT_LOSSES: &[u32] = &[10, 12, 18];

async fn make_ctx_default(parquet_dir: &str) -> SessionContext {
    let ctx = SessionContext::new();
    for table in TPCH_TABLES {
        let path = format!("{parquet_dir}/{table}.parquet");
        ctx.register_parquet(*table, &path, Default::default())
            .await
            .unwrap();
    }
    ctx
}

async fn make_ctx_fast(parquet_dir: &str) -> SessionContext {
    let ctx = SessionContext::new();
    for table in TPCH_TABLES {
        let path = format!("{parquet_dir}/{table}.parquet");
        let prov = FastParquetTableProvider::try_new(path).unwrap();
        ctx.register_table(*table, Arc::new(prov)).unwrap();
    }
    ctx
}

async fn print_explain(ctx: &SessionContext, label: &str, sql: &str) {
    // Warm-up: ensure timings reflect a hot run.
    let _ = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let plan_sql = format!("EXPLAIN ANALYZE {sql}");
    let batches: Vec<RecordBatch> = ctx
        .sql(&plan_sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    println!("--- {label} ---");
    for batch in &batches {
        let plan_col = batch.column(1).as_string::<i32>();
        for i in 0..plan_col.len() {
            for line in plan_col.value(i).lines() {
                println!("    {line}");
            }
        }
    }
    println!();
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let queries_dir = manifest
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("examples/tpch/queries");
    let data_dir_buf = match std::env::var("TPCH_DATA_DIR") {
        Ok(s) => PathBuf::from(s),
        Err(_) => manifest
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("examples/tpch/data/sf1"),
    };
    let data_dir = data_dir_buf.to_str().unwrap();

    let args: Vec<u32> = env::args()
        .skip(1)
        .filter_map(|a| a.parse().ok())
        .collect();
    let queries: Vec<u32> = if args.is_empty() {
        DEFAULT_LOSSES.to_vec()
    } else {
        args
    };

    println!("==> EXPLAIN ANALYZE: DataFusion default vs FastParquet");
    println!("==> data: {data_dir}");
    println!("==> queries: {queries:?}");
    println!();

    let ctx_default = make_ctx_default(data_dir).await;
    let ctx_fast = make_ctx_fast(data_dir).await;

    for n in queries {
        let path = queries_dir.join(format!("q{n:02}.sql"));
        let sql_raw = match fs::read_to_string(&path) {
            Ok(s) => s,
            Err(e) => {
                println!("Q{n:02}: missing {path:?}: {e}");
                continue;
            }
        };
        let sql = sql_raw.trim().trim_end_matches(';').trim();
        if sql.matches(';').count() > 0 {
            println!("Q{n:02}: multi-statement, skipping");
            continue;
        }

        println!("================ Q{n:02} ================");
        print_explain(&ctx_default, "DataFusion default", sql).await;
        print_explain(&ctx_fast, "FastParquetTableProvider", sql).await;
        println!();
    }
}
