//! Σ.E2 day-4: run every TPC-H query with `FastParquetTableProvider`
//! and report per-query medians alongside DataFusion's default
//! parquet path.
//!
//! Decision criterion: does row-group-parallel parquet-rs (via the
//! custom TableProvider) actually close the Polars-wins-by gap on
//! Q9/Q12/Q14 measured in the README? The day-3 probe said it should;
//! this harness proves or disproves it end-to-end.
//!
//! Usage:
//!   cargo run --release -p ematix-flow-core --example tpch_fast_parquet_bench

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::RecordBatch;
use datafusion::prelude::SessionContext;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

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

async fn bench_one(ctx: &SessionContext, sql: &str) -> (f64, usize) {
    let _: Vec<RecordBatch> = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut times = Vec::with_capacity(5);
    let mut row_count = 0;
    for _ in 0..5 {
        let start = Instant::now();
        let out: Vec<RecordBatch> = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        times.push(start.elapsed().as_secs_f64() * 1000.0);
        row_count = out.iter().map(|b| b.num_rows()).sum();
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (times[2], row_count)
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
    println!("==> DataFusion default vs FastParquetTableProvider (Σ.E2)");
    println!("==> M3 Pro / SF=1 / 5-trial median");
    println!("==> data: {data_dir}");
    println!();
    println!("| Query | DataFusion (ms) | FastParquet (ms) | Δ % | rows | notes |");
    println!("|---|---|---|---|---|---|");

    let ctx_default = make_ctx_default(data_dir).await;
    let ctx_fast = make_ctx_fast(data_dir).await;

    let mut wins = 0;
    let mut losses = 0;
    let mut sum_speedup = 0.0_f64;
    let mut measured = 0;
    for n in 1..=22 {
        let path = queries_dir.join(format!("q{:02}.sql", n));
        let sql_raw = match fs::read_to_string(&path) {
            Ok(s) => s,
            Err(e) => {
                println!("| Q{n:02} | — | — | — | — | missing {path:?}: {e} |");
                continue;
            }
        };
        let sql = sql_raw.trim().trim_end_matches(';').trim();
        if sql.matches(';').count() > 0 {
            println!("| Q{n:02} | — | — | — | — | multi-statement, skipped |");
            continue;
        }

        let result_default = std::panic::AssertUnwindSafe(bench_one(&ctx_default, sql));
        let default = futures_util::future::FutureExt::catch_unwind(result_default).await;
        let result_fast = std::panic::AssertUnwindSafe(bench_one(&ctx_fast, sql));
        let fast = futures_util::future::FutureExt::catch_unwind(result_fast).await;
        match (default, fast) {
            (Ok((d, rows)), Ok((f, _))) => {
                let delta_pct = (d - f) / d * 100.0;
                let arrow = if f < d * 0.95 {
                    wins += 1;
                    " ✓"
                } else if f > d * 1.05 {
                    losses += 1;
                    " ✗"
                } else {
                    ""
                };
                sum_speedup += d / f;
                measured += 1;
                println!(
                    "| Q{n:02} | {d:>7.2} | {f:>7.2} | {delta_pct:>+6.1}% | {rows} |{arrow} |"
                );
            }
            (Err(_), _) => {
                println!("| Q{n:02} | panic | — | — | — | DataFusion panicked |");
            }
            (_, Err(_)) => {
                println!("| Q{n:02} | — | panic | — | — | FastParquet panicked |");
            }
        }
    }

    println!();
    if measured > 0 {
        let mean_speedup = sum_speedup / measured as f64;
        println!(
            "Summary: measured={measured}  wins(>5% faster)={wins}  losses(>5% slower)={losses}  mean_speedup={mean_speedup:.2}×"
        );
    }
}
