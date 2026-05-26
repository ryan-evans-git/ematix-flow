//! Σ.AB — dump Polars LogicalPlan for a TPC-H query at SF=10.
//!
//! Usage:
//!   cargo run --release -p ematix-flow-core \
//!     --example polars_plan_dump --features triangulation -- 6
//!
//! Mirrors `duckdb_q18_plan_dump.rs` but uses Polars's SQL frontend.
//! Dumps both unoptimized and optimized LazyFrame plan strings.

use std::path::PathBuf;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let q: u8 = args.get(1).map(|s| s.parse().unwrap_or(6)).unwrap_or(6);

    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf10"));
    let queries_dir = PathBuf::from("examples/tpch/queries");
    // Prefer the Polars-dialect variant when present.
    let polars_sql_path = queries_dir.join(format!("q{q:02}.polars.sql"));
    let canonical_sql_path = queries_dir.join(format!("q{q:02}.sql"));
    let sql_path = if polars_sql_path.exists() {
        polars_sql_path
    } else {
        canonical_sql_path
    };
    let sql = std::fs::read_to_string(&sql_path)?;

    // Polars Tokio-free path: run on a worker thread to avoid the
    // "outer multi-thread runtime" panic.
    let dump = std::thread::spawn(move || -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        use polars::prelude::*;
        use polars::sql::SQLContext;
        let mut ctx = SQLContext::new();
        for t in TPCH_TABLES {
            let path = data_dir.join(format!("{t}.parquet"));
            let pl_path = polars::prelude::PlPath::new(path.to_string_lossy().as_ref());
            let lf = LazyFrame::scan_parquet(pl_path, ScanArgsParquet::default())
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
            ctx.register(t, lf);
        }
        let lf = ctx
            .execute(&sql)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
        let unopt = lf
            .clone()
            .describe_plan()
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
        let opt = lf
            .describe_optimized_plan()
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
        Ok(format!(
            "==== Polars unoptimized LogicalPlan Q{q:02} ====\n\n{unopt}\n\n==== Polars optimized LogicalPlan Q{q:02} ====\n\n{opt}\n"
        ))
    })
    .join()
    .map_err(|_| "polars worker thread panicked")?;

    match dump {
        Ok(s) => {
            println!("{s}");
            Ok(())
        }
        Err(e) => Err(format!("polars dump: {e}").into()),
    }
}
