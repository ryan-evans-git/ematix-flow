//! Parameterized profile loop for any TPC-H query. Runs the named
//! query repeatedly so a sampling profiler (samply / Instruments)
//! sees enough samples to localise the bottleneck.
//!
//! Run:
//!   cargo build --release -p ematix-flow-core --example sigma_e5_regression_profile
//!   EMAT_PROFILE_QUERY=q16 samply record \
//!     ./target/release/examples/sigma_e5_regression_profile
//!
//! Tuning knobs:
//!   EMAT_PROFILE_QUERY  — q01..q22, default q19
//!   EMAT_PROFILE_WARMUPS — default 5
//!   EMAT_PROFILE_ITERS   — default 200
//!   TPCH_DATA_DIR        — default examples/tpch/data/sf1

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use futures_util::TryStreamExt;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

fn data_dir() -> String {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    match std::env::var("TPCH_DATA_DIR") {
        Ok(s) => s,
        Err(_) => manifest
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("examples/tpch/data/sf1")
            .to_string_lossy()
            .into_owned(),
    }
}

fn make_ctx() -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features()
        .build();
    SessionContext::new_with_state(state)
}

fn register_tables(ctx: &SessionContext, dir: &str) {
    for t in TPCH_TABLES {
        let path = format!("{dir}/{t}.parquet");
        let prov = EmatixFastParquetTableProvider::try_new(path).unwrap();
        ctx.register_table(*t, Arc::new(prov)).unwrap();
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let query = std::env::var("EMAT_PROFILE_QUERY").unwrap_or_else(|_| "q19".to_string());
    let warmups: usize = std::env::var("EMAT_PROFILE_WARMUPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let iters: usize = std::env::var("EMAT_PROFILE_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);

    let dir = data_dir();
    let sql_path = format!("examples/tpch/queries/{}.sql", query);
    let sql = std::fs::read_to_string(&sql_path)
        .or_else(|_| std::fs::read_to_string(format!("../{sql_path}")))
        .unwrap_or_else(|e| panic!("read {sql_path}: {e}"));

    eprintln!("profile harness: query={query} warmups={warmups} iters={iters}");

    let ctx = make_ctx();
    register_tables(&ctx, &dir);

    for _ in 0..warmups {
        let df = ctx.sql(&sql).await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let stream = datafusion::physical_plan::execute_stream(plan, ctx.task_ctx()).unwrap();
        let _ = stream.try_collect::<Vec<_>>().await.unwrap();
    }

    eprintln!("warm-up done; running {iters} iterations for profiler");
    let t0 = std::time::Instant::now();
    for _ in 0..iters {
        let df = ctx.sql(&sql).await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let stream = datafusion::physical_plan::execute_stream(plan, ctx.task_ctx()).unwrap();
        let _ = stream.try_collect::<Vec<_>>().await.unwrap();
    }
    let elapsed = t0.elapsed();
    eprintln!(
        "done — {iters} iterations of {query} in {:.2}s ({:.2} ms / iter)",
        elapsed.as_secs_f64(),
        elapsed.as_secs_f64() * 1000.0 / iters as f64
    );
}
