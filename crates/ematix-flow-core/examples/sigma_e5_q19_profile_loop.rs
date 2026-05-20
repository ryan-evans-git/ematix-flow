//! Σ.E5.6 (2026-05-18) profiling harness: run Q19 EmatixFastParquet
//! in a tight loop so a sampling profiler (samply / Instruments)
//! gets enough samples to localise the parallelism-inefficiency gap.
//!
//! Q19 was identified as the worst remaining regression (+39-52%
//! depending on bench run). Per-column micro-bench shows Emat is
//! -36% on sequential decode but +45% on parallel scan compute —
//! the gap is in scheduling, not decode work.
//!
//! Run:
//!   cargo build --release -p ematix-flow-core --example sigma_e5_q19_profile_loop
//!   samply record ./target/release/examples/sigma_e5_q19_profile_loop
//!
//! Stops after WARM_ITERATIONS + LOOP_ITERATIONS Q19 runs (default
//! 5 + 200 ≈ 5-7 seconds of work).

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

const WARM_ITERATIONS: usize = 5;
const LOOP_ITERATIONS: usize = 200;

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
    let dir = data_dir();
    let sql_path = "examples/tpch/queries/q19.sql";
    let sql = std::fs::read_to_string(sql_path)
        .or_else(|_| std::fs::read_to_string(format!("../{sql_path}")))
        .unwrap_or_else(|e| panic!("read {sql_path}: {e}"));

    let ctx = make_ctx();
    register_tables(&ctx, &dir);

    // Warmups — get caches hot.
    for _ in 0..WARM_ITERATIONS {
        let df = ctx.sql(&sql).await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let stream = datafusion::physical_plan::execute_stream(plan, ctx.task_ctx()).unwrap();
        let _ = stream.try_collect::<Vec<_>>().await.unwrap();
    }

    eprintln!(
        "warm-up done; running {} Q19 iterations for the profiler",
        LOOP_ITERATIONS
    );
    let t0 = std::time::Instant::now();
    for _ in 0..LOOP_ITERATIONS {
        let df = ctx.sql(&sql).await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let stream = datafusion::physical_plan::execute_stream(plan, ctx.task_ctx()).unwrap();
        let _ = stream.try_collect::<Vec<_>>().await.unwrap();
    }
    let elapsed = t0.elapsed();
    eprintln!(
        "done — {LOOP_ITERATIONS} iterations in {:.2}s ({:.2} ms / iter)",
        elapsed.as_secs_f64(),
        elapsed.as_secs_f64() * 1000.0 / LOOP_ITERATIONS as f64
    );
}
