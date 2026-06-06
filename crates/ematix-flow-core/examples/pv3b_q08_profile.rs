//! PV.3b fused-Q08 phase split — where does the +53% go?
//!
//! Runs the production preset Q08 at SF=10 with the push-fusion gate ON, reading
//! the operator's BUILD_NANOS / PROBE_NANOS counters to attribute the fused wall
//! to: (1) dim-probe BUILD (once, on the critical path before any probe),
//! (2) the 60M-row fuse_batch PROBE (summed CPU across partitions → /nparts for
//! a wall estimate), (3) REMAINDER (stock supplier⋈n2 + adapter + agg, by diff).
//!
//! Usage:
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 \
//!     cargo run --release -p ematix-flow-core --example pv3b_q08_profile

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::emat_push_pipeline_exec::{BUILD_NANOS, PROBE_NANOS};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf10"));
    let sql = std::fs::read_to_string("examples/tpch/queries/q08.sql")?;
    let parts = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(14);

    unsafe { std::env::set_var("EMAT_PUSH_PIPELINE", "1") };
    let builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(parts))
        .with_default_features();
    let ctx = SessionContext::new_with_state(
        ematix_flow_core::preset::with_optimizer_rules(builder).build(),
    );
    register(&ctx, &data_dir)?;

    let run = |ctx: &SessionContext, sql: String| {
        let ctx = ctx.clone();
        async move {
            let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
            let t = Instant::now();
            let _ = collect(plan, ctx.task_ctx()).await?;
            Ok::<f64, Box<dyn std::error::Error>>(t.elapsed().as_secs_f64() * 1e3)
        }
    };

    // Warmup, then profile one clean run.
    for _ in 0..3 {
        let _ = run(&ctx, sql.clone()).await?;
    }
    BUILD_NANOS.store(0, Ordering::Relaxed);
    PROBE_NANOS.store(0, Ordering::Relaxed);
    let total = run(&ctx, sql.clone()).await?;

    let build_ms = BUILD_NANOS.load(Ordering::Relaxed) as f64 / 1e6;
    let probe_cpu_ms = PROBE_NANOS.load(Ordering::Relaxed) as f64 / 1e6;
    let probe_wall_est = probe_cpu_ms / parts as f64;
    let remainder_est = (total - build_ms - probe_wall_est).max(0.0);

    println!("PV.3b fused Q08 SF=10 phase split (parts={parts})");
    println!("  total fused wall    = {total:.1} ms");
    println!("  BUILD (critical)    = {build_ms:.1} ms   (dim probes, once, before probe starts)");
    println!(
        "  PROBE fuse_batch    = {probe_cpu_ms:.1} ms CPU  (~{probe_wall_est:.1} ms wall / {parts} parts)"
    );
    println!("  REMAINDER (≈ diff)  = {remainder_est:.1} ms  (supplier⋈n2 + adapter + agg + glue)");
    Ok(())
}

fn register(ctx: &SessionContext, dir: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    for t in TPCH_TABLES {
        let p = dir.join(format!("{t}.parquet"));
        if *t == "lineitem" || *t == "orders" {
            ctx.register_table(
                *t,
                Arc::new(EmatixFastParquetTableProvider::try_new(
                    p.to_string_lossy(),
                )?),
            )?;
        } else {
            ctx.register_table(
                *t,
                Arc::new(FastParquetTableProvider::try_new(p.to_string_lossy())?),
            )?;
        }
    }
    Ok(())
}
