//! PV.3b Q08 perf A/B — does the push-fusion win materialize in-plan?
//!
//! Production preset path, SF=10. Interleaved trials (stock gate-OFF, fused
//! gate-ON) to cancel drift; reports median wall time + ratio. Confirms the
//! recognizer-produced plan realizes the de-risked −10.8% (operator lower
//! bound; in-plan splice should approach the −13% PV.2 measured).
//!
//! Usage:
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 \
//!     cargo run --release -p ematix-flow-core --example pv3b_q08_perf

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

fn set_gate(on: bool) {
    unsafe {
        if on {
            std::env::set_var("EMAT_PUSH_PIPELINE", "1");
        } else {
            std::env::remove_var("EMAT_PUSH_PIPELINE");
        }
    }
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf10"));
    let sql = std::fs::read_to_string("examples/tpch/queries/q08.sql")?;

    set_gate(false);
    let parts = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(14);
    let builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(parts))
        .with_default_features();
    let ctx = SessionContext::new_with_state(
        ematix_flow_core::preset::with_optimizer_rules(builder).build(),
    );
    register(&ctx, &data_dir)?;

    // Confirm the fused plan actually carries the operator before timing.
    set_gate(true);
    let fired = format!(
        "{}",
        displayable(ctx.sql(&sql).await?.create_physical_plan().await?.as_ref()).indent(true)
    )
    .contains("EmatPushPipelineExec");
    set_gate(false);
    println!(
        "PV.3b Q08 perf A/B — data={} fired={fired}",
        data_dir.display()
    );
    if !fired {
        println!("WARN: fused plan does NOT contain EmatPushPipelineExec — win cannot materialize");
    }

    // Warmup (both arms).
    for _ in 0..3 {
        set_gate(false);
        let _ = run_once(&ctx, &sql).await?;
        set_gate(true);
        let _ = run_once(&ctx, &sql).await?;
    }

    let trials = 11;
    let mut stock = Vec::with_capacity(trials);
    let mut fused = Vec::with_capacity(trials);
    for _ in 0..trials {
        set_gate(false);
        stock.push(run_once(&ctx, &sql).await?);
        set_gate(true);
        fused.push(run_once(&ctx, &sql).await?);
    }
    set_gate(false);

    let s = median(stock);
    let f = median(fused);
    println!("stock  median = {s:.1} ms");
    println!("fused  median = {f:.1} ms");
    println!(
        "delta         = {:+.1}% ({})",
        (f - s) / s * 100.0,
        if f < s {
            "fused FASTER"
        } else {
            "fused slower"
        }
    );
    Ok(())
}

async fn run_once(ctx: &SessionContext, sql: &str) -> Result<f64, Box<dyn std::error::Error>> {
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let t = Instant::now();
    let _ = collect(plan, ctx.task_ctx()).await?;
    Ok(t.elapsed().as_secs_f64() * 1e3)
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
