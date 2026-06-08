//! PV.4.0 overlap spike A/B — does decoding the fact CONCURRENTLY with the dim
//! build recover the +53% PV.3b measured for the serial build-then-probe splice?
//!
//! Three arms, interleaved to cancel drift (SF=10, production preset):
//!   stock         — EMAT_PUSH_PIPELINE unset            (DataFusion pull plan)
//!   fused-serial  — EMAT_PUSH_PIPELINE=1                (PV.3b: +53% vs stock)
//!   fused-overlap — EMAT_PUSH_PIPELINE=1 + EMAT_PV4_OVERLAP=1   (this spike)
//!
//! GATE: fused-overlap must recover ≥ half the serial regression. The trailing
//! BUILD/DECODE/PROBE split (counters in emat_push_pipeline_exec) shows whether
//! the ~90 ms build actually hid under the ~140 ms decode.
//!
//! Usage:
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 EMAT_PV4_BUFFER=256 \
//!     cargo run --release -p ematix-flow-core --example pv4_q08_overlap_ab

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::emat_push_pipeline_exec::{BUILD_NANOS, DECODE_NANOS, PROBE_NANOS};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

#[derive(Clone, Copy)]
enum Arm {
    Stock,
    FusedSerial,
    FusedOverlap,
}

fn set_arm(arm: Arm) {
    unsafe {
        match arm {
            Arm::Stock => {
                std::env::remove_var("EMAT_PUSH_PIPELINE");
                std::env::remove_var("EMAT_PV4_OVERLAP");
            }
            Arm::FusedSerial => {
                std::env::set_var("EMAT_PUSH_PIPELINE", "1");
                std::env::remove_var("EMAT_PV4_OVERLAP");
            }
            Arm::FusedOverlap => {
                std::env::set_var("EMAT_PUSH_PIPELINE", "1");
                std::env::set_var("EMAT_PV4_OVERLAP", "1");
            }
        }
    }
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

async fn run_once(ctx: &SessionContext, sql: &str) -> Result<f64, Box<dyn std::error::Error>> {
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let t = Instant::now();
    let _ = collect(plan, ctx.task_ctx()).await?;
    Ok(t.elapsed().as_secs_f64() * 1e3)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf10"));
    let sql = std::fs::read_to_string("examples/tpch/queries/q08.sql")?;
    let parts = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(14);
    let buf: usize = std::env::var("EMAT_PV4_BUFFER")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(64);

    set_arm(Arm::Stock);
    let builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(parts))
        .with_default_features();
    let ctx = SessionContext::new_with_state(
        ematix_flow_core::preset::with_optimizer_rules(builder).build(),
    );
    register(&ctx, &data_dir)?;

    // Confirm the fused arms carry the operator before timing.
    set_arm(Arm::FusedSerial);
    let fired = format!(
        "{}",
        displayable(ctx.sql(&sql).await?.create_physical_plan().await?.as_ref()).indent(true)
    )
    .contains("EmatPushPipelineExec");
    set_arm(Arm::Stock);
    println!(
        "PV.4.0 overlap A/B — data={} parts={parts} buf={buf} fired={fired}",
        data_dir.display()
    );
    if !fired {
        println!("WARN: fused plan lacks EmatPushPipelineExec — cannot measure");
    }

    // Warmup (all arms).
    for _ in 0..3 {
        set_arm(Arm::Stock);
        let _ = run_once(&ctx, &sql).await?;
        set_arm(Arm::FusedSerial);
        let _ = run_once(&ctx, &sql).await?;
        set_arm(Arm::FusedOverlap);
        let _ = run_once(&ctx, &sql).await?;
    }

    let trials = 11;
    let (mut stock, mut serial, mut overlap) = (Vec::new(), Vec::new(), Vec::new());
    for _ in 0..trials {
        set_arm(Arm::Stock);
        stock.push(run_once(&ctx, &sql).await?);
        set_arm(Arm::FusedSerial);
        serial.push(run_once(&ctx, &sql).await?);
        set_arm(Arm::FusedOverlap);
        overlap.push(run_once(&ctx, &sql).await?);
    }
    set_arm(Arm::Stock);

    let (s, fs, fo) = (median(stock), median(serial), median(overlap));
    println!("\nstock          median = {s:.1} ms");
    println!(
        "fused-serial   median = {fs:.1} ms  ({:+.1}% vs stock)",
        (fs - s) / s * 100.0
    );
    println!(
        "fused-overlap  median = {fo:.1} ms  ({:+.1}% vs stock)",
        (fo - s) / s * 100.0
    );
    let recovered = if (fs - s).abs() > 1e-9 {
        (fs - fo) / (fs - s) * 100.0
    } else {
        0.0
    };
    println!("overlap recovered {recovered:.0}% of the serial regression");
    println!(
        "GATE (≥50% recovered): {}",
        if recovered >= 50.0 {
            "PASS → proceed PV.4.1"
        } else {
            "FAIL → escalate/stop"
        }
    );

    // Phase split for ONE clean overlap run (counters are cumulative → reset).
    BUILD_NANOS.store(0, Ordering::Relaxed);
    DECODE_NANOS.store(0, Ordering::Relaxed);
    PROBE_NANOS.store(0, Ordering::Relaxed);
    set_arm(Arm::FusedOverlap);
    let one = run_once(&ctx, &sql).await?;
    set_arm(Arm::Stock);
    let b = BUILD_NANOS.load(Ordering::Relaxed) as f64 / 1e6;
    let d = DECODE_NANOS.load(Ordering::Relaxed) as f64 / 1e6;
    let p = PROBE_NANOS.load(Ordering::Relaxed) as f64 / 1e6;
    println!("\noverlap phase split (1 run, total {one:.1} ms, parts={parts}):");
    println!("  BUILD  = {b:.1} ms  (dim probes, once, on the critical path until ready)");
    println!(
        "  DECODE = {d:.1} ms summed → {:.1} ms wall  (fact, runs concurrent w/ BUILD)",
        d / parts as f64
    );
    println!(
        "  PROBE  = {p:.1} ms summed → {:.1} ms wall  (fuse_batch)",
        p / parts as f64
    );
    println!("  (if BUILD hides under DECODE wall, the serial prefix is gone → overlap wins)");
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
