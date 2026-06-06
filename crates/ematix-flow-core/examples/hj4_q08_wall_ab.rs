//! HJ.4 wall A/B — does the SIMD-tag probe (3.9× the RobinHood KERNEL in the
//! de-risk) move Q08 SF=10 WALL time, or is the probe too small a slice (HJ.3)?
//!
//! Three arms, interleaved (SF=10, production preset + SwapEmatixHashJoinRule):
//!   native       — stock DataFusion HashJoinExec        (no env)
//!   robinhood-HJ — EMAT_HASH_JOIN=1                      (chained kernel; HJ.3 = ~flat)
//!   tag-HJ       — EMAT_HASH_JOIN=1 + EMAT_HJ_TAG=1      (SIMD-tag, this lever)
//!
//! Confirms (a) EmatixHashJoinExec fires, (b) the tag arm actually built a Tag
//! table (TAG_BUILDS counter), (c) all three arms produce the same result.
//!
//! Usage:
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 \
//!     cargo run --release -p ematix-flow-core --example hj4_q08_wall_ab

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use datafusion::arrow::array::{Array, Float64Array};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::emat_hash_join::{RH_BUILDS, TAG_BUILDS};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::swap_emat_hash_join_rule::SwapEmatixHashJoinRule;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

#[derive(Clone, Copy)]
enum Arm {
    Native,
    RobinHood,
    Tag,
}

fn set_arm(arm: Arm) {
    unsafe {
        match arm {
            Arm::Native => {
                std::env::remove_var("EMAT_HASH_JOIN");
                std::env::remove_var("EMAT_HJ_TAG");
            }
            Arm::RobinHood => {
                std::env::set_var("EMAT_HASH_JOIN", "1");
                std::env::remove_var("EMAT_HJ_TAG");
            }
            Arm::Tag => {
                std::env::set_var("EMAT_HASH_JOIN", "1");
                std::env::set_var("EMAT_HJ_TAG", "1");
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

/// Sum of all Float64 output values + row count — a cheap correctness checksum.
async fn checksum(
    ctx: &SessionContext,
    sql: &str,
) -> Result<(f64, usize), Box<dyn std::error::Error>> {
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let batches = collect(plan, ctx.task_ctx()).await?;
    let mut sum = 0.0f64;
    let mut rows = 0usize;
    for b in &batches {
        rows += b.num_rows();
        for c in b.columns() {
            if let Some(a) = c.as_any().downcast_ref::<Float64Array>() {
                for v in a.values() {
                    sum += *v;
                }
            }
        }
    }
    Ok((sum, rows))
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

    set_arm(Arm::Native);
    let builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(parts))
        .with_default_features();
    let state = ematix_flow_core::preset::with_optimizer_rules(builder)
        .with_physical_optimizer_rule(Arc::new(SwapEmatixHashJoinRule))
        .build();
    let ctx = SessionContext::new_with_state(state);
    register(&ctx, &data_dir)?;

    // Confirm EmatixHashJoinExec fires under the HJ arms.
    set_arm(Arm::RobinHood);
    let fired = format!(
        "{}",
        displayable(ctx.sql(&sql).await?.create_physical_plan().await?.as_ref()).indent(true)
    )
    .contains("EmatixHashJoinExec");
    set_arm(Arm::Native);
    println!(
        "HJ.4 Q08 wall A/B — data={} parts={parts} fired={fired}",
        data_dir.display()
    );
    if !fired {
        println!("WARN: EmatixHashJoinExec does NOT appear — swap rule not firing on Q08");
    }

    // Correctness: all three arms must produce the same checksum + row count.
    set_arm(Arm::Native);
    let (cs_n, rows_n) = checksum(&ctx, &sql).await?;
    set_arm(Arm::RobinHood);
    let (cs_r, rows_r) = checksum(&ctx, &sql).await?;
    set_arm(Arm::Tag);
    TAG_BUILDS.store(0, Ordering::Relaxed);
    RH_BUILDS.store(0, Ordering::Relaxed);
    let (cs_t, rows_t) = checksum(&ctx, &sql).await?;
    let tag_builds = TAG_BUILDS.load(Ordering::Relaxed);
    let rh_builds = RH_BUILDS.load(Ordering::Relaxed);
    set_arm(Arm::Native);
    println!(
        "correctness: native(sum={cs_n:.4},rows={rows_n}) robinhood(sum={cs_r:.4},rows={rows_r}) tag(sum={cs_t:.4},rows={rows_t})"
    );
    println!(
        "tag arm built: TAG_BUILDS={tag_builds} RH_BUILDS={rh_builds}  (tag fired iff TAG_BUILDS>0)"
    );
    let rel = |a: f64, b: f64| (a - b).abs() / a.abs().max(1.0);
    if rel(cs_n, cs_r) > 1e-9 || rel(cs_n, cs_t) > 1e-9 || rows_n != rows_r || rows_n != rows_t {
        println!("WARN: results DIFFER across arms — measurement invalid");
    }
    if tag_builds == 0 {
        println!("WARN: tag arm did NOT build a Tag table — measuring RobinHood twice");
    }

    // Warmup (all arms).
    for _ in 0..3 {
        set_arm(Arm::Native);
        let _ = run_once(&ctx, &sql).await?;
        set_arm(Arm::RobinHood);
        let _ = run_once(&ctx, &sql).await?;
        set_arm(Arm::Tag);
        let _ = run_once(&ctx, &sql).await?;
    }

    let trials = 11;
    let (mut native, mut rh, mut tag) = (Vec::new(), Vec::new(), Vec::new());
    for _ in 0..trials {
        set_arm(Arm::Native);
        native.push(run_once(&ctx, &sql).await?);
        set_arm(Arm::RobinHood);
        rh.push(run_once(&ctx, &sql).await?);
        set_arm(Arm::Tag);
        tag.push(run_once(&ctx, &sql).await?);
    }
    set_arm(Arm::Native);

    let (n, r, t) = (median(native), median(rh), median(tag));
    println!("\nnative        median = {n:.1} ms");
    println!(
        "robinhood-HJ  median = {r:.1} ms  ({:+.1}% vs native)",
        (r - n) / n * 100.0
    );
    println!(
        "tag-HJ        median = {t:.1} ms  ({:+.1}% vs native)",
        (t - n) / n * 100.0
    );
    println!("tag vs robinhood     = {:+.1}%", (t - r) / r * 100.0);
    let verdict = if (t - n) / n < -0.02 {
        "TAG WINS vs native → wire default-on (gated), proceed HJ.5"
    } else if (t - r) / r < -0.05 {
        "tag beats robinhood but ≈ native → probe not a wall lever (HJ.3 stands)"
    } else {
        "NO-GO — tag ≈ native at wall (probe too small a slice / native already fast)"
    };
    println!("VERDICT: {verdict}");
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
