//! PV.3b correctness gate — A/A on the push-fusion REORDER (`reconstruct`).
//!
//! For each TPC-H query, take the SAME standard-optimized logical plan and:
//!   - stock: plan it with the default physical planner;
//!   - fused: `push_fusion_rule::reconstruct` it (→ a `FusedProbeNode`), then
//!     plan with `FusedProbePlanner` registered.
//! Both start from one identical logical plan, so the ONLY difference is the
//! reorder — an exact A/A. Compare (rows, f64 checksum, i64 checksum) with a
//! RELATIVE f64 tolerance (the fused emits survivors in a different order, so
//! float SUM differs in the last ULPs — bit-equality would false-fail).
//!
//! Stock-ematix is verified cell-for-cell vs DuckDB, so fused==stock ⟹
//! fused==DuckDB. Reports per query whether the reorder FIRED and whether it
//! matched; any mismatch / hard error (e.g. a `require_unique` violation) fails
//! the gate.
//!
//! Usage:
//!   TPCH_DATA_DIR=examples/tpch/data/sf1 \
//!     cargo run --release -p ematix-flow-core --example pv3b_validate
#![allow(clippy::doc_lazy_continuation)]

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch};
use datafusion::physical_plan::{collect, displayable};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fused_probe_node::FusedProbePlanner;
use ematix_flow_core::push_fusion_rule::reconstruct;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

/// (rows, f64 checksum, i64 checksum) — a value-equivalence fingerprint.
fn fingerprint(batches: &[RecordBatch]) -> (usize, f64, i64) {
    let mut rows = 0usize;
    let mut fsum = 0.0f64;
    let mut isum = 0i64;
    for b in batches {
        rows += b.num_rows();
        for c in b.columns() {
            if let Some(a) = c.as_any().downcast_ref::<Float64Array>() {
                for i in 0..a.len() {
                    if a.is_valid(i) {
                        fsum += a.value(i);
                    }
                }
            } else if let Some(a) = c.as_any().downcast_ref::<Int64Array>() {
                for i in 0..a.len() {
                    if a.is_valid(i) {
                        isum = isum.wrapping_add(a.value(i));
                    }
                }
            }
        }
    }
    (rows, fsum, isum)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf1"));

    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(8));
    register(&ctx, &data_dir)?;

    println!("PV.3b A/A correctness gate — data={}", data_dir.display());
    println!("{:<6} {:<6} {:<9} detail", "query", "fired", "result");

    let stock_planner = DefaultPhysicalPlanner::default();
    let fused_planner =
        DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(FusedProbePlanner)]);

    let mut fired_count = 0;
    let mut fail = 0;
    for q in 1..=22u8 {
        let path = format!("examples/tpch/queries/q{q:02}.sql");
        let Ok(sql) = std::fs::read_to_string(&path) else {
            continue;
        };

        // ONE optimized logical plan feeds both arms.
        let logical = ctx.sql(&sql).await?.into_optimized_plan()?;
        let state = ctx.state();

        let stock_phys = stock_planner.create_physical_plan(&logical, &state).await?;
        let sfp = fingerprint(&collect(stock_phys, ctx.task_ctx()).await?);

        let (fired, fused_phys) = match reconstruct(&logical) {
            Some(r) => (true, fused_planner.create_physical_plan(&r, &state).await?),
            None => (
                false,
                stock_planner.create_physical_plan(&logical, &state).await?,
            ),
        };
        if fired {
            fired_count += 1;
        }
        let used_op = format!("{}", displayable(fused_phys.as_ref()).indent(true))
            .contains("EmatPushPipelineExec");

        match collect(fused_phys, ctx.task_ctx()).await {
            Ok(fused) => {
                let ffp = fingerprint(&fused);
                let ok = ffp.0 == sfp.0
                    && (ffp.1 - sfp.1).abs() <= 1e-9 * sfp.1.abs().max(1.0)
                    && ffp.2 == sfp.2;
                if ok {
                    println!(
                        "Q{q:02}    {:<6} {:<9} rows={} fsum={:.3} op={}",
                        fired, "PASS", sfp.0, sfp.1, used_op
                    );
                } else {
                    fail += 1;
                    println!(
                        "Q{q:02}    {:<6} {:<9} stock=({},{:.3},{}) fused=({},{:.3},{})",
                        fired, "MISMATCH", sfp.0, sfp.1, sfp.2, ffp.0, ffp.1, ffp.2
                    );
                }
            }
            Err(e) => {
                fail += 1;
                println!(
                    "Q{q:02}    {:<6} {:<9} {}",
                    fired,
                    "ERROR",
                    e.to_string().lines().next().unwrap_or("")
                );
            }
        }
    }

    println!("\n=== PV.3b A/A: reorder fired on {fired_count} queries, {fail} mismatch/error ===");
    if fail == 0 {
        println!("PASS — the push-fusion reorder is value-equivalent on every query it fires on.");
    } else {
        println!("FAIL — {fail} queries regressed; investigate before any default-on.");
        std::process::exit(1);
    }
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
