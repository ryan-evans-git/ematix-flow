//! PV.3 correctness gate — A/A through the PRODUCTION preset path.
//!
//! For each TPC-H query: build the stock physical plan (preset ctx, recognizer
//! OFF), then a FRESH plan transformed by the PV.3 recognizer
//! (`fuse_push_pipelines_always`), and compare results. Reports, per query,
//! whether the recognizer FIRED and whether the fused result matches stock
//! (row count + f64/i64 column checksums). Since stock-ematix is already
//! verified cell-for-cell against DuckDB, fused==stock ⟹ fused==DuckDB.
//!
//! This isolates EXACTLY the recognizer's effect (an A/A swap), and surfaces
//! any over-fire (a query that fires but mismatches) or hard error (a
//! `require_unique` violation → the fused collect errors).
//!
//! Usage:
//!   TPCH_DATA_DIR=examples/tpch/data/sf1 \
//!     cargo run --release -p ematix-flow-core --example pv3_validate

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fuse_push_pipeline_rule::fuse_push_pipelines_always;

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

    let builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(8))
        .with_default_features();
    let ctx = SessionContext::new_with_state(
        ematix_flow_core::preset::with_optimizer_rules(builder).build(),
    );
    register(&ctx, &data_dir)?;

    println!("PV.3 A/A correctness gate — data={}", data_dir.display());
    println!("{:<6} {:<6} {:<8} detail", "query", "fired", "result");

    let mut fired_count = 0;
    let mut fail = 0;
    for q in 1..=22u8 {
        let path = format!("examples/tpch/queries/q{q:02}.sql");
        let Ok(sql) = std::fs::read_to_string(&path) else {
            continue;
        };

        // Stock (recognizer OFF by default env).
        let stock_plan = ctx.sql(&sql).await?.create_physical_plan().await?;
        let stock = collect(stock_plan, ctx.task_ctx()).await?;
        let sfp = fingerprint(&stock);

        // Fresh plan, recognizer applied.
        let fresh = ctx.sql(&sql).await?.create_physical_plan().await?;
        let fused_plan = fuse_push_pipelines_always(fresh)?;
        let fired = format!("{}", displayable(fused_plan.as_ref()).indent(true))
            .contains("EmatPushPipelineExec");
        if fired {
            fired_count += 1;
        }

        match collect(fused_plan, ctx.task_ctx()).await {
            Ok(fused) => {
                let ffp = fingerprint(&fused);
                let ok = ffp.0 == sfp.0
                    && (ffp.1 - sfp.1).abs() <= 1e-6 * sfp.1.abs().max(1.0)
                    && ffp.2 == sfp.2;
                if ok {
                    println!(
                        "Q{q:02}    {:<6} {:<8} rows={} fsum={:.3}",
                        fired, "PASS", sfp.0, sfp.1
                    );
                } else {
                    fail += 1;
                    println!(
                        "Q{q:02}    {:<6} {:<8} stock=({},{:.3},{}) fused=({},{:.3},{})",
                        fired, "MISMATCH", sfp.0, sfp.1, sfp.2, ffp.0, ffp.1, ffp.2
                    );
                }
            }
            Err(e) => {
                fail += 1;
                println!(
                    "Q{q:02}    {:<6} {:<8} {}",
                    fired,
                    "ERROR",
                    e.to_string().lines().next().unwrap_or("")
                );
            }
        }
    }

    println!("\n=== PV.3 A/A: fired on {fired_count} queries, {fail} mismatch/error ===");
    if fail == 0 {
        println!("PASS — recognizer is value-equivalent on every query it fires on.");
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
