//! WIN campaign probe: run arbitrary SQL through the PRODUCTION preset
//! (`preset::with_optimizer_rules` + FlowQueryPlanner + all-Emat providers),
//! timed, optionally with EXPLAIN ANALYZE output. Isolates sub-query costs
//! (single-table scans, filters) from full-query pipeline contention.
//!
//!   EMAT_SQL="select count(*) from part where p_name like 'forest%'" \
//!   TPCH_DATA_DIR=examples/tpch/data/sf100 TRIALS=3 \
//!     ./target/release/examples/sql_probe
//!
//! Env: EMAT_SQL (required), TPCH_DATA_DIR (default sf100), TRIALS (3),
//!      EMAT_SQL_ANALYZE=1 (dump per-operator metrics after the timed trials).
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::preset;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

fn build_ctx(data_dir: &Path) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let state = preset::with_optimizer_rules(
        SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_default_features(),
    )
    .build();
    let ctx = SessionContext::new_with_state(state);
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        if !path.exists() {
            continue;
        }
        let prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy().to_string())?;
        ctx.register_table(*t, Arc::new(prov))?;
    }
    Ok(ctx)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sql = std::env::var("EMAT_SQL").expect("set EMAT_SQL");
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("examples/tpch/data/sf100"));
    let trials: usize = std::env::var("TRIALS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);

    for i in 0..trials {
        let ctx = build_ctx(&data_dir)?; // fresh ctx per trial = production faithful
        let t0 = Instant::now();
        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;
        let dt = t0.elapsed().as_secs_f64() * 1000.0;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!("trial {i}: {dt:.1}ms rows={rows}");
        if i == 0 {
            if let Some(b) = batches.first() {
                if let Some(c) = b.columns().first() {
                    println!("first col head: {:?}", c.slice(0, c.len().min(3)));
                }
            }
        }
    }

    if std::env::var("EMAT_SQL_ANALYZE").as_deref() == Ok("1") {
        let ctx = build_ctx(&data_dir)?;
        let df = ctx.sql(&sql).await?;
        let plan = df.create_physical_plan().await?;
        let batches = datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx()).await?;
        let _ = batches;
        println!(
            "{}",
            displayable(plan.as_ref())
                .set_show_statistics(false)
                .indent(true)
        );
    }
    Ok(())
}
