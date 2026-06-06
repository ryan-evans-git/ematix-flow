//! PV.3b PRODUCTION-path A/A — the triple-walker gate.
//!
//! Unlike `pv3b_validate` (which exercises `reconstruct` in isolation off the
//! plain-optimized plan), this drives the FULL production preset
//! (`preset::with_optimizer_rules` → `FlowQueryPlanner`: agg_semi → dim_push →
//! reorder → re-optimize → reconstruct → FusedProbePlanner → PV.3 physical
//! fuse). For each query it runs the preset path with `EMAT_PUSH_PIPELINE`
//! OFF (stock) then ON (fused) and compares values — so it covers the
//! interaction between the upstream logical rewrites and the push-fusion
//! reorder that the isolated gate cannot.
//!
//! Since stock-preset ematix is verified cell-for-cell vs DuckDB, fused==stock
//! ⟹ fused==DuckDB. A RELATIVE f64 tolerance absorbs last-ULP SUM-order drift.
//!
//! Usage:
//!   TPCH_DATA_DIR=examples/tpch/data/sf1 \
//!     cargo run --release -p ematix-flow-core --example pv3b_prod_validate

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch};
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

fn set_gate(on: bool) {
    // Edition 2024: env mutation is `unsafe`. Single-threaded at the toggle
    // point (no plan in flight), so this is sound.
    unsafe {
        if on {
            std::env::set_var("EMAT_PUSH_PIPELINE", "1");
        } else {
            std::env::remove_var("EMAT_PUSH_PIPELINE");
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf1"));

    set_gate(false);
    let builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(8))
        .with_default_features();
    let ctx = SessionContext::new_with_state(
        ematix_flow_core::preset::with_optimizer_rules(builder).build(),
    );
    register(&ctx, &data_dir)?;

    println!(
        "PV.3b PRODUCTION A/A (triple-walker) — data={}",
        data_dir.display()
    );
    println!("{:<6} {:<6} {:<9} {}", "query", "fired", "result", "detail");

    let mut fired_count = 0;
    let mut fail = 0;
    for q in 1..=22u8 {
        let path = format!("examples/tpch/queries/q{q:02}.sql");
        let Ok(sql) = std::fs::read_to_string(&path) else {
            continue;
        };

        // Stock: gate OFF.
        set_gate(false);
        let sfp = fingerprint(&ctx.sql(&sql).await?.collect().await?);

        // Fused: gate ON (reconstruct + FusedProbePlanner + PV.3 physical fuse).
        set_gate(true);
        let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
        let fired =
            format!("{}", displayable(plan.as_ref()).indent(true)).contains("EmatPushPipelineExec");
        if fired {
            fired_count += 1;
        }
        let res = collect(plan, ctx.task_ctx()).await;
        set_gate(false);

        match res {
            Ok(fused) => {
                let ffp = fingerprint(&fused);
                let ok = ffp.0 == sfp.0
                    && (ffp.1 - sfp.1).abs() <= 1e-9 * sfp.1.abs().max(1.0)
                    && ffp.2 == sfp.2;
                if ok {
                    println!(
                        "Q{q:02}    {:<6} {:<9} rows={} fsum={:.3}",
                        fired, "PASS", sfp.0, sfp.1
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

    println!("\n=== PV.3b PROD A/A: fired on {fired_count} queries, {fail} mismatch/error ===");
    if fail == 0 {
        println!("PASS — production path is value-equivalent with the push-fusion gate ON.");
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
