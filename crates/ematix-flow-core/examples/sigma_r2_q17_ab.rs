//! Σ.R.2.d — Q17 SF=10 A/B gate for `RobinHoodAvgF64Exec`.
//!
//! Runs Q17 SF=10 twice in the same process:
//! - **OFF**: stock DataFusion (no opt-in rule installed).
//! - **ON**: `install_robin_hood_avg_f64_rule` installed; rule
//!   replaces the `AVG(l_quantity) GROUP BY l_partkey` Partial +
//!   FinalPartitioned with `RobinHoodAvgF64Exec`.
//!
//! Gate: ≥10% improvement ON vs OFF to proceed to the full 22q SF=10
//! publishable run. Default trials = 5 (×2 warmup); override with
//! `TPCH_TRIALS`.
//!
//! Sister to `sigma_q_l11_spike_q17.rs`. Reuses the same TPC-H table
//! registration pattern: lineitem via `EmatixFastParquet` (where
//! Σ.E5 / Σ.L decode wins live); others via `FastParquet`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::robin_hood_avg_f64_exec::install_robin_hood_avg_f64_rule;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

fn register_tables(ctx: &SessionContext, dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    for t in TPCH_TABLES {
        let path = PathBuf::from(dir).join(format!("{t}.parquet"));
        if *t == "lineitem" {
            let prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy())?;
            ctx.register_table(*t, Arc::new(prov))?;
        } else {
            let prov = FastParquetTableProvider::try_new(path.to_string_lossy())?;
            ctx.register_table(*t, Arc::new(prov))?;
        }
    }
    Ok(())
}

async fn time_query(ctx: &SessionContext, sql: &str, trials: usize) -> (f64, f64, Vec<f64>) {
    for _ in 0..2 {
        ctx.sql(sql).await.unwrap().collect().await.unwrap();
    }
    let mut samples = Vec::with_capacity(trials);
    for _ in 0..trials {
        let t = Instant::now();
        ctx.sql(sql).await.unwrap().collect().await.unwrap();
        samples.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    let mut sorted = samples.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = sorted[sorted.len() / 2];
    let mean: f64 = samples.iter().sum::<f64>() / samples.len() as f64;
    let var: f64 = samples.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / samples.len() as f64;
    (median, var.sqrt(), samples)
}

async fn plan_contains(ctx: &SessionContext, sql: &str, needle: &str) -> bool {
    let df = ctx.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    format!("{plan:?}").contains(needle)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir =
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf10".to_string());
    let trials: usize = std::env::var("TPCH_TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let q17 = std::fs::read_to_string("examples/tpch/queries/q17.sql")?;

    println!(
        "Σ.R.2.d gate — Q17 SF=10 A/B, {trials} trials × 2 warmup, data={dir}\n"
    );

    // OFF — stock DataFusion (no rule installed).
    let off_state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features()
        .build();
    let off_ctx = SessionContext::new_with_state(off_state);
    register_tables(&off_ctx, &dir)?;

    // ON — RobinHoodAvgF64 rule installed.
    let on_state = install_robin_hood_avg_f64_rule(
        SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(14))
            .with_default_features(),
    )
    .build();
    let on_ctx = SessionContext::new_with_state(on_state);
    register_tables(&on_ctx, &dir)?;

    // Sanity check: rule actually fires under ON, not under OFF.
    let off_has_rh = plan_contains(&off_ctx, &q17, "RobinHoodAvgF64Exec").await;
    let on_has_rh = plan_contains(&on_ctx, &q17, "RobinHoodAvgF64Exec").await;
    println!("Plan check  OFF: contains RobinHoodAvgF64Exec = {off_has_rh}");
    println!("Plan check  ON : contains RobinHoodAvgF64Exec = {on_has_rh}");
    if off_has_rh {
        println!("  WARNING: rule fired in OFF context — unexpected (test should fail).");
    }
    if !on_has_rh {
        println!(
            "  WARNING: rule did NOT fire in ON context — bench will measure no-op A/B. \
             Q17's correlated subquery may shape the AVG aggregate differently from the \
             rule's expected pattern. Inspect the physical plan."
        );
    }
    println!();

    // Bench order: OFF first, then ON, then OFF again so we can spot
    // session-state drift (allocator / page-cache effects identified
    // in [[optimizer-codegen-sensitivity]] notes).
    let (off_med, off_sd, off_samples) = time_query(&off_ctx, &q17, trials).await;
    println!(
        "Q17 OFF (stock):   {off_med:7.2} ms ± {off_sd:.2}   samples={:?}",
        off_samples.iter().map(|x| format!("{x:.1}")).collect::<Vec<_>>()
    );

    let (on_med, on_sd, on_samples) = time_query(&on_ctx, &q17, trials).await;
    println!(
        "Q17 ON  (rh-avg):  {on_med:7.2} ms ± {on_sd:.2}   samples={:?}",
        on_samples.iter().map(|x| format!("{x:.1}")).collect::<Vec<_>>()
    );

    let (off2_med, off2_sd, _) = time_query(&off_ctx, &q17, trials).await;
    println!("Q17 OFF (re-run):  {off2_med:7.2} ms ± {off2_sd:.2}");

    let delta = (on_med - off_med) / off_med * 100.0;
    println!(
        "\nΔ vs OFF #1 = {:+.2} ms ({:+.1}%)",
        on_med - off_med,
        delta,
    );
    println!(
        "Δ vs OFF #2 = {:+.2} ms ({:+.1}%)",
        on_med - off2_med,
        (on_med - off2_med) / off2_med * 100.0,
    );

    let verdict = if delta <= -10.0 {
        "✓ gate PASSED (≥10% improvement) — proceed to 22q SF=10 publishable run"
    } else if delta <= -3.0 {
        "~ marginal win — closer look before committing to 22q"
    } else if delta < 3.0 {
        "✗ no meaningful improvement — investigate before more bench effort"
    } else {
        "✗ REGRESSION — fix correctness/perf bug before proceeding"
    };
    println!("\n{verdict}");

    Ok(())
}
