//! Σ.Q.L11 SPIKE — does manually casting Q17's join/agg keys from
//! i64 to u32 measurably speed up the query? If yes, the broader
//! plan-rule lever is worth building. If no, we pivot.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

async fn time_query(ctx: &SessionContext, sql: &str, trials: usize) -> (f64, f64) {
    // Warmup
    for _ in 0..2 {
        ctx.sql(sql).await.unwrap().collect().await.unwrap();
    }
    let mut samples = Vec::with_capacity(trials);
    for _ in 0..trials {
        let t = Instant::now();
        ctx.sql(sql).await.unwrap().collect().await.unwrap();
        samples.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = samples[samples.len() / 2];
    let mean: f64 = samples.iter().sum::<f64>() / samples.len() as f64;
    let var: f64 =
        samples.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / samples.len() as f64;
    (median, var.sqrt())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf10".to_string());
    let trials: usize = std::env::var("TPCH_TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(11);

    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);

    for t in TPCH_TABLES {
        let path = PathBuf::from(&dir).join(format!("{t}.parquet"));
        if *t == "lineitem" {
            let prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy())?;
            ctx.register_table(*t, Arc::new(prov))?;
        } else {
            let prov = FastParquetTableProvider::try_new(path.to_string_lossy())?;
            ctx.register_table(*t, Arc::new(prov))?;
        }
    }

    let q17 = std::fs::read_to_string("examples/tpch/queries/q17.sql")?;
    let q17_u32 = std::fs::read_to_string("examples/tpch/queries/q17_u32.sql")?;

    println!("Σ.Q.L11 SPIKE — Q17 i64 keys vs u32 keys, SF=10, {trials} trials\n");

    let (med_i64, sigma_i64) = time_query(&ctx, &q17, trials).await;
    println!("Q17 (i64 keys, baseline):   {med_i64:7.2} ms ± {sigma_i64:.2}");

    let (med_u32, sigma_u32) = time_query(&ctx, &q17_u32, trials).await;
    println!("Q17 (u32 keys, ARROW_CAST): {med_u32:7.2} ms ± {sigma_u32:.2}");

    let delta = (med_u32 - med_i64) / med_i64 * 100.0;
    println!(
        "\nΔ = {:+.2} ms ({:+.1}%)  {}",
        med_u32 - med_i64,
        delta,
        if delta < -3.0 {
            "✓ u32 wins meaningfully — Σ.Q.L11 worth building"
        } else if delta < 0.0 {
            "~ marginal win — Σ.Q.L11 may not be worth full scope"
        } else {
            "✗ no win from u32 — pivot, don't build Σ.Q.L11"
        }
    );

    Ok(())
}
