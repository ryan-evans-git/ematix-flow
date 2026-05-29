//! Σ.BR Phase 2 — preset-path timing harness.
//!
//! Times a single TPC-H query through the *library* path: a context built by
//! `preset::with_optimizer_rules` (so the `ReorderQueryPlanner` and all the
//! milestone rules fire exactly as a library user gets them), with every
//! table registered as `EmatixFastParquetTableProvider`. This is the gate the
//! triangulation bench can't cover — it builds its own `SessionStateBuilder`
//! and applies the reorder manually, so it never exercises the preset
//! QueryPlanner.
//!
//! A/B the reorder by toggling `EMAT_REORDER_QP`:
//!   Q=5 EMAT_REORDER_QP=1 TPCH_DATA_DIR=examples/tpch/data/sf100 \
//!     ./target/release/examples/tpch_preset_bench      # reorder ON  (default)
//!   Q=5 EMAT_REORDER_QP=0 TPCH_DATA_DIR=examples/tpch/data/sf100 \
//!     ./target/release/examples/tpch_preset_bench      # reorder OFF
//!
//! Env: Q (default 5), TPCH_DATA_DIR (default examples/tpch/data/sf1),
//!      TRIALS (default 5), WARMUPS (default 2), PARTITIONS (default cores).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::preset;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let q: u8 = std::env::var("Q").ok().and_then(|s| s.parse().ok()).unwrap_or(5);
    let dir =
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf1".to_string());
    let trials: usize = std::env::var("TRIALS").ok().and_then(|s| s.parse().ok()).unwrap_or(5);
    let warmups: usize = std::env::var("WARMUPS").ok().and_then(|s| s.parse().ok()).unwrap_or(2);
    let partitions: usize = std::env::var("PARTITIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(14));

    // The library path: preset installs the milestone rules + the
    // ReorderQueryPlanner (gated by EMAT_REORDER_QP, default ON).
    let state = preset::with_optimizer_rules(
        SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(partitions))
            .with_default_features(),
    )
    .build();
    let ctx = SessionContext::new_with_state(state);
    for t in TPCH_TABLES {
        let path = PathBuf::from(&dir).join(format!("{t}.parquet"));
        let prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy())?;
        ctx.register_table(*t, Arc::new(prov))?;
    }

    let sql = std::fs::read_to_string(format!("examples/tpch/queries/q{q:02}.sql"))?;
    let qp = std::env::var("EMAT_REORDER_QP").unwrap_or_else(|_| "1(default)".to_string());

    // Warmups (untimed) prime the OS page cache + JIT-free steady state.
    for _ in 0..warmups {
        let _ = ctx.sql(&sql).await?.collect().await?;
    }
    let mut times_ms: Vec<f64> = Vec::with_capacity(trials);
    let mut rows = 0usize;
    for _ in 0..trials {
        let start = Instant::now();
        let batches = ctx.sql(&sql).await?.collect().await?;
        times_ms.push(start.elapsed().as_secs_f64() * 1000.0);
        rows = batches.iter().map(|b| b.num_rows()).sum();
    }

    let mean = times_ms.iter().sum::<f64>() / times_ms.len() as f64;
    let var = times_ms.iter().map(|t| (t - mean).powi(2)).sum::<f64>() / times_ms.len() as f64;
    let std = var.sqrt();
    println!(
        "Q{q:02}  preset-path  {mean:8.2} ms ± {std:6.2}  ({rows} rows)  EMAT_REORDER_QP={qp}  partitions={partitions}  trials={trials}"
    );
    Ok(())
}
