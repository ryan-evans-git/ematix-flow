//! Π.16 — samply-friendly Q06 SF=10 profile target.
//!
//! Runs Q06 in a tight loop (default 200 reps) with the same ematix
//! providers + preset rule set the bench uses. Long enough for samply
//! to gather ~10K stack samples at 1 kHz.
//!
//! Run:
//!   cargo build --release -p ematix-flow-core --example q06_profile_loop \
//!     --features triangulation
//!   samply record --rate 1000 -- ./target/release/examples/q06_profile_loop
//!   open the recording in samply's UI and look at the flame graph for
//!   the post-warmup tail.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::preset;
use futures_util::TryStreamExt;

const Q06_SQL: &str = "SELECT SUM(l_extendedprice * l_discount) AS revenue \
                       FROM lineitem \
                       WHERE l_shipdate >= DATE '1994-01-01' \
                         AND l_shipdate <  DATE '1995-01-01' \
                         AND l_discount BETWEEN 0.05 AND 0.07 \
                         AND l_quantity < 24";

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf10"));
    let reps: usize = std::env::var("REPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);
    let warmups: usize = std::env::var("WARMUPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);

    let state = preset::with_optimizer_rules(
        SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(14))
            .with_default_features(),
    )
    .build();
    let ctx = SessionContext::new_with_state(state);
    for t in TPCH_TABLES {
        let path = dir.join(format!("{t}.parquet"));
        let prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy().as_ref())?;
        ctx.register_table(*t, Arc::new(prov))?;
    }

    println!("Π.16 Q06 profile loop: reps={reps} warmups={warmups}");
    let t_start = std::time::Instant::now();
    for i in 0..(reps + warmups) {
        let df = ctx.sql(Q06_SQL).await?;
        let plan = df.create_physical_plan().await?;
        let mut total = 0usize;
        for p in 0..plan.output_partitioning().partition_count() {
            let mut s = plan.execute(p, ctx.task_ctx())?;
            while let Some(b) = s.try_next().await? {
                total += b.num_rows();
            }
        }
        if i == warmups {
            println!("warmup done; measuring next {reps} reps");
        }
        if total == 0 {
            eprintln!("warning: rep {i} returned 0 rows");
        }
    }
    let total_ms = t_start.elapsed().as_secs_f64() * 1000.0;
    println!("Π.16 Q06 profile loop: total {total_ms:.0} ms ({:.1} ms/rep)", total_ms / (reps + warmups) as f64);
    Ok(())
}
