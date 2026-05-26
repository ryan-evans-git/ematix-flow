//! Σ.AG.3 — verify plan cache correctness for queries with stateful
//! operators (SharedSubtreeExec, etc.). Runs Q15 and Q21 a few times
//! with the cache enabled and prints row counts per iteration.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::plan_cache::PlanCache;
use ematix_flow_core::preset;
use futures_util::TryStreamExt;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

const Q15_SQL: &str = include_str!("../../../examples/tpch/queries/q15.sql");
const Q21_SQL: &str = include_str!("../../../examples/tpch/queries/q21.sql");
const Q06_SQL: &str = include_str!("../../../examples/tpch/queries/q06.sql");

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf1"));

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

    let cache = PlanCache::new();

    for (name, sql) in [("Q06", Q06_SQL), ("Q15", Q15_SQL), ("Q21", Q21_SQL)] {
        println!("\n=== {name} ===");
        for rep in 0..5 {
            let t = std::time::Instant::now();
            let plan = cache.get_or_plan(&ctx, sql).await?;
            let mut total = 0usize;
            for p in 0..plan.output_partitioning().partition_count() {
                let mut s = plan.execute(p, ctx.task_ctx())?;
                while let Some(b) = s.try_next().await? {
                    total += b.num_rows();
                }
            }
            let ms = t.elapsed().as_secs_f64() * 1000.0;
            println!("  rep {rep}: {ms:7.2} ms, {total} rows");
        }
    }
    let (h, m) = cache.stats();
    println!("\ncache: hits={h} misses={m}");
    Ok(())
}
