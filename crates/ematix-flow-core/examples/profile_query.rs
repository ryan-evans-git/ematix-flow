//! Run a single TPC-H query in a tight loop for profiling.
//!
//! ## Usage
//!
//! Build with debug symbols:
//!
//! ```
//! cargo build --release -p ematix-flow-core --example profile_query
//! ```
//!
//! Profile with samply (macOS, no sudo needed):
//!
//! ```
//! TPCH_DATA_DIR=examples/tpch/data/sf10 TPCH_QUERY=17 TRIALS=30 \
//!   samply record ./target/release/examples/profile_query
//! ```
//!
//! Environment:
//!   - TPCH_DATA_DIR (default examples/tpch/data/sf1)
//!   - TPCH_QUERY    (1..=22, required)
//!   - TRIALS        (default 30)
//!
//! Mirrors tpch_validate's run_ematix configuration so the profile
//! reflects production behavior. No DuckDB/Polars comparison; pure
//! ematix-flow path.

use std::path::Path;
use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::dedupe_aggregate_rule::DedupeAggregateForFloatDeterminism;
use ematix_flow_core::dict_aggregate_rule::EnableDictGroupCountRule;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fused_aggregate_filter_multi_agg_rule::InjectFilterMultiAggRule;
use ematix_flow_core::fused_aggregate_filter_sum_rule::InjectFilterSumRule;
use ematix_flow_core::push_down_left_semi_rule::PushDownLeftSemiRule;
use ematix_flow_core::robin_hood_sum_f64_exec::EnableRobinHoodSumF64Rule;
use ematix_flow_core::runtime_bloom_sideband_rule::EnableRuntimeBloomSidebandRule;
use ematix_flow_core::swap_semi_join_build_rule::SwapSemiJoinBuildSideRule;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("crates/ematix-flow-core/examples/tpch/data/sf1"));
    let q: u8 = std::env::var("TPCH_QUERY")
        .ok()
        .and_then(|s| s.parse().ok())
        .expect("set TPCH_QUERY=N");
    let trials: usize = std::env::var("TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);

    let sql_path = format!("examples/tpch/queries/q{q:02}.sql");
    let sql = std::fs::read_to_string(&sql_path)?;
    eprintln!(
        "profiling Q{q:02} ({}), {trials} trials, data={}",
        sql_path,
        data_dir.display()
    );

    let mut builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(DedupeAggregateForFloatDeterminism::default()))
        .with_physical_optimizer_rule(Arc::new(EnableDictGroupCountRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterMultiAggRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterSumRule));
    builder = builder.with_optimizer_rule(Arc::new(PushDownLeftSemiRule));
    builder = builder.with_physical_optimizer_rule(Arc::new(SwapSemiJoinBuildSideRule));
    builder = builder.with_physical_optimizer_rule(Arc::new(EnableRobinHoodSumF64Rule));
    builder = builder.with_physical_optimizer_rule(Arc::new(
        EnableRuntimeBloomSidebandRule::default(),
    ));
    let state = builder.build();
    let ctx = SessionContext::new_with_state(state);

    register_tables(&ctx, &data_dir)?;

    // Warmup (1 run)
    let _ = ctx.sql(&sql).await?.collect().await?;

    // Timed loop: profiler samples here.
    let t0 = std::time::Instant::now();
    for _ in 0..trials {
        let _ = ctx.sql(&sql).await?.collect().await?;
    }
    let elapsed = t0.elapsed();
    eprintln!(
        "done — {trials} trials in {:.3}s ({:.2} ms/trial)",
        elapsed.as_secs_f64(),
        elapsed.as_secs_f64() * 1000.0 / trials as f64
    );
    Ok(())
}

fn register_tables(ctx: &SessionContext, data_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        let use_emat = *t == "lineitem" || *t == "orders";
        if use_emat {
            let prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy())?;
            ctx.register_table(*t, Arc::new(prov))?;
        } else {
            let prov = FastParquetTableProvider::try_new(path.to_string_lossy())?;
            ctx.register_table(*t, Arc::new(prov))?;
        }
    }
    Ok(())
}
