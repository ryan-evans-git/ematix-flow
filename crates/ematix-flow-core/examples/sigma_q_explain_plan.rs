//! Σ.Q diagnostic — dump structural physical plan (NOT ANALYZE) so we
//! can see which side of each HashJoinExec is build vs probe and whether
//! statistics propagated.
//!
//! Usage:
//!   Q=18 TPCH_DATA_DIR=examples/tpch/data/sf10 \
//!     ./target/release/examples/sigma_q_explain_plan

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::dedupe_aggregate_rule::DedupeAggregateForFloatDeterminism;
use ematix_flow_core::dict_aggregate_rule::EnableDictGroupCountRule;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fused_aggregate_filter_multi_agg_rule::InjectFilterMultiAggRule;
use ematix_flow_core::fused_aggregate_filter_sum_rule::InjectFilterSumRule;
use ematix_flow_core::push_down_left_semi_rule::PushDownLeftSemiRule;
use ematix_flow_core::robin_hood_sum_f64_exec::EnableRobinHoodSumF64Rule;
use ematix_flow_core::swap_semi_join_build_rule::SwapSemiJoinBuildSideRule;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let q: u8 = std::env::var("Q")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(18);
    let dir =
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf10".to_string());

    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features()
        .with_optimizer_rule(Arc::new(PushDownLeftSemiRule))
        .with_physical_optimizer_rule(Arc::new(DedupeAggregateForFloatDeterminism::default()))
        .with_physical_optimizer_rule(Arc::new(EnableDictGroupCountRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterMultiAggRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterSumRule))
        .with_physical_optimizer_rule(Arc::new(SwapSemiJoinBuildSideRule))
        .with_physical_optimizer_rule(Arc::new(EnableRobinHoodSumF64Rule))
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

    let sql = std::fs::read_to_string(format!("examples/tpch/queries/q{q:02}.sql"))?;
    let df = ctx.sql(&sql).await?;
    if std::env::var("EMAT_DUMP_LOGICAL").is_ok() {
        println!("=== Q{q:02} unoptimized LogicalPlan ===");
        println!("{}", df.logical_plan().display_indent());
        let optimized = df.clone().into_optimized_plan()?;
        println!("\n=== Q{q:02} optimized LogicalPlan ===");
        println!("{}", optimized.display_indent());
    }
    let df = if std::env::var("EMAT_REORDER").is_ok() || std::env::var("EMAT_AGG_SEMI").is_ok() {
        let optimized = df.into_optimized_plan()?;
        println!("=== Q{q:02} optimized LogicalPlan (pre-rewrite) ===");
        println!("{}", optimized.display_indent());
        let mut rewritten = optimized;
        if std::env::var("EMAT_AGG_SEMI").is_ok() {
            rewritten = ematix_flow_core::agg_filter_pushdown::push_filter_into_agg(rewritten)?;
        }
        if std::env::var("EMAT_REORDER").is_ok() {
            rewritten = ematix_flow_core::join_reorder::reorder_inner_joins(rewritten)?;
        }
        println!("\n=== Q{q:02} optimized LogicalPlan (POST-rewrite) ===");
        println!("{}", rewritten.display_indent());
        ctx.execute_logical_plan(rewritten).await?
    } else {
        df
    };
    let plan = df.create_physical_plan().await?;
    println!("\n=== Q{q:02} physical plan ===");
    println!("{}", displayable(plan.as_ref()).indent(true));
    Ok(())
}
