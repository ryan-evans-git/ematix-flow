//! TEMP (orchestration Phase-0, 2026-06-03): EXPLAIN ANALYZE a TPC-H query
//! through the SAME production context as profile_query (ForceCollectLeft,
//! PushDownLeftSemi, bloom sideband, dict/fused rules) so per-operator
//! `elapsed_compute` + `output_rows` + the partition structure reflect the
//! real plan. Localises which stage is the serial tail at SF=10.
//!
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 TPCH_QUERY=8 \
//!     cargo run --release -p ematix-flow-core --example explain_query --features triangulation
use std::path::Path;
use std::sync::Arc;

use arrow_array::Array;

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

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("examples/tpch/data/sf10"));
    let q: u8 = std::env::var("TPCH_QUERY")
        .ok()
        .and_then(|s| s.parse().ok())
        .expect("set TPCH_QUERY=N");
    let sql_path = format!("examples/tpch/queries/q{q:02}.sql");
    let sql = std::fs::read_to_string(&sql_path)?;
    eprintln!("EXPLAIN ANALYZE Q{q:02}, data={}", data_dir.display());

    let mut builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(DedupeAggregateForFloatDeterminism::default()))
        .with_physical_optimizer_rule(Arc::new(EnableDictGroupCountRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterMultiAggRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterSumRule));
    builder = builder.with_optimizer_rule(Arc::new(PushDownLeftSemiRule));
    builder = builder.with_physical_optimizer_rule(Arc::new(SwapSemiJoinBuildSideRule));
    builder = builder.with_physical_optimizer_rule(Arc::new(
        ematix_flow_core::force_collect_left_semi_build_rule::ForceCollectLeftForSemiBoundedBuildRule::default(),
    ));
    if std::env::var("EMAT_RH_SUM_F64")
        .ok()
        .map(|v| v == "1")
        .unwrap_or(false)
    {
        builder =
            builder.with_physical_optimizer_rule(Arc::new(EnableRobinHoodSumF64Rule::default()));
    }
    // ::default() reads the production env gates: EMAT_RT_BLOOM_INNER_JOIN,
    // EMAT_L9_REQUIRE_FILTERED_BUILD, EMAT_L9_MAX_EXPECTED_KEYS, EMAT_RT_BLOOM_RATIO.
    builder =
        builder.with_physical_optimizer_rule(Arc::new(EnableRuntimeBloomSidebandRule::default()));
    // HJ.3: swap rule runs last; no-op unless EMAT_HASH_JOIN=1.
    builder = builder.with_physical_optimizer_rule(Arc::new(
        ematix_flow_core::swap_emat_hash_join_rule::SwapEmatixHashJoinRule,
    ));
    let ctx = SessionContext::new_with_state(builder.build());
    register_tables(&ctx, &data_dir)?;

    // EXPLAIN_PLAN_ONLY=1 → plain EXPLAIN (no execution, no warm-up): inspect the
    // physical plan STRUCTURE (join mode, RepartitionExec) without paying the
    // (possibly pathological / timeout-prone) execution. Default = EXPLAIN ANALYZE.
    let plan_only = std::env::var("EXPLAIN_PLAN_ONLY").ok().as_deref() == Some("1");
    if !plan_only {
        // Warm OS cache + any process-level caches.
        let _ = ctx.sql(&sql).await?.collect().await?;
        let _ = ctx.sql(&sql).await?.collect().await?;
    }
    let explain_kw = if plan_only { "EXPLAIN" } else { "EXPLAIN ANALYZE" };
    let batches = ctx
        .sql(&format!("{explain_kw} {sql}"))
        .await?
        .collect()
        .await?;
    for b in &batches {
        let col = b
            .column(b.num_columns() - 1)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("plan column is Utf8");
        for i in 0..col.len() {
            println!("{}", col.value(i));
        }
    }
    Ok(())
}

fn register_tables(
    ctx: &SessionContext,
    data_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let all_emat = std::env::var("EMAT_ALL_TABLES_EMAT")
        .ok()
        .map(|v| v == "1")
        .unwrap_or(false);
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        let use_emat = all_emat || *t == "lineitem" || *t == "orders";
        if use_emat {
            ctx.register_table(
                *t,
                Arc::new(EmatixFastParquetTableProvider::try_new(
                    path.to_string_lossy(),
                )?),
            )?;
        } else {
            ctx.register_table(
                *t,
                Arc::new(FastParquetTableProvider::try_new(path.to_string_lossy())?),
            )?;
        }
    }
    Ok(())
}
