//! Σ.Q diagnostic — run EXPLAIN ANALYZE on a single query so we get
//! per-operator wall-time + row counts. Used to identify which
//! operator is the SF=10 hot path before picking a lever.
//!
//! Usage:
//!   Q=18 TPCH_DATA_DIR=examples/tpch/data/sf10 \
//!     ./target/release/examples/sigma_q_explain_analyze

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::util::pretty::pretty_format_batches;
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
use ematix_flow_core::force_collect_left_semi_build_rule::ForceCollectLeftForSemiBoundedBuildRule;
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

    let env_on = |name: &str| {
        std::env::var(name)
            .ok()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    };
    let swap_semi = env_on("EMAT_SWAP_SEMI");
    let rt_bloom = env_on("EMAT_RT_BLOOM_SIDEBAND");
    let rh_sum_f64 = env_on("EMAT_RH_SUM_F64");
    let push_semi = env_on("EMAT_PUSH_SEMI");
    let force_collect_left = env_on("EMAT_FORCE_COLLECT_LEFT");

    let mut builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(DedupeAggregateForFloatDeterminism::default()))
        .with_physical_optimizer_rule(Arc::new(EnableDictGroupCountRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterMultiAggRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterSumRule));
    if push_semi {
        // Σ.Q.L10 — logical rule, installed via with_optimizer_rule.
        builder = builder.with_optimizer_rule(Arc::new(PushDownLeftSemiRule));
    }
    if swap_semi {
        builder = builder.with_physical_optimizer_rule(Arc::new(SwapSemiJoinBuildSideRule));
    }
    if rh_sum_f64 {
        builder = builder.with_physical_optimizer_rule(Arc::new(EnableRobinHoodSumF64Rule));
    }
    if rt_bloom {
        builder = builder
            .with_physical_optimizer_rule(Arc::new(EnableRuntimeBloomSidebandRule::default()));
    }
    if force_collect_left {
        // REV.3 — appended last so it runs AFTER SwapSemiJoinBuildSide
        // (which produces the RightSemi that build_subtree_has_semi_filter
        // detects) and after the built-in JoinSelection/EnforceDistribution.
        builder = builder
            .with_physical_optimizer_rule(Arc::new(ForceCollectLeftForSemiBoundedBuildRule::default()));
    }
    let state = builder.build();

    eprintln!(
        "sigma_q_explain_analyze rules: push_semi={push_semi} swap_semi={swap_semi} \
         rt_bloom={rt_bloom} rh_sum_f64={rh_sum_f64}"
    );
    let ctx = SessionContext::new_with_state(state);

    // Σ.Q.L9 only wraps EmatixFastParquetExec. If we want the
    // RightSemi 624-key bloom to push into the orders scan as a
    // filter, orders has to be registered as Emat too. Probe via
    // EMAT_REGISTER_ORDERS_AS_EMAT=1.
    let orders_as_emat = env_on("EMAT_REGISTER_ORDERS_AS_EMAT");
    for t in TPCH_TABLES {
        let path = PathBuf::from(&dir).join(format!("{t}.parquet"));
        let use_emat = *t == "lineitem" || (orders_as_emat && *t == "orders");
        if use_emat {
            let prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy())?;
            ctx.register_table(*t, Arc::new(prov))?;
        } else {
            let prov = FastParquetTableProvider::try_new(path.to_string_lossy())?;
            ctx.register_table(*t, Arc::new(prov))?;
        }
    }

    let sql = std::fs::read_to_string(format!("examples/tpch/queries/q{q:02}.sql"))?;

    // Warmup (3 runs to stabilize)
    for _ in 0..3 {
        ctx.sql(&sql).await?.collect().await?;
    }

    // Σ.Q.L10 — also dump the logical plan (initial + optimized) so
    // we can see where the IN-subquery's semi-filter ends up
    // *before* physical planning. EXPLAIN (no ANALYZE) returns both.
    if std::env::var("EMAT_EXPLAIN_LOGICAL").ok().as_deref() == Some("1") {
        println!("==== EXPLAIN (logical) ====");
        let logical_sql = format!("EXPLAIN {sql}");
        let lp_df = ctx.sql(&logical_sql).await?;
        let lp_batches = lp_df.collect().await?;
        println!("{}", pretty_format_batches(&lp_batches)?);
        println!();
    }

    // Now run with EXPLAIN ANALYZE
    let analyze_sql = format!("EXPLAIN ANALYZE {sql}");
    let df = ctx.sql(&analyze_sql).await?;
    let batches = df.collect().await?;
    println!("{}", pretty_format_batches(&batches)?);
    Ok(())
}
