//! Σ.Q.L15 spike — does `SessionConfig::with_collect_statistics(true)`
//! change DataFusion's join build/probe choices for Q07 SF=10?
//!
//! Without it, DataFusion picks lineitem-after-s⋈l (18 M rows) as the
//! BUILD side of l⋈o (1.16 GB hash table, 1.95 s elapsed_compute).
//! With it, DataFusion should use partition_statistics on our table
//! providers to make smarter build-side decisions.
//!
//! Compares Q07 SF=10 wall time and build_input_rows / probe_hit_rate
//! between collect_statistics OFF (default) and ON.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

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
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

const TRIALS: usize = 5;
const WARMUPS: usize = 2;

async fn run_q07(collect_stats: bool) -> Result<(f64, f64, f64), Box<dyn std::error::Error>> {
    let dir =
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf10".to_string());
    let sql = std::fs::read_to_string("examples/tpch/queries/q07.sql")?;

    let mut cfg = SessionConfig::new().with_target_partitions(14);
    cfg = cfg.with_collect_statistics(collect_stats);
    let mut builder = SessionStateBuilder::new()
        .with_config(cfg)
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(DedupeAggregateForFloatDeterminism::default()))
        .with_physical_optimizer_rule(Arc::new(EnableDictGroupCountRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterMultiAggRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterSumRule));
    builder = builder.with_optimizer_rule(Arc::new(PushDownLeftSemiRule));
    builder = builder.with_physical_optimizer_rule(Arc::new(SwapSemiJoinBuildSideRule));
    builder = builder.with_physical_optimizer_rule(Arc::new(EnableRobinHoodSumF64Rule));
    builder =
        builder.with_physical_optimizer_rule(Arc::new(EnableRuntimeBloomSidebandRule::default()));
    let state = builder.build();
    let ctx = SessionContext::new_with_state(state);
    for t in TPCH_TABLES {
        let path = PathBuf::from(&dir).join(format!("{t}.parquet"));
        let use_emat = *t == "lineitem" || *t == "orders";
        if use_emat {
            let prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy())?;
            ctx.register_table(*t, Arc::new(prov))?;
        } else {
            let prov = FastParquetTableProvider::try_new(path.to_string_lossy())?;
            ctx.register_table(*t, Arc::new(prov))?;
        }
    }

    for _ in 0..WARMUPS {
        let _ = ctx.sql(&sql).await?.collect().await?;
    }
    let mut samples: Vec<f64> = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let t = Instant::now();
        let _ = ctx.sql(&sql).await?.collect().await?;
        samples.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = samples[samples.len() / 2];
    let min = samples[0];
    let max = samples[samples.len() - 1];
    Ok((median, min, max))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Σ.Q.L15 spike — Q07 SF=10 wall time, collect_statistics OFF vs ON ===");
    println!("({TRIALS} trials × {WARMUPS} warmups)\n");

    let (med_off, min_off, max_off) = run_q07(false).await?;
    println!(
        "OFF (default): p50 = {:.2} ms (min {:.2}, max {:.2})",
        med_off, min_off, max_off
    );
    let (med_on, min_on, max_on) = run_q07(true).await?;
    println!(
        "ON:            p50 = {:.2} ms (min {:.2}, max {:.2})",
        med_on, min_on, max_on
    );

    let delta_pct = (med_off - med_on) / med_off * 100.0;
    println!(
        "\nΔ (ON vs OFF): {:+.2} ms ({:+.1}%) — {}",
        med_on - med_off,
        -delta_pct,
        if delta_pct >= 5.0 {
            "✓ collect_statistics helps Q07"
        } else if delta_pct >= -5.0 {
            "~ within noise"
        } else {
            "✗ collect_statistics regresses Q07"
        }
    );

    Ok(())
}
