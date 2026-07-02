//! Σ.Q.L15 spike — register ALL TPC-H tables as EmatixFastParquet, so
//! L9 can apply blooms to supplier/customer/nation scans (currently
//! only lineitem/orders are Emat). For Q07 SF=10 this should enable
//! a 2-row nation-filtered bloom to prune supplier 100K→8K at the
//! scan, and customer 1.5M→120K similarly.
//!
//! Tests Q07 SF=10 in 4 modes:
//!   A. Inner-L9 OFF, current default (lineitem+orders Emat)
//!   B. Inner-L9 ON ratio=64, current default
//!   C. Inner-L9 ON ratio=64, ALL tables Emat
//!   D. Inner-L9 ON ratio=1024, ALL tables Emat

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

async fn run_q07(
    allow_inner: bool,
    ratio: usize,
    all_emat: bool,
) -> Result<(f64, f64, usize), Box<dyn std::error::Error>> {
    let dir =
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf10".to_string());
    let sql = std::fs::read_to_string("examples/tpch/queries/q07.sql")?;

    let mut builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(DedupeAggregateForFloatDeterminism::default()))
        .with_physical_optimizer_rule(Arc::new(EnableDictGroupCountRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterMultiAggRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterSumRule));
    builder = builder.with_optimizer_rule(Arc::new(PushDownLeftSemiRule));
    builder = builder.with_physical_optimizer_rule(Arc::new(SwapSemiJoinBuildSideRule));
    builder = builder.with_physical_optimizer_rule(Arc::new(EnableRobinHoodSumF64Rule::default()));
    builder = builder.with_physical_optimizer_rule(Arc::new(EnableRuntimeBloomSidebandRule {
        min_probe_to_build_ratio: ratio,
        allow_inner_join: allow_inner,
        require_filtered_build: false,
        max_expected_keys_per_partition: 0,
        min_probe_proj_cols: 0,
        // Σ.AH.2: env-resolved NDV ceiling (EMAT_L9_NDV_MAX_ROWS /
        // EMAT_L9_PARTITIONED) + any future fields track the default.
        ..EnableRuntimeBloomSidebandRule::default()
    }));
    let state = builder.build();
    let ctx = SessionContext::new_with_state(state);
    for t in TPCH_TABLES {
        let path = PathBuf::from(&dir).join(format!("{t}.parquet"));
        let use_emat = all_emat || *t == "lineitem" || *t == "orders";
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
    let mut rows = 0usize;
    for _ in 0..TRIALS {
        let t = Instant::now();
        let batches = ctx.sql(&sql).await?.collect().await?;
        samples.push(t.elapsed().as_secs_f64() * 1000.0);
        rows = batches.iter().map(|b| b.num_rows()).sum();
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = samples[samples.len() / 2];
    let min = samples[0];
    Ok((median, min, rows))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Σ.Q.L15 spike — Q07 SF=10, ALL-EMAT × Inner-L9 ratio matrix ===");
    println!("({TRIALS} trials × {WARMUPS} warmups)\n");

    println!(
        "{:<42} {:>10} {:>10} {:>6}",
        "Setting", "p50 ms", "min ms", "rows"
    );
    println!("{}", "-".repeat(70));

    let (m, mn, r) = run_q07(false, 64, false).await?;
    println!(
        "{:<42} {:>9.2} {:>10.2} {:>6}",
        "A: L9 OFF, lineitem+orders Emat", m, mn, r
    );

    let (m, mn, r) = run_q07(true, 64, false).await?;
    println!(
        "{:<42} {:>9.2} {:>10.2} {:>6}",
        "B: L9 ON ratio=64, lineitem+orders", m, mn, r
    );

    let (m, mn, r) = run_q07(true, 64, true).await?;
    println!(
        "{:<42} {:>9.2} {:>10.2} {:>6}",
        "C: L9 ON ratio=64, ALL Emat", m, mn, r
    );

    let (m, mn, r) = run_q07(true, 1024, true).await?;
    println!(
        "{:<42} {:>9.2} {:>10.2} {:>6}",
        "D: L9 ON ratio=1024, ALL Emat", m, mn, r
    );

    Ok(())
}
