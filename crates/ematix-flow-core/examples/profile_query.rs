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

// Match the triangulation bench / stage_profiler: production runs on mimalloc,
// not the macOS system allocator. Profiling without this overstates malloc cost.
// Match the triangulation bench / stage_profiler: production runs on mimalloc,
// not the macOS system allocator. Profiling without this overstates malloc cost.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| {
            std::path::PathBuf::from("crates/ematix-flow-core/examples/tpch/data/sf1")
        });
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
    // REV.17.3: mirror production preset — force CollectLeft on
    // semi-bounded builds + scale-relative broadcast of small dim builds
    // (both via ::default()), so profiles reflect the production hot path.
    builder = builder.with_physical_optimizer_rule(Arc::new(
        ematix_flow_core::force_collect_left_semi_build_rule::ForceCollectLeftForSemiBoundedBuildRule::default(),
    ));
    // Σ.Q.L1b: matches the triangulation bench's gating on
    // EMAT_RH_SUM_F64=1. Default-off avoids the ~7% geomean codegen
    // tax from [[optimizer-codegen-sensitivity]] when the operator
    // doesn't fire.
    if std::env::var("EMAT_RH_SUM_F64")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        builder =
            builder.with_physical_optimizer_rule(Arc::new(EnableRobinHoodSumF64Rule::default()));
    }
    // Σ.Q.L15: ratio=1024 is the perf-validated setting from the
    // triangulation bench. `Default::default()` uses ratio=64 which
    // is the original L9 default but fires on net-negative shapes
    // (s⋈l, etc.) per the L15 memory. allow_inner_join is read from
    // EMAT_RT_BLOOM_INNER_JOIN at rule-construction time inside
    // Default::default(); reading it here mirrors the bench wiring.
    let bloom_ratio = std::env::var("EMAT_RT_BLOOM_RATIO")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1024);
    let allow_inner = std::env::var("EMAT_RT_BLOOM_INNER_JOIN").is_ok();
    builder = builder.with_physical_optimizer_rule(Arc::new(EnableRuntimeBloomSidebandRule {
        min_probe_to_build_ratio: bloom_ratio,
        allow_inner_join: allow_inner,
        require_filtered_build: false,
        max_expected_keys_per_partition: 0,
    }));
    // HJ.3: swap rule runs last; no-op unless EMAT_HASH_JOIN=1.
    builder = builder.with_physical_optimizer_rule(Arc::new(
        ematix_flow_core::swap_emat_hash_join_rule::SwapEmatixHashJoinRule,
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

fn register_tables(
    ctx: &SessionContext,
    data_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    // Σ.Q.L15: when EMAT_ALL_TABLES_EMAT=1, every TPC-H table goes
    // through EmatixFastParquetTableProvider so the L9 runtime bloom
    // sideband can target supplier/customer/part scans (without it
    // those land on FastParquet and L9's find_probe_scan_for_column
    // walks past them, causing Inner-L9 firings to wait on side-
    // bands that never get consumed).
    let all_emat = std::env::var("EMAT_ALL_TABLES_EMAT")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        let use_emat = all_emat || *t == "lineitem" || *t == "orders";
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
