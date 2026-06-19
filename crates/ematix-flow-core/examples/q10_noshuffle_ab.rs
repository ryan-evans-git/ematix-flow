//! SF100.6 — Q10 SF=100 no-shuffle-join A/B. Times Q10 under three arms:
//!   STOCK         — pure DataFusion (Partitioned orders⋈lineitem + 2 shuffles)
//!   HJ_COLLECT    — EMAT_HASH_JOIN=1 (swaps only the CollectLeft joins)
//!   HJ_NOSHUFFLE  — + EMAT_HJ_PARTITIONED=1 (swaps the Partitioned orders⋈lineitem
//!                   to EmatixHashJoinExec, stripping BOTH RepartitionExecs)
//! Interleaved across rounds; per-arm median ms + f64 checksum (must match → the
//! no-shuffle join is byte-correct vs stock). Probe floor 300M selects only the
//! lineitem-fact join at SF=100.
//!
//! Usage: TRIALS=5 ROUNDS=2 TPCH_DATA_DIR=examples/tpch/data/sf100 \
//!   cargo run --release -p ematix-flow-core --example q10_noshuffle_ab --features triangulation

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Array, Float64Array};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::dedupe_aggregate_rule::DedupeAggregateForFloatDeterminism;
use ematix_flow_core::dict_aggregate_rule::EnableDictGroupCountRule;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fused_aggregate_filter_multi_agg_rule::InjectFilterMultiAggRule;
use ematix_flow_core::fused_aggregate_filter_sum_rule::InjectFilterSumRule;
use ematix_flow_core::push_down_left_semi_rule::PushDownLeftSemiRule;
use ematix_flow_core::runtime_bloom_sideband_rule::EnableRuntimeBloomSidebandRule;
use ematix_flow_core::swap_semi_join_build_rule::SwapSemiJoinBuildSideRule;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

fn build_ctx(data_dir: &Path) -> Result<SessionContext, Box<dyn std::error::Error>> {
    // Mirror examples/explain_query.rs's production rule set so STOCK reproduces
    // the bench's Q10 plan (Partitioned orders⋈lineitem + RepartitionExecs).
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
    builder =
        builder.with_physical_optimizer_rule(Arc::new(EnableRuntimeBloomSidebandRule::default()));
    builder = builder.with_physical_optimizer_rule(Arc::new(
        ematix_flow_core::swap_emat_hash_join_rule::SwapEmatixHashJoinRule,
    ));
    let ctx = SessionContext::new_with_state(builder.build());
    for t in TPCH_TABLES {
        let p = data_dir.join(format!("{t}.parquet"));
        if *t == "lineitem" || *t == "orders" {
            ctx.register_table(
                *t,
                Arc::new(EmatixFastParquetTableProvider::try_new(
                    p.to_string_lossy(),
                )?),
            )?;
        } else {
            ctx.register_table(
                *t,
                Arc::new(FastParquetTableProvider::try_new(p.to_string_lossy())?),
            )?;
        }
    }
    Ok(ctx)
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn set_arm(envs: &[(&str, &str)]) {
    unsafe {
        std::env::remove_var("EMAT_HASH_JOIN");
        std::env::remove_var("EMAT_HJ_PARTITIONED");
        std::env::remove_var("EMAT_HJ_MIN_PROBE");
        std::env::remove_var("EMAT_HJ_RADIX");
        std::env::remove_var("EMAT_HJ_TAG");
        for (k, v) in envs {
            std::env::set_var(k, v);
        }
    }
}

async fn run_once(
    data_dir: &Path,
    sql: &str,
) -> Result<(f64, usize, f64), Box<dyn std::error::Error>> {
    let ctx = build_ctx(data_dir)?;
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let t = Instant::now();
    let batches = collect(plan, ctx.task_ctx()).await?;
    let ms = t.elapsed().as_secs_f64() * 1e3;
    let mut rows = 0usize;
    let mut cs = 0.0f64;
    for b in &batches {
        rows += b.num_rows();
        for c in b.columns() {
            if let Some(a) = c.as_any().downcast_ref::<Float64Array>() {
                for i in 0..a.len() {
                    if a.is_valid(i) {
                        cs += a.value(i);
                    }
                }
            }
        }
    }
    Ok((ms, rows, cs))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf100"));
    let trials: usize = std::env::var("TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let rounds: usize = std::env::var("ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);
    let sql = std::fs::read_to_string(data_dir.join("../../queries/q10.sql"))
        .or_else(|_| std::fs::read_to_string("examples/tpch/queries/q10.sql"))?;

    let arms: Vec<(&str, Vec<(&str, &str)>)> = vec![
        ("STOCK", vec![]),
        (
            "HJ_NOSHUFFLE",
            vec![
                ("EMAT_HASH_JOIN", "1"),
                ("EMAT_HJ_PARTITIONED", "1"),
                ("EMAT_HJ_MIN_PROBE", "300000000"),
            ],
        ),
        // RADIX.2 spike: the per-partition radix build + per-execute B-scatter probe.
        (
            "HJ_RADIX",
            vec![
                ("EMAT_HASH_JOIN", "1"),
                ("EMAT_HJ_PARTITIONED", "1"),
                ("EMAT_HJ_RADIX", "1"),
                ("EMAT_HJ_MIN_PROBE", "300000000"),
            ],
        ),
    ];

    // Warm each arm once (binary load, page cache, planner).
    for (_, envs) in &arms {
        set_arm(envs);
        let _ = run_once(&data_dir, &sql).await?;
    }

    let mut per_arm: Vec<Vec<f64>> = vec![Vec::new(); arms.len()];
    let mut cs_rows = vec![(0usize, 0.0f64); arms.len()];
    for _ in 0..rounds {
        for (ai, (_, envs)) in arms.iter().enumerate() {
            set_arm(envs);
            let mut ts = Vec::with_capacity(trials);
            for _ in 0..trials {
                let (ms, rows, cs) = run_once(&data_dir, &sql).await?;
                ts.push(ms);
                cs_rows[ai] = (rows, cs);
            }
            per_arm[ai].push(median(ts));
        }
    }

    println!(
        "Q10 SF=100 no-shuffle A/B  data={}  trials={trials} rounds={rounds}",
        data_dir.display()
    );
    println!(
        "{:<14} {:>10} {:>10} {:>18}",
        "arm", "median_ms", "rows", "checksum"
    );
    let mut meds = Vec::new();
    for (ai, (name, _)) in arms.iter().enumerate() {
        let m = median(per_arm[ai].clone());
        let (rows, cs) = cs_rows[ai];
        meds.push(m);
        println!("{:<14} {:>10.1} {:>10} {:>18.1}", name, m, rows, cs);
    }
    let stock = meds[0];
    for (ai, (name, _)) in arms.iter().enumerate().skip(1) {
        println!(
            "{name:<14} vs STOCK: {:.3}× ({:+.1}%)",
            meds[ai] / stock,
            (meds[ai] - stock) / stock * 100.0
        );
    }
    // Confirm the radix table actually built (vs silently falling back to the
    // chained RobinHood on a non-unique build) — RADIX_BUILDS>0 ⟹ HJ_RADIX engaged.
    use std::sync::atomic::Ordering::Relaxed;
    println!(
        "[build counters] radix={} tag={} robinhood={}",
        ematix_flow_core::emat_hash_join::RADIX_BUILDS.load(Relaxed),
        ematix_flow_core::emat_hash_join::TAG_BUILDS.load(Relaxed),
        ematix_flow_core::emat_hash_join::RH_BUILDS.load(Relaxed),
    );
    set_arm(&[]);
    Ok(())
}
