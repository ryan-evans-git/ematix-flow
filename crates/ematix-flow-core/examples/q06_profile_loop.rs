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

    let use_plan_cache = std::env::var("PLAN_CACHE").ok().as_deref() == Some("1");
    let cache = ematix_flow_core::plan_cache::PlanCache::new();
    println!("Π.16 Q06 profile loop: reps={reps} warmups={warmups} plan_cache={use_plan_cache}");
    let t_start = std::time::Instant::now();
    let mut samples: Vec<f64> = Vec::with_capacity(reps);
    let mut plan_ms_sum = 0.0_f64;
    let mut exec_ms_sum = 0.0_f64;
    for i in 0..(reps + warmups) {
        let t_iter = std::time::Instant::now();
        let plan = if use_plan_cache {
            cache.get_or_plan(&ctx, Q06_SQL).await?
        } else {
            let df = ctx.sql(Q06_SQL).await?;
            df.create_physical_plan().await?
        };
        let t_plan = t_iter.elapsed().as_secs_f64() * 1000.0;
        let t_exec_start = std::time::Instant::now();
        let mut total = 0usize;
        for p in 0..plan.output_partitioning().partition_count() {
            let mut s = plan.execute(p, ctx.task_ctx())?;
            while let Some(b) = s.try_next().await? {
                total += b.num_rows();
            }
        }
        let t_exec = t_exec_start.elapsed().as_secs_f64() * 1000.0;
        let ms = t_iter.elapsed().as_secs_f64() * 1000.0;
        if i >= warmups {
            samples.push(ms);
            plan_ms_sum += t_plan;
            exec_ms_sum += t_exec;
        }
        if i == warmups {
            println!("warmup done; measuring next {reps} reps");
        }
        if total == 0 {
            eprintln!("warning: rep {i} returned 0 rows");
        }
    }
    let total_ms = t_start.elapsed().as_secs_f64() * 1000.0;
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = samples.len();
    let n_meas = n as f64;
    let p50 = samples[n / 2];
    let p10 = samples[n / 10];
    let p90 = samples[n * 9 / 10];
    let p99 = samples[(n * 99 / 100).min(n - 1)];
    let min = samples[0];
    let max = samples[n - 1];
    let mean: f64 = samples.iter().sum::<f64>() / n_meas;
    println!(
        "Π.16 breakdown: plan_avg={:.2}ms exec_avg={:.2}ms",
        plan_ms_sum / n_meas,
        exec_ms_sum / n_meas
    );
    println!(
        "Π.16 Q06 profile loop: total {total_ms:.0} ms; per-rep mean={mean:.2} p10={p10:.2} p50={p50:.2} p90={p90:.2} p99={p99:.2} min={min:.2} max={max:.2} ms"
    );
    if use_plan_cache {
        let (h, m) = cache.stats();
        println!("cache: hits={h} misses={m} entries={}", cache.len());
    }
    Ok(())
}
