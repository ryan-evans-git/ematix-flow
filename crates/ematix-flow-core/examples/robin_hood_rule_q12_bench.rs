//! Σ.N.d wall-time bench — does `EnableRobinHoodAggregateRule`
//! convert the 1.16-1.54× microbench win into a real SQL-path win?
//!
//! Three Int64 GROUP BY cardinalities on TPC-H SF=1 lineitem:
//!
//!   l_linenumber  — 7 keys (matches Q12 l_shipmode shape but i64)
//!   l_suppkey     — 10K keys (where the microbench showed 1.54×)
//!   l_partkey     — 200K keys (Robin Hood's load-factor advantage)
//!
//! Each query runs against two contexts:
//!   - OFF:  default SessionStateBuilder (stock hash agg)
//!   - ON:   install_robin_hood_rule applied
//!
//! 5-rep median; reports wall time + speedup per query.
//!
//! Run:
//!   cargo run --release -p ematix-flow-core --example robin_hood_rule_q12_bench

use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::SessionContext;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::robin_hood_agg_rule::install_robin_hood_rule;
use futures_util::TryStreamExt;

const REPS: usize = 5;

const QUERIES: &[(&str, &str)] = &[
    (
        "l_linenumber  (7 keys)",
        "SELECT l_linenumber, COUNT(*) FROM lineitem GROUP BY l_linenumber",
    ),
    (
        "l_suppkey     (10K keys)",
        "SELECT l_suppkey, COUNT(*) FROM lineitem GROUP BY l_suppkey",
    ),
    (
        "l_partkey     (200K keys)",
        "SELECT l_partkey, COUNT(*) FROM lineitem GROUP BY l_partkey",
    ),
];

fn target_partitions() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8)
}

fn build_ctx(dir: &str, with_rule: bool) -> SessionContext {
    let mut builder = SessionStateBuilder::new()
        .with_default_features()
        .with_config(
            datafusion::prelude::SessionConfig::new()
                .with_target_partitions(target_partitions()),
        );
    if with_rule {
        builder = install_robin_hood_rule(builder);
    }
    let ctx = SessionContext::new_with_state(builder.build());
    let path = format!("{dir}/lineitem.parquet");
    let prov = EmatixFastParquetTableProvider::try_new(&path).unwrap();
    ctx.register_table("lineitem", Arc::new(prov)).unwrap();
    ctx
}

async fn run_one(ctx: &SessionContext, sql: &str) -> Result<Duration, String> {
    let t = Instant::now();
    let df = ctx.sql(sql).await.map_err(|e| e.to_string())?;
    let plan = df.create_physical_plan().await.map_err(|e| e.to_string())?;
    let mut n = 0usize;
    for p in 0..plan.output_partitioning().partition_count() {
        let mut s = plan.execute(p, ctx.task_ctx()).map_err(|e| e.to_string())?;
        while let Some(b) = s.try_next().await.map_err(|e| e.to_string())? {
            n += b.num_rows();
        }
    }
    std::hint::black_box(n);
    Ok(t.elapsed())
}

async fn measure(ctx: &SessionContext, sql: &str) -> Option<f64> {
    let _ = run_one(ctx, sql).await; // warmup
    let mut times = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        match run_one(ctx, sql).await {
            Ok(d) => times.push(d.as_secs_f64() * 1000.0),
            Err(_) => return None,
        }
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    Some(times[REPS / 2])
}

async fn rule_fires_in_plan(ctx: &SessionContext, sql: &str) -> bool {
    let Ok(df) = ctx.sql(sql).await else {
        return false;
    };
    let Ok(plan) = df.create_physical_plan().await else {
        return false;
    };
    format!("{plan:?}").contains("RobinHoodAggregateExec")
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let dir = std::env::var("TPCH_DATA_DIR")
        .unwrap_or_else(|_| "examples/tpch/data/sf1".to_string());
    println!("=== Σ.N.d wall-time bench (rule on vs off) — {dir} ===\n");
    println!(
        "{:<28} {:>10} {:>10} {:>10} {:>10}",
        "Query", "OFF (ms)", "ON (ms)", "Δ%", "rule fired?"
    );
    println!("{}", "-".repeat(70));

    let off_ctx = build_ctx(&dir, false);
    let on_ctx = build_ctx(&dir, true);

    for (label, sql) in QUERIES {
        let fired = rule_fires_in_plan(&on_ctx, sql).await;

        let off = match measure(&off_ctx, sql).await {
            Some(t) => t,
            None => {
                println!("{label:<28} (failed)");
                continue;
            }
        };
        let on = match measure(&on_ctx, sql).await {
            Some(t) => t,
            None => {
                println!("{label:<28} (failed)");
                continue;
            }
        };
        let delta = (on - off) / off * 100.0;
        let arrow = if delta < -1.0 {
            "✓"
        } else if delta > 1.0 {
            "✗"
        } else {
            "~"
        };
        println!(
            "{:<28} {:>10.2} {:>10.2} {:>9.1}% {:>10}  {}",
            label,
            off,
            on,
            delta,
            if fired { "YES" } else { "NO" },
            arrow,
        );
    }

    println!();
    println!("Legend: ✓ = ON faster (rule helped)   ✗ = ON slower (regression)");
}
