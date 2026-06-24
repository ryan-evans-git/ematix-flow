//! Drain A/B/C — isolate the triangulation-bench-vs-preset SF=10 gap.
//!
//! All three arms use the EXACT preset ctx (preset::with_optimizer_rules +
//! EmatixFastParquetTableProvider == run_shard.rs). The ONLY thing that varies
//! is how the (identical) physical plan is driven:
//!
//!   C  = production path: FRESH ctx per trial + `df.collect()`  (== rebench == run_shard)
//!   A  = reused ctx, plan built ONCE, then `collect(plan)` per trial (planning factored OUT)
//!   B  = reused ctx, plan built ONCE, then CoalescePartitions + execute(0) manual drain (== bench)
//!
//! Reading:
//!   C ≫ A ≈ B   → the gap is per-query PLANNING/CTX-setup overhead (bench amortizes via plan cache)
//!   C ≈ A ≫ B   → the gap is the DRAIN METHOD (collect vs manual execute) — adoptable in run_shard
//!   C ≈ A ≈ B   → no harness lever; the bench's speed is something this test doesn't capture
//!
//!   TPCH_DATA_DIR (default examples/tpch/data/sf10), TRIALS (10), TPCH_QUERIES (5,17,8,6)
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::preset;
use futures_util::TryStreamExt;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TABLES: &[&str] = &[
    "lineitem", "orders", "customer", "supplier", "nation", "region", "part", "partsupp",
];

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn build_ctx(data_dir: &str) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let state = preset::with_optimizer_rules(
        SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_default_features(),
    )
    .build();
    let ctx = SessionContext::new_with_state(state);
    for t in TABLES {
        let p = format!("{data_dir}/{t}.parquet");
        ctx.register_table(*t, Arc::new(EmatixFastParquetTableProvider::try_new(p)?))?;
    }
    Ok(ctx)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir =
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf10".to_string());
    let trials: usize = std::env::var("TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let warmups = 3usize;
    let qids: Vec<u8> = std::env::var("TPCH_QUERIES")
        .ok()
        .map(|s| s.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![5, 17, 8, 6]);

    println!("drain A/B/C  data={data_dir}  trials={trials} warmups={warmups}");
    println!(
        "{:<5} {:>22} {:>22} {:>22}",
        "Q", "C fresh+collect", "A reused+collect", "B reused+drain(exec0)"
    );

    for q in &qids {
        let sql = std::fs::read_to_string(format!("examples/tpch/queries/q{q:02}.sql"))?;
        let sql = sql.trim().trim_end_matches(';').to_string();

        // Arm C: production — fresh ctx per trial + df.collect()
        let mut c = Vec::new();
        for i in 0..(warmups + trials) {
            let ctx = build_ctx(&data_dir)?;
            let t = Instant::now();
            let df = ctx.sql(&sql).await?;
            let _b = df.collect().await?;
            let ms = t.elapsed().as_secs_f64() * 1000.0;
            if i >= warmups {
                c.push(ms);
            }
        }

        // Reused ctx (warm providers); plan RE-CREATED per trial (stateful ops
        // like RepartitionExec can't re-execute the same instance). Re-planning
        // is ~1ms and identical in A & B, so it doesn't bias the collect-vs-drain
        // comparison; vs arm C it isolates fresh-ctx/provider-cold-start.
        let ctx = build_ctx(&data_dir)?;
        let logical = ctx.sql(&sql).await?.logical_plan().clone();

        // Arm A: warm ctx + create_physical_plan + collect()
        let mut a = Vec::new();
        for i in 0..(warmups + trials) {
            let t = Instant::now();
            let plan = ctx.state().create_physical_plan(&logical).await?;
            let _b = collect(plan, ctx.task_ctx()).await?;
            let ms = t.elapsed().as_secs_f64() * 1000.0;
            if i >= warmups {
                a.push(ms);
            }
        }

        // Arm B: warm ctx + create_physical_plan + manual CoalescePartitions + execute(0) (== bench)
        let mut b = Vec::new();
        for i in 0..(warmups + trials) {
            let t = Instant::now();
            let plan = ctx.state().create_physical_plan(&logical).await?;
            let drain: Arc<dyn ExecutionPlan> = if plan.output_partitioning().partition_count() > 1
            {
                Arc::new(CoalescePartitionsExec::new(plan))
            } else {
                plan
            };
            let mut s = drain.execute(0, ctx.task_ctx())?;
            let mut rows = 0usize;
            while let Some(batch) = s.try_next().await? {
                rows += batch.num_rows();
            }
            std::hint::black_box(rows);
            let ms = t.elapsed().as_secs_f64() * 1000.0;
            if i >= warmups {
                b.push(ms);
            }
        }

        println!(
            "Q{q:02}  {:>21.1} {:>22.1} {:>22.1}",
            median(&mut c),
            median(&mut a),
            median(&mut b)
        );
    }
    Ok(())
}
