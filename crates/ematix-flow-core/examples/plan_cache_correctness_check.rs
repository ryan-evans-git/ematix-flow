//! Σ.AG.3 — verify plan cache correctness for queries with stateful
//! operators (SharedSubtreeExec, etc.). Runs Q15 and Q21 a few times
//! with the cache enabled and prints row counts per iteration.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::plan_cache::PlanCache;
use ematix_flow_core::preset;
use futures_util::TryStreamExt;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

const Q15_SQL: &str = include_str!("../../../examples/tpch/queries/q15.sql");
#[allow(dead_code)] // reference SQL kept alongside Q15 for ad-hoc plan-cache checks
const Q21_SQL: &str = include_str!("../../../examples/tpch/queries/q21.sql");
#[allow(dead_code)] // reference SQL kept alongside Q15 for ad-hoc plan-cache checks
const Q06_SQL: &str = include_str!("../../../examples/tpch/queries/q06.sql");

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf1"));

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

    // Σ.AG.5 (2026-05-26): Q15 hits SharedSubtreeExec batch replay
    // at ~0.6 ms — flagging "too fast?" check. Run Q15 once with NO
    // cache (fresh ctx; produces baseline rows), then 3x via the
    // cache, dumping the first ~5 cells of the result row each time
    // to verify byte-identical output.
    {
        let df = ctx.sql(Q15_SQL).await?;
        let batches = df.collect().await?;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!("Q15 baseline (no cache): {total} rows");
        if let Some(b) = batches.iter().find(|b| b.num_rows() > 0) {
            let pretty =
                datafusion::arrow::util::pretty::pretty_format_batches(std::slice::from_ref(b))?;
            println!("{pretty}");
        }
    }

    let cache = PlanCache::new();

    println!("\nQ15 via cache, 3 reps:");
    for rep in 0..3 {
        let plan = cache.get_or_plan(&ctx, Q15_SQL).await?;
        let plan: Arc<dyn datafusion::physical_plan::ExecutionPlan> =
            if plan.output_partitioning().partition_count() > 1 {
                Arc::new(
                    datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(
                        plan,
                    ),
                )
            } else {
                plan
            };
        let mut s = plan.execute(0, ctx.task_ctx())?;
        let mut batches = Vec::new();
        while let Some(b) = s.try_next().await? {
            batches.push(b);
        }
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!("  rep {rep}: {total} rows");
        if let Some(b) = batches.iter().find(|b| b.num_rows() > 0) {
            let pretty =
                datafusion::arrow::util::pretty::pretty_format_batches(std::slice::from_ref(b))?;
            println!("{pretty}");
        }
    }
    println!();

    // Test all 22 queries; print row counts per rep so we can spot
    // any query where the cache returns different rows on rep 0 vs
    // reps 1-4 (i.e. stateful operators that aren't reset by
    // with_new_children).
    let dir_str = dir.to_string_lossy();
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .unwrap()
        .to_path_buf();
    let queries_dir = workspace.join("examples/tpch/queries");
    let mut broken: Vec<u8> = Vec::new();
    for q in 1u8..=22 {
        let sql_path = queries_dir.join(format!("q{q:02}.sql"));
        let sql = std::fs::read_to_string(&sql_path)?;
        print!("Q{q:02} ");
        let mut row_counts: Vec<usize> = Vec::with_capacity(5);
        for _ in 0..5 {
            let plan = cache.get_or_plan(&ctx, &sql).await?;
            let mut total = 0usize;
            for p in 0..plan.output_partitioning().partition_count() {
                let mut s = plan.execute(p, ctx.task_ctx())?;
                while let Some(b) = s.try_next().await? {
                    total += b.num_rows();
                }
            }
            row_counts.push(total);
        }
        let ok = row_counts.iter().all(|&c| c == row_counts[0]);
        if ok {
            println!("OK ({} rows × 5)", row_counts[0]);
        } else {
            println!("BROKEN {row_counts:?}");
            broken.push(q);
        }
    }
    println!("\nData dir: {dir_str}");
    let (h, m) = cache.stats();
    println!("cache: hits={h} misses={m}");
    if broken.is_empty() {
        println!("ALL 22 queries cache-safe ✓");
    } else {
        println!("BROKEN queries: {broken:?}");
    }
    Ok(())
}
