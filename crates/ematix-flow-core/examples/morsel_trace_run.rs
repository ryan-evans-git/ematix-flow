//! Morsel-engine P1 de-risk runner: execute one TPC-H query with the
//! preset rule set, driving every output partition concurrently (the
//! faithful execution path), and dump the per-RG decode trace
//! (`EMAT_MORSEL_TRACE=1`) for the last trial so we can reconstruct a
//! per-core busy/idle timeline of the decode.
//!
//! Run:
//!   EMAT_MORSEL_TRACE=1 TPCH_QUERY=6 PARTITIONS=14 \
//!   EMAT_MORSEL_TRACE_OUT=/tmp/morsel_trace/q6_p14.csv \
//!     cargo run --release -p ematix-flow-core --example morsel_trace_run \
//!     --features triangulation
//!   python3 /tmp/morsel_trace/analyze.py /tmp/morsel_trace/q6_p14.csv
//!
//! Env:
//!   TPCH_QUERY            query number 1..=22 (default 6)
//!   TPCH_DATA_DIR         parquet root (default examples/tpch/data/sf10)
//!   PARTITIONS            target_partitions (default 14)
//!   TPCH_WARMUPS          untimed warmups (default 5)
//!   TPCH_TRIALS           timed trials (default 10; last one is traced)
//!   EMAT_MORSEL_TRACE     1 to record (the trace is empty otherwise)
//!   EMAT_MORSEL_TRACE_OUT dump path (default /tmp/morsel_trace/trace.csv)

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::preset;
use futures_util::TryStreamExt;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .ok_or("workspace root not found")?
        .to_path_buf();
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| workspace.join("examples/tpch/data/sf10"));
    let q: u8 = std::env::var("TPCH_QUERY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(6);
    let partitions: usize = std::env::var("PARTITIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(14);
    let warmups: usize = std::env::var("TPCH_WARMUPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let trials: usize = std::env::var("TPCH_TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let out = std::env::var("EMAT_MORSEL_TRACE_OUT")
        .unwrap_or_else(|_| "/tmp/morsel_trace/trace.csv".to_string());

    let sql_path = workspace.join(format!("examples/tpch/queries/q{q:02}.sql"));
    let sql = std::fs::read_to_string(&sql_path)?;

    let state = preset::with_optimizer_rules(
        SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(partitions))
            .with_default_features(),
    )
    .build();
    let ctx = SessionContext::new_with_state(state);
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        let prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy().as_ref())?;
        ctx.register_table(*t, Arc::new(prov))?;
    }

    println!(
        "morsel_trace_run: Q{q:02} partitions={partitions} warmups={warmups} trials={trials} trace={}",
        ematix_flow_core::morsel_trace::enabled()
    );
    println!("  data: {}", data_dir.display());

    // Warmups (untimed).
    for _ in 0..warmups {
        let df = ctx.sql(&sql).await?;
        let stream = df.execute_stream().await?;
        let _: Vec<_> = stream.try_collect().await?;
    }

    // Timed trials. Drive every output partition concurrently — this is
    // the real decode-parallelism path (each EmatixFastParquetExec
    // partition stream spawns its own blocking decode thread). Reset the
    // trace right before the final trial so the dump is exactly that run.
    let mut walls = Vec::with_capacity(trials);
    let mut traced_wall = 0.0_f64;
    let mut traced_rows = 0usize;
    for trial in 0..trials {
        let is_last = trial == trials - 1;
        if is_last {
            ematix_flow_core::morsel_trace::reset();
        }
        let df = ctx.sql(&sql).await?;
        let plan = df.create_physical_plan().await?;
        let task_ctx = ctx.task_ctx();
        let n_parts = plan.output_partitioning().partition_count();
        let t0 = Instant::now();
        let mut handles = Vec::with_capacity(n_parts);
        for p in 0..n_parts {
            let plan_c = Arc::clone(&plan);
            let ctx_c = Arc::clone(&task_ctx);
            handles.push(tokio::spawn(async move {
                let stream = plan_c.execute(p, ctx_c)?;
                let batches: Vec<_> = stream.try_collect().await?;
                Ok::<_, datafusion::error::DataFusionError>(batches)
            }));
        }
        let mut rows = 0usize;
        for h in handles {
            let b = h.await??;
            rows += b.iter().map(|rb| rb.num_rows()).sum::<usize>();
        }
        let wall = t0.elapsed().as_secs_f64() * 1000.0;
        walls.push(wall);
        if is_last {
            traced_wall = wall;
            traced_rows = rows;
        }
    }

    walls.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = walls[walls.len() / 2];
    println!("  walls (ms): {walls:?}");
    println!("  median wall: {median:.2} ms");
    println!("  traced trial: wall={traced_wall:.2} ms rows={traced_rows}");

    if ematix_flow_core::morsel_trace::enabled() {
        if let Some(parent) = std::path::Path::new(&out).parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let n = ematix_flow_core::morsel_trace::dump(&out)?;
        println!("  dumped {n} decode events → {out}");
    } else {
        println!("  (EMAT_MORSEL_TRACE not set — no trace dumped)");
    }

    Ok(())
}
