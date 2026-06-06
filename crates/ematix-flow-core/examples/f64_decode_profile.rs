//! PV.M.1 — f64-decode leaf-profiling harness. Loops ONE lineitem
//! scan→filter→agg query on a SHARED ctx (tables registered once, no
//! per-trial setup noise; no RG decode cache by default) so `sample`/samply
//! captures a clean decode hot path. ematix-only.
//!
//! The Q15 gap (PV.M.0) is entirely the f64 columns (l_extendedprice,
//! l_discount): SCALAR-stage is 2.2× Polars on Snappy, 4.3× on LZ4. This
//! harness splits that 37.7 ms (Snappy) into snap-decompress vs
//! read_column_f64_masked_into vs plain_sparse_decode_f64_into vs the
//! multiply-sum — to decide dense-vs-masked routing vs a new PLAIN-f64 kernel.
//!
//! Usage (then `sample <pid> 15 1 -file out.txt` during the loop):
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 F64_PROF=scalar F64_PROF_TRIALS=1500 \
//!     cargo run --release -p ematix-flow-core --example f64_decode_profile

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

const WHERE: &str = "where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-04-01'";

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn build_ctx(data_dir: &Path, parts: usize) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(parts))
        .with_default_features();
    let state = ematix_flow_core::preset::with_optimizer_rules(builder).build();
    let ctx = SessionContext::new_with_state(state);
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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf10"));
    let which = std::env::var("F64_PROF").unwrap_or_else(|_| "scalar".to_string());
    let trials: usize = std::env::var("F64_PROF_TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1500);
    // PV.M.4: PARTS override to sweep scan partition granularity (work-stealing
    // test). target_partitions > cores exposes finer tasks to tokio's
    // work-stealing scheduler → balances stragglers in the per-RG decode.
    let parts = std::env::var("PARTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .or_else(|| std::thread::available_parallelism().map(|n| n.get()).ok())
        .unwrap_or(14);

    let sql = match which.as_str() {
        "count" => format!("select count(*) from lineitem {WHERE}"),
        "revenue" => format!(
            "select l_suppkey, sum(l_extendedprice * (1 - l_discount)) from lineitem {WHERE} group by l_suppkey"
        ),
        _ => format!("select sum(l_extendedprice * (1 - l_discount)) from lineitem {WHERE}"),
    };

    // Tables registered ONCE → the loop re-scans+re-decodes each trial with no
    // setup noise. Plan built once; only execution is timed/sampled.
    let ctx = build_ctx(&data_dir, parts)?;
    let plan = ctx.sql(&sql).await?.create_physical_plan().await?;

    println!(
        "PV.M.1 f64-decode profile — which={which} trials={trials} parts={parts}\n  sql: {sql}\n  PID for sample: {}",
        std::process::id()
    );

    // Warmup.
    for _ in 0..5 {
        let _ = collect(plan.clone(), ctx.task_ctx()).await?;
    }
    let mut ms = Vec::with_capacity(trials);
    for _ in 0..trials {
        let t = Instant::now();
        let _ = collect(plan.clone(), ctx.task_ctx()).await?;
        ms.push(t.elapsed().as_secs_f64() * 1e3);
    }
    let m = median(ms.clone());
    let mn = ms.iter().cloned().fold(f64::INFINITY, f64::min);
    println!("done: median {m:.1} ms  min {mn:.1} ms  ({trials} trials)");
    Ok(())
}
