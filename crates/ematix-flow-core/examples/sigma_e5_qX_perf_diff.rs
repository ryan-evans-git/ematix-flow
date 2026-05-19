//! Σ.E5 (2026-05-18) diagnostic: run a single TPC-H query end-to-end
//! against both providers, collect per-node DataFusion metrics, and
//! print them side-by-side so the bottleneck node is human-spottable.
//!
//! Usage:
//!   QID=4 cargo run --release -p ematix-flow-core --example sigma_e5_qX_perf_diff
//!   QID=13 cargo run --release ...
//!
//! Env:
//!   QID            — TPC-H query number 1..22 (default 4)
//!   TPCH_DATA_DIR  — override the SF=1 data dir
//!   WARMUPS        — untimed warmups before measurement (default 3)
//!   TRIALS         — measured trials per provider (default 11; min-of-k)

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use futures_util::TryStreamExt;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

fn data_dir() -> String {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    match std::env::var("TPCH_DATA_DIR") {
        Ok(s) => s,
        Err(_) => manifest
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("examples/tpch/data/sf1")
            .to_string_lossy()
            .into_owned(),
    }
}

#[derive(Copy, Clone)]
enum Provider {
    FastParquet,
    EmatixFastParquet,
}

fn make_ctx() -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features()
        .build();
    SessionContext::new_with_state(state)
}

fn register_tables(ctx: &SessionContext, dir: &str, provider: Provider) {
    for t in TPCH_TABLES {
        let path = format!("{dir}/{t}.parquet");
        match provider {
            Provider::FastParquet => {
                let prov = FastParquetTableProvider::try_new(path).unwrap();
                ctx.register_table(*t, Arc::new(prov)).unwrap();
            }
            Provider::EmatixFastParquet => {
                let prov = EmatixFastParquetTableProvider::try_new(path).unwrap();
                ctx.register_table(*t, Arc::new(prov)).unwrap();
            }
        }
    }
}

/// Walk a plan and accumulate `elapsed_compute` (ns) per node name.
fn collect_metrics(plan: &Arc<dyn ExecutionPlan>, acc: &mut BTreeMap<String, u64>) {
    let name = plan.name().to_string();
    if let Some(set) = plan.metrics() {
        if let Some(t) = set.elapsed_compute() {
            *acc.entry(name).or_default() += t as u64;
        }
    }
    for c in plan.children() {
        collect_metrics(c, acc);
    }
}

async fn measure(label: &str, ctx: SessionContext, sql: &str, trials: usize, warmups: usize) {
    // Warmups (untimed).
    for _ in 0..warmups {
        let df = ctx.sql(sql).await.unwrap();
        let _ = df.collect().await.unwrap();
    }

    // Run trials. Take the MIN-time run's metrics (least noise).
    let mut best_ms: f64 = f64::MAX;
    let mut best_metrics: BTreeMap<String, u64> = BTreeMap::new();
    let mut best_plan_str = String::new();
    for _ in 0..trials {
        let df = ctx.sql(sql).await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let stream =
            datafusion::physical_plan::execute_stream(plan.clone(), ctx.task_ctx()).unwrap();
        let t0 = Instant::now();
        let _ = stream.try_collect::<Vec<_>>().await.unwrap();
        let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
        if elapsed_ms < best_ms {
            best_ms = elapsed_ms;
            let mut m = BTreeMap::new();
            collect_metrics(&plan, &mut m);
            best_metrics = m;
            best_plan_str = displayable(plan.as_ref()).indent(true).to_string();
        }
    }

    println!("=== {label} ===");
    println!("  total wall: {:.2} ms", best_ms);
    println!("  per-node elapsed_compute:");
    let mut entries: Vec<(String, u64)> = best_metrics.into_iter().collect();
    entries.sort_by_key(|e| std::cmp::Reverse(e.1));
    for (name, ns) in entries {
        println!("    {:<30} {:>10.2} ms", name, ns as f64 / 1e6);
    }
    println!();
    println!("--- plan with metrics ---");
    println!("{best_plan_str}");
    println!();
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let qid: usize = std::env::var("QID")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);
    let trials: usize = std::env::var("TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(11);
    let warmups: usize = std::env::var("WARMUPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);

    let dir = data_dir();
    let sql_path = format!("examples/tpch/queries/q{:02}.sql", qid);
    let sql = std::fs::read_to_string(&sql_path)
        .or_else(|_| std::fs::read_to_string(format!("../{sql_path}")))
        .unwrap_or_else(|e| panic!("read {sql_path}: {e}"));

    let ctx_fp = make_ctx();
    register_tables(&ctx_fp, &dir, Provider::FastParquet);
    measure(
        &format!("Q{:02} / FastParquet (parquet-rs)", qid),
        ctx_fp,
        &sql,
        trials,
        warmups,
    )
    .await;

    let ctx_emat = make_ctx();
    register_tables(&ctx_emat, &dir, Provider::EmatixFastParquet);
    measure(
        &format!("Q{:02} / EmatixFastParquet", qid),
        ctx_emat,
        &sql,
        trials,
        warmups,
    )
    .await;
}
