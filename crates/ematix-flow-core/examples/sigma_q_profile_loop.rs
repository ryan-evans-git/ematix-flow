//! Σ.Q profiling harness — runs ONE TPC-H query in a tight loop so a
//! sampling profiler (samply / Instruments) gets enough samples to
//! localise the SF=10 hot path.
//!
//! Why: 2026-05-22 SF=10 single-node bench shows ematix-flow loses to
//! DuckDB on Q01/Q02/Q03/Q05/Q17/Q18. Need to know where the time
//! goes before picking a lever (parallel multi-column decode vs
//! bloom-on-build vs custom hash join).
//!
//! ## Env
//!
//!   Q                query number 1..22 (default 18 — biggest loss)
//!   TPCH_DATA_DIR    parquet root (default ../../examples/tpch/data/sf10)
//!   ITERATIONS       measured iterations after warmup (default 50)
//!   WARMUPS          untimed iterations to prime caches (default 5)
//!
//! ## Run
//!
//!   cargo build --release -p ematix-flow-core --example sigma_q_profile_loop
//!   Q=18 samply record ./target/release/examples/sigma_q_profile_loop
//!
//! ## Match the bench setup
//!
//! Installs the same optimizer-rule chain as the triangulation bench
//! when EMAT_RULES=all (Σ.P dedupe + dict-group-count + multi-agg +
//! filter-sum). Uses EmatixFastParquetTableProvider with
//! dict-preservation for parity with the bench's
//! `register_dict_aware_parquet` path.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::dedupe_aggregate_rule::DedupeAggregateForFloatDeterminism;
use ematix_flow_core::dict_aggregate_rule::EnableDictGroupCountRule;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fused_aggregate_filter_multi_agg_rule::InjectFilterMultiAggRule;
use ematix_flow_core::fused_aggregate_filter_sum_rule::InjectFilterSumRule;
use futures_util::TryStreamExt;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn data_dir() -> String {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    match std::env::var("TPCH_DATA_DIR") {
        Ok(s) => s,
        Err(_) => manifest
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("examples/tpch/data/sf10")
            .to_string_lossy()
            .into_owned(),
    }
}

fn make_ctx() -> SessionContext {
    // Mirror tpch_triangulation_bench's EMAT_RULES=all session.
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(DedupeAggregateForFloatDeterminism::default()))
        .with_physical_optimizer_rule(Arc::new(EnableDictGroupCountRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterMultiAggRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterSumRule))
        .build();
    SessionContext::new_with_state(state)
}

fn register_tables(ctx: &SessionContext, dir: &str) {
    // Mirror tpch_triangulation_bench's build_ematix_ctx: Emat for
    // lineitem (no dict preservation — fails on SF=10 lineitem chunks
    // where the writer fell back from dict), FastParquet for the rest.
    for t in TPCH_TABLES {
        let path = format!("{dir}/{t}.parquet");
        if *t == "lineitem" {
            let prov = EmatixFastParquetTableProvider::try_new(path).unwrap();
            ctx.register_table(*t, Arc::new(prov)).unwrap();
        } else {
            let prov = FastParquetTableProvider::try_new(path).unwrap();
            ctx.register_table(*t, Arc::new(prov)).unwrap();
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let q: u8 = env_or("Q", 18);
    let dir = data_dir();
    let iterations: usize = env_or("ITERATIONS", 50);
    let warmups: usize = env_or("WARMUPS", 5);

    // Resolve query SQL — try workspace-relative first, then one-up.
    let sql_path = format!("examples/tpch/queries/q{q:02}.sql");
    let sql = std::fs::read_to_string(&sql_path)
        .or_else(|_| std::fs::read_to_string(format!("../{sql_path}")))
        .or_else(|_| std::fs::read_to_string(format!("../../{sql_path}")))
        .unwrap_or_else(|e| panic!("read {sql_path}: {e}"));

    eprintln!("=== Σ.Q profile loop ===");
    eprintln!("  Q:           {q:02}");
    eprintln!("  data_dir:    {dir}");
    eprintln!("  warmups:     {warmups}");
    eprintln!("  iterations:  {iterations}");

    let ctx = make_ctx();
    register_tables(&ctx, &dir);

    // Warmup
    for _ in 0..warmups {
        let df = ctx.sql(&sql).await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let stream = datafusion::physical_plan::execute_stream(plan, ctx.task_ctx()).unwrap();
        let _ = stream.try_collect::<Vec<_>>().await.unwrap();
    }
    eprintln!("warm-up done; entering measured loop");

    let t0 = std::time::Instant::now();
    let mut rows = 0usize;
    for _ in 0..iterations {
        let df = ctx.sql(&sql).await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let stream = datafusion::physical_plan::execute_stream(plan, ctx.task_ctx()).unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        rows = batches.iter().map(|b| b.num_rows()).sum();
    }
    let elapsed = t0.elapsed();
    eprintln!(
        "done — {iterations} iterations in {:.2}s ({:.2} ms / iter), {rows} rows/query",
        elapsed.as_secs_f64(),
        elapsed.as_secs_f64() * 1000.0 / iterations as f64
    );
}
