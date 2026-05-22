//! Σ.O.c.2 — does the wired-in RG decode cache speed up rep 2+?
//!
//! Method:
//!   1. Run REPS reps of the query with cache OFF (baseline).
//!   2. Install a process-wide RowGroupDecodeCache.
//!   3. Run REPS reps with cache ON.
//!
//! Each rep uses a FRESH SessionContext + TableProvider — that's the
//! workload pattern this cache is designed for (different queries,
//! same data file).
//!
//! Run:
//!   cargo run --release -p ematix-flow-core --example decode_cache_provider_bench

use std::sync::Arc;
use std::time::Instant;

use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::emat_arrow_reader::{RowGroupDecodeCache, set_process_rg_decode_cache};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use futures_util::TryStreamExt;

const REPS: usize = 5;

const QUERIES: &[(&str, &str)] = &[
    ("scan-only (COUNT)", "SELECT COUNT(*) FROM lineitem"),
    (
        "project-3 cols",
        "SELECT l_orderkey, l_partkey, l_suppkey FROM lineitem",
    ),
    (
        "project-all + agg",
        "SELECT l_returnflag, COUNT(*) FROM lineitem GROUP BY l_returnflag",
    ),
];

async fn time_query(ctx: &SessionContext, sql: &str) -> f64 {
    let t = Instant::now();
    let df = ctx.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let mut n = 0usize;
    for p in 0..plan.output_partitioning().partition_count() {
        let mut s = plan.execute(p, ctx.task_ctx()).unwrap();
        while let Some(b) = s.try_next().await.unwrap() {
            n += b.num_rows();
        }
    }
    std::hint::black_box(n);
    t.elapsed().as_secs_f64() * 1000.0
}

async fn run_reps(label: &str, dir: &str, sql: &str) -> Vec<f64> {
    let mut times: Vec<f64> = Vec::new();
    for rep in 0..REPS {
        let cfg = SessionConfig::new().with_target_partitions(
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(8),
        );
        let ctx = SessionContext::new_with_config(cfg);
        let path = format!("{dir}/lineitem.parquet");
        let prov = EmatixFastParquetTableProvider::try_new(&path).unwrap();
        ctx.register_table("lineitem", Arc::new(prov)).unwrap();
        let ms = time_query(&ctx, sql).await;
        times.push(ms);
        println!("  [{label}] rep {} : {ms:>8.2} ms", rep + 1);
    }
    times
}

fn median(v: &[f64]) -> f64 {
    let mut s = v.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    s[s.len() / 2]
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let dir =
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf1".to_string());
    println!("=== Σ.O.c.2 — RG decode cache wire-up bench ===");
    println!("REPS={REPS}, fresh ctx+provider per rep.\n");

    // Make sure we start with cache OFF, regardless of env.
    set_process_rg_decode_cache(None);

    for (label, sql) in QUERIES {
        println!("--- {label}: `{sql}` ---");

        // Phase 1: cache OFF.
        let off = run_reps("OFF", &dir, sql).await;

        // Phase 2: install cache, run again.
        let cache = Arc::new(RowGroupDecodeCache::new());
        set_process_rg_decode_cache(Some(cache.clone()));
        let on = run_reps("ON ", &dir, sql).await;
        set_process_rg_decode_cache(None);

        let (h, m, bytes) = cache.stats();
        let tail_off = median(&off[1..]);
        let tail_on = median(&on[1..]);
        let speedup = tail_off / tail_on;
        println!(
            "  tail median: OFF {tail_off:>8.2} ms | ON {tail_on:>8.2} ms | {speedup:.2}× speedup"
        );
        println!(
            "  cache stats: hits={h} misses={m} bytes_used={:.1} MB\n",
            bytes as f64 / 1_048_576.0
        );
    }

    println!("Interpretation:");
    println!("  ≥ 1.5× tail speedup → cache is doing real work; promote to default-on");
    println!("  1.0-1.5× → OS cache already wins most of it; provider wire-up is correct,");
    println!("             but additional savings will need decode-output sharing across queries");
    println!("  < 1.0× → cache hurts (insert + Arc bookkeeping > re-decode); revert");
}
