//! Σ.O.c measurement spike — does caching decoded RGs actually help?
//!
//! Hypothesis: re-decoding the same parquet file 5× in a row is wasted
//! work; a process-shared cache would cut the 2nd-5th rep to ~0.
//!
//! Alternate hypothesis: the OS file-cache + Arrow's Arc-shared buffers
//! make the 2nd decode nearly free already, in which case Σ.O.c is
//! the wrong lever and we should pursue something else.
//!
//! Method: run `SELECT COUNT(*) FROM lineitem` (forces full decode of
//! every row group) 5× back-to-back through a fresh provider each
//! time. Compare first-rep wall time to subsequent reps. Report the
//! speedup ratio.
//!
//! Run:
//!   cargo run --release -p ematix-flow-core --example decode_cache_value_bench

use std::sync::Arc;
use std::time::Instant;

use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::{SessionConfig, SessionContext};
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

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let dir =
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf1".to_string());
    println!("=== Σ.O.c measurement: does same-file second-decode get cheaper? ===");
    println!("Method: {REPS} reps in one process, FRESH ctx each rep.\n");

    for (label, sql) in QUERIES {
        println!("--- {label}: `{sql}` ---");
        let mut times: Vec<f64> = Vec::new();
        for rep in 0..REPS {
            // Fresh ctx + provider every rep — simulates "different
            // query, same data" workload pattern.
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
            println!("  rep {} : {ms:>8.2} ms", rep + 1);
        }
        // Speedup ratio: rep 1 / median of reps 2..N
        let mut tail: Vec<f64> = times[1..].to_vec();
        tail.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let tail_median = tail[tail.len() / 2];
        let cold_to_warm = times[0] / tail_median;
        println!(
            "  speedup (rep 1 / tail median): {cold_to_warm:.2}× (tail = {tail_median:.2} ms)\n"
        );
    }

    println!("Interpretation:");
    println!("  ≥ 1.5×  → caching would clearly help, build Σ.O.c proper");
    println!("  1.0-1.5× → marginal, prioritise other levers");
    println!("  < 1.0×  → OS cache already wins; Σ.O.c is wrong lever");
}
