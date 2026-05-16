//! Σ.E5a: Q14 head-to-head — `EmatixFastParquetTableProvider` with vs
//! without `with_late_mat(true)`.
//!
//! Q14 is the canonical late-mat shape: the lineitem shipdate filter
//! passes ~1.4% of rows, so the three non-filter columns (partkey,
//! extendedprice, discount) should decode only the matched rows under
//! late-mat. Pre-Π.10 the bridge's sparse_gather path does the same
//! thing logically but with a different per-page code path; the Π.10
//! `read_column_*_masked_into` façade adds per-page popcount skip +
//! sparse PLAIN decode.
//!
//! Run:
//!   cargo run --release -p ematix-flow-core --example tpch_q14_late_mat_bench
//!   TPCH_DATA_DIR=examples/tpch/data/sf1 ... # default
//!   TPCH_BENCH_TRIALS=15 ...                 # default 10
//!
//! Output: median ± σ for each path, plus the Δ%.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Float64Array, RecordBatch};
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use futures_util::stream::TryStreamExt;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

const Q14_SQL: &str = "
    SELECT 100.00 * sum(case when p_type like 'PROMO%'
                              then l_extendedprice * (1 - l_discount)
                              else 0 end)
                   / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
    FROM lineitem, part
    WHERE l_partkey = p_partkey
      AND l_shipdate >= date '1995-09-01'
      AND l_shipdate <  date '1995-10-01'
";

struct Stats {
    median: f64,
    stdev: f64,
}

fn summarize(mut times: Vec<f64>) -> Stats {
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = times.len();
    let median = if n % 2 == 1 {
        times[n / 2]
    } else {
        (times[n / 2 - 1] + times[n / 2]) / 2.0
    };
    let mean = times.iter().sum::<f64>() / n as f64;
    let var = times.iter().map(|t| (t - mean).powi(2)).sum::<f64>() / (n - 1) as f64;
    Stats {
        median,
        stdev: var.sqrt(),
    }
}

async fn make_emat_ctx(parquet_dir: &str, late_mat: bool) -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(14));
    // Q14 only joins lineitem + part. part has p_type (Utf8/Utf8View)
    // which Emat doesn't yet handle in dict-encoded byte_array form on
    // the predicate side, so we route part through FastParquetTableProvider
    // and lineitem through Emat — that's where the late-mat win lives.
    let li_path = format!("{parquet_dir}/lineitem.parquet");
    let li_prov = EmatixFastParquetTableProvider::try_new(li_path)
        .unwrap()
        .with_late_mat(late_mat);
    ctx.register_table("lineitem", Arc::new(li_prov)).unwrap();
    for table in TPCH_TABLES.iter().filter(|t| **t != "lineitem") {
        let path = format!("{parquet_dir}/{table}.parquet");
        let prov = FastParquetTableProvider::try_new(path).unwrap();
        ctx.register_table(*table, Arc::new(prov)).unwrap();
    }
    ctx
}

async fn run_q14_once(ctx: &SessionContext) -> RecordBatch {
    let df = ctx.sql(Q14_SQL).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let mut combined: Vec<RecordBatch> = Vec::new();
    let parts = plan.properties().partitioning.partition_count();
    for p in 0..parts {
        let mut s = plan.execute(p, ctx.task_ctx()).unwrap();
        while let Some(b) = s.try_next().await.unwrap() {
            combined.push(b);
        }
    }
    assert!(!combined.is_empty(), "Q14 returned empty result");
    combined.into_iter().next().unwrap()
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let parquet_dir: PathBuf = std::env::var("TPCH_DATA_DIR")
        .unwrap_or_else(|_| "examples/tpch/data/sf1".into())
        .into();
    let trials: usize = std::env::var("TPCH_BENCH_TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let warmups: usize = 2;

    println!("=== tpch_q14_late_mat_bench ===");
    println!("data: {}", parquet_dir.display());
    println!("trials: {trials} (after {warmups} warmups)");
    println!();

    let parquet_dir = parquet_dir.to_string_lossy().to_string();

    // Sanity check: both paths produce the same numeric answer.
    let answer_default = {
        let ctx = make_emat_ctx(&parquet_dir, false).await;
        let b = run_q14_once(&ctx).await;
        b.column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
    };
    let answer_latemat = {
        let ctx = make_emat_ctx(&parquet_dir, true).await;
        let b = run_q14_once(&ctx).await;
        b.column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
    };
    let diff = (answer_default - answer_latemat).abs() / answer_default.abs().max(1e-12);
    assert!(
        diff < 1e-9,
        "late-mat answer diverges from default: {answer_default} vs {answer_latemat} (rel {diff:.2e})"
    );
    println!("answer agreement: {answer_default} ✓ (rel diff {diff:.2e})");
    println!();

    // Without late-mat.
    let mut times: Vec<f64> = Vec::with_capacity(trials);
    for trial in 0..(trials + warmups) {
        let ctx = make_emat_ctx(&parquet_dir, false).await;
        let t0 = Instant::now();
        let _ = run_q14_once(&ctx).await;
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        if trial >= warmups {
            times.push(ms);
        }
    }
    let s_default = summarize(times);

    // With late-mat.
    let mut times: Vec<f64> = Vec::with_capacity(trials);
    for trial in 0..(trials + warmups) {
        let ctx = make_emat_ctx(&parquet_dir, true).await;
        let t0 = Instant::now();
        let _ = run_q14_once(&ctx).await;
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        if trial >= warmups {
            times.push(ms);
        }
    }
    let s_latemat = summarize(times);

    println!(
        "Emat sparse_gather (pre-Π.10): median {:.2} ms ± {:.2}",
        s_default.median, s_default.stdev,
    );
    println!(
        "Emat masked_into   (Π.10 v0.3.0): median {:.2} ms ± {:.2}",
        s_latemat.median, s_latemat.stdev,
    );
    let delta_pct = 100.0 * (s_default.median - s_latemat.median) / s_default.median;
    println!("Δ median: {delta_pct:+.1}% (positive = late-mat wins)");
}
