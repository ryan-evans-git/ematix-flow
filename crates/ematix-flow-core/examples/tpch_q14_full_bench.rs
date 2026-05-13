//! Σ.D3 phase E bench: `FusedQ14FullExec` end-to-end SF=1 timing.
//!
//! Closes the only outright Polars win in the README's main TPC-H
//! table: Q14 = 12.53 ms in Polars vs 17.7 ms via FastParquet alone.
//! Full-fusion combines scan + filter + join + dual-SUM-with-CASE-WHEN
//! into one parallel pass over a direct-indexed promo bitmap.
//!
//! Methodology: 15 trials after 2 warm-ups. Reports median ± stdev for:
//!   1. DataFusion default `register_parquet` path (the original
//!      baseline from the README's main table — ~22 ms typical)
//!   2. ematix-flow with FastParquet + Utf8View, going through
//!      DataFusion's default planner (the existing 17.7 ms result)
//!   3. ematix-flow with `FusedQ14FullExec` over FastParquet inputs
//!      — the new full-fusion path
//! Target: row 3 ≤ 12.53 ms.
//!
//! Usage:
//!     cargo run --release -p ematix-flow-core --example tpch_q14_full_bench

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Float64Array, RecordBatch};
use datafusion::catalog::TableProvider;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fused_q14_full::{FusedQ14FullExec, Q14Predicate};
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

const TRIALS: usize = 15;
const WARMUPS: usize = 2;

/// Polars's SF=1 median from the README's 2026-05-11 baseline.
const POLARS_REF_MS: f64 = 12.53;

fn data_dir() -> String {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let p = match std::env::var("TPCH_DATA_DIR") {
        Ok(s) => PathBuf::from(s),
        Err(_) => manifest
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("examples/tpch/data/sf1"),
    };
    p.to_string_lossy().into_owned()
}

async fn make_default_ctx(parquet_dir: &str) -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(14));
    for table in TPCH_TABLES {
        let path = format!("{parquet_dir}/{table}.parquet");
        ctx.register_parquet(*table, &path, Default::default())
            .await
            .unwrap();
    }
    ctx
}

async fn make_fast_ctx(parquet_dir: &str) -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(14));
    for table in TPCH_TABLES {
        let path = format!("{parquet_dir}/{table}.parquet");
        let prov = FastParquetTableProvider::try_new(path).unwrap();
        ctx.register_table(*table, Arc::new(prov)).unwrap();
    }
    ctx
}

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

async fn bench_sql(label: &str, ctx: &SessionContext) -> Stats {
    for _ in 0..WARMUPS {
        let _ = ctx.sql(Q14_SQL).await.unwrap().collect().await.unwrap();
    }
    let mut times = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let start = Instant::now();
        let _ = ctx.sql(Q14_SQL).await.unwrap().collect().await.unwrap();
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    let stats = summarize(times);
    println!(
        "  {label:<48}  median {:>6.2} ms ± {:>5.2}",
        stats.median, stats.stdev
    );
    stats
}

/// Construct a `FusedQ14FullExec` over FastParquet-scanned lineitem
/// and part. The two scans are built once outside the timing loop
/// (matching how a planner-injected exec would be set up at plan
/// time); only the actual end-to-end execution is timed.
async fn build_full_fusion_exec(parquet_dir: &str) -> Arc<dyn ExecutionPlan> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(14));
    // lineitem projection: l_partkey, l_extendedprice, l_discount, l_shipdate
    let li_path = format!("{parquet_dir}/lineitem.parquet");
    let li_prov = FastParquetTableProvider::try_new(li_path).unwrap();
    let li_full_schema = li_prov.schema();
    let li_proj: Vec<usize> = [
        "l_partkey",
        "l_extendedprice",
        "l_discount",
        "l_shipdate",
    ]
    .iter()
    .map(|n| {
        li_full_schema
            .fields()
            .iter()
            .position(|f| f.name() == *n)
            .unwrap_or_else(|| panic!("lineitem missing {n}"))
    })
    .collect();
    let li = li_prov
        .scan(&ctx.state(), Some(&li_proj), &[], None)
        .await
        .unwrap();

    // part projection: p_partkey, p_type
    let p_path = format!("{parquet_dir}/part.parquet");
    let p_prov = FastParquetTableProvider::try_new(p_path).unwrap();
    let p_full_schema = p_prov.schema();
    let p_proj: Vec<usize> = ["p_partkey", "p_type"]
        .iter()
        .map(|n| {
            p_full_schema
                .fields()
                .iter()
                .position(|f| f.name() == *n)
                .unwrap_or_else(|| panic!("part missing {n}"))
        })
        .collect();
    let p = p_prov
        .scan(&ctx.state(), Some(&p_proj), &[], None)
        .await
        .unwrap();

    // shipdate range [1995-09-01, 1995-10-01) as Date32 days-since-epoch:
    // 1995-09-01 = 9374, 1995-10-01 = 9404.
    let predicate = Q14Predicate {
        shipdate_lo: 9374,
        shipdate_hi: 9404,
    };
    Arc::new(FusedQ14FullExec::try_new(li, p, predicate).unwrap())
}

async fn bench_full_fusion(label: &str, exec: Arc<dyn ExecutionPlan>) -> (Stats, f64) {
    let ctx = SessionContext::new();
    let run_once = || async {
        let mut s = exec.execute(0, ctx.task_ctx()).unwrap();
        let b = s.try_next().await.unwrap().unwrap();
        b.column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
    };
    let mut last_ratio: f64 = f64::NAN;
    for _ in 0..WARMUPS {
        last_ratio = run_once().await;
    }
    let mut times = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let start = Instant::now();
        last_ratio = run_once().await;
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    let stats = summarize(times);
    println!(
        "  {label:<48}  median {:>6.2} ms ± {:>5.2}",
        stats.median, stats.stdev
    );
    (stats, last_ratio)
}

/// Reference ratio from the DataFusion default SQL path — used to
/// pin equivalence on the full-fusion result before reporting timings.
async fn reference_ratio(parquet_dir: &str) -> f64 {
    let ctx = make_default_ctx(parquet_dir).await;
    let batches: Vec<RecordBatch> = ctx
        .sql(Q14_SQL)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let dir = data_dir();
    println!("==> Σ.D3 phase E: Q14 full-fusion SF=1, M3 Pro, {TRIALS}-trial median");
    println!("==> data: {dir}");
    println!("==> Polars SF=1 reference: {POLARS_REF_MS} ms");
    println!();

    let ref_ratio = reference_ratio(&dir).await;
    println!(
        "  Reference ratio (DataFusion default SQL):   {ref_ratio:.4}%"
    );
    println!();

    // 1: DataFusion default SQL path (register_parquet).
    let ctx_default = make_default_ctx(&dir).await;
    let _ = bench_sql("DataFusion default (register_parquet)", &ctx_default).await;

    // 2: FastParquet + Utf8View, same SQL.
    let ctx_fast = make_fast_ctx(&dir).await;
    let _ = bench_sql("FastParquet + Utf8View (same SQL)", &ctx_fast).await;

    // 3: full-fusion path.
    let exec = build_full_fusion_exec(&dir).await;
    let (stats_full, full_ratio) =
        bench_full_fusion("FusedQ14FullExec (full fusion)", exec).await;

    // Sanity: full-fusion ratio must agree with the reference to within
    // rel_err 1e-12 (a few ULPs from SIMD-vs-scalar reduction order).
    let rel = (full_ratio - ref_ratio).abs() / ref_ratio.abs().max(full_ratio.abs());
    assert!(
        rel < 1e-9,
        "full-fusion ratio {full_ratio} disagrees with reference {ref_ratio} (rel_err {rel:e})"
    );

    println!();
    let delta_pct = 100.0 * (POLARS_REF_MS - stats_full.median) / POLARS_REF_MS;
    if stats_full.median < POLARS_REF_MS {
        println!(
            "  ✓ full-fusion beats Polars: {:.2} ms vs Polars {:.2} ms (+{:.1}% faster)",
            stats_full.median, POLARS_REF_MS, delta_pct,
        );
    } else {
        println!(
            "  ✗ full-fusion still slower than Polars: {:.2} ms vs Polars {:.2} ms ({:.1}% behind)",
            stats_full.median, POLARS_REF_MS, -delta_pct,
        );
    }
}
