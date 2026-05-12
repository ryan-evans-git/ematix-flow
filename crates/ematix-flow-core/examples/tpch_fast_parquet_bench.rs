//! Σ.E2 day-4: run every TPC-H query with `FastParquetTableProvider`
//! and report per-query medians alongside DataFusion's default
//! parquet path.
//!
//! Decision criterion: does row-group-parallel parquet-rs (via the
//! custom TableProvider) actually close the Polars-wins-by gap on
//! Q9/Q12/Q14 measured in the README? The day-3 probe said it should;
//! this harness proves or disproves it end-to-end.
//!
//! Statistical mode: each query runs `TPCH_BENCH_TRIALS` trials (default
//! 10) after two warm-ups, and we report median ± stdev for each side.
//! A query is only flagged win/loss when the median difference exceeds
//! the combined-stdev noise envelope; otherwise it's marked "≈" (noise).
//! Set `TPCH_BENCH_QUERIES=1,6,10` to restrict the run.
//!
//! Usage:
//!   cargo run --release -p ematix-flow-core --example tpch_fast_parquet_bench
//!   TPCH_BENCH_TRIALS=20 cargo run --release -p ematix-flow-core --example tpch_fast_parquet_bench
//!   TPCH_DATA_DIR=…/sf10 TPCH_BENCH_QUERIES=1,6,10,12,15,19 cargo run --release -p ematix-flow-core --example tpch_fast_parquet_bench

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::RecordBatch;
use datafusion::prelude::SessionContext;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

/// Build a `SessionConfig` that — when `TPCH_BENCH_COLLECT_LEFT=1` is
/// set — raises the broadcast-join thresholds so DataFusion's
/// `JoinSelection` rule picks `PartitionMode::CollectLeft` for small
/// builds. Σ.E2 follow-up probe: Q14 SF=1 sits at 18 ms with
/// FastParquet but Polars does 12.5 ms; the EXPLAIN ANALYZE points at
/// hash-repartitioning the 200 K-row `part` table on every probe. If
/// CollectLeft closes the gap, that motivates either bumping the
/// default thresholds or having FastParquetTableProvider hint stats
/// that fall under the threshold.
fn session_config() -> datafusion::prelude::SessionConfig {
    use datafusion::prelude::SessionConfig;
    let cfg = SessionConfig::new();
    if std::env::var("TPCH_BENCH_COLLECT_LEFT").ok().as_deref() == Some("1") {
        // Targeted threshold: high enough for Q14's `part` build
        // (200 K rows / 1.27 MB encoded) to qualify for CollectLeft,
        // but low enough that Q18's `orders` build (1.5 M rows /
        // ~14 MB encoded) stays Partitioned. The earlier attempt with
        // 10 M-row / 256 MB thresholds regressed Q18 by 2.5× because
        // forcing every probe partition to read a 1.5 M-row build hash
        // table dwarfed the savings on small builds.
        cfg.set_str(
            "datafusion.optimizer.hash_join_single_partition_threshold",
            "8388608", // 8 MB
        )
        .set_str(
            "datafusion.optimizer.hash_join_single_partition_threshold_rows",
            "500000",
        )
    } else {
        cfg
    }
}

async fn make_ctx_default(parquet_dir: &str) -> SessionContext {
    let ctx = SessionContext::new_with_config(session_config());
    for table in TPCH_TABLES {
        let path = format!("{parquet_dir}/{table}.parquet");
        ctx.register_parquet(*table, &path, Default::default())
            .await
            .unwrap();
    }
    ctx
}

async fn make_ctx_fast(parquet_dir: &str) -> SessionContext {
    let ctx = SessionContext::new_with_config(session_config());
    for table in TPCH_TABLES {
        let path = format!("{parquet_dir}/{table}.parquet");
        let prov = FastParquetTableProvider::try_new(path).unwrap();
        ctx.register_table(*table, Arc::new(prov)).unwrap();
    }
    ctx
}

#[derive(Clone, Copy)]
#[allow(dead_code)] // mean/min/max kept for ad-hoc diagnosis; not in the default table.
struct Stats {
    median: f64,
    mean: f64,
    stdev: f64,
    min: f64,
    max: f64,
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
    let variance = if n > 1 {
        times.iter().map(|t| (t - mean).powi(2)).sum::<f64>() / (n - 1) as f64
    } else {
        0.0
    };
    Stats {
        median,
        mean,
        stdev: variance.sqrt(),
        min: times[0],
        max: times[n - 1],
    }
}

async fn bench_one(ctx: &SessionContext, sql: &str, trials: usize) -> (Stats, usize) {
    // Two warm-ups: first hit primes filesystem/page cache + jit, second
    // settles the parquet metadata caches and DataFusion's internal
    // state so trial #1 looks like trial #N.
    for _ in 0..2 {
        let _: Vec<RecordBatch> = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    }
    let mut times = Vec::with_capacity(trials);
    let mut row_count = 0;
    for _ in 0..trials {
        let start = Instant::now();
        let out: Vec<RecordBatch> = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        times.push(start.elapsed().as_secs_f64() * 1000.0);
        row_count = out.iter().map(|b| b.num_rows()).sum();
    }
    (summarize(times), row_count)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let queries_dir = manifest
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("examples/tpch/queries");
    let data_dir_buf = match std::env::var("TPCH_DATA_DIR") {
        Ok(s) => PathBuf::from(s),
        Err(_) => manifest
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("examples/tpch/data/sf1"),
    };
    let data_dir = data_dir_buf.to_str().unwrap();

    let trials: usize = std::env::var("TPCH_BENCH_TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let queries: Vec<u32> = match std::env::var("TPCH_BENCH_QUERIES") {
        Ok(s) => s
            .split(',')
            .filter_map(|t| t.trim().parse().ok())
            .collect(),
        Err(_) => (1u32..=22).collect(),
    };

    println!("==> DataFusion default vs FastParquetTableProvider (Σ.E2)");
    println!("==> M3 Pro / {trials}-trial after 2 warm-ups / median ± stdev");
    println!("==> data: {data_dir}");
    println!("==> queries: {queries:?}");
    println!();
    // A query is flagged only when |median_diff| exceeds combined stdev —
    // anything tighter than that is inside the run-to-run noise envelope
    // and shouldn't drive optimization decisions.
    println!(
        "| Query | DataFusion median±σ (ms) | FastParquet median±σ (ms) | Δ median % | classification | rows |"
    );
    println!("|---|---|---|---|---|---|");

    let ctx_default = make_ctx_default(data_dir).await;
    let ctx_fast = make_ctx_fast(data_dir).await;

    let mut wins = 0;
    let mut losses = 0;
    let mut noise = 0;
    let mut sum_speedup = 0.0_f64;
    let mut measured = 0;
    for n in queries {
        let path = queries_dir.join(format!("q{:02}.sql", n));
        let sql_raw = match fs::read_to_string(&path) {
            Ok(s) => s,
            Err(e) => {
                println!("| Q{n:02} | — | — | — | — | missing {path:?}: {e} |");
                continue;
            }
        };
        let sql = sql_raw.trim().trim_end_matches(';').trim();
        if sql.matches(';').count() > 0 {
            println!("| Q{n:02} | — | — | — | — | multi-statement, skipped |");
            continue;
        }

        let result_default = std::panic::AssertUnwindSafe(bench_one(&ctx_default, sql, trials));
        let default = futures_util::future::FutureExt::catch_unwind(result_default).await;
        let result_fast = std::panic::AssertUnwindSafe(bench_one(&ctx_fast, sql, trials));
        let fast = futures_util::future::FutureExt::catch_unwind(result_fast).await;
        match (default, fast) {
            (Ok((d, rows)), Ok((f, _))) => {
                let delta_pct = (d.median - f.median) / d.median * 100.0;
                let combined_stdev = d.stdev + f.stdev;
                let median_diff = (d.median - f.median).abs();
                let class = if median_diff < combined_stdev {
                    noise += 1;
                    "≈ noise"
                } else if f.median < d.median {
                    wins += 1;
                    "✓ win"
                } else {
                    losses += 1;
                    "✗ loss"
                };
                sum_speedup += d.median / f.median;
                measured += 1;
                println!(
                    "| Q{n:02} | {:>7.2} ± {:>5.2} | {:>7.2} ± {:>5.2} | {:>+6.1}% | {} | {} |",
                    d.median, d.stdev, f.median, f.stdev, delta_pct, class, rows,
                );
            }
            (Err(_), _) => {
                println!("| Q{n:02} | panic | — | — | — | DataFusion panicked |");
            }
            (_, Err(_)) => {
                println!("| Q{n:02} | — | panic | — | — | FastParquet panicked |");
            }
        }
    }

    println!();
    if measured > 0 {
        let mean_speedup = sum_speedup / measured as f64;
        println!(
            "Summary: measured={measured}  wins={wins}  losses={losses}  noise={noise}  mean_speedup={mean_speedup:.2}× (median/median)"
        );
        println!(
            "  · win/loss classification requires |Δ median| > σ(default) + σ(fast); otherwise marked noise."
        );
    }
}
