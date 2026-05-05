//! Σ.A1 PR 2: TPC-H criterion benches at SF=1.
//!
//! Runs Q1 / Q3 / Q6 / Q19 against the Parquet files under
//! `examples/tpch/data/sf1/` (generate via `cargo run --release -p
//! ematix-flow-core --example tpch_generate -- --sf 1 --out
//! examples/tpch/data/sf1`). Materializes results into `Vec<RecordBatch>`
//! so we measure execution wall-time, not just plan-building.
//!
//! Per-query measurement windows are sized to fit ~10 criterion
//! samples within reasonable wall-clock cost on a typical M-series
//! laptop. Override via `TPCH_MEASUREMENT_TIME_S` if your hardware
//! is materially different (e.g., set to 600 on a slow CI runner).
//!
//! Data path is configurable via the `TPCH_DATA_DIR` env var; default
//! `examples/tpch/data/sf1` (relative to workspace root). The bench
//! panics with a clear message if the directory or any expected
//! Parquet file is missing — easier to debug than a registration
//! error mid-run.
//!
//! Acceptance gate: all four queries complete on the canonical Linux
//! x86_64 host within their measurement windows. Numbers committed
//! to `docs/BENCHMARKS.md` as the Σ.A1 baseline; subsequent PRs that
//! touch transform.rs or upgrade DataFusion re-run + diff against
//! the baseline.
//!
//! Plan: `docs/PHASE_SIGMA_PLAN.md` Σ.A1 PR 2.

use std::path::{Path, PathBuf};
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::RecordBatch;
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

const Q1: &str = include_str!("../../../examples/tpch/queries/q01.sql");
const Q3: &str = include_str!("../../../examples/tpch/queries/q03.sql");
const Q6: &str = include_str!("../../../examples/tpch/queries/q06.sql");
const Q19: &str = include_str!("../../../examples/tpch/queries/q19.sql");

/// Tables registered against the DataFusion SessionContext. Order
/// matches dependency depth (region → nation → supplier ... → lineitem)
/// — irrelevant for correctness since DataFusion resolves on first
/// query, but makes the registration log easier to follow when
/// debugging.
const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

/// Resolve the SF=1 data directory. Honors `TPCH_DATA_DIR` env var;
/// defaults to `examples/tpch/data/sf1` relative to the workspace root
/// (which `cargo bench` runs from for crate benches).
fn data_dir() -> PathBuf {
    if let Ok(env) = std::env::var("TPCH_DATA_DIR") {
        return PathBuf::from(env);
    }
    // CARGO_MANIFEST_DIR resolves to crates/ematix-flow-core/ during
    // bench compilation; walk up two levels for the workspace root.
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(|p| p.parent())
        .map(|root| root.join("examples/tpch/data/sf1"))
        .unwrap_or_else(|| PathBuf::from("examples/tpch/data/sf1"))
}

/// Build a SessionContext with all 8 TPC-H tables registered against
/// their Parquet files. Panics with a clear message if any file is
/// missing (run `cargo run --release --example tpch_generate -- --sf
/// 1 --out examples/tpch/data/sf1` to populate).
async fn build_session(rt: &Runtime, dir: &Path) -> SessionContext {
    let _enter = rt.enter();
    let ctx = SessionContext::new();
    for table in TPCH_TABLES {
        let path = dir.join(format!("{table}.parquet"));
        if !path.exists() {
            panic!(
                "TPC-H Parquet missing: {}\n\
                 Generate first:\n\
                 \tcargo run --release -p ematix-flow-core --example tpch_generate -- \\\n\
                 \t    --sf 1 --out {}",
                path.display(),
                dir.display()
            );
        }
        ctx.register_parquet(*table, path.to_str().unwrap(), Default::default())
            .await
            .unwrap_or_else(|e| panic!("register {table}: {e}"));
    }
    ctx
}

fn run_query(rt: &Runtime, ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    rt.block_on(async {
        let df = ctx.sql(sql).await.expect("plan");
        df.collect().await.expect("execute")
    })
}

/// Per-query measurement window. Sized so criterion's default
/// `sample_size = 10` gets multiple iterations within the window
/// without dragging the whole suite past ~90s wall-clock on
/// M3-class hardware. Override with `TPCH_MEASUREMENT_TIME_S` for
/// noisier hosts (CI runners) or fast iteration during development.
fn measurement_time_for(query: &str) -> Duration {
    if let Ok(env) = std::env::var("TPCH_MEASUREMENT_TIME_S")
        && let Ok(s) = env.parse::<u64>()
    {
        return Duration::from_secs(s);
    }
    // M3 Pro SF=1 measured timings (committed baseline 2026-05-05):
    //   Q1 = 48.7 ms, Q3 = 34.6 ms, Q6 = 18.2 ms, Q19 = 38.0 ms.
    // 20s windows give 400+ iterations per query — plenty for stable
    // medians. Linux x86_64 m6i.4xlarge expected within ~2× of these.
    match query {
        "q01" | "q03" | "q19" => Duration::from_secs(20),
        "q06" => Duration::from_secs(15),
        _ => Duration::from_secs(20),
    }
}

fn bench_tpch(c: &mut Criterion) {
    let rt = Runtime::new().expect("build tokio runtime");
    let dir = data_dir();
    println!("==> TPC-H bench data dir: {}", dir.display());
    let ctx = rt.block_on(build_session(&rt, &dir));

    let queries: &[(&str, &str)] = &[("q01", Q1), ("q03", Q3), ("q06", Q6), ("q19", Q19)];

    for (name, sql) in queries {
        let mut group = c.benchmark_group("tpch_sf1");
        group
            .sample_size(10)
            .measurement_time(measurement_time_for(name));
        group.bench_function(*name, |b| {
            b.iter(|| {
                let result = run_query(&rt, &ctx, sql);
                std::hint::black_box(result);
            });
        });
        group.finish();
    }
}

criterion_group!(benches, bench_tpch);
criterion_main!(benches);
