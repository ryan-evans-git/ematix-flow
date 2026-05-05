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

/// Σ.C extension: full 22-query TPC-H suite. Each entry pairs the
/// criterion bench label with the SQL body. Add a new query here +
/// drop a `qNN.sql` under `examples/tpch/queries/` to extend; the
/// bench loop iterates this slice. Source files are produced by
/// `cargo run --release -p ematix-flow-core --example tpch_extract_queries`
/// and the `tpch_22_audit` example confirms all 22 plan + execute
/// against the SF=1 dataset.
const TPCH_QUERIES: &[(&str, &str)] = &[
    (
        "q01",
        include_str!("../../../examples/tpch/queries/q01.sql"),
    ),
    (
        "q02",
        include_str!("../../../examples/tpch/queries/q02.sql"),
    ),
    (
        "q03",
        include_str!("../../../examples/tpch/queries/q03.sql"),
    ),
    (
        "q04",
        include_str!("../../../examples/tpch/queries/q04.sql"),
    ),
    (
        "q05",
        include_str!("../../../examples/tpch/queries/q05.sql"),
    ),
    (
        "q06",
        include_str!("../../../examples/tpch/queries/q06.sql"),
    ),
    (
        "q07",
        include_str!("../../../examples/tpch/queries/q07.sql"),
    ),
    (
        "q08",
        include_str!("../../../examples/tpch/queries/q08.sql"),
    ),
    (
        "q09",
        include_str!("../../../examples/tpch/queries/q09.sql"),
    ),
    (
        "q10",
        include_str!("../../../examples/tpch/queries/q10.sql"),
    ),
    (
        "q11",
        include_str!("../../../examples/tpch/queries/q11.sql"),
    ),
    (
        "q12",
        include_str!("../../../examples/tpch/queries/q12.sql"),
    ),
    (
        "q13",
        include_str!("../../../examples/tpch/queries/q13.sql"),
    ),
    (
        "q14",
        include_str!("../../../examples/tpch/queries/q14.sql"),
    ),
    (
        "q15",
        include_str!("../../../examples/tpch/queries/q15.sql"),
    ),
    (
        "q16",
        include_str!("../../../examples/tpch/queries/q16.sql"),
    ),
    (
        "q17",
        include_str!("../../../examples/tpch/queries/q17.sql"),
    ),
    (
        "q18",
        include_str!("../../../examples/tpch/queries/q18.sql"),
    ),
    (
        "q19",
        include_str!("../../../examples/tpch/queries/q19.sql"),
    ),
    (
        "q20",
        include_str!("../../../examples/tpch/queries/q20.sql"),
    ),
    (
        "q21",
        include_str!("../../../examples/tpch/queries/q21.sql"),
    ),
    (
        "q22",
        include_str!("../../../examples/tpch/queries/q22.sql"),
    ),
];

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
    // M3 Pro SF=1 measured timings (2026-05-05 representative set):
    //   Q1 = 48.7 ms, Q3 = 34.6 ms, Q6 = 18.2 ms, Q19 = 38.0 ms.
    // The Σ.C extension audit (`tpch_22_audit`) confirmed all 22
    // queries plan + execute at SF=1 in 17–105 ms each. With 22
    // queries × ~10s windows the suite runs in ~5 min wall-clock;
    // any operator who wants tighter CIs can bump
    // `TPCH_MEASUREMENT_TIME_S` from the env. The original 4
    // representative queries keep their 15-20s windows for
    // backward-compat with the 2026-05-05 baseline.
    match query {
        "q01" | "q03" | "q19" => Duration::from_secs(20),
        "q06" => Duration::from_secs(15),
        _ => Duration::from_secs(10),
    }
}

/// Derive an SF tag (`sf1`, `sf10`, `sf100`, ...) from the data dir
/// basename so bench group labels reflect what was actually run.
/// Falls back to `custom` for paths that don't match `sf<N>`.
fn sf_tag(dir: &std::path::Path) -> String {
    let basename = dir.file_name().and_then(|s| s.to_str()).unwrap_or("custom");
    if basename.starts_with("sf")
        && basename.len() > 2
        && basename[2..].chars().all(|c| c.is_ascii_digit())
    {
        basename.to_string()
    } else {
        "custom".to_string()
    }
}

fn bench_tpch(c: &mut Criterion) {
    let rt = Runtime::new().expect("build tokio runtime");
    let dir = data_dir();
    let sf = sf_tag(&dir);
    println!("==> TPC-H bench data dir: {}", dir.display());
    println!("==> SF tag (group label): {sf}");
    let ctx = rt.block_on(build_session(&rt, &dir));

    let group_name = format!("tpch_{sf}");

    for (name, sql) in TPCH_QUERIES {
        let mut group = c.benchmark_group(&group_name);
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
