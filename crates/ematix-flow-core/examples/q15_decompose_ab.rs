//! PV.M.0 — Q15 SUBTREE-vs-TAIL decomposition, ematix vs Polars. The morsel
//! program's go/no-go de-risk: does the residual Q15 gap to Polars live in the
//! revenue SUBTREE (scan→filter→SUM-agg) or in the TAIL (supplier join → max →
//! sort)?
//!
//!   REVENUE = `select l_suppkey, sum(l_extendedprice*(1-l_discount))
//!              from lineitem where l_shipdate ∈ [1996-01-01,1996-04-01)
//!              group by l_suppkey`   (the revenue_s CTE, computed ONCE, no CSE)
//!   FULL    = the whole Q15.
//!
//! Per engine: TAIL = FULL − REVENUE. The decisive reads:
//!   * ematix_rev − polars_rev ≈ ematix_full − polars_full  ⇒ gap is in the
//!     SUBTREE (decode/agg) → morsel pipelining can't help; gap is decode-kernel
//!     (already-dead levers). STOP the morsel program for Q15.
//!   * ematix_rev ≈ polars_rev BUT ematix_full > polars_full ⇒ gap is in the
//!     TAIL / the decode→tail barrier → morsel pipelining is the right tool → GO.
//!
//! Both engines timed the SAME way (fresh ctx/SQLContext per call, timer wraps
//! plan-build + execute + collect) so the FULL query matches the canonical
//! triangulation numbers (ematix ~66, polars ~58.5). No prebuilt-slice artifact.
//!
//! Usage:
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 \
//!     cargo run --release -p ematix-flow-core --features triangulation \
//!     --example q15_decompose_ab

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

const WHERE: &str = "where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-04-01'";

// Finer decomposition of the revenue subtree, each adding ONE stage of work:
//   COUNT  = decode l_shipdate + filter                     (pure filter-decode)
//   SCALAR = + decode l_extendedprice,l_discount + f64 sum  (adds f64 decode+arith)
//   REVENUE= + decode l_suppkey + 100K-group SUM-agg        (adds key-decode+groupby)
// COUNT→SCALAR isolates f64-decode; SCALAR→REVENUE isolates suppkey-decode+groupby.
fn count_sql() -> String {
    format!("select count(*) from lineitem {WHERE}")
}
fn scalar_sql() -> String {
    format!("select sum(l_extendedprice * (1 - l_discount)) from lineitem {WHERE}")
}
fn revenue_sql() -> String {
    format!(
        "select l_suppkey, sum(l_extendedprice * (1 - l_discount)) as total_revenue \
         from lineitem {WHERE} group by l_suppkey"
    )
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn build_ematix_ctx(
    data_dir: &Path,
    parts: usize,
) -> Result<SessionContext, Box<dyn std::error::Error>> {
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

/// Time ematix: fresh ctx + (plan + execute + collect). Returns (ms, rows).
async fn run_ematix(
    data_dir: &Path,
    parts: usize,
    sql: &str,
) -> Result<(f64, usize), Box<dyn std::error::Error>> {
    let ctx = build_ematix_ctx(data_dir, parts)?;
    let t0 = Instant::now();
    let df = ctx.sql(sql).await?;
    let batches = df.collect().await?;
    let ms = t0.elapsed().as_secs_f64() * 1e3;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    Ok((ms, rows))
}

/// Time Polars: fresh SQLContext + (execute lazy + collect). Returns (ms, rows).
/// Runs on a scoped std thread — Polars `collect()` starts its own runtime,
/// which panics if called on a tokio worker (`#[tokio::main]`).
fn run_polars(data_dir: &Path, sql: &str) -> Result<(f64, usize), Box<dyn std::error::Error>> {
    let out: Result<(f64, usize), String> = std::thread::scope(|s| {
        s.spawn(|| -> Result<(f64, usize), String> {
            use polars::prelude::*;
            use polars::sql::SQLContext;
            let mut ctx = SQLContext::new();
            for t in TPCH_TABLES {
                let path = data_dir.join(format!("{t}.parquet"));
                let pl_path = polars::prelude::PlPath::new(path.to_str().unwrap_or_default());
                let lf = LazyFrame::scan_parquet(pl_path, ScanArgsParquet::default())
                    .map_err(|e| e.to_string())?;
                ctx.register(t, lf);
            }
            let t0 = Instant::now();
            let lf = ctx.execute(sql).map_err(|e| e.to_string())?;
            let df = lf.collect().map_err(|e| e.to_string())?;
            let ms = t0.elapsed().as_secs_f64() * 1e3;
            Ok((ms, df.height()))
        })
        .join()
        .unwrap_or_else(|_| Err("polars thread panicked".to_string()))
    });
    out.map_err(|e| e.into())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf10"));
    let qdir = PathBuf::from("examples/tpch/queries");
    let full_emat = std::fs::read_to_string(qdir.join("q15.sql"))?;
    let full_pol =
        std::fs::read_to_string(qdir.join("q15.polars.sql")).unwrap_or_else(|_| full_emat.clone());
    let parts = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(14);
    let warmups = 3;
    let trials = 15;

    println!(
        "PV.M.0 — Q15 staged decompose (count→scalar→groupby→full) — data={} parts={parts} trials={trials}\n",
        data_dir.display()
    );

    // (name, ematix_sql, polars_sql) — same SQL both engines except FULL.
    let stages: Vec<(&str, String, String)> = vec![
        ("COUNT", count_sql(), count_sql()),
        ("SCALAR", scalar_sql(), scalar_sql()),
        ("REVENUE", revenue_sql(), revenue_sql()),
        ("FULL", full_emat.clone(), full_pol.clone()),
    ];

    println!(
        "{:>9}  {:>11}  {:>11}  {:>9}  {:>14}",
        "stage", "ematix_ms", "polars_ms", "emat/pol", "Δgap(e−p)ms"
    );
    let mut emat = Vec::new();
    let mut pol = Vec::new();
    for (name, esql, psql) in &stages {
        for _ in 0..warmups {
            let _ = run_ematix(&data_dir, parts, esql).await;
            let _ = run_polars(&data_dir, psql);
        }
        let (mut ev, mut pv) = (Vec::with_capacity(trials), Vec::with_capacity(trials));
        for _ in 0..trials {
            ev.push(run_ematix(&data_dir, parts, esql).await?.0);
            pv.push(run_polars(&data_dir, psql)?.0);
        }
        let (e, p) = (median(ev), median(pv));
        emat.push(e);
        pol.push(p);
        println!(
            "{name:>9}  {e:>11.1}  {p:>11.1}  {:>8.2}×  {:>+14.1}",
            e / p,
            e - p
        );
    }

    // Incremental (stage k − stage k-1) attributes the gap to the work each
    // stage ADDS: COUNT=shipdate-decode+filter, SCALAR-COUNT=f64-decode+arith,
    // REVENUE-SCALAR=suppkey-decode+groupby-agg, FULL-REVENUE=join+max+sort.
    let labels = [
        "shipdate-decode+filter",
        "f64-decode+scalar-sum",
        "suppkey-decode+groupby-agg",
        "join+max+sort (tail)",
    ];
    println!("\n=== incremental gap attribution (where ematix loses to Polars) ===");
    let mut prev_e = 0.0;
    let mut prev_p = 0.0;
    for (i, lab) in labels.iter().enumerate() {
        let de = emat[i] - prev_e;
        let dp = pol[i] - prev_p;
        println!(
            "  {lab:<28} ematix {de:>6.1}  polars {dp:>6.1}  →  gap {:>+6.1} ms",
            de - dp
        );
        prev_e = emat[i];
        prev_p = pol[i];
    }
    println!(
        "\nThe stage with the biggest +gap is where Polars beats us. If it's a decode\n\
         stage → decode-kernel (mostly dead levers). If groupby-agg → SUM-agg kernel\n\
         (RobinHood / pre-size) is a LIVE lever. If tail → morsel (already shown free)."
    );
    Ok(())
}
