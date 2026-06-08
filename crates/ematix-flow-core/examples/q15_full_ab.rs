//! PV.M.6b — FULL Q15 A/B for the masked-fusion lever (EMAT_EXACT_PUSHDOWN).
//! Runs the real shipping Q15 (CTE revenue_s + max-revenue subquery, through the
//! current preset incl. Phase-2 CSE-parallel drain + float-determinism dedupe).
//! Mode via the EMAT_EXACT_PUSHDOWN env the provider already reads — run this
//! binary with and without it. Prints median/min wall + the result rows so the
//! masked path's correctness is checked against the dense path.
//!
//! Usage:
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 TRIALS=40 \
//!     cargo run --release -p ematix-flow-core --example q15_full_ab
//!   EMAT_EXACT_PUSHDOWN=1 TPCH_DATA_DIR=examples/tpch/data/sf10 TRIALS=40 \
//!     cargo run --release -p ematix-flow-core --example q15_full_ab

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Array, Float64Array, Int64Array};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

fn build_ctx(data_dir: &Path, parts: usize) -> Result<SessionContext, Box<dyn std::error::Error>> {
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

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf10"));
    let trials: usize = std::env::var("TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(40);
    let parts = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(14);
    let exact = std::env::var("EMAT_EXACT_PUSHDOWN").ok().as_deref() == Some("1");

    let sql = std::fs::read_to_string("examples/tpch/queries/q15.sql")?;

    // FRESH ctx per trial. Q15's revenue_s is a CSE'd subtree and the Σ.P
    // SharedSubtreeExec registry is SESSION-scoped, so reusing one ctx makes
    // trials 2+ REPLAY cached revenue batches instead of re-decoding lineitem
    // (the "Σ.P CSE-replay footgun" — it collapsed full Q15 to ~6 ms). A fresh
    // ctx per trial forces real decode, matching the bench methodology + how
    // Polars/DuckDB run each query cold.

    // Correctness: capture + print the result (and a fingerprint).
    let ctx0 = build_ctx(&data_dir, parts)?;
    let plan0 = ctx0.sql(&sql).await?.create_physical_plan().await?;
    if std::env::var_os("PLAN").is_some() {
        println!(
            "=== Q15 physical plan (EXACT={}) ===\n{}\n",
            exact as u8,
            datafusion::physical_plan::displayable(plan0.as_ref()).indent(true)
        );
    }
    let res = collect(plan0, ctx0.task_ctx()).await?;
    let rows: usize = res.iter().map(|b| b.num_rows()).sum();
    let mut fp = 0.0f64;
    let mut keyfp: i64 = 0;
    for b in &res {
        if let Some(rev) = b
            .column(b.num_columns() - 1)
            .as_any()
            .downcast_ref::<Float64Array>()
        {
            for i in 0..rev.len() {
                if rev.is_valid(i) {
                    fp += rev.value(i);
                }
            }
        }
        if let Some(k) = b.column(0).as_any().downcast_ref::<Int64Array>() {
            for i in 0..k.len() {
                if k.is_valid(i) {
                    keyfp ^= k.value(i);
                }
            }
        }
    }
    println!(
        "FULL Q15 — EMAT_EXACT_PUSHDOWN={}  rows={rows}  Σtotal_revenue={fp:.4}  suppkey_xor={keyfp}",
        exact as u8
    );
    println!("{}\n", pretty_format_batches(&res)?);

    // Warmup + timed. Fresh ctx+plan each trial built OUTSIDE the timer; the
    // timed region is execute-only (decode+agg+join), no CSE replay.
    for _ in 0..5 {
        let ctx = build_ctx(&data_dir, parts)?;
        let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
        let _ = collect(plan, ctx.task_ctx()).await?;
    }
    let mut ms = Vec::with_capacity(trials);
    for _ in 0..trials {
        let ctx = build_ctx(&data_dir, parts)?;
        let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
        let t = Instant::now();
        let _ = collect(plan, ctx.task_ctx()).await?;
        ms.push(t.elapsed().as_secs_f64() * 1e3);
    }
    let m = median(ms.clone());
    let mn = ms.iter().cloned().fold(f64::INFINITY, f64::min);
    println!(
        "FULL Q15 wall (EXACT={}) : median {m:.1} ms   min {mn:.1} ms   ({trials} trials, parts={parts})",
        exact as u8
    );
    Ok(())
}
