//! PV.M.7 #308 — Q15-only interleaved A/B, ORDER-BALANCED, high-trial.
//!
//! The 22q harness can't resolve Q15's masked-fusion win: with 7 trials
//! and ~60 ms of fresh-ctx + plan overhead per run, the ~5 ms decode
//! saving sits inside the ±4% run-to-run jitter (Q15 swung −2.3%→+4.1%
//! across two 22q runs). And q15_full_ab runs ONE mode per process, so
//! comparing two invocations reintroduces cross-process thermal drift
//! (the phantom "5 ms orchestration" gap that was really drift).
//!
//! This isolates Q15, runs MANY interleaved trials in ONE process, and
//! ALTERNATES which arm goes first each trial (off-first on even trials,
//! on-first on odd) so the warm-cache/thermal tailwind that gave the
//! 22q "inert band" its spurious −1.2% cancels out. FRESH CTX PER TRIAL
//! (no Σ.P CSE replay). Reports off/on medians, the delta, and a
//! checksum+rowcount correctness guard.
//!
//! Usage:
//!   TRIALS=31 TPCH_DATA_DIR=examples/tpch/data/sf10 \
//!     cargo run --release -p ematix-flow-core --example q15_interleaved_ab

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use datafusion::arrow::array::{Array, Float64Array};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::drop_redundant_filter_rule::PV_M7_FUSES;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

fn set_fusion(on: bool) {
    unsafe {
        std::env::set_var("EMAT_CSE_FILTER_FUSION", if on { "1" } else { "0" });
    }
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

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

async fn run_once(
    data_dir: &Path,
    parts: usize,
    sql: &str,
) -> Result<(f64, f64, usize, u64), Box<dyn std::error::Error>> {
    let fires_before = PV_M7_FUSES.load(Ordering::Relaxed);
    let ctx = build_ctx(data_dir, parts)?;
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let t = Instant::now();
    let batches = collect(plan, ctx.task_ctx()).await?;
    let ms = t.elapsed().as_secs_f64() * 1e3;
    let mut sum = 0.0f64;
    let mut rows = 0usize;
    for b in &batches {
        rows += b.num_rows();
        for c in b.columns() {
            if let Some(a) = c.as_any().downcast_ref::<Float64Array>() {
                for v in a.values() {
                    sum += *v;
                }
            }
        }
    }
    let fires = PV_M7_FUSES.load(Ordering::Relaxed) - fires_before;
    Ok((ms, sum, rows, fires))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf10"));
    let trials: usize = std::env::var("TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(31);
    let parts = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(14);
    let sql = std::fs::read_to_string(PathBuf::from("examples/tpch/queries/q15.sql"))?;

    println!(
        "PV.M.7 #308 — Q15 interleaved order-balanced A/B — data={} parts={parts} trials={trials}",
        data_dir.display()
    );

    // Warmups (both arms).
    for _ in 0..3 {
        set_fusion(false);
        let _ = run_once(&data_dir, parts, &sql).await;
        set_fusion(true);
        let _ = run_once(&data_dir, parts, &sql).await;
    }

    let mut off = Vec::with_capacity(trials);
    let mut on = Vec::with_capacity(trials);
    let (mut cs_off, mut rows_off) = (0.0f64, 0usize);
    let (mut cs_on, mut rows_on) = (0.0f64, 0usize);
    let mut total_fires = 0u64;

    for i in 0..trials {
        // Alternate arm order each trial to cancel the off-first tailwind.
        let off_first = i % 2 == 0;
        for &run_on in if off_first {
            &[false, true]
        } else {
            &[true, false]
        } {
            set_fusion(run_on);
            let (ms, sum, rows, fires) = run_once(&data_dir, parts, &sql).await?;
            if run_on {
                on.push(ms);
                cs_on = sum;
                rows_on = rows;
                total_fires += fires;
            } else {
                off.push(ms);
                cs_off = sum;
                rows_off = rows;
            }
        }
    }
    set_fusion(false);

    let s = median(off.clone());
    let p = median(on.clone());
    let delta = (p - s) / s * 100.0;
    let ok = (cs_off - cs_on).abs() / cs_off.abs().max(1.0) <= 1e-9 && rows_off == rows_on;

    // Trimmed mean (drop top/bottom 10%) as a second estimator robust to
    // the occasional thermal spike a single median might miss.
    let trimmed = |mut v: Vec<f64>| -> f64 {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let k = v.len() / 10;
        let slice = &v[k..v.len() - k];
        slice.iter().sum::<f64>() / slice.len() as f64
    };
    let ts = trimmed(off);
    let tp = trimmed(on);
    let tdelta = (tp - ts) / ts * 100.0;

    println!("\noff  median {s:>8.2} ms   trimmed-mean {ts:>8.2} ms");
    println!("on   median {p:>8.2} ms   trimmed-mean {tp:>8.2} ms");
    println!("delta  median {delta:>+6.1}%   trimmed-mean {tdelta:>+6.1}%");
    println!(
        "fires/trial {:.1}   correctness {}   Σrev_off={cs_off:.4} Σrev_on={cs_on:.4} rows={rows_off}",
        total_fires as f64 / trials as f64,
        if ok { "OK ✓" } else { "MISMATCH ✗" }
    );
    println!(
        "VERDICT: {}",
        if !ok {
            "BLOCKED — correctness mismatch"
        } else if delta <= -2.0 && tdelta <= -2.0 {
            "WIN — fusion nets ≥2% on Q15 by both estimators"
        } else if delta <= 0.0 && tdelta <= 0.0 {
            "MARGINAL-WIN — both estimators ≤0, win below 2%"
        } else {
            "NEUTRAL/NOISE — within jitter at this isolation"
        }
    );
    Ok(())
}
