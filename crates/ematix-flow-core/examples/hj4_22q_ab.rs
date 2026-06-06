//! HJ.5 — 22q SF=10 native-vs-Tag A/B. Does the SIMD-tag join probe (−15% on
//! Q08) net-win across the suite, or regress some queries (tiny / non-unique
//! builds)? Gates the default-on decision alongside tpch_validate correctness.
//!
//! Two arms, interleaved per query (fresh ctx per run → no Σ.P CSE-replay /
//! plan-cache artifact; matches the canonical bench protocol):
//!   native — stock DataFusion HashJoinExec
//!   tag    — EMAT_HASH_JOIN=1 + EMAT_HJ_TAG=1  (swap rule registered)
//!
//! Per query: median wall, delta%, whether Tag fired (TAG_BUILDS), and a
//! native-vs-Tag checksum equality guard. Reports geomean + regressions.
//!
//! Usage:
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 \
//!     cargo run --release -p ematix-flow-core --example hj4_22q_ab

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use datafusion::arrow::array::{Array, Float64Array};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::emat_hash_join::TAG_BUILDS;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::swap_emat_hash_join_rule::SwapEmatixHashJoinRule;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

fn set_arm(tag: bool) {
    unsafe {
        if tag {
            std::env::set_var("EMAT_HASH_JOIN", "1");
            std::env::set_var("EMAT_HJ_TAG", "1");
        } else {
            std::env::remove_var("EMAT_HASH_JOIN");
            std::env::remove_var("EMAT_HJ_TAG");
        }
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
    let state = ematix_flow_core::preset::with_optimizer_rules(builder)
        .with_physical_optimizer_rule(Arc::new(SwapEmatixHashJoinRule))
        .build();
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

/// One fresh-ctx run: returns (wall_ms, float_checksum, rows). Arm env must be
/// set by the caller (read at plan + execute time).
async fn run_once(
    data_dir: &Path,
    parts: usize,
    sql: &str,
) -> Result<(f64, f64, usize), Box<dyn std::error::Error>> {
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
    Ok((ms, sum, rows))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf10"));
    let queries_dir = PathBuf::from("examples/tpch/queries");
    let parts = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(14);
    let warmups = 2;
    let trials = 7;

    println!(
        "HJ.5 22q SF=10 native-vs-Tag A/B — data={} parts={parts} trials={trials}\n",
        data_dir.display()
    );
    println!(
        "{:>4}  {:>10}  {:>10}  {:>8}  {:>5}  {:>4}",
        "Q", "native_ms", "tag_ms", "delta%", "fired", "ok"
    );

    let mut log_ratio_sum = 0.0f64;
    let mut counted = 0usize;
    let mut fires = 0usize;
    let mut regressions: Vec<(u8, f64)> = Vec::new();
    let mut wins: Vec<(u8, f64)> = Vec::new();
    let mut mismatches: Vec<u8> = Vec::new();

    for q in 1..=22u8 {
        let sql_path = queries_dir.join(format!("q{q:02}.sql"));
        let sql = match std::fs::read_to_string(&sql_path) {
            Ok(s) => s,
            Err(_) => {
                println!("Q{q:02}: SKIP (no SQL)");
                continue;
            }
        };

        // Warmup both arms (fresh ctx each).
        for _ in 0..warmups {
            set_arm(false);
            let _ = run_once(&data_dir, parts, &sql).await;
            set_arm(true);
            let _ = run_once(&data_dir, parts, &sql).await;
        }

        // Timed, interleaved. Capture checksum from the first trial of each arm.
        let mut nat = Vec::with_capacity(trials);
        let mut tg = Vec::with_capacity(trials);
        let (mut cs_n, mut rows_n) = (0.0f64, 0usize);
        let (mut cs_t, mut rows_t) = (0.0f64, 0usize);
        let mut fired = false;
        let mut errored: Option<String> = None;
        for i in 0..trials {
            set_arm(false);
            match run_once(&data_dir, parts, &sql).await {
                Ok((ms, sum, rows)) => {
                    nat.push(ms);
                    if i == 0 {
                        cs_n = sum;
                        rows_n = rows;
                    }
                }
                Err(e) => {
                    errored = Some(format!("native: {e}"));
                    break;
                }
            }
            set_arm(true);
            let before = TAG_BUILDS.load(Ordering::Relaxed);
            match run_once(&data_dir, parts, &sql).await {
                Ok((ms, sum, rows)) => {
                    tg.push(ms);
                    if TAG_BUILDS.load(Ordering::Relaxed) > before {
                        fired = true;
                    }
                    if i == 0 {
                        cs_t = sum;
                        rows_t = rows;
                    }
                }
                Err(e) => {
                    errored = Some(format!("tag: {e}"));
                    break;
                }
            }
        }
        set_arm(false);

        if let Some(e) = errored {
            println!("Q{q:02}: ERROR — {e}");
            mismatches.push(q);
            continue;
        }

        let n = median(nat);
        let t = median(tg);
        let delta = (t - n) / n * 100.0;
        let ok = (cs_n - cs_t).abs() / cs_n.abs().max(1.0) <= 1e-9 && rows_n == rows_t;
        if !ok {
            mismatches.push(q);
        }
        if fired {
            fires += 1;
        }
        // Only count fired queries toward the kernel geomean (others are noise).
        if ok {
            log_ratio_sum += (t / n).ln();
            counted += 1;
            if delta > 3.0 {
                regressions.push((q, delta));
            } else if delta < -3.0 {
                wins.push((q, delta));
            }
        }
        println!(
            "Q{q:02}  {n:>10.1}  {t:>10.1}  {delta:>+7.1}  {:>5}  {:>4}",
            if fired { "yes" } else { "—" },
            if ok { "ok" } else { "DIFF" }
        );
    }

    let geomean = if counted > 0 {
        (log_ratio_sum / counted as f64).exp()
    } else {
        1.0
    };
    println!("\n=== HJ.5 summary ===");
    println!(
        "tag/native geomean (all {counted} ok queries) = {geomean:.4}  ({:+.1}%)",
        (geomean - 1.0) * 100.0
    );
    println!("Tag fired on {fires} / 22 queries");
    println!(
        "wins (≤−3%): {}",
        if wins.is_empty() {
            "none".to_string()
        } else {
            wins.iter()
                .map(|(q, d)| format!("Q{q:02}({d:+.0}%)"))
                .collect::<Vec<_>>()
                .join(" ")
        }
    );
    println!(
        "regressions (≥+3%): {}",
        if regressions.is_empty() {
            "NONE".to_string()
        } else {
            regressions
                .iter()
                .map(|(q, d)| format!("Q{q:02}({d:+.0}%)"))
                .collect::<Vec<_>>()
                .join(" ")
        }
    );
    if !mismatches.is_empty() {
        println!(
            "⚠️ CORRECTNESS DIFF / ERROR on: {mismatches:?} — investigate before any default-on"
        );
    }
    let verdict = if !mismatches.is_empty() {
        "BLOCKED — correctness diff/error"
    } else if regressions.is_empty() && geomean < 0.99 {
        "GO — net win, no regressions → default-on candidate (gate SF=100 next)"
    } else if regressions.is_empty() {
        "NEUTRAL — no regressions but ~flat geomean"
    } else {
        "MIXED — wins + regressions → needs a shape gate, not blanket default-on"
    };
    println!("VERDICT: {verdict}");
    Ok(())
}
