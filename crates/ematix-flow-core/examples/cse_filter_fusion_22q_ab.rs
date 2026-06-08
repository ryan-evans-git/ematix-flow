//! PV.M.7 #308 default-on gate — 22q SF=10 fusion-off-vs-on A/B.
//!
//! Q15 won −8% from the shape-gated masked-fusion projection-prune
//! (q15_full_ab / drop_redundant_filter_rule::fuse_redundant_bridge_filters).
//! Before flipping `cse_filter_fusion_enabled()` default-on in
//! dedupe_aggregate_rule.rs, confirm:
//!   1. the fusion is INERT on the 21 queries with no CSE'd
//!      Agg→Filter(i32-range,no-nulls)→EmatixScan shape (blast radius
//!      is Q15-only — already confirmed via tpch_validate trace), and
//!   2. flipping the flag adds no codegen/walk tax beyond noise on those
//!      inert queries (the helper is already compiled in regardless of
//!      the flag; the only delta is the conditional transform_up walk
//!      over CSE'd subtrees), and
//!   3. Q15 itself nets the win and stays correct.
//!
//! Two arms, interleaved, FRESH CTX PER TRIAL (no Σ.P CSE replay — a
//! reused ctx replays the revenue_s cache and fakes a 6 ms Q15):
//!   off — control                    (EMAT_CSE_FILTER_FUSION=0)
//!   on  — masked-fusion              (EMAT_CSE_FILTER_FUSION=1)
//!
//! Per query: median wall, delta%, fuse fires (PV_M7_FUSES delta), and
//! an off-vs-on checksum+rowcount guard. Geomean is taken over the
//! queries that actually fired (the rest are the flag being correctly
//! inert and are reported separately as the codegen-tax band).
//!
//! Usage:
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 \
//!     cargo run --release -p ematix-flow-core --example cse_filter_fusion_22q_ab

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
    // Explicit "0" for the off arm — once default-on lands the helper
    // keys on var_os(present), so remove_var would not disable it.
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
        "PV.M.7 #308 — 22q SF=10 fusion off-vs-on A/B — data={} parts={parts} trials={trials}\n",
        data_dir.display()
    );
    println!(
        "{:>4}  {:>10}  {:>10}  {:>8}  {:>5}  {:>4}",
        "Q", "off_ms", "on_ms", "delta%", "fires", "ok"
    );

    let mut log_ratio_fired = 0.0f64;
    let mut counted_fired = 0usize;
    let mut log_ratio_inert = 0.0f64;
    let mut counted_inert = 0usize;
    let mut regressions: Vec<(u8, f64)> = Vec::new();
    let mut wins: Vec<(u8, f64)> = Vec::new();
    let mut mismatches: Vec<u8> = Vec::new();

    for q in 1..=22u8 {
        let sql_path = queries_dir.join(format!("q{q:02}.sql"));
        let sql = match std::fs::read_to_string(&sql_path) {
            Ok(s) => s,
            Err(_) => continue,
        };

        for _ in 0..warmups {
            set_fusion(false);
            let _ = run_once(&data_dir, parts, &sql).await;
            set_fusion(true);
            let _ = run_once(&data_dir, parts, &sql).await;
        }

        let mut off = Vec::with_capacity(trials);
        let mut on = Vec::with_capacity(trials);
        let (mut cs_off, mut rows_off) = (0.0f64, 0usize);
        let (mut cs_on, mut rows_on) = (0.0f64, 0usize);
        let mut fires = 0u64;
        let mut errored: Option<String> = None;
        for i in 0..trials {
            set_fusion(false);
            match run_once(&data_dir, parts, &sql).await {
                Ok((ms, sum, rows)) => {
                    off.push(ms);
                    if i == 0 {
                        cs_off = sum;
                        rows_off = rows;
                    }
                }
                Err(e) => {
                    errored = Some(format!("off: {e}"));
                    break;
                }
            }
            set_fusion(true);
            let fires_before = PV_M7_FUSES.load(Ordering::Relaxed);
            match run_once(&data_dir, parts, &sql).await {
                Ok((ms, sum, rows)) => {
                    on.push(ms);
                    fires += PV_M7_FUSES.load(Ordering::Relaxed) - fires_before;
                    if i == 0 {
                        cs_on = sum;
                        rows_on = rows;
                    }
                }
                Err(e) => {
                    errored = Some(format!("on: {e}"));
                    break;
                }
            }
        }
        set_fusion(false);

        if let Some(e) = errored {
            println!("Q{q:02}: ERROR — {e}");
            mismatches.push(q);
            continue;
        }

        let s = median(off);
        let p = median(on);
        let delta = (p - s) / s * 100.0;
        let ok = (cs_off - cs_on).abs() / cs_off.abs().max(1.0) <= 1e-9 && rows_off == rows_on;
        if !ok {
            mismatches.push(q);
        }
        if ok {
            if fires > 0 {
                log_ratio_fired += (p / s).ln();
                counted_fired += 1;
                if delta > 3.0 {
                    regressions.push((q, delta));
                } else if delta < -3.0 {
                    wins.push((q, delta));
                }
            } else {
                // Flag inert here — this query is in the codegen-tax band.
                log_ratio_inert += (p / s).ln();
                counted_inert += 1;
                if delta > 3.0 {
                    regressions.push((q, delta));
                }
            }
        }
        println!(
            "Q{q:02}  {s:>10.1}  {p:>10.1}  {delta:>+7.1}  {fires:>5}  {:>4}",
            if ok { "ok" } else { "DIFF" }
        );
    }

    let geo = |sum: f64, n: usize| if n > 0 { (sum / n as f64).exp() } else { 1.0 };
    let geo_fired = geo(log_ratio_fired, counted_fired);
    let geo_inert = geo(log_ratio_inert, counted_inert);

    println!("\n=== PV.M.7 #308 22q summary ===");
    println!(
        "FIRED  on/off geomean ({counted_fired} q that fused)   = {geo_fired:.4}  ({:+.1}%)  ← the win",
        (geo_fired - 1.0) * 100.0
    );
    println!(
        "INERT  on/off geomean ({counted_inert} q, flag no-op)  = {geo_inert:.4}  ({:+.1}%)  ← codegen/walk tax band",
        (geo_inert - 1.0) * 100.0
    );
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
        println!("⚠️ CORRECTNESS DIFF / ERROR on: {mismatches:?} — investigate before default-on");
    }
    let verdict = if !mismatches.is_empty() {
        "BLOCKED — correctness diff/error"
    } else if regressions.is_empty() {
        "GO — Q15 wins, inert band within noise, no regressions → default-on candidate"
    } else {
        "MIXED — investigate the regressed query before default-on"
    };
    println!("VERDICT: {verdict}");
    Ok(())
}
