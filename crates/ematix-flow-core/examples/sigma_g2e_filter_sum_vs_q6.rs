//! Σ.G.2e bench gate: `FusedAggregateExec<FilterSumSpec>` vs
//! `FusedAggregateExec<Q6Spec(JIT)>` on TPC-H SF=1 Q6.
//!
//! Both paths construct the same physical operator (`FusedAggregateExec<S>`)
//! over the same scan, and both share the same Cranelift kernel — only
//! the `S` parameter differs:
//!
//! - `InjectFusedQ6Rule` → `Q6Spec(JIT)`. The spec carries a fixed
//!   `Q6Predicate` struct; `process_batch` reads input columns into a
//!   stack-allocated `[*const u8; 4]` and calls the JIT.
//! - `InjectFilterSumRule` → `FilterSumSpec`. The spec carries a
//!   runtime-built `FusedFilterAggSpec`; `process_batch` allocates a
//!   `Vec<*const u8>` per batch sized to `jit_spec.inputs.len()`.
//!
//! The Σ.G.2e-2 PR shipped `FilterSumSpec` + `InjectFilterSumRule`
//! without a bench gate — this file ships the gate. If the
//! `Vec<*const u8>` allocation stays within 3 % of the Q6Spec path, the
//! runtime-configured spec is confirmed perf-equivalent and
//! `InjectFusedQ6Rule` can retire (Σ.G.2e-4).
//!
//! ## Methodology
//!
//! Matches the Σ.G.2c "operator vs hand" gate that landed with
//! `FusedAggregateExec<S>`:
//!
//! - 41 trials per round, 3 rounds.
//! - Trials are **interleaved within a round** — for each i ∈ 0..41,
//!   run Q6Spec trial then FilterSumSpec trial back-to-back. This
//!   neutralises slow-drift system noise (turbo-boost ramp,
//!   page-cache state).
//! - After each round take MIN time per path → 3 mins per path.
//! - Take MEDIAN of the 3 mins per path → comparable point estimates.
//! - Pass criterion: `|filter_sum_median - q6_median| / q6_median < 0.03`.
//!
//! ## Run
//!
//! ```sh
//! cargo run --release --example sigma_g2e_filter_sum_vs_q6
//! ```
//!
//! Requires SF=1 TPC-H lineitem.parquet. Resolves from `TPCH_DATA_DIR`
//! env var or `examples/tpch/data/sf1/lineitem.parquet`. Exits with code
//! 0 on PASS, 1 on FAIL, 2 if data is missing.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::Float64Array;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fused_aggregate_filter_sum_rule::InjectFilterSumRule;
use ematix_flow_core::fused_jit_rule::InjectFusedQ6Rule;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const Q6_SQL: &str = "
    SELECT sum(l_extendedprice * l_discount) AS revenue
    FROM lineitem
    WHERE l_shipdate >= DATE '1994-01-01'
      AND l_shipdate <  DATE '1995-01-01'
      AND l_discount BETWEEN 0.05 AND 0.07
      AND l_quantity <  24
";

const TRIALS_PER_ROUND: usize = 41;
const ROUNDS: usize = 3;
const WARMUPS: usize = 3;
const PASS_THRESHOLD: f64 = 0.03;

/// Resolve the SF=1 lineitem.parquet path. Returns `None` and prints a
/// skip notice if not present — examples called via `cargo run` should
/// exit cleanly so the surrounding harness doesn't false-fail.
fn data_path() -> Option<String> {
    let env = std::env::var("TPCH_DATA_DIR").ok().map(PathBuf::from);
    let dir = match env {
        Some(p) if p.exists() => p,
        _ => {
            let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            manifest.parent()?.parent()?.join("examples/tpch/data/sf1")
        }
    };
    let p = dir.join("lineitem.parquet");
    p.exists().then(|| p.to_string_lossy().into_owned())
}

async fn build_ctx_with_rule(
    path: &str,
    rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
) -> SessionContext {
    let cfg = SessionConfig::new().with_target_partitions(14);
    let state = SessionStateBuilder::new()
        .with_config(cfg)
        .with_default_features()
        .with_physical_optimizer_rule(rule)
        .build();
    let ctx = SessionContext::new_with_state(state);
    let prov = FastParquetTableProvider::try_new(path.to_string()).unwrap();
    ctx.register_table("lineitem", Arc::new(prov)).unwrap();
    ctx
}

/// Run Q6 once, return wall-clock ms + the scalar result for sanity.
async fn run_once(ctx: &SessionContext) -> (f64, f64) {
    let t = Instant::now();
    let batches = ctx.sql(Q6_SQL).await.unwrap().collect().await.unwrap();
    let ms = t.elapsed().as_secs_f64() * 1000.0;
    let revenue = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    (ms, revenue)
}

fn min(xs: &[f64]) -> f64 {
    xs.iter().copied().fold(f64::INFINITY, f64::min)
}

fn median(xs: &mut [f64]) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    xs[xs.len() / 2]
}

#[tokio::main(flavor = "multi_thread", worker_threads = 14)]
async fn main() {
    let path = match data_path() {
        Some(p) => p,
        None => {
            eprintln!("SKIP: SF=1 TPC-H lineitem.parquet not found.");
            eprintln!("       Set TPCH_DATA_DIR or generate examples/tpch/data/sf1/.");
            std::process::exit(2);
        }
    };

    println!("Σ.G.2e bench gate: FilterSumSpec vs Q6Spec(JIT)");
    println!("data: {path}");
    println!(
        "methodology: {TRIALS_PER_ROUND} trials × {ROUNDS} rounds × interleaved, \
         {WARMUPS} warmups/path, MIN per round, median of mins, ≤{:.0}% threshold",
        PASS_THRESHOLD * 100.0
    );
    println!();

    let ctx_q6 = build_ctx_with_rule(&path, Arc::new(InjectFusedQ6Rule)).await;
    let ctx_filter_sum = build_ctx_with_rule(&path, Arc::new(InjectFilterSumRule)).await;

    // Warmups (separately so the first measured trial doesn't pay the
    // page-cache + JIT-build amortisation cost).
    print!("warmup: ");
    for _ in 0..WARMUPS {
        let (_, _) = run_once(&ctx_q6).await;
        let (_, _) = run_once(&ctx_filter_sum).await;
        print!(".");
    }
    println!(" done");
    println!();

    // Reference scalar from each path — confirm bit-equivalent answer
    // before measuring (a perf gate on diverging numerics is useless).
    let (_, ref_q6) = run_once(&ctx_q6).await;
    let (_, ref_fs) = run_once(&ctx_filter_sum).await;
    let rel_err = ((ref_fs - ref_q6) / ref_q6).abs();
    if rel_err > 1e-10 {
        eprintln!(
            "FAIL: answer mismatch before timing — Q6={ref_q6}, FilterSum={ref_fs}, \
             rel_err={rel_err:e}"
        );
        std::process::exit(1);
    }
    println!("reference: Q6 revenue = {ref_q6:.6}, rel_err vs FilterSum = {rel_err:e}");
    println!();

    // 3 rounds × 41 interleaved trials.
    let mut mins_q6 = Vec::with_capacity(ROUNDS);
    let mut mins_fs = Vec::with_capacity(ROUNDS);
    for round in 1..=ROUNDS {
        print!("round {round}/{ROUNDS}: ");
        let mut q6_times = Vec::with_capacity(TRIALS_PER_ROUND);
        let mut fs_times = Vec::with_capacity(TRIALS_PER_ROUND);
        for _ in 0..TRIALS_PER_ROUND {
            // Interleave: Q6 then FilterSum, neutralises drift inside
            // a single trial pair.
            let (q6_ms, _) = run_once(&ctx_q6).await;
            q6_times.push(q6_ms);
            let (fs_ms, _) = run_once(&ctx_filter_sum).await;
            fs_times.push(fs_ms);
        }
        let q6_min = min(&q6_times);
        let fs_min = min(&fs_times);
        mins_q6.push(q6_min);
        mins_fs.push(fs_min);
        println!(
            "Q6 min = {q6_min:>6.2} ms, FilterSum min = {fs_min:>6.2} ms, \
             ratio = {:.4}",
            fs_min / q6_min
        );
    }
    println!();

    let median_q6 = median(&mut mins_q6.clone());
    let median_fs = median(&mut mins_fs.clone());
    let delta = ((median_fs - median_q6) / median_q6).abs();

    println!("median of round-mins:");
    println!("  Q6Spec(JIT)     : {median_q6:>6.2} ms");
    println!("  FilterSumSpec   : {median_fs:>6.2} ms");
    println!(
        "  delta           : {:.2}% ({} threshold)",
        delta * 100.0,
        if delta < PASS_THRESHOLD {
            format!("≤ {:.0}%", PASS_THRESHOLD * 100.0)
        } else {
            format!("> {:.0}%", PASS_THRESHOLD * 100.0)
        }
    );
    println!();

    if delta < PASS_THRESHOLD {
        println!("PASS: FilterSumSpec is perf-equivalent to Q6Spec(JIT) within {:.0}%.", PASS_THRESHOLD * 100.0);
        println!("       Σ.G.2e-4 (retire InjectFusedQ6Rule) is unblocked.");
        std::process::exit(0);
    } else {
        eprintln!(
            "FAIL: FilterSumSpec is {:.2}% from Q6Spec(JIT), threshold is {:.0}%.",
            delta * 100.0,
            PASS_THRESHOLD * 100.0
        );
        eprintln!("       Investigate `Vec<*const u8>` per-batch allocation cost.");
        std::process::exit(1);
    }
}
