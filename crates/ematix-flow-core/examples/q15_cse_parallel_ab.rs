//! POL.0 Phase-2 — Q15 SF=10 CSE-populate serial-vs-parallel wall A/B.
//!
//! HYPOTHESIS (from the Phase-1a leaf profile): Q15's gap to Polars is NOT
//! per-byte Snappy speed (already-rejected lever) but UNDER-PARALLELIZED
//! decode. The `revenue_s` CTE is referenced twice, so Σ.P wraps its
//! Final aggregate in a `SharedSubtreeExec`. That exec populates its cache
//! by draining the wrapped aggregate's partitions SERIALLY
//! (`for p in 0..n_parts`). When the agg is `FinalPartitioned` (N hash
//! partitions — the SF≥10 shape), the `RepartitionExec` beneath it
//! backpressures the N-1 un-drained channels, throttling the lineitem
//! decode below N-wide. The `mutexwait`/`cvwait` peaks in the profile are
//! that backpressure.
//!
//! Two arms, interleaved, FRESH CTX PER TRIAL (a shared ctx would let the
//! SharedSubtreeRegistry replay the cache on trial ≥2 → 7.5 ms not 76 ms,
//! contaminating the measurement):
//!   serial   — control, current behaviour          (no env)
//!   parallel — concurrent per-partition drain       (EMAT_CSE_PARALLEL=1)
//!
//! First does ONE trace run (`EMAT_CSE_TRACE=1`) to confirm n_parts>1
//! inside the wrapper (the precondition for the parallel path to matter),
//! then a checksum-equality guard, then the timed A/B.
//!
//! Usage:
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 \
//!     cargo run --release -p ematix-flow-core --example q15_cse_parallel_ab

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Array, Float64Array};
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

fn set_parallel(on: bool) {
    // Explicit "0" for the off arm — the populate default is now ON, so
    // remove_var would leave the parallel path enabled.
    unsafe {
        std::env::set_var("EMAT_CSE_PARALLEL", if on { "1" } else { "0" });
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
    // preset includes DedupeAggregateForFloatDeterminism (→ SharedSubtreeExec)
    // with a FRESH SharedSubtreeRegistry per call, so a fresh ctx per trial
    // never replays a prior trial's cache.
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

/// One fresh-ctx run → (wall_ms, total_revenue_sum, rows).
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
    let sql = std::fs::read_to_string("examples/tpch/queries/q15.sql")?;
    let parts = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(14);

    println!(
        "POL.0 Phase-2 — Q15 CSE serial-vs-parallel A/B — data={} parts={parts}",
        data_dir.display()
    );

    // (1) Trace run: confirm the wrapped aggregate has n_parts>1 (else the
    // parallel path is a no-op and the hypothesis is wrong). The
    // `[CSE] populate: n_parts=.. parallel=true` line comes from
    // shared_subtree_exec::populate.
    unsafe {
        std::env::set_var("EMAT_CSE_TRACE", "1");
    }
    set_parallel(true);
    println!(
        "\n--- trace run (expect a [CSE] populate line; n_parts>1 confirms the hypothesis precondition) ---"
    );
    let _ = run_once(&data_dir, parts, &sql).await?;
    unsafe {
        std::env::remove_var("EMAT_CSE_TRACE");
    }

    // (2) Correctness: serial vs parallel must match (sum + rows).
    set_parallel(false);
    let (_, cs_s, rows_s) = run_once(&data_dir, parts, &sql).await?;
    set_parallel(true);
    let (_, cs_p, rows_p) = run_once(&data_dir, parts, &sql).await?;
    set_parallel(false);
    let ok = (cs_s - cs_p).abs() / cs_s.abs().max(1.0) <= 1e-9 && rows_s == rows_p;
    println!(
        "\ncorrectness: serial(sum={cs_s:.4},rows={rows_s})  parallel(sum={cs_p:.4},rows={rows_p})  {}",
        if ok {
            "OK"
        } else {
            "*** DIFF — measurement invalid ***"
        }
    );

    // (3) Warmup, then timed interleaved A/B.
    for _ in 0..3 {
        set_parallel(false);
        let _ = run_once(&data_dir, parts, &sql).await?;
        set_parallel(true);
        let _ = run_once(&data_dir, parts, &sql).await?;
    }

    let trials = 15;
    let (mut serial, mut parallel) = (Vec::with_capacity(trials), Vec::with_capacity(trials));
    for _ in 0..trials {
        set_parallel(false);
        serial.push(run_once(&data_dir, parts, &sql).await?.0);
        set_parallel(true);
        parallel.push(run_once(&data_dir, parts, &sql).await?.0);
    }
    set_parallel(false);

    let s = median(serial.clone());
    let p = median(parallel.clone());
    let smin = serial.iter().cloned().fold(f64::INFINITY, f64::min);
    let pmin = parallel.iter().cloned().fold(f64::INFINITY, f64::min);
    println!("\n=== POL.0 Phase-2 Q15 result (n={trials}) ===");
    println!("serial   (control) : median {s:.1} ms   min {smin:.1} ms");
    println!(
        "parallel (EMAT_CSE_PARALLEL=1) : median {p:.1} ms   min {pmin:.1} ms   ({:+.1}% vs serial)",
        (p - s) / s * 100.0
    );
    println!("Polars reference (POL.0 Phase-0): 58.5 ms");
    let verdict = if !ok {
        "BLOCKED — correctness diff"
    } else if (p - s) / s < -0.05 {
        "GO — concurrent CSE drain is a real Q15 win → confirm hypothesis, scope default-on gate"
    } else if (p - s) / s > 0.05 {
        "NEG — parallel drain WORSE; serialization is not the bottleneck (or backpressure helps cache locality)"
    } else {
        "NEUTRAL — decode already parallel here; the gap is elsewhere (re-examine n_parts / Polars plan)"
    };
    println!("VERDICT: {verdict}");
    Ok(())
}
