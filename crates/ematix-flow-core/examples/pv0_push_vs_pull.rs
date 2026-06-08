//! PV.0 — Phase-0 de-risk spike for the push-vectorized engine.
//!
//! THE KILL-GATE. Isolates the one thing the 7 prior Q08 investigations blamed:
//! the pull-model **materialization tax** (build a match-index array, then
//! `arrow::compute::take`-gather the output columns) vs a **push** emit that
//! appends survivor payload directly in a single fused pass — no index array,
//! no take.
//!
//! Models Q08's `part ⋈ lineitem` semijoin: part (filtered `p_type`) contributes
//! NO output columns; it filters lineitem to rows whose `l_partkey` is one of the
//! ~13,333 surviving parts (0.67% survival, uniformly scattered — REV.20), carrying
//! 4 payload cols forward (`l_orderkey, l_suppkey, l_extendedprice, l_discount`).
//!
//! Decode is held CONSTANT: lineitem is decoded ONCE into in-memory Arrow batches
//! (decode is at parity-or-better vs DuckDB/Polars per prior findings; the lever is
//! emit). Both arms run over the SAME decoded batches. The probe (hashset lookup) is
//! identical in both arms — the ONLY difference is index-array+take (PULL) vs
//! direct-append (PUSH). Both arms compute the same checksum (Σ extendedprice*(1-discount))
//! so correctness parity is asserted.
//!
//! Usage:
//!   cargo build --release -p ematix-flow-core --example pv0_push_vs_pull
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 TRIALS=9 ./target/release/examples/pv0_push_vs_pull
//!
//! Env: TPCH_DATA_DIR (dir holding lineitem.parquet), TRIALS (default 9),
//!      NDV (build-side distinct key count, default 13333).
#![allow(clippy::type_complexity)]

use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, UInt32Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;

// Match production / the bench: mimalloc, not the system allocator. Pull-model
// `take` allocates per output batch; measuring on system malloc would overstate it.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Decoded Q08 lineitem columns, held in memory so decode is excluded from timing.
struct LineitemCols {
    /// One entry per source batch; each is (partkey, orderkey, suppkey, extprice, discount).
    batches: Vec<(
        Vec<i64>,
        Arc<Int64Array>,
        Arc<Int64Array>,
        Arc<Float64Array>,
        Arc<Float64Array>,
    )>,
    total_rows: usize,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir =
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf10".to_string());
    let trials: usize = std::env::var("TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(9);
    let ndv: usize = std::env::var("NDV")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(13333);

    let path = format!("{data_dir}/lineitem.parquet");
    eprintln!("PV.0 push-vs-pull spike — lineitem={path}, trials={trials}, build_ndv={ndv}");

    // ---- decode lineitem ONCE (excluded from timing) ----
    let ctx = SessionContext::new();
    let prov = EmatixFastParquetTableProvider::try_new(&path)?;
    ctx.register_table("lineitem", Arc::new(prov))?;
    let df = ctx
        .sql("SELECT l_partkey, l_orderkey, l_suppkey, l_extendedprice, l_discount FROM lineitem")
        .await?;
    let raw = df.collect().await?;
    let cols = extract_cols(&raw)?;
    eprintln!(
        "decoded {} lineitem rows in {} batches",
        cols.total_rows,
        cols.batches.len()
    );

    // ---- build side: `ndv` distinct keys uniformly over the partkey domain ----
    // l_partkey ∈ [1, 2_000_000] at SF=10; stride keeps the keys uniformly scattered
    // and deterministic (no rng). hit-rate ≈ ndv / 2_000_000 ≈ 0.67% for ndv=13333.
    // Membership = a DENSE BITSET (Vec<bool>[domain+1]) → ~0.6 ns/probe (prior finding),
    // so the probe is near-free and the emit (take vs append) gets its FAIREST test:
    // if PUSH still ties PULL with an almost-free probe, the emit truly doesn't matter.
    let domain = 2_000_000i64;
    let stride = (domain / ndv as i64).max(1);
    let mut build = vec![false; (domain + 1) as usize];
    let mut nkeys = 0usize;
    for i in 0..ndv as i64 {
        let k = 1 + i * stride;
        if (k as usize) < build.len() && !build[k as usize] {
            build[k as usize] = true;
            nkeys += 1;
        }
    }
    eprintln!(
        "build bitset: {nkeys} keys, stride={stride} (dense Vec<bool>[{}])",
        build.len()
    );

    // ---- warmup (one of each, untimed) ----
    let (_w1, r0, c0) = pull_arm(&cols, &build);
    let (_w2, r1, c1) = push_arm(&cols, &build);
    assert_eq!(r0, r1, "PULL/PUSH survivor row count mismatch");
    assert!(
        (c0 - c1).abs() < 1.0,
        "PULL/PUSH checksum mismatch: {c0} vs {c1}"
    );
    eprintln!(
        "warmup ok — survivors={r0} ({:.3}%), checksum={c0:.1}",
        100.0 * r0 as f64 / cols.total_rows as f64
    );

    // ---- interleaved timed trials ----
    let mut pull_ms = Vec::new();
    let mut push_ms = Vec::new();
    for _ in 0..trials {
        let (d_pull, _, _) = pull_arm(&cols, &build);
        let (d_push, _, _) = push_arm(&cols, &build);
        pull_ms.push(d_pull);
        push_ms.push(d_push);
    }
    let med = |v: &mut Vec<f64>| {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    };
    let pull_med = med(&mut pull_ms);
    let push_med = med(&mut push_ms);
    println!("\n=== PV.0 RESULT (single-thread emit, decode excluded) ===");
    println!("PULL (index-array + arrow take):  {pull_med:.2} ms (median of {trials})");
    println!("PUSH (fused direct-append):       {push_med:.2} ms (median of {trials})");
    let speedup = pull_med / push_med;
    println!(
        "PUSH speedup over PULL:           {speedup:.2}x  ({:+.1}%)",
        (speedup - 1.0) * 100.0
    );
    println!("\nKILL-GATE READ: push engine attacks exactly this emit tax. If PUSH only");
    println!("ties PULL here (~1.0x), the materialization tax isn't the real cost and the");
    println!("engine won't move Q08 either. A clear win (>~1.3x) means it's worth building.");
    Ok(())
}

/// PULL arm — faithful to DataFusion's HashJoinExec emit: per source batch, probe
/// every row, collect surviving row indices into a UInt32 array, then
/// `arrow::compute::take`-gather the 4 payload columns from that batch.
fn pull_arm(cols: &LineitemCols, build: &[bool]) -> (f64, usize, f64) {
    let t = Instant::now();
    let mut rows = 0usize;
    let mut checksum = 0.0f64;
    for (partkey, ok, sk, ep, disc) in &cols.batches {
        // probe → survivor indices within this batch
        let mut idx: Vec<u32> = Vec::new();
        for (i, &pk) in partkey.iter().enumerate() {
            if (pk as usize) < build.len() && build[pk as usize] {
                idx.push(i as u32);
            }
        }
        if idx.is_empty() {
            continue;
        }
        let idx_arr = UInt32Array::from(idx);
        // materialize the 4 payload cols via take (the pull-model tax)
        let ok_out = take(
            ok.as_ref() as &dyn datafusion::arrow::array::Array,
            &idx_arr,
            None,
        )
        .unwrap();
        let _sk_out = take(
            sk.as_ref() as &dyn datafusion::arrow::array::Array,
            &idx_arr,
            None,
        )
        .unwrap();
        let ep_out = take(
            ep.as_ref() as &dyn datafusion::arrow::array::Array,
            &idx_arr,
            None,
        )
        .unwrap();
        let disc_out = take(
            disc.as_ref() as &dyn datafusion::arrow::array::Array,
            &idx_arr,
            None,
        )
        .unwrap();
        // consume (checksum = Σ extprice*(1-disc)) so the take isn't optimized away
        let ep_a = ep_out.as_any().downcast_ref::<Float64Array>().unwrap();
        let disc_a = disc_out.as_any().downcast_ref::<Float64Array>().unwrap();
        for i in 0..ep_a.len() {
            checksum += ep_a.value(i) * (1.0 - disc_a.value(i));
        }
        rows += ok_out.len();
    }
    (t.elapsed().as_secs_f64() * 1000.0, rows, checksum)
}

/// PUSH arm — single fused pass: probe inline, append survivors' payload values
/// directly into output builders. No per-row index array, no take-gather.
fn push_arm(cols: &LineitemCols, build: &[bool]) -> (f64, usize, f64) {
    let t = Instant::now();
    // pre-size to the expected survivor count to avoid growth churn
    let cap = (cols.total_rows / 128).max(1024);
    let mut out_ok: Vec<i64> = Vec::with_capacity(cap);
    let mut out_sk: Vec<i64> = Vec::with_capacity(cap);
    let mut out_ep: Vec<f64> = Vec::with_capacity(cap);
    let mut out_disc: Vec<f64> = Vec::with_capacity(cap);
    for (partkey, ok, sk, ep, disc) in &cols.batches {
        for (i, &pk) in partkey.iter().enumerate() {
            if (pk as usize) < build.len() && build[pk as usize] {
                out_ok.push(ok.value(i));
                out_sk.push(sk.value(i));
                out_ep.push(ep.value(i));
                out_disc.push(disc.value(i));
            }
        }
    }
    // build the output arrays from the dense survivor buffers
    let _ok_arr: ArrayRef = Arc::new(Int64Array::from(out_ok));
    let _sk_arr: ArrayRef = Arc::new(Int64Array::from(out_sk));
    let ep_arr = Float64Array::from(out_ep);
    let disc_arr = Float64Array::from(out_disc);
    let mut checksum = 0.0f64;
    for i in 0..ep_arr.len() {
        checksum += ep_arr.value(i) * (1.0 - disc_arr.value(i));
    }
    (t.elapsed().as_secs_f64() * 1000.0, ep_arr.len(), checksum)
}

/// Pull the 5 typed columns out of the decoded batches into owned/Arc form.
fn extract_cols(batches: &[RecordBatch]) -> Result<LineitemCols, Box<dyn std::error::Error>> {
    let mut out = Vec::new();
    let mut total = 0usize;
    for b in batches {
        let pk = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("l_partkey not Int64 (downcast off expected)")?;
        let ok = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("l_orderkey not Int64")?;
        let sk = b
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("l_suppkey not Int64")?;
        let ep = b
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or("l_extendedprice not Float64")?;
        let disc = b
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or("l_discount not Float64")?;
        let partkey_vec: Vec<i64> = pk.values().to_vec();
        total += partkey_vec.len();
        out.push((
            partkey_vec,
            Arc::new(ok.clone()),
            Arc::new(sk.clone()),
            Arc::new(ep.clone()),
            Arc::new(disc.clone()),
        ));
    }
    Ok(LineitemCols {
        batches: out,
        total_rows: total,
    })
}
