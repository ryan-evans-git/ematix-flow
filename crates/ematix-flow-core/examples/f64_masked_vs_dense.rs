//! PV.M.2 de-risk — masked-sparse vs dense+masked-sum f64 decode, on the
//! CURRENT published ematix-parquet kernels + real lineitem. Re-confirms
//! (fresh, not the stale Σ.Q06.SF10.7 "~12×" header note) that routing f64
//! AGG/payload columns to dense decode beats the per-survivor sparse push-loop
//! (`plain_sparse_decode_f64_into`) at Q15's ~3.6% shipdate selectivity.
//!
//!   (A) masked  : masked_decode_f64 (sparse push-loop) → sum survivors
//!   (B) dense   : read_column_f64 (dense full decode)  → masked-sum (no materialize)
//!
//! Both must produce the same sum. If (B) ≪ (A), the dense-route lever is real
//! and PV.M.2 wires it into the f64 payload decode path.
//!
//! Usage:
//!   cargo run --release -p ematix-flow-core --example f64_masked_vs_dense -- \
//!     examples/tpch/data/sf10/lineitem.parquet
#![allow(clippy::needless_range_loop)]

use std::time::Instant;

use ematix_flow_core::ematix_parquet_bridge::{masked_decode_f64, open_cached};
use ematix_parquet_codec::read::read_column_f64;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Deterministic ~`num/den` bitmap of `n` rows, scattered (xorshift) like
/// Q15's shipdate-random selectivity — no 8-row block clustering.
fn build_mask(n: usize, num: u64, den: u64) -> (Vec<u8>, usize) {
    let mut bitmap = vec![0u8; n.div_ceil(8)];
    let mut state: u64 = 0x9e3779b97f4a7c15;
    let mut set = 0usize;
    for row in 0..n {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        if state % den < num {
            bitmap[row >> 3] |= 1 << (row & 7);
            set += 1;
        }
    }
    (bitmap, set)
}

#[inline]
fn masked_sum(all: &[f64], mask: &[u8]) -> f64 {
    let mut s = 0.0f64;
    for (row, v) in all.iter().enumerate() {
        if (mask[row >> 3] >> (row & 7)) & 1 == 1 {
            s += *v;
        }
    }
    s
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "examples/tpch/data/sf10/lineitem.parquet".to_string());
    let reps: usize = std::env::var("REPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let f64_col: usize = std::env::var("COL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5); // l_extendedprice
    // Q15 shipdate selectivity ≈ 3.6%.
    let (num, den) = (36u64, 1000u64);

    let file = open_cached(std::path::Path::new(&path))?;
    let md = file.cached_metadata().map_err(|e| format!("meta: {e}"))?;
    let n_rg = md.row_groups.len();

    // Pre-build per-RG masks (mask-build out of the timed region).
    let mut masks: Vec<(Vec<u8>, usize)> = Vec::with_capacity(n_rg);
    let (mut total_rows, mut total_set) = (0usize, 0usize);
    for rg in 0..n_rg {
        let nv = md.row_groups[rg].columns[f64_col]
            .meta_data
            .as_ref()
            .unwrap()
            .num_values as usize;
        let m = build_mask(nv, num, den);
        total_rows += nv;
        total_set += m.1;
        masks.push(m);
    }
    println!(
        "PV.M.2 de-risk — {path}\n  col={f64_col} row_groups={n_rg} reps={reps}\n  rows={total_rows} selected={total_set} ({:.2}%)\n",
        100.0 * total_set as f64 / total_rows as f64
    );

    // Warmup (prime OS cache + any lazy state).
    for rg in 0..n_rg {
        let _ = masked_decode_f64(&file, rg, f64_col, &masks[rg].0)?;
        let _ = read_column_f64(&file, rg, f64_col).map_err(|e| format!("dense: {e}"))?;
    }

    let (mut a_ms, mut b_ms) = (Vec::new(), Vec::new());
    let (mut sum_a, mut sum_b) = (0.0f64, 0.0f64);
    for _ in 0..reps {
        // (A) masked sparse → sum survivors.
        let t = Instant::now();
        let mut s = 0.0f64;
        for rg in 0..n_rg {
            let out = masked_decode_f64(&file, rg, f64_col, &masks[rg].0)?;
            s += out.iter().sum::<f64>();
        }
        a_ms.push(t.elapsed().as_secs_f64() * 1e3);
        sum_a = s;

        // (B) dense decode → masked-sum (no survivor materialization).
        let t = Instant::now();
        let mut s = 0.0f64;
        for rg in 0..n_rg {
            let all = read_column_f64(&file, rg, f64_col).map_err(|e| format!("dense: {e}"))?;
            s += masked_sum(&all, &masks[rg].0);
        }
        b_ms.push(t.elapsed().as_secs_f64() * 1e3);
        sum_b = s;
    }

    let (a, b) = (median(a_ms), median(b_ms));
    let ok = (sum_a - sum_b).abs() / sum_a.abs().max(1.0) <= 1e-9;
    println!("(A) masked-sparse  median {a:>7.1} ms   sum={sum_a:.2}");
    println!("(B) dense+masksum  median {b:>7.1} ms   sum={sum_b:.2}");
    println!(
        "speedup A/B = {:.2}×   correctness: {}",
        a / b,
        if ok {
            "OK (sums match)"
        } else {
            "*** SUM MISMATCH ***"
        }
    );
    let verdict = if !ok {
        "BLOCKED — sums differ"
    } else if a / b >= 1.3 {
        "GO — dense beats masked-sparse on current kernels → wire PV.M.2 (route f64 payload to dense)"
    } else if a / b <= 0.95 {
        "NO-GO — masked-sparse is faster now; the ~12× header note is STALE (kernels improved). Redirect."
    } else {
        "MARGINAL — ~parity; dense win not worth the routing complexity"
    };
    println!("VERDICT: {verdict}");
    Ok(())
}
