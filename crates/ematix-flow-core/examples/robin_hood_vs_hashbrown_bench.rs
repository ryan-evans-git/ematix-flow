//! Σ.N microbench — RobinHoodI64U64 vs hashbrown `HashMap<i64, u64>`.
//!
//! Single thread, COUNT(*) GROUP BY i64. Stresses the agg hot loop:
//! 6M insert-or-update calls against varying key cardinality (7, 100,
//! 10K — the TPC-H spectrum from l_shipmode through o_orderpriority
//! through l_partkey).
//!
//! Run:
//!   cargo run --release -p ematix-flow-core --example robin_hood_vs_hashbrown_bench

use std::time::Instant;

use datafusion::arrow::buffer::Buffer;
use ematix_flow_core::robin_hood_agg::{RobinHoodI64U64, count_accumulator};

const N_ROWS: usize = 6_000_000;
const CARDINALITIES: &[i64] = &[7, 100, 10_000];
const REPS: usize = 5;

fn bench_robin_hood(keys: &[i64]) -> f64 {
    let t = Instant::now();
    let mut t_map = RobinHoodI64U64::with_capacity((keys.len() / 2).max(64));
    for &k in keys {
        t_map.insert_or_update(k, 1, count_accumulator);
    }
    std::hint::black_box(&t_map);
    t.elapsed().as_secs_f64() * 1000.0
}

fn bench_hashbrown(keys: &[i64]) -> f64 {
    use std::collections::HashMap;
    let t = Instant::now();
    let mut t_map: HashMap<i64, u64> = HashMap::with_capacity(keys.len() / 2);
    for &k in keys {
        *t_map.entry(k).or_insert(0) += 1;
    }
    std::hint::black_box(&t_map);
    t.elapsed().as_secs_f64() * 1000.0
}

fn gen_keys(card: i64) -> Vec<i64> {
    // Deterministic — use a splitmix-style generator on (i, seed) so
    // adjacent indices don't map to adjacent slots (avoids accidental
    // best-case for either impl).
    let mut out = Vec::with_capacity(N_ROWS);
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    for i in 0..N_ROWS {
        x = (x ^ (i as u64)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        x = (x ^ (x >> 30)).wrapping_mul(0x94d0_49bb_1331_11eb);
        let k = (x as i64).rem_euclid(card);
        out.push(k);
    }
    let _ = Buffer::from(vec![0u8; 8]); // touch arrow_buffer so dep is "used"
    out
}

fn median(xs: &mut [f64]) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    xs[xs.len() / 2]
}

fn main() {
    println!("=== Σ.N: RobinHoodI64U64 vs hashbrown (HashMap<i64, u64>) ===");
    println!("{N_ROWS} rows, COUNT(*) GROUP BY i64, {REPS}-rep median\n");
    println!(
        "{:<13} {:>12} {:>12} {:>12}",
        "Cardinality", "RobinHood", "hashbrown", "RH speedup"
    );
    println!("{}", "-".repeat(54));

    for &card in CARDINALITIES {
        let keys = gen_keys(card);

        // Warm-ups
        let _ = bench_robin_hood(&keys);
        let _ = bench_hashbrown(&keys);

        let mut rh: Vec<f64> = (0..REPS).map(|_| bench_robin_hood(&keys)).collect();
        let mut hb: Vec<f64> = (0..REPS).map(|_| bench_hashbrown(&keys)).collect();
        let rh_med = median(&mut rh);
        let hb_med = median(&mut hb);
        let speedup = hb_med / rh_med;
        println!(
            "{:<13} {:>10.2}ms {:>10.2}ms {:>11.2}x",
            card, rh_med, hb_med, speedup
        );
    }
}
