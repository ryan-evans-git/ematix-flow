//! Σ.Q.L1b retry — does a vectorised batch path beat scalar
//! `insert_or_sum` for `SUM(f64) GROUP BY i64`?
//!
//! Variants on 6M-row SUM(f64) GROUP BY i64:
//!
//!   RH scalar (default-cap)      : current baseline, lets it grow
//!   RH scalar + pre-grow         : isolates the grow() cost
//!   RH vectorised (default-cap)  : new 4-stage pipeline, default cap
//!   RH vectorised + pre-grow     : new pipeline, pre-sized
//!   hashbrown row (default-cap)  : sanity comparison
//!   hashbrown + reserve          : pre-sized comparison
//!
//! Three cardinalities track Q18's shape across scale factors:
//!   200K   → Q18 SF=1 (~1.5M orders, sample 200K)
//!   2M     → Q18 SF=2-3
//!   15M    → Q18 SF=10 worst case
//!
//! Run:
//!   cargo run --release -p ematix-flow-core \
//!     --example robin_hood_sum_f64_batch_microbench

use std::collections::HashMap;
use std::time::Instant;

use ematix_flow_core::robin_hood_agg::RobinHoodI64F64;

const N_ROWS: usize = 6_000_000;
const CARDINALITIES: &[i64] = &[200_000, 2_000_000, 15_000_000];
const REPS: usize = 5;

fn gen_keys_and_values(card: i64) -> (Vec<i64>, Vec<f64>) {
    let mut keys = Vec::with_capacity(N_ROWS);
    let mut vals = Vec::with_capacity(N_ROWS);
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    for i in 0..N_ROWS {
        x = (x ^ (i as u64)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        x = (x ^ (x >> 30)).wrapping_mul(0x94d0_49bb_1331_11eb);
        keys.push((x as i64).rem_euclid(card));
        // Use a deterministic Float64 derived from the same stream.
        vals.push(((x >> 11) & 0xfffff) as f64 / 1024.0);
    }
    (keys, vals)
}

fn bench_rh_scalar(keys: &[i64], vals: &[f64]) -> f64 {
    let t = Instant::now();
    let mut t_map = RobinHoodI64F64::new();
    t_map.insert_or_sum_batch(keys, vals);
    std::hint::black_box(&t_map);
    t.elapsed().as_secs_f64() * 1000.0
}

fn bench_rh_scalar_pregrown(keys: &[i64], vals: &[f64], cap: usize) -> f64 {
    let t = Instant::now();
    let mut t_map = RobinHoodI64F64::with_capacity(cap);
    t_map.insert_or_sum_batch(keys, vals);
    std::hint::black_box(&t_map);
    t.elapsed().as_secs_f64() * 1000.0
}

fn bench_rh_vectorised(keys: &[i64], vals: &[f64]) -> Option<f64> {
    let t = Instant::now();
    let mut t_map = RobinHoodI64F64::new();
    t_map.insert_or_sum_batch_vectorised(keys, vals);
    std::hint::black_box(&t_map);
    Some(t.elapsed().as_secs_f64() * 1000.0)
}

fn bench_rh_vectorised_pregrown(keys: &[i64], vals: &[f64], cap: usize) -> Option<f64> {
    let t = Instant::now();
    let mut t_map = RobinHoodI64F64::with_capacity(cap);
    t_map.insert_or_sum_batch_vectorised(keys, vals);
    std::hint::black_box(&t_map);
    Some(t.elapsed().as_secs_f64() * 1000.0)
}

fn bench_hb_row(keys: &[i64], vals: &[f64]) -> f64 {
    let t = Instant::now();
    let mut m: HashMap<i64, f64> = HashMap::new();
    for i in 0..keys.len() {
        *m.entry(keys[i]).or_insert(0.0) += vals[i];
    }
    std::hint::black_box(&m);
    t.elapsed().as_secs_f64() * 1000.0
}

fn bench_hb_row_reserve(keys: &[i64], vals: &[f64], cap: usize) -> f64 {
    let t = Instant::now();
    let mut m: HashMap<i64, f64> = HashMap::with_capacity(cap);
    for i in 0..keys.len() {
        *m.entry(keys[i]).or_insert(0.0) += vals[i];
    }
    std::hint::black_box(&m);
    t.elapsed().as_secs_f64() * 1000.0
}

fn median(xs: &mut [f64]) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    xs[xs.len() / 2]
}

fn throughput_mrows_per_sec(ms: f64, n: usize) -> f64 {
    (n as f64 / 1e6) / (ms / 1000.0)
}

fn print_row(card_label: &str, variant: &str, ms_opt: Option<f64>) {
    match ms_opt {
        Some(ms) => println!(
            "{:<10} {:<22} {:>10.2} {:>14.1}",
            card_label,
            variant,
            ms,
            throughput_mrows_per_sec(ms, N_ROWS)
        ),
        None => println!(
            "{:<10} {:<22} {:>10} {:>14}",
            card_label, variant, "n/a", "(not built)"
        ),
    }
}

fn main() {
    println!(
        "=== Σ.Q.L1b retry — SUM(f64) GROUP BY i64 microbench, {N_ROWS} rows, {REPS}-rep median ===\n"
    );
    println!(
        "{:<10} {:<22} {:>10} {:>14}",
        "Card", "Variant", "ms", "M rows/sec"
    );
    println!("{}", "-".repeat(60));

    for &card in CARDINALITIES {
        let (keys, vals) = gen_keys_and_values(card);
        let cap = (card as usize * 2).max(64);

        // Warm-up.
        let _ = bench_rh_scalar(&keys, &vals);
        let _ = bench_hb_row(&keys, &vals);

        let mut rh_scalar: Vec<f64> = (0..REPS).map(|_| bench_rh_scalar(&keys, &vals)).collect();
        let mut rh_scalar_pre: Vec<f64> = (0..REPS)
            .map(|_| bench_rh_scalar_pregrown(&keys, &vals, cap))
            .collect();
        let mut hb_row: Vec<f64> = (0..REPS).map(|_| bench_hb_row(&keys, &vals)).collect();
        let mut hb_res: Vec<f64> = (0..REPS)
            .map(|_| bench_hb_row_reserve(&keys, &vals, cap))
            .collect();

        let rh_vec_med: Option<f64> = {
            let xs: Vec<f64> = (0..REPS)
                .filter_map(|_| bench_rh_vectorised(&keys, &vals))
                .collect();
            if xs.is_empty() {
                None
            } else {
                let mut xs = xs;
                Some(median(&mut xs))
            }
        };
        let rh_vec_pre_med: Option<f64> = {
            let xs: Vec<f64> = (0..REPS)
                .filter_map(|_| bench_rh_vectorised_pregrown(&keys, &vals, cap))
                .collect();
            if xs.is_empty() {
                None
            } else {
                let mut xs = xs;
                Some(median(&mut xs))
            }
        };

        let card_label = if card >= 1_000_000 {
            format!("{}M", card / 1_000_000)
        } else if card >= 1_000 {
            format!("{}K", card / 1_000)
        } else {
            format!("{card}")
        };

        print_row(&card_label, "RH scalar", Some(median(&mut rh_scalar)));
        print_row("", "RH scalar + pre-grow", Some(median(&mut rh_scalar_pre)));
        print_row("", "RH vectorised", rh_vec_med);
        print_row("", "RH vec + pre-grow", rh_vec_pre_med);
        print_row("", "hashbrown row", Some(median(&mut hb_row)));
        print_row("", "hashbrown + reserve", Some(median(&mut hb_res)));
        println!();
    }

    println!("Target for the vectorised path: ≥60M rows/sec at 15M cardinality");
    println!("(≈ matches the COUNT win shape from Σ.N.f.3; closes the +5.7% Q18 SF=10 gap).");
}
