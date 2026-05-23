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

use ematix_flow_core::robin_hood_agg::{RobinHoodI64F64, RobinHoodSumF64RadixAgg, TaggedI64F64};

const N_ROWS: usize = 6_000_000;
const CARDINALITIES: &[i64] = &[200_000, 2_000_000, 15_000_000];
const REPS: usize = 5;

/// Σ.Q.L12 — Q17 SF=10 shape: 60M rows / 2M groups = 30 rows/group.
const Q17_N_ROWS: usize = 60_000_000;
const Q17_CARD: i64 = 2_000_000;
const Q17_REPS: usize = 3;

fn gen_keys_and_values(card: i64) -> (Vec<i64>, Vec<f64>) {
    gen_keys_and_values_n(card, N_ROWS)
}

fn gen_keys_and_values_n(card: i64, n_rows: usize) -> (Vec<i64>, Vec<f64>) {
    let mut keys = Vec::with_capacity(n_rows);
    let mut vals = Vec::with_capacity(n_rows);
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    for i in 0..n_rows {
        x = (x ^ (i as u64)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        x = (x ^ (x >> 30)).wrapping_mul(0x94d0_49bb_1331_11eb);
        keys.push((x as i64).rem_euclid(card));
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

/// Σ.R.1 — radix-partitioned ingest. `per_table_cap` is sized so the
/// total capacity across `2^radix_bits` tables matches the single-table
/// `cap` (apples-to-apples memory footprint).
fn bench_rh_radix(keys: &[i64], vals: &[f64], cap: usize, radix_bits: u8) -> f64 {
    let n_tables = 1usize << radix_bits;
    let per_table_cap = (cap / n_tables).max(64);
    let t = Instant::now();
    let mut t_map = RobinHoodSumF64RadixAgg::new(radix_bits, per_table_cap);
    t_map.ingest_batch_radix(keys, vals);
    std::hint::black_box(&t_map);
    t.elapsed().as_secs_f64() * 1000.0
}

/// Σ.Q.L12 — SwissTable-style tagged kernel, default capacity.
fn bench_tagged(keys: &[i64], vals: &[f64]) -> f64 {
    let t = Instant::now();
    let mut t_map = TaggedI64F64::new();
    t_map.insert_or_sum_batch_vectorised(keys, vals);
    std::hint::black_box(&t_map);
    t.elapsed().as_secs_f64() * 1000.0
}

/// Σ.Q.L12 — SwissTable-style tagged kernel, pre-grown.
fn bench_tagged_pregrown(keys: &[i64], vals: &[f64], cap: usize) -> f64 {
    let t = Instant::now();
    let mut t_map = TaggedI64F64::with_capacity(cap);
    t_map.insert_or_sum_batch_vectorised(keys, vals);
    std::hint::black_box(&t_map);
    t.elapsed().as_secs_f64() * 1000.0
}

/// Σ.Q.L12 — scalar tagged path (for isolating SIMD vs vectorised pipeline).
fn bench_tagged_scalar(keys: &[i64], vals: &[f64], cap: usize) -> f64 {
    let t = Instant::now();
    let mut t_map = TaggedI64F64::with_capacity(cap);
    t_map.insert_or_sum_batch(keys, vals);
    std::hint::black_box(&t_map);
    t.elapsed().as_secs_f64() * 1000.0
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

        // Σ.R.1 — sweep radix_bits across the L1/L2-fitting range.
        let radix_results: Vec<(u8, f64)> = [4u8, 5, 6, 7, 8]
            .iter()
            .map(|&rb| {
                let mut xs: Vec<f64> = (0..REPS)
                    .map(|_| bench_rh_radix(&keys, &vals, cap, rb))
                    .collect();
                (rb, median(&mut xs))
            })
            .collect();

        let card_label = if card >= 1_000_000 {
            format!("{}M", card / 1_000_000)
        } else if card >= 1_000 {
            format!("{}K", card / 1_000)
        } else {
            format!("{card}")
        };

        // Σ.Q.L12 — tagged kernel reps.
        let mut tagged_vec: Vec<f64> = (0..REPS).map(|_| bench_tagged(&keys, &vals)).collect();
        let mut tagged_vec_pre: Vec<f64> = (0..REPS)
            .map(|_| bench_tagged_pregrown(&keys, &vals, cap))
            .collect();
        let mut tagged_scalar_pre: Vec<f64> = (0..REPS)
            .map(|_| bench_tagged_scalar(&keys, &vals, cap))
            .collect();

        print_row(&card_label, "RH scalar", Some(median(&mut rh_scalar)));
        print_row("", "RH scalar + pre-grow", Some(median(&mut rh_scalar_pre)));
        print_row("", "RH vectorised", rh_vec_med);
        print_row("", "RH vec + pre-grow", rh_vec_pre_med);
        print_row("", "Tagged vec", Some(median(&mut tagged_vec)));
        print_row("", "Tagged vec + pre-grow", Some(median(&mut tagged_vec_pre)));
        print_row("", "Tagged scalar + pregrow", Some(median(&mut tagged_scalar_pre)));
        for (rb, ms) in &radix_results {
            print_row("", &format!("RH radix rb={rb}"), Some(*ms));
        }
        print_row("", "hashbrown row", Some(median(&mut hb_row)));
        print_row("", "hashbrown + reserve", Some(median(&mut hb_res)));
        println!();
    }

    println!("Target for the vectorised path: ≥60M rows/sec at 15M cardinality");
    println!("(≈ matches the COUNT win shape from Σ.N.f.3; closes the +5.7% Q18 SF=10 gap).");

    // -------------------------------------------------------------
    // Σ.Q.L12 — Q17 SF=10 shape: 60M rows / 2M groups (30 rows/grp).
    // -------------------------------------------------------------
    println!();
    println!("=== Σ.Q.L12 — Q17 SF=10 shape: {Q17_N_ROWS} rows / {Q17_CARD} groups, {Q17_REPS}-rep median ===\n");
    println!(
        "{:<10} {:<22} {:>10} {:>14}",
        "Card", "Variant", "ms", "M rows/sec"
    );
    println!("{}", "-".repeat(60));
    let (keys, vals) = gen_keys_and_values_n(Q17_CARD, Q17_N_ROWS);
    let cap = (Q17_CARD as usize * 2).max(64);

    // Warm-up.
    let _ = bench_rh_vectorised_pregrown(&keys, &vals, cap);
    let _ = bench_tagged_pregrown(&keys, &vals, cap);

    let mut rh_vec_q17: Vec<f64> = (0..Q17_REPS)
        .filter_map(|_| bench_rh_vectorised_pregrown(&keys, &vals, cap))
        .collect();
    let mut tagged_vec_q17: Vec<f64> = (0..Q17_REPS)
        .map(|_| bench_tagged_pregrown(&keys, &vals, cap))
        .collect();

    let rh_med = median(&mut rh_vec_q17);
    let tagged_med = median(&mut tagged_vec_q17);
    let delta_pct = (rh_med - tagged_med) / rh_med * 100.0;

    fn fmt(ms: f64, n: usize) -> (f64, f64) {
        (ms, (n as f64 / 1e6) / (ms / 1000.0))
    }
    let (r_ms, r_tput) = fmt(rh_med, Q17_N_ROWS);
    let (t_ms, t_tput) = fmt(tagged_med, Q17_N_ROWS);
    println!("{:<10} {:<22} {:>10.2} {:>14.1}", "Q17/2M", "RH vec + pre-grow", r_ms, r_tput);
    println!("{:<10} {:<22} {:>10.2} {:>14.1}", "", "Tagged vec + pre-grow", t_ms, t_tput);
    println!(
        "\nΔ (Tagged vs RH): {:+.1}% — {}",
        delta_pct,
        if delta_pct >= 20.0 {
            "✓ exceeds 20% gate, proceed to Q17 wall-time"
        } else if delta_pct >= 5.0 {
            "~ borderline; Q17 wall-time may still translate"
        } else if delta_pct >= 0.0 {
            "~ marginal kernel win; unlikely to translate"
        } else {
            "✗ kernel regression at Q17 shape — NEG"
        }
    );
}
