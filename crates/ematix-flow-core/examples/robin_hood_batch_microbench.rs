//! Σ.N.f.1 microbench — does the batch API + pre-grow beat row-by-row?
//!
//! Three configurations on 6M-row COUNT(*) GROUP BY:
//!   RH row-API (default-cap)     : current baseline
//!   RH row-API + pre-grow        : isolates the grow() cost
//!   RH batch-API                  : pre-grow + dispatch through row
//!   hashbrown row-API (default)   : current comparison target
//!   hashbrown row-API + reserve  : isolates hashbrown's grow() cost
//!
//! Three cardinalities (7 / 100 / 10K keys) match the actual SQL
//! workloads (l_shipmode / o_orderpriority / l_suppkey).
//!
//! Goal: identify which optimisation (pre-grow vs batch dispatch
//! vs both) buys the most throughput. If pre-grow is the bulk of
//! the win, that's a 5-line change to the row path. If batch
//! dispatch buys more, we need to push further.
//!
//! Run:
//!   cargo run --release -p ematix-flow-core --example robin_hood_batch_microbench

use std::collections::HashMap;
use std::time::Instant;

use ematix_flow_core::robin_hood_agg::{RobinHoodI64U64, count_accumulator};

const N_ROWS: usize = 6_000_000;
const CARDINALITIES: &[i64] = &[7, 100, 10_000];
const REPS: usize = 5;

fn gen_keys(card: i64) -> Vec<i64> {
    let mut out = Vec::with_capacity(N_ROWS);
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    for i in 0..N_ROWS {
        x = (x ^ (i as u64)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        x = (x ^ (x >> 30)).wrapping_mul(0x94d0_49bb_1331_11eb);
        out.push((x as i64).rem_euclid(card));
    }
    out
}

fn bench_rh_row(keys: &[i64]) -> f64 {
    let t = Instant::now();
    let mut t_map = RobinHoodI64U64::new();
    for &k in keys {
        t_map.insert_or_update(k, 1, count_accumulator);
    }
    std::hint::black_box(&t_map);
    t.elapsed().as_secs_f64() * 1000.0
}

fn bench_rh_row_pregrown(keys: &[i64], cap: usize) -> f64 {
    let t = Instant::now();
    let mut t_map = RobinHoodI64U64::with_capacity(cap);
    for &k in keys {
        t_map.insert_or_update(k, 1, count_accumulator);
    }
    std::hint::black_box(&t_map);
    t.elapsed().as_secs_f64() * 1000.0
}

fn bench_rh_batch(keys: &[i64]) -> f64 {
    let t = Instant::now();
    let mut t_map = RobinHoodI64U64::new();
    t_map.insert_or_update_batch_count(keys);
    std::hint::black_box(&t_map);
    t.elapsed().as_secs_f64() * 1000.0
}

fn bench_hb_row(keys: &[i64]) -> f64 {
    let t = Instant::now();
    let mut m: HashMap<i64, u64> = HashMap::new();
    for &k in keys {
        *m.entry(k).or_insert(0) += 1;
    }
    std::hint::black_box(&m);
    t.elapsed().as_secs_f64() * 1000.0
}

fn bench_hb_row_reserve(keys: &[i64], cap: usize) -> f64 {
    let t = Instant::now();
    let mut m: HashMap<i64, u64> = HashMap::with_capacity(cap);
    for &k in keys {
        *m.entry(k).or_insert(0) += 1;
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

fn main() {
    println!("=== Σ.N.f.1 batch-API microbench — {N_ROWS} rows, {REPS}-rep median ===\n");
    println!(
        "{:<6} {:<18} {:>10} {:>14}",
        "Card", "Variant", "ms", "M rows/sec"
    );
    println!("{}", "-".repeat(56));

    for &card in CARDINALITIES {
        let keys = gen_keys(card);
        let cap = (card as usize * 2).max(64);

        let _ = bench_rh_row(&keys);
        let _ = bench_rh_batch(&keys);
        let _ = bench_hb_row(&keys);

        let mut rh_row: Vec<f64> = (0..REPS).map(|_| bench_rh_row(&keys)).collect();
        let mut rh_pre: Vec<f64> = (0..REPS)
            .map(|_| bench_rh_row_pregrown(&keys, cap))
            .collect();
        let mut rh_bat: Vec<f64> = (0..REPS).map(|_| bench_rh_batch(&keys)).collect();
        let mut hb_row: Vec<f64> = (0..REPS).map(|_| bench_hb_row(&keys)).collect();
        let mut hb_res: Vec<f64> = (0..REPS)
            .map(|_| bench_hb_row_reserve(&keys, cap))
            .collect();

        let rh_row_med = median(&mut rh_row);
        let rh_pre_med = median(&mut rh_pre);
        let rh_bat_med = median(&mut rh_bat);
        let hb_row_med = median(&mut hb_row);
        let hb_res_med = median(&mut hb_res);

        println!(
            "{:<6} {:<18} {:>10.2} {:>14.1}",
            card,
            "RH row",
            rh_row_med,
            throughput_mrows_per_sec(rh_row_med, N_ROWS)
        );
        println!(
            "{:<6} {:<18} {:>10.2} {:>14.1}",
            "",
            "RH row + pre-grow",
            rh_pre_med,
            throughput_mrows_per_sec(rh_pre_med, N_ROWS)
        );
        println!(
            "{:<6} {:<18} {:>10.2} {:>14.1}",
            "",
            "RH batch",
            rh_bat_med,
            throughput_mrows_per_sec(rh_bat_med, N_ROWS)
        );
        println!(
            "{:<6} {:<18} {:>10.2} {:>14.1}",
            "",
            "hashbrown row",
            hb_row_med,
            throughput_mrows_per_sec(hb_row_med, N_ROWS)
        );
        println!(
            "{:<6} {:<18} {:>10.2} {:>14.1}",
            "",
            "hashbrown +reserve",
            hb_res_med,
            throughput_mrows_per_sec(hb_res_med, N_ROWS)
        );
        println!();
    }

    println!("Target: ≥60M rows/sec single-thread (0.5× of DataFusion's");
    println!("vectorised hash agg's ~120M rows/sec/core measured in Σ.N.f profile).");
}
