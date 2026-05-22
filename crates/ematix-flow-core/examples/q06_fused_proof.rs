//! Σ.E7 proof: Q06 SF=10 implemented as a single tight Rust function,
//! no DataFusion, no Arrow. Direct ematix-parquet kernel calls.
//!
//! If this BEATS Polars (62 ms), then the architectural rewrite to a
//! fused scan-filter-agg operator is justified — we have proof the
//! lever wins at SF=10 Q06.
//!
//! If it MATCHES Polars or is slower, the per-byte decode is the wall;
//! no operator-level rewrite would help.
//!
//! Q06 SQL:
//!   SELECT SUM(l_extendedprice * l_discount) AS revenue
//!   FROM lineitem
//!   WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
//!     AND l_discount BETWEEN 0.05 AND 0.07
//!     AND l_quantity < 24
//!
//! Run:
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 \
//!     cargo run --release -p ematix-flow-core --example q06_fused_proof

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Instant;

use ematix_flow_core::ematix_parquet_bridge::{
    filter_f64_column_to_bitmap, filter_f64_column_to_bitmap_dense, filter_i32_column_to_bitmap,
};
use ematix_parquet_codec::read::read_column_f64_masked_into;
use ematix_parquet_io::ParquetFile;

// TPC-H lineitem column indices (0-based, from the file schema):
//   0: l_orderkey
//   1: l_partkey
//   2: l_suppkey
//   3: l_linenumber
//   4: l_quantity     (F64)
//   5: l_extendedprice (F64)
//   6: l_discount     (F64)
//   7: l_tax
//   8: l_returnflag
//   9: l_linestatus
//  10: l_shipdate     (Date32 = i32 days-since-epoch)
const COL_QUANTITY: usize = 4;
const COL_EXTPRICE: usize = 5;
const COL_DISCOUNT: usize = 6;
const COL_SHIPDATE: usize = 10;

// Q06 constants.
const SHIPDATE_LOW: i32 = 8766; // 1994-01-01
const SHIPDATE_HIGH: i32 = 9131; // 1995-01-01
const QUANTITY_MAX: f64 = 24.0;
const DISCOUNT_LOW: f64 = 0.05;
const DISCOUNT_HIGH: f64 = 0.07;

/// AND `rhs` into `lhs` in place. Lengths must match.
#[inline]
fn and_bitmap_into(lhs: &mut [u8], rhs: &[u8]) {
    assert_eq!(lhs.len(), rhs.len());
    for i in 0..lhs.len() {
        lhs[i] &= rhs[i];
    }
}

#[inline]
fn popcount(bitmap: &[u8]) -> usize {
    bitmap.iter().map(|b| b.count_ones() as usize).sum()
}

// Per-phase totals across all RGs (microseconds). Atomic for thread safety.
static T_SHIPDATE: AtomicU64 = AtomicU64::new(0);
static T_DISCOUNT: AtomicU64 = AtomicU64::new(0);
static T_QUANTITY: AtomicU64 = AtomicU64::new(0);
static T_AND: AtomicU64 = AtomicU64::new(0);
static T_PROJ_PRICE: AtomicU64 = AtomicU64::new(0);
static T_PROJ_DISC: AtomicU64 = AtomicU64::new(0);
static T_AGG: AtomicU64 = AtomicU64::new(0);

/// Process one row group: returns partial sum.
fn process_rg(path: &std::path::Path, rg: usize) -> f64 {
    let file = ParquetFile::open(path).expect("open");

    // 1. Build shipdate bitmap (i32 dict-fused, NEON kernel).
    let t = Instant::now();
    let (mut bitmap, _total) = filter_i32_column_to_bitmap(path, rg, COL_SHIPDATE, |v: i32| {
        (SHIPDATE_LOW..SHIPDATE_HIGH).contains(&v)
    })
    .expect("shipdate filter");
    T_SHIPDATE.fetch_add(t.elapsed().as_micros() as u64, Ordering::Relaxed);

    // 2. Discount bitmap.
    let t = Instant::now();
    let (dc_bitmap, _) = match filter_f64_column_to_bitmap(path, rg, COL_DISCOUNT, |v: f64| {
        (DISCOUNT_LOW..=DISCOUNT_HIGH).contains(&v)
    }) {
        Ok(r) => r,
        Err(_) => filter_f64_column_to_bitmap_dense(path, rg, COL_DISCOUNT, |v: f64| {
            (DISCOUNT_LOW..=DISCOUNT_HIGH).contains(&v)
        })
        .expect("discount filter"),
    };
    T_DISCOUNT.fetch_add(t.elapsed().as_micros() as u64, Ordering::Relaxed);

    // 3. Quantity bitmap.
    let t = Instant::now();
    let (qy_bitmap, _) =
        match filter_f64_column_to_bitmap(path, rg, COL_QUANTITY, |v: f64| v < QUANTITY_MAX) {
            Ok(r) => r,
            Err(_) => {
                filter_f64_column_to_bitmap_dense(path, rg, COL_QUANTITY, |v: f64| v < QUANTITY_MAX)
                    .expect("quantity filter")
            }
        };
    T_QUANTITY.fetch_add(t.elapsed().as_micros() as u64, Ordering::Relaxed);

    // AND step.
    let t = Instant::now();
    and_bitmap_into(&mut bitmap, &dc_bitmap);
    and_bitmap_into(&mut bitmap, &qy_bitmap);
    if popcount(&bitmap) == 0 {
        T_AND.fetch_add(t.elapsed().as_micros() as u64, Ordering::Relaxed);
        return 0.0;
    }
    T_AND.fetch_add(t.elapsed().as_micros() as u64, Ordering::Relaxed);

    // 4. Projection masked-decodes.
    let t = Instant::now();
    let mut prices: Vec<f64> = Vec::new();
    read_column_f64_masked_into(&file, rg, COL_EXTPRICE, &bitmap, &mut prices)
        .expect("extprice masked");
    T_PROJ_PRICE.fetch_add(t.elapsed().as_micros() as u64, Ordering::Relaxed);
    let t = Instant::now();
    let mut discounts: Vec<f64> = Vec::new();
    read_column_f64_masked_into(&file, rg, COL_DISCOUNT, &bitmap, &mut discounts)
        .expect("discount masked");
    T_PROJ_DISC.fetch_add(t.elapsed().as_micros() as u64, Ordering::Relaxed);
    assert_eq!(prices.len(), discounts.len());

    // 5. Tight multiply-sum.
    let t = Instant::now();
    let mut sum = 0.0;
    for i in 0..prices.len() {
        sum += prices[i] * discounts[i];
    }
    T_AGG.fetch_add(t.elapsed().as_micros() as u64, Ordering::Relaxed);
    sum
}

fn main() {
    let dir =
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf10".to_string());
    let file_name =
        std::env::var("LINEITEM_FILE").unwrap_or_else(|_| "lineitem.parquet".to_string());
    let path = PathBuf::from(&dir).join(&file_name);
    if !path.exists() {
        eprintln!("missing {}", path.display());
        std::process::exit(2);
    }
    let path = Arc::new(path);
    let file = ParquetFile::open(&*path).expect("open");
    let n_rgs = file.metadata().expect("md").row_groups.len();
    let n_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);

    println!(
        "=== Σ.E7 Q06 fused-proof ({}, {n_rgs} RGs, {n_threads} threads) ===\n",
        path.display()
    );

    // Run REPS, take median.
    let reps: usize = std::env::var("REPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);

    // Warmup.
    let _ = run_once(&path, n_rgs, n_threads);

    let mut times = Vec::with_capacity(reps);
    let mut last_sum = 0.0;
    for rep in 0..reps {
        let t = Instant::now();
        let sum = run_once(&path, n_rgs, n_threads);
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        times.push(ms);
        last_sum = sum;
        println!("  rep {}: {ms:>7.2} ms  sum = {sum:.4}", rep + 1);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = times[times.len() / 2];

    println!();
    println!("Per-phase totals (microseconds, summed across all RGs × all reps):");
    println!(
        "  shipdate filter (i32):  {:>10} us",
        T_SHIPDATE.load(Ordering::Relaxed)
    );
    println!(
        "  discount filter (f64):  {:>10} us",
        T_DISCOUNT.load(Ordering::Relaxed)
    );
    println!(
        "  quantity filter (f64):  {:>10} us",
        T_QUANTITY.load(Ordering::Relaxed)
    );
    println!(
        "  AND + popcount:         {:>10} us",
        T_AND.load(Ordering::Relaxed)
    );
    println!(
        "  extprice masked decode: {:>10} us",
        T_PROJ_PRICE.load(Ordering::Relaxed)
    );
    println!(
        "  discount masked decode: {:>10} us",
        T_PROJ_DISC.load(Ordering::Relaxed)
    );
    println!(
        "  multiply-sum:           {:>10} us",
        T_AGG.load(Ordering::Relaxed)
    );
    println!();
    println!("Σ.E7 (Q06 fused, no DataFusion):  median = {median:.2} ms");
    println!("Triangulation reference baseline:");
    println!("  ematix-flow (rules + Arrow path):     ~78 ms");
    println!("  DuckDB:                                ~74 ms");
    println!("  Polars:                                ~62 ms");
    println!();
    let _ = last_sum;
    println!("Interpretation:");
    println!("  median ≤ 62 ms → architectural win, build the generalised operator");
    println!("  median 62-78 → partial win, evaluate cost/benefit before scaling up");
    println!("  median ≥ 78 → no architectural win available; gap is below the operator layer");
}

fn run_once(path: &Arc<PathBuf>, n_rgs: usize, n_threads: usize) -> f64 {
    // Atomic counter for work-stealing across threads.
    let next_rg = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(n_threads);
    for _ in 0..n_threads {
        let path = path.clone();
        let next_rg = next_rg.clone();
        handles.push(thread::spawn(move || -> f64 {
            let mut local = 0.0;
            loop {
                let rg = next_rg.fetch_add(1, Ordering::Relaxed) as usize;
                if rg >= n_rgs {
                    break;
                }
                local += process_rg(&path, rg);
            }
            local
        }));
    }
    let mut sum = 0.0;
    for h in handles {
        sum += h.join().expect("thread");
    }
    sum
}
