//! Σ.E5.2 H3 isolation: how much does the per-RG `ParquetFile::open`
//! call inside the bridge actually cost?
//!
//! The late-mat decode path opens the parquet file once per row group;
//! the byte-array bridge functions also re-open inside themselves
//! (`ParquetFile::open` in `ematix_parquet_bridge.rs`). Q1 reads 7
//! columns over 6 RGs; depending on which path runs, that's ~6 to ~42
//! file opens per query.
//!
//! This bench times a single warm `ParquetFile::open` to bound the
//! contribution.

use std::path::PathBuf;
use std::time::Instant;

use ematix_parquet_io::ParquetFile;

const TRIALS: usize = 200;

fn data_path() -> String {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    match std::env::var("TPCH_DATA_DIR") {
        Ok(s) => format!("{s}/lineitem.parquet"),
        Err(_) => manifest.parent().unwrap().parent().unwrap()
            .join("examples/tpch/data/sf1/lineitem.parquet")
            .to_string_lossy().into_owned(),
    }
}

fn main() {
    let p = data_path();
    // Warm OS page cache
    for _ in 0..10 {
        let _f = ParquetFile::open(&p).unwrap();
    }
    let mut times_us = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let s = Instant::now();
        let f = ParquetFile::open(&p).unwrap();
        std::hint::black_box(f);
        times_us.push(s.elapsed().as_secs_f64() * 1_000_000.0);
    }
    times_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median_us = times_us[TRIALS / 2];
    let p99_us = times_us[(TRIALS as f64 * 0.99) as usize];
    let max_us = *times_us.last().unwrap();
    println!("ParquetFile::open (warm cache, {TRIALS} trials):");
    println!("  median: {median_us:.1} µs");
    println!("  p99   : {p99_us:.1} µs");
    println!("  max   : {max_us:.1} µs");
    println!();
    println!("Q1 worst-case opens per query:");
    println!("  late-mat path: 6 RGs × 1 open per RG     = 6 opens ≈ {:.2} ms", 6.0 * median_us / 1000.0);
    println!("  byte-array bridge: 6 RGs × 2 string cols = 12 opens ≈ {:.2} ms", 12.0 * median_us / 1000.0);
    println!("  worst case (every column reopens):       42 opens ≈ {:.2} ms", 42.0 * median_us / 1000.0);
}
