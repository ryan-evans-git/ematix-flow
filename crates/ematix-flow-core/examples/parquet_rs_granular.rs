//! Σ.E2 day-3: granular parquet decode probe. The day-2 result said
//! parquet-rs single-thread (62 ms for Q6 4-col) is 1.25× slower than
//! polars-parquet single-thread (49.63 ms). Day-3 asks: *where* in the
//! pipeline is that gap?
//!
//! Day-2 used the default reader configuration — most importantly,
//! `batch_size = 1024`. polars-parquet reads in much larger units
//! (whole pages, typically 64k+ rows). Small batches mean more
//! allocations and less SIMD reuse. Test 1 sweeps batch size; if that
//! closes most of the gap, we don't need polars at all.
//!
//! Other axes:
//!   - I/O only (no decode) — isolates the file read itself
//!   - mmap vs `File::open` — does the kernel copy hurt?
//!   - full decode at varying batch sizes
//!
//! Usage:
//!   cargo run --release -p ematix-flow-core --example parquet_rs_granular

use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::RecordBatch;
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::schema::types::SchemaDescriptor;

const TRIALS: usize = 5;

fn median_of(times: &mut [f64]) -> (f64, f64, f64) {
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (times[times.len() / 2], times[0], times[times.len() - 1])
}

/// Test 0: just `std::fs::read` the entire file. Worst-case I/O baseline.
fn bench_io_only(parquet: &str) {
    let mut times = vec![0.0; TRIALS];
    let mut bytes = 0usize;
    for t in 0..TRIALS {
        let start = Instant::now();
        let buf = std::fs::read(parquet).unwrap();
        times[t] = start.elapsed().as_secs_f64() * 1000.0;
        bytes = buf.len();
    }
    let (med, lo, hi) = median_of(&mut times);
    println!(
        "  {:<58} median {:>6.2} ms  (min {:>5.2}  max {:>5.2})  bytes={bytes}",
        "Test 0: std::fs::read full file (I/O only)", med, lo, hi
    );
}

/// Test 1: full parquet decode at varying batch sizes.
fn bench_decode(label: &str, parquet: &str, projection_indices: &[usize], batch_size: usize) {
    let mut times = vec![0.0; TRIALS];
    let mut total_rows = 0usize;
    let mut num_batches = 0usize;
    for t in 0..TRIALS {
        let file = File::open(parquet).unwrap();
        let start = Instant::now();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let schema_desc: Arc<SchemaDescriptor> = builder.parquet_schema().clone().into();
        let mask = ProjectionMask::leaves(&schema_desc, projection_indices.iter().copied());
        let reader = builder
            .with_projection(mask)
            .with_batch_size(batch_size)
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.into_iter().map(|r| r.unwrap()).collect();
        times[t] = start.elapsed().as_secs_f64() * 1000.0;
        total_rows = batches.iter().map(|b| b.num_rows()).sum();
        num_batches = batches.len();
    }
    let (med, lo, hi) = median_of(&mut times);
    println!(
        "  {label:<58} median {med:>6.2} ms  (min {lo:>5.2}  max {hi:>5.2})  batches={num_batches} rows={total_rows}"
    );
}

/// Test 2: read+decompress pages only, no Arrow conversion. This uses
/// `SerializedFileReader` at the column-chunk level to force page
/// decompression without paying for Arrow assembly.
fn bench_pages_only(parquet: &str, projection_indices: &[usize]) {
    use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};

    let mut times = vec![0.0; TRIALS];
    let mut total_values = 0i64;
    for t in 0..TRIALS {
        let file = File::open(parquet).unwrap();
        let start = Instant::now();
        let reader = SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();
        let mut values_touched = 0i64;
        for rg_idx in 0..metadata.num_row_groups() {
            let rg = reader.get_row_group(rg_idx).unwrap();
            for &col_idx in projection_indices {
                let mut page_reader = rg.get_column_page_reader(col_idx).unwrap();
                while let Some(page) = page_reader.get_next_page().unwrap() {
                    // Page is decompressed at this point. Touch the
                    // buffer so the compiler can't elide it.
                    values_touched += page.buffer().len() as i64;
                }
            }
        }
        times[t] = start.elapsed().as_secs_f64() * 1000.0;
        total_values = values_touched;
    }
    let (med, lo, hi) = median_of(&mut times);
    println!(
        "  {:<58} median {:>6.2} ms  (min {:>5.2}  max {:>5.2})  bytes={total_values}",
        "Test 2: pages decompressed, no Arrow decode", med, lo, hi
    );
}

/// Test 3: read raw page bytes only — no decompression. Isolates pure
/// I/O + parquet structural reading from snappy decompress.
fn bench_pages_compressed(parquet: &str) {
    let mut times = vec![0.0; TRIALS];
    let mut bytes = 0usize;
    for t in 0..TRIALS {
        let mut file = File::open(parquet).unwrap();
        let start = Instant::now();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();
        // Sum first-byte parity so the read can't be elided.
        let parity: u8 = buf.iter().fold(0u8, |a, &b| a.wrapping_add(b));
        times[t] = start.elapsed().as_secs_f64() * 1000.0;
        bytes = buf.len() + parity as usize;
    }
    let (med, lo, hi) = median_of(&mut times);
    println!(
        "  {:<58} median {:>6.2} ms  (min {:>5.2}  max {:>5.2})  bytes={bytes}",
        "Test 3: File::read_to_end (touched bytes)", med, lo, hi
    );
}

fn main() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let parquet = manifest
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("examples/tpch/data/sf1/lineitem.parquet");
    let parquet = parquet.to_str().unwrap();
    println!("==> Σ.E2 day-3: granular parquet-rs probe");
    println!("==> data: {parquet}");
    println!();

    // Q6 4-col projection: l_quantity, l_extendedprice, l_discount, l_shipdate
    let q6_cols = [4usize, 5, 6, 10];

    println!("=== Layer 1: pure I/O baselines ===");
    bench_io_only(parquet);
    bench_pages_compressed(parquet);
    println!();

    println!("=== Layer 2: parquet structural + decompress (Q6 cols) ===");
    bench_pages_only(parquet, &q6_cols);
    println!();

    println!("=== Layer 3: full Arrow decode, batch-size sweep (Q6 4-col) ===");
    for &bs in &[1024usize, 4096, 8192, 32768, 65536, 131072, 1048576] {
        let label = format!("Q6 4-col, batch_size = {:>7}", bs);
        bench_decode(&label, parquet, &q6_cols, bs);
    }
    println!();

    println!("=== Q14 4-col with batch-size sweep ===");
    let q14_cols = [1usize, 5, 6, 10];
    for &bs in &[1024usize, 8192, 65536, 1048576] {
        let label = format!("Q14 4-col, batch_size = {:>7}", bs);
        bench_decode(&label, parquet, &q14_cols, bs);
    }
    println!();

    println!("Reference (day-2, default batch_size=1024):");
    println!("  parquet-rs single-thread Q6  4-col: 62.00 ms");
    println!("  parquet-rs single-thread Q14 4-col: 88.64 ms");
    println!("  polars      single-thread Q6  4-col: 49.63 ms  (1.25× faster)");
    println!("  polars     14-thread       Q6  4-col: 15.08 ms");
    println!("  DataFusion 14-thread       Q6  4-col: 21.87 ms  (1.45× slower than polars)");
}
