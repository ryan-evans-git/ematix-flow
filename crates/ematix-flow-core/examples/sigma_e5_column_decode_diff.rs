//! Σ.E5 (2026-05-18) diagnostic: direct column-decode micro-bench.
//!
//! Strips away DataFusion entirely. For a given parquet file +
//! column, times:
//!   1. Our flow-side `decode_byte_array_to_string_view` (which calls
//!      into ematix-parquet under the hood)
//!   2. parquet-rs's native byte_array → StringViewArray decode
//!
//! If timing 1 ≫ timing 2 on the same RG / column, the gap lives in
//! the ematix-parquet stack (Snappy decompression, PageWalker, or
//! the flow-side view construction). If timings are close, the gap
//! in the 22-query bench is downstream of the scan.
//!
//! Run:
//!   COLUMN=o_comment FILE=orders.parquet \
//!     cargo run --release -p ematix-flow-core --example sigma_e5_column_decode_diff
//!
//! Defaults to o_comment on orders.parquet (the Q13 hot column —
//! 1.5M unique values, PLAIN-encoded, biggest known regression).

use std::path::PathBuf;
use std::time::Instant;

use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use ematix_parquet_io::ParquetFile;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn data_dir() -> String {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    match std::env::var("TPCH_DATA_DIR") {
        Ok(s) => s,
        Err(_) => manifest
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("examples/tpch/data/sf1")
            .to_string_lossy()
            .into_owned(),
    }
}

// Leaf-column index by name in the ematix-parquet schema. The
// schema vec is flat (root group first, then leaves); we skip the
// root and find by position.
fn leaf_idx_by_name(file: &ParquetFile, name: &str) -> Option<usize> {
    let md = file.metadata().ok()?;
    md.schema
        .iter()
        .skip(1) // skip root group node
        .position(|f| f.name.as_ref() == name.as_bytes())
}

fn bench_emat(file_path: &PathBuf, col_idx: usize, trials: usize) -> (f64, usize) {
    use ematix_flow_core::emat_arrow_reader::decode_byte_array_to_string_view_for_bench;
    let file = ParquetFile::open(file_path).expect("emat open");
    let md = file.metadata().expect("emat metadata");
    let num_rgs = md.row_groups.len();
    let mut best = f64::MAX;
    let mut rows_decoded = 0usize;
    for _ in 0..trials {
        let t0 = Instant::now();
        let mut total_rows = 0usize;
        for rg in 0..num_rgs {
            let (rows, _bytes) = decode_byte_array_to_string_view_for_bench(&file, rg, col_idx)
                .expect("emat decode");
            total_rows += rows;
        }
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        if ms < best {
            best = ms;
        }
        rows_decoded = total_rows;
    }
    (best, rows_decoded)
}

fn bench_parquet_rs(file_path: &PathBuf, col_name: &str, trials: usize) -> (f64, usize) {
    use std::fs::File;
    let mut best = f64::MAX;
    let mut rows_decoded = 0usize;

    for _ in 0..trials {
        let file = File::open(file_path).unwrap();
        let opts = ArrowReaderOptions::new();
        let arrow_md = ArrowReaderMetadata::load(&file, opts).unwrap();
        let schema = arrow_md.schema();
        let col_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == col_name)
            .expect("col name not in schema");
        let parquet_meta = arrow_md.metadata().clone();
        let parquet_schema = parquet_meta.file_metadata().schema_descr();
        let mask = ProjectionMask::leaves(parquet_schema, [col_idx]);

        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
            file.try_clone().unwrap(),
            arrow_md.clone(),
        )
        .with_projection(mask)
        .with_batch_size(65536);
        let reader = builder.build().unwrap();
        let t0 = Instant::now();
        let mut total_rows = 0usize;
        for batch in reader {
            let b = batch.unwrap();
            total_rows += b.num_rows();
        }
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        if ms < best {
            best = ms;
        }
        rows_decoded = total_rows;
    }
    (best, rows_decoded)
}

fn main() {
    let dir = data_dir();
    let file_name = std::env::var("FILE").unwrap_or_else(|_| "orders.parquet".into());
    let col_name = std::env::var("COLUMN").unwrap_or_else(|_| "o_comment".into());
    let trials: usize = std::env::var("TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(11);

    let file_path = PathBuf::from(format!("{dir}/{file_name}"));

    println!("--- column-decode micro-bench ---");
    println!("file: {}", file_path.display());
    println!("column: {col_name}");
    println!("trials: {trials} (min)");
    println!();

    // Emat side. Find the leaf column index by name.
    let emat_file = ParquetFile::open(&file_path).expect("emat open");
    let leaf_idx = leaf_idx_by_name(&emat_file, &col_name)
        .expect("column not found in ematix-parquet schema leaves");
    drop(emat_file);

    let (emat_ms, emat_rows) = bench_emat(&file_path, leaf_idx, trials);
    let (pq_ms, pq_rows) = bench_parquet_rs(&file_path, &col_name, trials);

    println!("emat decode_byte_array_to_string_view:");
    println!("  min: {:>7.2} ms  ({rows} rows)", emat_ms, rows = emat_rows);
    println!("parquet-rs ParquetRecordBatchReader (StringView output):");
    println!("  min: {:>7.2} ms  ({rows} rows)", pq_ms, rows = pq_rows);
    println!();
    let delta = (emat_ms - pq_ms) / pq_ms * 100.0;
    println!(
        "delta (emat / parquet-rs): {:+.1}%  (emat / pq = {:.3}x)",
        delta,
        emat_ms / pq_ms
    );
}

// Wrapper required to expose decode_byte_array_to_string_view publicly
// for this bench without permanently widening its visibility. Trick:
// the function is `pub(crate)` plus a re-export under a `_for_bench`
// alias in the public surface.
