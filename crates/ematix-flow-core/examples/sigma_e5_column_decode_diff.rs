//! Σ.E5 (2026-05-18) diagnostic: direct column-decode micro-bench.
//!
//! Strips away DataFusion entirely. For a given parquet file +
//! column, times:
//!   1. Our flow-side `decode_one_column` (which calls into
//!      ematix-parquet under the hood)
//!   2. parquet-rs's native column decode via `ParquetRecordBatchReader`
//!
//! Dispatches by Arrow type — works for byte_array → StringView, plus
//! Int32 / Int64 / Float64 / Decimal numerics. Use this to localise
//! where a per-query gap lives (specific column? specific Arrow type?).
//!
//! Run:
//!   COLUMN=o_comment FILE=orders.parquet \
//!     cargo run --release -p ematix-flow-core --example sigma_e5_column_decode_diff
//!
//!   COLUMN=l_partkey FILE=lineitem.parquet ...
//!
//! Defaults to o_comment on orders.parquet (the Q13 hot column —
//! 1.5M unique values, PLAIN-encoded, biggest known regression).

use std::path::PathBuf;
use std::time::Instant;

use datafusion::arrow::datatypes::DataType;
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

/// Promote `Utf8` → `Utf8View` to match the EmatArrowBatchReader
/// pipeline's behaviour (the provider always promotes byte_array
/// outputs to view types — that's the Σ.E5.1.d hot path).
fn promote_target(dt: &DataType) -> DataType {
    match dt {
        DataType::Utf8 | DataType::LargeUtf8 => DataType::Utf8View,
        other => other.clone(),
    }
}

fn bench_emat(
    file_path: &PathBuf,
    col_idx: usize,
    target: &DataType,
    trials: usize,
) -> (f64, usize) {
    use ematix_flow_core::emat_arrow_reader::decode_one_column_for_bench;
    let file = ParquetFile::open(file_path).expect("emat open");
    let md = file.metadata().expect("emat metadata");
    let num_rgs = md.row_groups.len();
    let mut best = f64::MAX;
    let mut rows_decoded = 0usize;
    for _ in 0..trials {
        let t0 = Instant::now();
        let mut total_rows = 0usize;
        for rg in 0..num_rgs {
            let (rows, _bytes) =
                decode_one_column_for_bench(&file, rg, col_idx, target).expect("emat decode");
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

/// Look up the column's Arrow data type via parquet-rs (single source
/// of truth — what callers downstream would see).
fn col_arrow_type(file_path: &PathBuf, col_name: &str) -> DataType {
    use std::fs::File;
    let file = File::open(file_path).unwrap();
    let opts = ArrowReaderOptions::new();
    let arrow_md = ArrowReaderMetadata::load(&file, opts).unwrap();
    let schema = arrow_md.schema();
    let field = schema
        .fields()
        .iter()
        .find(|f| f.name() == col_name)
        .unwrap_or_else(|| panic!("col {col_name} not in schema"));
    field.data_type().clone()
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

    let raw_type = col_arrow_type(&file_path, &col_name);
    let target = promote_target(&raw_type);

    println!("--- column-decode micro-bench ---");
    println!("file: {}", file_path.display());
    println!("column: {col_name}");
    println!("type (parquet-rs schema): {raw_type:?}");
    println!("type (emat target):       {target:?}");
    println!("trials: {trials} (min)");
    println!();

    // Emat side. Find the leaf column index by name.
    let emat_file = ParquetFile::open(&file_path).expect("emat open");
    let leaf_idx = leaf_idx_by_name(&emat_file, &col_name)
        .expect("column not found in ematix-parquet schema leaves");
    drop(emat_file);

    let (emat_ms, emat_rows) = bench_emat(&file_path, leaf_idx, &target, trials);
    let (pq_ms, pq_rows) = bench_parquet_rs(&file_path, &col_name, trials);

    println!("emat decode_one_column ({target:?}):");
    println!("  min: {:>7.2} ms  ({rows} rows)", emat_ms, rows = emat_rows);
    println!("parquet-rs ParquetRecordBatchReader:");
    println!("  min: {:>7.2} ms  ({rows} rows)", pq_ms, rows = pq_rows);
    println!();
    let delta = (emat_ms - pq_ms) / pq_ms * 100.0;
    println!(
        "delta (emat / parquet-rs): {:+.1}%  (emat / pq = {:.3}x)",
        delta,
        emat_ms / pq_ms
    );
}
