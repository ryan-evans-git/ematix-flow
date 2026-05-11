//! Σ.E2 day-2: read lineitem.parquet via `parquet-rs` directly,
//! bypassing DataFusion's adapter (`ParquetExec`, `DataSourceExec`,
//! `RepartitionExec`, etc.). If the read time matches DataFusion's,
//! the gap is in `parquet-rs` itself. If it's faster, DataFusion's
//! adapter is adding overhead we can eliminate.
//!
//! Baseline from `parquet_read_bench.rs` (DataFusion):
//!   4-col Q6 projection                    18.62 ms cold / 21.87 ms warm
//!   4-col Q6 projection + Q6 filter        21.42 ms cold / 20.80 ms warm
//!   Polars equivalent                       12.82 ms (filter pushed)
//!
//! Σ.E2 day-2 goal: pin down whether the 6-10 ms gap is in parquet-rs
//! itself or in DataFusion's wrapping.

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::RecordBatch;
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::arrow::arrow_reader::{
    ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
};
use datafusion::parquet::schema::types::SchemaDescriptor;

fn bench_direct(label: &str, parquet: &str, projection_indices: &[usize]) {
    let mut times = Vec::with_capacity(5);
    let mut total_rows = 0usize;
    for _ in 0..5 {
        let file = File::open(parquet).unwrap();
        let start = Instant::now();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let schema_desc: Arc<SchemaDescriptor> = builder.parquet_schema().clone().into();
        let mask = ProjectionMask::leaves(&schema_desc, projection_indices.iter().copied());
        let reader = builder.with_projection(mask).build().unwrap();
        let batches: Vec<RecordBatch> = reader.into_iter().map(|r| r.unwrap()).collect();
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        times.push(elapsed);
        total_rows = batches.iter().map(|b| b.num_rows()).sum();
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "  {label:<60}  median {:>6.2} ms  (min {:>5.2}  max {:>5.2})  rows={total_rows}",
        times[2], times[0], times[4],
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
    println!("==> Σ.E2 day-2: parquet-rs direct read (bypasses DataFusion)");
    println!("==> data: {parquet}");
    println!();

    // First print schema so we know column indices.
    let file = File::open(parquet).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let schema = builder.schema();
    println!("Schema:");
    for (i, f) in schema.fields().iter().enumerate() {
        println!("  {i:>2}: {} {:?}", f.name(), f.data_type());
    }
    drop(builder);
    println!();

    // Read all 16 columns.
    let all_indices: Vec<usize> = (0..16).collect();
    bench_direct(
        "all 16 columns (sync, single-threaded)",
        parquet,
        &all_indices,
    );

    // 4-col Q6 projection. Schema-discovered indices:
    //   l_quantity        col 4
    //   l_extendedprice   col 5
    //   l_discount        col 6
    //   l_shipdate        col 10
    bench_direct(
        "Q6 4-col projection (sync, single-threaded)",
        parquet,
        &[4, 5, 6, 10],
    );

    // 4-col Q14 projection:
    //   l_partkey         col 1
    //   l_extendedprice   col 5
    //   l_discount        col 6
    //   l_shipdate        col 10
    bench_direct(
        "Q14 4-col projection (sync, single-threaded)",
        parquet,
        &[1, 5, 6, 10],
    );

    println!();
    println!("==> compare to DataFusion `select 4-col from lineitem`:");
    println!("    Q6 4-col warm:                 21.87 ms");
    println!("    Q14 4-col warm:                20.36 ms");
    println!();
    println!("    If parquet-rs direct ≈ DataFusion, the gap is in parquet-rs.");
    println!("    If direct is much faster, the gap is in DataFusion's adapter.");

    // Silence unused warning for RowSelection / RowSelector — they're
    // intentionally imported as a marker that the day-3+ work would
    // push the filter into the reader via `with_row_selection`.
    let _ = RowSelection::from(Vec::<RowSelector>::new());
}
