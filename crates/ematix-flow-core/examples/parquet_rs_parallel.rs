//! Σ.E2 day-3c: row-group-parallel parquet-rs read via rayon,
//! bypassing DataFusion entirely. This tests the hypothesis that the
//! 1.45× multi-threaded gap (DataFusion 21.87 ms vs Polars 15.08 ms
//! on Q6 4-col) is in DataFusion's wrapping (ParquetExec →
//! DataSourceExec → RepartitionExec), not in parquet-rs.
//!
//! If parquet-rs + rayon over row groups matches Polars, the right
//! answer is a custom DataFusion TableProvider that does this
//! orchestration itself, not a polars-io wrap or a custom decoder.
//!
//! If parquet-rs + rayon still trails Polars meaningfully, polars-
//! parquet's per-thread decoder is the bottleneck and we need
//! decoder-level work (option B/C from day-2's commit).

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::RecordBatch;
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::schema::types::SchemaDescriptor;

const TRIALS: usize = 5;

fn median(times: &mut [f64]) -> (f64, f64, f64) {
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (times[times.len() / 2], times[0], times[times.len() - 1])
}

/// Read N row groups across `num_threads` worker threads using
/// `std::thread::scope`. Each thread takes a slice of the row-group
/// index range, opens its own File, and reads its assigned row
/// groups serially. We measure end-to-end wall time.
fn bench_row_group_parallel(
    label: &str,
    parquet: &str,
    proj: &[usize],
    batch_size: usize,
    num_threads: usize,
) {
    // Figure out how many row groups we have.
    let file = File::open(parquet).unwrap();
    let probe = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let num_rgs = probe.metadata().num_row_groups();
    let schema_desc: Arc<SchemaDescriptor> = probe.parquet_schema().clone().into();
    drop(probe);

    let proj_vec: Vec<usize> = proj.to_vec();

    let mut times = vec![0.0; TRIALS];
    let mut total_rows = 0usize;
    for t in 0..TRIALS {
        let start = Instant::now();
        let all_batches: Vec<Vec<RecordBatch>> = std::thread::scope(|s| {
            // Round-robin row groups across threads.
            let mut handles = Vec::with_capacity(num_threads);
            for tid in 0..num_threads {
                let path = parquet.to_string();
                let proj_vec = proj_vec.clone();
                let schema_desc = schema_desc.clone();
                handles.push(s.spawn(move || {
                    let mut out: Vec<RecordBatch> = Vec::new();
                    let mut rg = tid;
                    while rg < num_rgs {
                        let file = File::open(&path).unwrap();
                        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
                        let mask = ProjectionMask::leaves(&schema_desc, proj_vec.iter().copied());
                        let reader = builder
                            .with_projection(mask)
                            .with_row_groups(vec![rg])
                            .with_batch_size(batch_size)
                            .build()
                            .unwrap();
                        for b in reader {
                            out.push(b.unwrap());
                        }
                        rg += num_threads;
                    }
                    out
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        times[t] = start.elapsed().as_secs_f64() * 1000.0;
        total_rows = all_batches
            .iter()
            .flat_map(|v| v.iter())
            .map(|b| b.num_rows())
            .sum();
    }
    let (med, lo, hi) = median(&mut times);
    println!(
        "  {label:<58} median {med:>6.2} ms  (min {lo:>5.2}  max {hi:>5.2})  rows={total_rows}  threads={num_threads}"
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
    println!("==> Σ.E2 day-3c: parquet-rs + rayon over row groups");
    println!("==> data: {parquet}");
    println!("==> file has 6 row groups, ~1M rows each");
    println!();

    let q6 = [4usize, 5, 6, 10];
    let q14 = [1usize, 5, 6, 10];

    println!("=== Q6 4-col, batch_size=65536, varying thread count ===");
    for &nt in &[1usize, 2, 4, 6, 8, 14] {
        let label = format!("Q6 4-col, rayon n_threads = {:>2}", nt);
        bench_row_group_parallel(&label, parquet, &q6, 65536, nt);
    }

    println!();
    println!("=== Q14 4-col, batch_size=65536, varying thread count ===");
    for &nt in &[1usize, 6, 14] {
        let label = format!("Q14 4-col, rayon n_threads = {:>2}", nt);
        bench_row_group_parallel(&label, parquet, &q14, 65536, nt);
    }

    println!();
    println!("Reference (14-thread):");
    println!("  Polars     Q6 4-col:  15.08 ms");
    println!("  DataFusion Q6 4-col:  21.87 ms");
    println!();
    println!("If rayon-over-row-groups ≈ 15 ms, the gap is in DataFusion's");
    println!("ParquetExec/RepartitionExec orchestration, NOT in parquet-rs.");
    println!("Fix: custom TableProvider that does row-group-parallel reads.");
}
