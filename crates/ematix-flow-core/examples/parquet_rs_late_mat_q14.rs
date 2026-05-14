//! Σ.E2 follow-up: late-materialization probe for Q14's predicate shape.
//!
//! The day-3 probe (`parquet_rs_late_mat.rs`, since deleted — see commit
//! 5712480) tested `with_row_filter` against Q6's compound predicate
//! (date range AND discount range AND quantity bound) and found it 21 %
//! *slower* than full-decode (64.25 → 81.80 ms). The compound predicate
//! made per-row eval expensive — three Arrow kernel calls per batch.
//!
//! Q14 is a different shape: a single `l_shipdate` Date32 half-open
//! range. That's one Arrow kernel call per batch, ~order-of-magnitude
//! cheaper. If the day-3 `with_row_filter` slowdown was per-row predicate
//! cost rather than something inherent to the late-mat path, Q14's
//! single-comparison predicate should swing positive.
//!
//! Reports four cells:
//!   1. No filter, 1 thread     (raw decode cost)
//!   2. with_row_filter, 1 thread (late mat single-thread)
//!   3. No filter, 6 threads     (row-group-parallel baseline)
//!   4. with_row_filter, 6 threads (the candidate path)
//!
//! Cell 4 vs cell 3 is the decision number — that's what FastParquet
//! would do at SF=1 (6 row groups).
//!
//! Q14 SF=1: Polars 12.53 ms median; FusedQ14FullExec at 15.15 ms
//! (commit 0941417); FastParquet+Utf8View SQL at 16.57 ms (commit 30e8ce2).
//! If cell 4 lands ≤ 10 ms we have headroom to absorb the join + agg
//! work on top and still beat Polars; if ≥ 14 ms there's no win
//! available via this path.
//!
//! Usage:
//!   cargo run --release -p ematix-flow-core --example parquet_rs_late_mat_q14

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Date32Array, RecordBatch};
use datafusion::arrow::compute::kernels::boolean::and;
use datafusion::arrow::compute::kernels::cmp::{gt_eq, lt};
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter,
};
use datafusion::parquet::schema::types::SchemaDescriptor;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TRIALS: usize = 7;
const WARMUPS: usize = 2;

/// Date32 days since 1970-01-01.
const SHIPDATE_LO: i32 = 9374; // 1995-09-01
const SHIPDATE_HI: i32 = 9404; // 1995-10-01

/// lineitem column indices (parquet leaf order matches Arrow field order).
const L_PARTKEY: usize = 1;
const L_EXTENDEDPRICE: usize = 5;
const L_DISCOUNT: usize = 6;
const L_SHIPDATE: usize = 10;

fn data_path() -> String {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let from_env = std::env::var("TPCH_DATA_DIR").ok().map(PathBuf::from);
    let p = match from_env {
        Some(p) => p,
        None => manifest
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("examples/tpch/data/sf1"),
    };
    p.join("lineitem.parquet").to_string_lossy().into_owned()
}

fn median(times: &mut [f64]) -> f64 {
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    times[times.len() / 2]
}

/// Q14 predicate as an ArrowPredicate over a shipdate-only projection.
fn q14_predicate(schema_desc: &Arc<SchemaDescriptor>) -> Box<dyn ArrowPredicate> {
    let proj = ProjectionMask::leaves(schema_desc, [L_SHIPDATE]);
    Box::new(ArrowPredicateFn::new(proj, move |batch: RecordBatch| {
        let ship = batch
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        let lo = Date32Array::new_scalar(SHIPDATE_LO);
        let hi = Date32Array::new_scalar(SHIPDATE_HI);
        let ge = gt_eq(ship, &lo).unwrap();
        let lt = lt(ship, &hi).unwrap();
        Ok(and(&ge, &lt).unwrap())
    }))
}

fn read_q14_proj_one_pass(path: &str, with_filter: bool, row_groups: Option<Vec<usize>>) -> usize {
    let file = File::open(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let schema_desc: Arc<SchemaDescriptor> = builder.parquet_schema().clone().into();
    let final_proj = ProjectionMask::leaves(
        &schema_desc,
        [L_PARTKEY, L_EXTENDEDPRICE, L_DISCOUNT, L_SHIPDATE],
    );
    let mut b = builder.with_projection(final_proj).with_batch_size(65_536);
    if let Some(rgs) = row_groups {
        b = b.with_row_groups(rgs);
    }
    if with_filter {
        b = b.with_row_filter(RowFilter::new(vec![q14_predicate(&schema_desc)]));
    }
    let reader = b.build().unwrap();
    let mut rows = 0usize;
    for batch in reader {
        let batch = batch.unwrap();
        rows += batch.num_rows();
    }
    rows
}

fn bench_single_thread(path: &str, with_filter: bool) -> (f64, usize) {
    let mut last_rows = 0usize;
    for _ in 0..WARMUPS {
        last_rows = read_q14_proj_one_pass(path, with_filter, None);
    }
    let mut times = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let start = Instant::now();
        last_rows = read_q14_proj_one_pass(path, with_filter, None);
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    (median(&mut times), last_rows)
}

fn bench_parallel(path: &str, with_filter: bool, workers: usize) -> (f64, usize) {
    // Read footer once to learn row-group count.
    let f = File::open(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
    let n_rgs = builder.metadata().num_row_groups();
    drop(builder);

    let assignments: Vec<Vec<usize>> = (0..workers)
        .map(|w| (0..n_rgs).filter(|rg| rg % workers == w).collect())
        .collect();

    let run_once = || -> usize {
        std::thread::scope(|s| {
            let handles: Vec<_> = assignments
                .iter()
                .cloned()
                .map(|rgs| {
                    s.spawn(move || {
                        if rgs.is_empty() {
                            return 0usize;
                        }
                        read_q14_proj_one_pass(path, with_filter, Some(rgs))
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).sum()
        })
    };

    let mut last_rows = 0usize;
    for _ in 0..WARMUPS {
        last_rows = run_once();
    }
    let mut times = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let start = Instant::now();
        last_rows = run_once();
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    (median(&mut times), last_rows)
}

fn main() {
    let path = data_path();
    println!("==> Σ.E2 follow-up: Q14 late-materialization probe");
    println!("==> data: {path}");
    println!("==> {TRIALS}-trial median after {WARMUPS} warm-ups");
    println!();

    let (st_no, st_no_rows) = bench_single_thread(&path, false);
    println!("  1-thread, no filter          median {st_no:>6.2} ms   rows={st_no_rows}");
    let (st_yes, st_yes_rows) = bench_single_thread(&path, true);
    println!("  1-thread, with_row_filter    median {st_yes:>6.2} ms   rows={st_yes_rows}");
    let (mt_no, mt_no_rows) = bench_parallel(&path, false, 6);
    println!("  6-thread, no filter          median {mt_no:>6.2} ms   rows={mt_no_rows}");
    let (mt_yes, mt_yes_rows) = bench_parallel(&path, true, 6);
    println!("  6-thread, with_row_filter    median {mt_yes:>6.2} ms   rows={mt_yes_rows}");

    println!();
    let st_speedup = st_no / st_yes;
    let mt_speedup = mt_no / mt_yes;
    println!("  Single-thread late-mat speedup vs full-decode: {st_speedup:.2}×");
    println!("  6-thread     late-mat speedup vs full-decode: {mt_speedup:.2}×");
    println!();
    println!("  Reference targets:");
    println!("    Polars Q14 SF=1 median:     12.53 ms");
    println!("    FusedQ14FullExec full-fuse:  15.15 ms");
    println!("    FastParquet+Utf8View SQL:    16.57 ms");
    println!();
    println!("  Decision: if 6-thread late-mat ≤ ~10 ms, wire `with_row_filter`");
    println!("  into FastParquet for selective single-column predicates.");
}
