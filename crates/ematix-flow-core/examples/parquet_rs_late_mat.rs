//! Σ.E2 day-3b: late-materialization probe via parquet-rs's
//! `with_row_filter`. The hypothesis: Polars's per-thread win comes
//! not from a faster decoder but from doing less work — reading the
//! filter column first, building a row mask, and only decoding the
//! matching rows of the projection columns.
//!
//! Q6 is the ideal test: it has a date+discount+quantity filter that
//! matches roughly 3% of rows. If late materialization works as
//! advertised, single-thread time should drop from ~55 ms (full
//! decode of 4 cols × 6M rows) to ~10-15 ms (decode of filter col +
//! 3% of three projection cols).
//!
//! If the result lands near Polars's 49 ms per-thread number, Polars
//! is NOT using late materialization here and the gap is in raw
//! decode speed. If it lands well below, late materialization is the
//! lever we should pull — and DataFusion's pushdown_filters is just
//! a poor implementation that we can replace with a custom
//! TableProvider.

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{ArrayRef, BooleanArray, RecordBatch};
use datafusion::arrow::array::{Date32Array, Float64Array};
use datafusion::arrow::compute::kernels::boolean::and;
use datafusion::arrow::compute::kernels::cmp::{gt_eq, lt, lt_eq};
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter,
};
use datafusion::parquet::schema::types::SchemaDescriptor;

const TRIALS: usize = 5;

fn median_of(times: &mut [f64]) -> (f64, f64, f64) {
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (times[times.len() / 2], times[0], times[times.len() - 1])
}

/// Q6 predicate, evaluated against the (l_shipdate, l_discount,
/// l_quantity) projection of the row group. Returns a BooleanArray
/// telling the reader which rows survive the filter.
///
/// Q6 SQL (TPC-H):
///   l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
///   AND l_discount BETWEEN 0.05 AND 0.07
///   AND l_quantity < 24
fn make_q6_predicate(schema_desc: &Arc<SchemaDescriptor>) -> Box<dyn ArrowPredicate> {
    // Project just the filter columns: l_quantity(4), l_discount(6), l_shipdate(10)
    let filter_proj = ProjectionMask::leaves(schema_desc, [4usize, 6, 10]);

    // Date constants: '1994-01-01' = 8766, '1995-01-01' = 9131 (days since epoch)
    let lo_date: i32 = 8766;
    let hi_date: i32 = 9131;

    Box::new(ArrowPredicateFn::new(
        filter_proj,
        move |batch: RecordBatch| {
            // Column order in the projected batch matches column index order,
            // which is l_quantity(0), l_discount(1), l_shipdate(2).
            let qty = batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let disc = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let ship = batch
                .column(2)
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap();

            // l_shipdate >= 1994-01-01 AND l_shipdate < 1995-01-01
            let lo_d = Date32Array::new_scalar(lo_date);
            let hi_d = Date32Array::new_scalar(hi_date);
            let ship_ge = gt_eq(ship, &lo_d).unwrap();
            let ship_lt = lt(ship, &hi_d).unwrap();
            let mask = and(&ship_ge, &ship_lt).unwrap();

            // discount BETWEEN 0.05 AND 0.07
            let lo_disc = Float64Array::new_scalar(0.05);
            let hi_disc = Float64Array::new_scalar(0.07);
            let disc_ge = gt_eq(disc, &lo_disc).unwrap();
            let disc_le = lt_eq(disc, &hi_disc).unwrap();
            let mask = and(&mask, &and(&disc_ge, &disc_le).unwrap()).unwrap();

            // quantity < 24
            let lo_q = Float64Array::new_scalar(24.0);
            let qty_lt = lt(qty, &lo_q).unwrap();
            let mask = and(&mask, &qty_lt).unwrap();
            Ok(mask)
        },
    ))
}

fn bench_no_filter(parquet: &str) -> f64 {
    let mut times = vec![0.0; TRIALS];
    for t in 0..TRIALS {
        let file = File::open(parquet).unwrap();
        let start = Instant::now();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let schema_desc: Arc<SchemaDescriptor> = builder.parquet_schema().clone().into();
        let mask = ProjectionMask::leaves(&schema_desc, [4usize, 5, 6, 10]);
        let reader = builder.with_projection(mask).build().unwrap();
        let _: Vec<RecordBatch> = reader.into_iter().map(|r| r.unwrap()).collect();
        times[t] = start.elapsed().as_secs_f64() * 1000.0;
    }
    let (med, _, _) = median_of(&mut times);
    med
}

fn bench_with_row_filter(parquet: &str) -> (f64, usize) {
    let mut times = vec![0.0; TRIALS];
    let mut rows_surviving = 0usize;
    for t in 0..TRIALS {
        let file = File::open(parquet).unwrap();
        let start = Instant::now();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let schema_desc: Arc<SchemaDescriptor> = builder.parquet_schema().clone().into();
        // Final projection: l_quantity(4), l_extendedprice(5), l_discount(6), l_shipdate(10)
        let final_proj = ProjectionMask::leaves(&schema_desc, [4usize, 5, 6, 10]);
        let predicate = make_q6_predicate(&schema_desc);
        let filter = RowFilter::new(vec![predicate]);

        let reader = builder
            .with_projection(final_proj)
            .with_row_filter(filter)
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.into_iter().map(|r| r.unwrap()).collect();
        times[t] = start.elapsed().as_secs_f64() * 1000.0;
        rows_surviving = batches.iter().map(|b| b.num_rows()).sum();
    }
    let (med, _, _) = median_of(&mut times);
    (med, rows_surviving)
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
    println!("==> Σ.E2 day-3b: parquet-rs `with_row_filter` (late materialization)");
    println!("==> data: {parquet}");
    println!();

    let no_filter = bench_no_filter(parquet);
    println!("  Q6 4-col, no filter (decodes all 6M rows)        median {no_filter:>6.2} ms");

    let (with_filter, rows) = bench_with_row_filter(parquet);
    println!(
        "  Q6 4-col, with_row_filter (Q6 predicate inline)  median {with_filter:>6.2} ms  surviving_rows={rows}"
    );

    println!();
    let speedup = no_filter / with_filter;
    println!("  Late-materialization speedup vs full scan: {speedup:.2}×");
    println!();
    println!("  References:");
    println!("    Polars single-thread Q6 4-col:  49.63 ms (no filter pushed)");
    println!("    Polars single-thread Q6 SQL:    ~10-15 ms (filter pushed via lazy frame)");
    println!(
        "    DataFusion Q6 default:           16.9 ms (14-thread, filter applied post-decode)"
    );
    println!("    DataFusion Q6 + pushdown_filters: 28.3 ms (14-thread, filter pushed to reader)");
}
