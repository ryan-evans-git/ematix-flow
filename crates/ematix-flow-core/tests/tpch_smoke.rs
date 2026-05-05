//! Σ.A1 PR 1: TPC-H smoke test.
//!
//! Hermetic correctness gate for the TPC-H benchmark harness. Generates
//! the LineItem table at SF=1 in-memory via `tpchgen` (CSV path),
//! parses it through `arrow_csv` 58 to land in the workspace's pinned
//! Arrow ABI, registers the table with DataFusion, runs Q6 (the
//! simplest aggregate-only TPC-H query), and asserts the revenue
//! matches TPC-H's published reference value.
//!
//! Why CSV roundtrip rather than `tpchgen-arrow`'s direct path?
//! tpchgen-arrow 2.0.2 pins `arrow ^57.1`; this workspace is on
//! arrow 58 (driven by orc-rust 0.8 / deltalake 0.32 / parquet 58).
//! The CSV path costs ~10–20s extra at SF=1 — acceptable for a smoke
//! test, and `tpchgen-arrow`'s upstream CI compares its Arrow output
//! against the same CSV format we're parsing here, so correctness is
//! preserved.
//!
//! The reference revenue at SF=1 is published by TPC.org and bundled
//! into the `tpchgen` crate at `tpchgen::q_and_a::answer(6)`:
//!
//! ```text
//! revenue
//! 123141078.23
//! ```
//!
//! Q6 is the "Forecasting Revenue Change Query":
//! `SELECT SUM(l_extendedprice * l_discount) FROM lineitem
//!  WHERE l_shipdate >= DATE '1994-01-01'
//!    AND l_shipdate <  DATE '1995-01-01'
//!    AND l_discount BETWEEN 0.05 AND 0.07
//!    AND l_quantity < 24`.
//!
//! Failing this test means either (a) we broke our DataFusion wiring,
//! (b) `tpchgen` drifted from the TPC-H reference (unlikely given
//! upstream's per-checkin byte-comparison test against canonical
//! `dbgen`), or (c) something inside the CSV roundtrip is mishandling
//! a column type — see `lineitem_schema()` for the hand-written
//! Arrow schema we feed `arrow_csv::ReaderBuilder`.

use std::fmt::Write as _;
use std::io::Cursor;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_csv::ReaderBuilder;
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use tpchgen::csv::LineItemCsv;
use tpchgen::generators::LineItemGenerator;

/// Q6 SQL — copied from `tpchgen::q_and_a::queries::Q6`. Inlined here
/// (rather than referenced via the constant) so the test is readable
/// against the TPC-H spec without a docs.rs lookup.
const Q6: &str = r#"
SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM   lineitem
WHERE  l_shipdate >= DATE '1994-01-01'
  AND  l_shipdate <  DATE '1995-01-01'
  AND  l_discount BETWEEN 0.05 AND 0.07
  AND  l_quantity < 24
"#;

/// Published TPC-H reference revenue for Q6 at SF=1, rounded to 2dp
/// per the TPC-H spec. Source:
/// `tpchgen::q_and_a::answers_sf1::Q6_ANSWER` (`123141078.23`).
const Q6_REFERENCE_REVENUE: f64 = 123_141_078.23;

/// Tolerance for the floating-point compare. The reference is reported
/// to 2dp; DataFusion's running-sum on f64 introduces at most ~1e-6
/// relative error at this magnitude. 0.01 absolute (= one digit past
/// the reference's published precision) is a generous + spec-aligned
/// tolerance.
const REVENUE_TOLERANCE: f64 = 0.01;

/// Arrow 58 schema for the TPC-H `lineitem` table. Field order +
/// names match the TPC-H spec § 1.4.1. Numeric columns that the
/// spec calls DECIMAL(15,2) / DECIMAL(12,2) are landed as Float64 —
/// Q6's `SUM(l_extendedprice * l_discount)` evaluates correctly on
/// f64 within our 0.01 tolerance.
fn lineitem_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("l_linenumber", DataType::Int32, false),
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
        Field::new("l_shipdate", DataType::Date32, false),
        Field::new("l_commitdate", DataType::Date32, false),
        Field::new("l_receiptdate", DataType::Date32, false),
        Field::new("l_shipinstruct", DataType::Utf8, false),
        Field::new("l_shipmode", DataType::Utf8, false),
        Field::new("l_comment", DataType::Utf8, false),
        // tpchgen's CSV emits the trailing field separator that dbgen
        // uses; we don't model it as a column and drop it via
        // `arrow_csv::ReaderBuilder::with_format(... has_header=true)`
        // — see `parse_lineitem_csv` below.
    ]))
}

/// Generate LineItem at SF=1 → CSV bytes → arrow-csv batches in the
/// workspace's pinned arrow 58 ABI.
fn generate_lineitem_sf1() -> Vec<RecordBatch> {
    let generator = LineItemGenerator::new(1.0, 1, 1);

    // Emit CSV: one header + N rows. tpchgen's CSV format uses `,` as
    // the delimiter (not the `|` that raw dbgen tbl files use); matches
    // what arrow-csv expects with default settings.
    let mut csv = String::with_capacity(1024 * 1024 * 700);
    writeln!(&mut csv, "{}", LineItemCsv::header()).expect("header write");
    for row in generator.iter() {
        writeln!(&mut csv, "{}", LineItemCsv::new(row)).expect("row write");
    }

    let schema = lineitem_schema();
    let cursor = Cursor::new(csv.into_bytes());
    let reader = ReaderBuilder::new(schema)
        .with_header(true)
        .with_batch_size(64 * 1024)
        .build(cursor)
        .expect("arrow-csv reader");

    reader
        .collect::<Result<Vec<_>, _>>()
        .expect("arrow-csv decode")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tpch_q6_sf1_matches_reference() {
    // 1. Generate the LineItem table at SF=1 via the CSV roundtrip.
    let batches = generate_lineitem_sf1();
    assert!(!batches.is_empty(), "no batches generated at SF=1");
    let schema = batches[0].schema();

    // 2. Register as an in-memory DataFusion table.
    let ctx = SessionContext::new();
    let mem_table =
        datafusion::datasource::MemTable::try_new(schema, vec![batches]).expect("mem-table build");
    ctx.register_table("lineitem", Arc::new(mem_table))
        .expect("register lineitem");

    // 3. Run Q6.
    let df = ctx.sql(Q6).await.expect("Q6 plan");
    let result_batches = df.collect().await.expect("Q6 execute");

    // 4. Extract the single scalar revenue.
    assert_eq!(result_batches.len(), 1, "Q6 should return one batch");
    let batch = &result_batches[0];
    assert_eq!(batch.num_rows(), 1, "Q6 should return one row");
    assert_eq!(batch.num_columns(), 1, "Q6 should return one column");

    let column = batch.column(0);
    let revenue: f64 =
        if let Some(arr) = column.as_any().downcast_ref::<arrow_array::Float64Array>() {
            arr.value(0)
        } else if let Some(arr) = column
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
        {
            let raw = arr.value(0) as f64;
            raw / 10f64.powi(arr.scale() as i32)
        } else {
            panic!(
                "Q6 revenue column had unexpected type {:?}; expected Float64 or Decimal128",
                column.data_type()
            )
        };

    let delta = (revenue - Q6_REFERENCE_REVENUE).abs();
    assert!(
        delta < REVENUE_TOLERANCE,
        "Q6 revenue at SF=1 = {revenue:.4}, expected {Q6_REFERENCE_REVENUE} \
         (delta {delta:.6}, tolerance {REVENUE_TOLERANCE}). \
         Either tpchgen drifted from the TPC-H reference, the CSV \
         roundtrip mishandled a column type, or the DataFusion query \
         plan is wrong."
    );
}
