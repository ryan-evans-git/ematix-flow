//! Σ.E5.1 — `EmatArrowBatchReader` vs `ParquetRecordBatchReaderBuilder`
//! on the Q1 column projection.
//!
//! Reads the Q1 columns from `lineitem.parquet` (SF=1 by default,
//! configurable via `TPCH_DATA_DIR`) — `l_returnflag` + `l_linestatus`
//! as `Utf8View`, plus `l_quantity / l_extendedprice / l_discount /
//! l_tax / l_shipdate` as their natural numeric / date types — and
//! reports ms-to-collect-all-batches side-by-side.
//!
//! Both readers are configured with the same supplied Arrow schema
//! (Utf8View on the strings) so the comparison isolates the
//! decode-pipeline shape, not the type the strings end up as.
//!
//! Σ.E5.2 perf target: within ±5 % of parquet-rs on this projection.
//! A slower reader still ships as the foundation for E5.4 (operator
//! wire-up) — see `docs/PHASE_SIGMA_E5_PARQUET_RS_ELIMINATION.md`.
//!
//! Usage:
//!   cargo run --release -p ematix-flow-core \
//!     --example sigma_e5_1_arrow_reader_bench
//!   TPCH_DATA_DIR=.../sf10 cargo run --release -p ematix-flow-core \
//!     --example sigma_e5_1_arrow_reader_bench
//!
//! Env knobs:
//!   `EMAT_E5_TRIALS`   number of timed trials per side (default 5)
//!   `EMAT_E5_WARMUPS`  warm-up runs ignored before timing (default 2)

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::arrow::arrow_reader::{
    ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};

use ematix_flow_core::emat_arrow_reader::{DEFAULT_BATCH_SIZE, EmatArrowBatchReaderBuilder};
use ematix_parquet_io::ParquetFile;

/// Q1 column names + their declared Arrow types (the supplied-schema
/// shape that drives Utf8View / typed promotion on both readers).
fn q1_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        // INT64 → Int64 (Q1 sums l_quantity / l_extendedprice as numerics)
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
        Field::new("l_returnflag", DataType::Utf8View, false),
        Field::new("l_linestatus", DataType::Utf8View, false),
        Field::new("l_shipdate", DataType::Date32, false),
    ]))
}

const Q1_COLS: &[&str] = &[
    "l_quantity",
    "l_extendedprice",
    "l_discount",
    "l_tax",
    "l_returnflag",
    "l_linestatus",
    "l_shipdate",
];

fn lineitem_path() -> Option<PathBuf> {
    if let Ok(s) = std::env::var("TPCH_DATA_DIR") {
        let p = PathBuf::from(s).join("lineitem.parquet");
        if p.exists() {
            return Some(p);
        }
    }
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    if let Some(real) = manifest
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.join("examples/tpch/data/sf1/lineitem.parquet"))
    {
        if real.exists() {
            return Some(real);
        }
    }
    None
}

fn trials() -> usize {
    std::env::var("EMAT_E5_TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5)
}

fn warmups() -> usize {
    std::env::var("EMAT_E5_WARMUPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2)
}

fn median(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    if xs.is_empty() {
        return 0.0;
    }
    let mid = xs.len() / 2;
    if xs.len() % 2 == 0 {
        (xs[mid - 1] + xs[mid]) / 2.0
    } else {
        xs[mid]
    }
}

fn run_emat(path: &std::path::Path, schema: SchemaRef, leaves: &[usize]) -> usize {
    let file = ParquetFile::open(path).expect("open ematix-parquet");
    let reader = EmatArrowBatchReaderBuilder::new(file, schema)
        .with_projection(leaves.to_vec())
        .with_batch_size(DEFAULT_BATCH_SIZE)
        .build()
        .expect("emat reader build");
    let mut total = 0usize;
    for b in reader {
        let b = b.expect("emat batch");
        total += b.num_rows();
    }
    total
}

fn run_parquet_rs(path: &std::path::Path, leaves: &[usize]) -> usize {
    let file = std::fs::File::open(path).expect("open file");
    // First open without a schema hint to learn the file's natural
    // Arrow schema, then promote Utf8/Binary → Utf8View/BinaryView
    // exactly the way FastParquetTableProvider does — that's the
    // shape the Q1 column projection sees in production.
    let probe = ParquetRecordBatchReaderBuilder::try_new(file).expect("parquet-rs probe");
    let promoted = promote_to_view_types(probe.schema().as_ref());

    let file = std::fs::File::open(path).expect("open file (2)");
    let options = ArrowReaderOptions::new().with_schema(Arc::new(promoted));
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)
        .expect("parquet-rs builder");
    let mask = ProjectionMask::leaves(builder.parquet_schema(), leaves.iter().copied());
    let reader = builder
        .with_projection(mask)
        .with_batch_size(DEFAULT_BATCH_SIZE)
        .build()
        .expect("parquet-rs reader build");
    let mut total = 0usize;
    for b in reader {
        let b: RecordBatch = b.expect("parquet-rs batch");
        total += b.num_rows();
    }
    total
}

/// Mirror of `fast_parquet::promote_to_view_types` — re-implemented
/// locally so the bench doesn't depend on a private helper.
fn promote_to_view_types(schema: &Schema) -> Schema {
    let promoted: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            let new_type = match f.data_type() {
                DataType::Utf8 | DataType::LargeUtf8 => DataType::Utf8View,
                DataType::Binary | DataType::LargeBinary => DataType::BinaryView,
                other => other.clone(),
            };
            Field::new(f.name(), new_type, f.is_nullable()).with_metadata(f.metadata().clone())
        })
        .collect();
    Schema::new_with_metadata(promoted, schema.metadata().clone())
}

/// Resolve the parquet leaf indices for `Q1_COLS` from the first row
/// group's column metadata. The parquet schema is flat for TPC-H —
/// one leaf per top-level field — so `col` from `RowGroup.columns`
/// is the leaf index, and `cm.path_in_schema` is `[<name>]`.
fn resolve_q1_leaves(path: &std::path::Path) -> Vec<usize> {
    let file = ParquetFile::open(path).unwrap();
    let md = file.metadata().unwrap();
    let rg0 = &md.row_groups[0];
    let want: std::collections::HashMap<&str, usize> =
        Q1_COLS.iter().enumerate().map(|(i, &n)| (n, i)).collect();
    let mut found: Vec<Option<usize>> = vec![None; Q1_COLS.len()];
    for (col_idx, c) in rg0.columns.iter().enumerate() {
        let cm = c.meta_data.as_ref().expect("meta_data");
        // Single-segment path for flat schemas; pick the last segment
        // (would be the leaf name even if nested).
        let last = cm.path_in_schema.last().expect("path_in_schema");
        let name = std::str::from_utf8(last).expect("utf8 col name");
        if let Some(&pos) = want.get(name) {
            found[pos] = Some(col_idx);
        }
    }
    found
        .into_iter()
        .enumerate()
        .map(|(i, o)| o.unwrap_or_else(|| panic!("missing Q1 column `{}`", Q1_COLS[i])))
        .collect()
}

fn main() {
    let Some(path) = lineitem_path() else {
        eprintln!(
            "no lineitem.parquet: set TPCH_DATA_DIR or place \
             examples/tpch/data/sf1/lineitem.parquet"
        );
        std::process::exit(2);
    };
    println!("file: {}", path.display());

    let schema = q1_schema();
    let leaves = resolve_q1_leaves(&path);
    println!("Q1 leaves (in supplied schema order): {leaves:?}");

    let trials = trials();
    let warmups = warmups();
    println!("trials={trials} warmups={warmups}");

    // Sanity: both readers should produce the same row count.
    let n_emat = run_emat(&path, schema.clone(), &leaves);
    let n_pr = run_parquet_rs(&path, &leaves);
    println!("row count sanity: emat={n_emat} parquet-rs={n_pr}");
    assert_eq!(n_emat, n_pr, "row counts differ — bench invalid");

    let mut emat_ms = Vec::with_capacity(trials);
    let mut pr_ms = Vec::with_capacity(trials);
    // Warmups (alternate).
    for _ in 0..warmups {
        let _ = run_emat(&path, schema.clone(), &leaves);
        let _ = run_parquet_rs(&path, &leaves);
    }
    // Timed.
    for _ in 0..trials {
        let t0 = Instant::now();
        let _ = run_emat(&path, schema.clone(), &leaves);
        emat_ms.push(t0.elapsed().as_secs_f64() * 1e3);
        let t1 = Instant::now();
        let _ = run_parquet_rs(&path, &leaves);
        pr_ms.push(t1.elapsed().as_secs_f64() * 1e3);
    }

    let m_emat = median(emat_ms.clone());
    let m_pr = median(pr_ms.clone());
    let ratio = m_emat / m_pr;
    println!();
    println!("  reader          median ms     trials");
    println!("  --------------  ----------    ---------------");
    println!(
        "  EmatArrowBatch  {:>8.3}     {:?}",
        m_emat,
        emat_ms
            .iter()
            .map(|x| format!("{x:.2}"))
            .collect::<Vec<_>>()
    );
    println!(
        "  parquet-rs      {:>8.3}     {:?}",
        m_pr,
        pr_ms.iter().map(|x| format!("{x:.2}")).collect::<Vec<_>>()
    );
    println!();
    println!(
        "  ratio emat / parquet-rs = {ratio:.3}  ({:+.1}% vs parquet-rs)",
        (ratio - 1.0) * 100.0
    );
    if (ratio - 1.0).abs() <= 0.05 {
        println!("  WITHIN ±5% gate — Σ.E5.2 target met");
    } else if ratio < 1.0 {
        println!("  faster than parquet-rs (gate met)");
    } else {
        println!("  slower than parquet-rs by {:.1}%", (ratio - 1.0) * 100.0);
    }
}
