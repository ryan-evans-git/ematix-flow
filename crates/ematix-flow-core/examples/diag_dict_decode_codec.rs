//! Σ.E5.2b — diagnostic 2: codec-only isolation.
//!
//! Strips off the provider / planner / Arrow-stream surface and times
//! the pure decode of `l_returnflag` (the canonical low-cardinality
//! dict-encoded BYTE_ARRAY column) across all row groups, for both
//! engines:
//!
//!   * **ematix-parquet**: `read_column_byte_array_dict_preserved`
//!     directly — the same call EmatixFastParquet's streaming reader
//!     and bridge make.
//!   * **parquet-rs**: `ParquetRecordBatchReaderBuilder::try_new_with_options`
//!     with a `Dictionary(UInt32, Utf8)` supplied schema and a
//!     `ProjectionMask::leaves` that selects only `l_returnflag`. This
//!     activates parquet-rs's `byte_array_dictionary` reader (the same
//!     path FastParquet+with_dict_preservation goes through).
//!
//! The output table is what tells us where the gap lives.
//!
//! Run:
//!     cargo run --release -p ematix-flow-core --example diag_dict_decode_codec

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::arrow::arrow_reader::{
    ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use ematix_parquet_codec::read::read_column_byte_array_dict_preserved;
use ematix_parquet_io::ParquetFile;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TRIALS: usize = 21;
const WARMUPS: usize = 3;

// `l_returnflag` is column 8 in lineitem (see
// ematix_parquet_bridge.rs:821).
const L_RETURNFLAG_COL: usize = 8;
const L_RETURNFLAG_NAME: &str = "l_returnflag";

fn stats(xs: &[f64]) -> (f64, f64, f64) {
    let mut s = xs.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = s[s.len() / 2];
    let p95 = s[(s.len() as f64 * 0.95) as usize];
    let mean = s.iter().sum::<f64>() / s.len() as f64;
    let var = s.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / s.len() as f64;
    (median, var.sqrt(), p95)
}

/// ematix-parquet: dict-preserved decode of one column across all RGs.
/// This is exactly what `decode_byte_array_to_string_view` (default
/// streaming path) and `decode_column_chunk_byte_array_dict_preserved`
/// (explicit dict-preserve bridge path) call, minus the Arrow assembly.
fn emat_decode_all_rgs(path: &str) -> (f64, usize, usize) {
    let t0 = Instant::now();
    let file = ParquetFile::open(path).unwrap();
    let md = file.metadata().unwrap();
    let num_rgs = md.row_groups.len();
    let mut total_rows = 0usize;
    for rg in 0..num_rgs {
        let col = read_column_byte_array_dict_preserved(&file, rg, L_RETURNFLAG_COL).unwrap();
        total_rows += col.indices.len();
    }
    (t0.elapsed().as_secs_f64() * 1000.0, num_rgs, total_rows)
}

/// parquet-rs: dict-preserved decode of one column across all RGs.
/// Uses `ArrowReaderOptions::with_schema` to force the supplied schema
/// to `Dictionary(UInt32, Utf8)` for `l_returnflag` — which activates
/// parquet-rs's `byte_array_dictionary` decode path (same code FastParquet
/// + with_dict_preservation goes through).
fn parquet_rs_decode_all_rgs(path: &str) -> (f64, usize, usize) {
    let t0 = Instant::now();
    let file = File::open(path).unwrap();
    // Read footer first to discover schema + row-group count.
    let no_opts = ArrowReaderOptions::new();
    let builder0 =
        ParquetRecordBatchReaderBuilder::try_new_with_options(file.try_clone().unwrap(), no_opts)
            .unwrap();
    let parquet_schema = builder0.parquet_schema().clone();
    let arrow_schema_raw = builder0.schema().clone();
    let num_rgs = builder0.metadata().num_row_groups();
    drop(builder0);

    // Build a "dict-promoted" supplied schema covering all columns of
    // the parquet (parquet-rs requires schemas to match leaf count),
    // promoting `l_returnflag` to Dictionary(UInt32, Utf8).
    let fields: Vec<Field> = arrow_schema_raw
        .fields()
        .iter()
        .map(|f| {
            if f.name() == L_RETURNFLAG_NAME {
                Field::new(
                    f.name(),
                    DataType::Dictionary(
                        Box::new(DataType::UInt32),
                        Box::new(f.data_type().clone()),
                    ),
                    f.is_nullable(),
                )
            } else {
                f.as_ref().clone()
            }
        })
        .collect();
    let supplied = Arc::new(Schema::new_with_metadata(
        fields,
        arrow_schema_raw.metadata().clone(),
    ));

    let opts = ArrowReaderOptions::new().with_schema(supplied);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new_with_options(file.try_clone().unwrap(), opts)
            .unwrap();
    // Find the leaf index for l_returnflag.
    let leaf_idx = (0..parquet_schema.num_columns())
        .find(|&i| parquet_schema.column(i).name() == L_RETURNFLAG_NAME)
        .unwrap();
    let mask = ProjectionMask::leaves(&parquet_schema, [leaf_idx]);
    let reader = builder
        .with_projection(mask)
        .with_batch_size(65_536)
        .build()
        .unwrap();

    let mut total_rows = 0usize;
    for batch in reader {
        let b = batch.unwrap();
        total_rows += b.num_rows();
        // Sanity: the column should be a Dictionary(UInt32, Utf8).
        let arr = b.column(0);
        debug_assert!(matches!(arr.data_type(), DataType::Dictionary(_, _)));
    }
    (t0.elapsed().as_secs_f64() * 1000.0, num_rgs, total_rows)
}

fn main() {
    let dir = std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf1".into());
    let path: PathBuf = PathBuf::from(&dir).join("lineitem.parquet");
    assert!(
        path.exists(),
        "lineitem.parquet not found at {}; set TPCH_DATA_DIR",
        path.display()
    );
    let path_s = path.to_string_lossy().to_string();

    println!("=== Σ.E5.2b diag 2: codec-only isolation, l_returnflag ===");
    println!("file: {}", path_s);
    println!("trials: {TRIALS} (warmups {WARMUPS})\n");

    // ---- ematix-parquet ----
    let mut emat: Vec<f64> = Vec::with_capacity(TRIALS);
    let mut num_rgs_e = 0;
    let mut rows_e = 0;
    for t in 0..(TRIALS + WARMUPS) {
        let (ms, n_rg, n_rows) = emat_decode_all_rgs(&path_s);
        if t >= WARMUPS {
            emat.push(ms);
            num_rgs_e = n_rg;
            rows_e = n_rows;
        }
    }

    // ---- parquet-rs ----
    let mut prs: Vec<f64> = Vec::with_capacity(TRIALS);
    let mut num_rgs_p = 0;
    let mut rows_p = 0;
    for t in 0..(TRIALS + WARMUPS) {
        let (ms, n_rg, n_rows) = parquet_rs_decode_all_rgs(&path_s);
        if t >= WARMUPS {
            prs.push(ms);
            num_rgs_p = n_rg;
            rows_p = n_rows;
        }
    }

    assert_eq!(num_rgs_e, num_rgs_p, "RG count mismatch");
    assert_eq!(rows_e, rows_p, "row count mismatch");

    let (e_med, e_sd, e_p95) = stats(&emat);
    let (p_med, p_sd, p_p95) = stats(&prs);

    println!("rg_count: {num_rgs_e}");
    println!("rows: {rows_e}\n");
    println!("ematix-parquet  median ± σ (p95): {e_med:7.3} ± {e_sd:5.3} ms ({e_p95:7.3} ms)");
    println!("parquet-rs      median ± σ (p95): {p_med:7.3} ± {p_sd:5.3} ms ({p_p95:7.3} ms)");
    let delta = 100.0 * (e_med - p_med) / p_med;
    println!("Δ (emat / parquet-rs): {delta:+.1}%");
    let ns_per_row_emat = 1.0e6 * e_med / rows_e as f64;
    let ns_per_row_prs = 1.0e6 * p_med / rows_p as f64;
    println!(
        "ns/row: emat = {ns_per_row_emat:.2}  parquet-rs = {ns_per_row_prs:.2}  Δ = {:+.2} ns",
        ns_per_row_emat - ns_per_row_prs
    );
}
