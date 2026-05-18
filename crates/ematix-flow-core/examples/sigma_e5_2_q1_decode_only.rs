//! Σ.E5.2 diagnostic: decode-only wall-clock for Q1's 7 columns.
//!
//! Two paths timed end-to-end, mimicking what each provider does at
//! scan time:
//!
//!   * **Bridge path** (what EmatixFastParquet calls): iterate the 6
//!     row groups; per-RG, call `decode_column_chunk_*` from
//!     `ematix_parquet_bridge` for each column; build one `RecordBatch`
//!     per RG. This includes the per-RG `ParquetFile::open` cost for
//!     byte-array columns inside the bridge.
//!
//!   * **parquet-rs path** (what FastParquet calls): one
//!     `ParquetRecordBatchReaderBuilder` per scan, `batch_size = 65_536`,
//!     iterate batches until done.
//!
//! Q1 projection: `l_quantity, l_extendedprice, l_discount, l_tax,
//! l_returnflag, l_linestatus, l_shipdate` (indices computed from the
//! reported schema).

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema, SchemaRef};

use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};

use ematix_flow_core::ematix_parquet_bridge::{
    decode_column_chunk_byte_array, decode_column_chunk_byte_array_dict_preserved,
    decode_column_chunk_f64, decode_column_chunk_i32, decode_column_chunk_i64,
};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TRIALS: usize = 15;
const WARMUPS: usize = 3;

const Q1_COLS: &[&str] = &[
    "l_quantity",
    "l_extendedprice",
    "l_discount",
    "l_tax",
    "l_returnflag",
    "l_linestatus",
    "l_shipdate",
];

fn data_path() -> String {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    match std::env::var("TPCH_DATA_DIR") {
        Ok(s) => format!("{s}/lineitem.parquet"),
        Err(_) => manifest
            .parent().unwrap().parent().unwrap()
            .join("examples/tpch/data/sf1/lineitem.parquet")
            .to_string_lossy().into_owned(),
    }
}

fn median(times: &mut [f64]) -> f64 {
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    times[times.len() / 2]
}
fn stdev(times: &[f64], mean: f64) -> f64 {
    let n = times.len();
    let var = times.iter().map(|t| (t - mean).powi(2)).sum::<f64>() / (n - 1) as f64;
    var.sqrt()
}

fn print_stats(label: &str, times: &mut Vec<f64>) {
    let med = median(times);
    let mean = times.iter().sum::<f64>() / times.len() as f64;
    let sd = stdev(times, mean);
    println!("  {label:<50}  median {med:>6.2} ms ± {sd:>5.2}");
}

// --- Bridge path: 1 RG at a time, all 7 columns, build a RecordBatch.
fn bridge_decode_rg(
    path: &std::path::Path,
    rg: usize,
    schema: &SchemaRef,
    parquet_indices: &[usize],
    dict_preserve: bool,
) -> RecordBatch {
    let mut cols: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(parquet_indices.len());
    for (out_idx, &pq_idx) in parquet_indices.iter().enumerate() {
        let dt = schema.field(out_idx).data_type();
        let arr: Arc<dyn arrow_array::Array> = match dt {
            DataType::Int32 => decode_column_chunk_i32(path, rg, pq_idx).unwrap() as Arc<dyn arrow_array::Array>,
            DataType::Date32 => {
                let i = decode_column_chunk_i32(path, rg, pq_idx).unwrap();
                let v: Vec<i32> = i.values().to_vec();
                Arc::new(arrow_array::Date32Array::from(v))
            }
            DataType::Int64 => decode_column_chunk_i64(path, rg, pq_idx).unwrap() as Arc<dyn arrow_array::Array>,
            DataType::Float64 => decode_column_chunk_f64(path, rg, pq_idx).unwrap() as Arc<dyn arrow_array::Array>,
            DataType::Utf8 | DataType::Dictionary(_, _) => {
                if dict_preserve {
                    decode_column_chunk_byte_array_dict_preserved(path, rg, pq_idx).unwrap()
                        as Arc<dyn arrow_array::Array>
                } else {
                    decode_column_chunk_byte_array(path, rg, pq_idx).unwrap()
                        as Arc<dyn arrow_array::Array>
                }
            }
            other => panic!("unexpected dtype {other:?}"),
        };
        cols.push(arr);
    }
    RecordBatch::try_new(schema.clone(), cols).unwrap()
}

fn time_bridge(path: &std::path::Path, schema: &SchemaRef, pq_idx: &[usize], rgs: usize, dict_preserve: bool) -> Vec<f64> {
    let mut times = Vec::with_capacity(TRIALS);
    for _ in 0..WARMUPS {
        for rg in 0..rgs {
            std::hint::black_box(bridge_decode_rg(path, rg, schema, pq_idx, dict_preserve));
        }
    }
    for _ in 0..TRIALS {
        let start = Instant::now();
        for rg in 0..rgs {
            std::hint::black_box(bridge_decode_rg(path, rg, schema, pq_idx, dict_preserve));
        }
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    times
}

fn time_parquet_rs(path: &str, pq_idx: &[usize], force_utf8view: bool) -> Vec<f64> {
    use arrow_schema::Field;
    // Preload metadata once with the optional Utf8View promotion so each
    // trial does only open + decode (parity with FastParquet's
    // partition stream which uses cached metadata).
    let file = File::open(path).unwrap();
    let pre_meta = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new()).unwrap();
    let base_schema = pre_meta.schema().clone();
    let opts = if force_utf8view {
        let new_fields = base_schema.fields().iter().map(|f| {
            let dt = match f.data_type() {
                DataType::Utf8 | DataType::LargeUtf8 => DataType::Utf8View,
                other => other.clone(),
            };
            Arc::new(Field::new(f.name(), dt, f.is_nullable()))
        }).collect::<Vec<_>>();
        ArrowReaderOptions::new().with_schema(Arc::new(Schema::new(new_fields)))
    } else {
        ArrowReaderOptions::new()
    };
    let file = File::open(path).unwrap();
    let meta = ArrowReaderMetadata::load(&file, opts).unwrap();
    let parquet_schema = meta.parquet_schema().clone();

    let mut times = Vec::with_capacity(TRIALS);
    for _ in 0..WARMUPS {
        let file = File::open(path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, meta.clone());
        let mask = ProjectionMask::leaves(&parquet_schema, pq_idx.iter().copied());
        let reader = builder.with_projection(mask).with_batch_size(65_536).build().unwrap();
        for b in reader {
            std::hint::black_box(b.unwrap());
        }
    }
    for _ in 0..TRIALS {
        let start = Instant::now();
        let file = File::open(path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, meta.clone());
        let mask = ProjectionMask::leaves(&parquet_schema, pq_idx.iter().copied());
        let reader = builder.with_projection(mask).with_batch_size(65_536).build().unwrap();
        for b in reader {
            std::hint::black_box(b.unwrap());
        }
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    times
}

fn main() {
    let path = data_path();
    println!("==> Σ.E5.2 decode-only: Q1 7-col projection, all 6 RGs");
    println!("==> data: {path}");
    println!("==> {TRIALS}-trial median after {WARMUPS} warm-ups\n");

    // Resolve column indices + build a schema for the bridge path.
    let file = File::open(&path).unwrap();
    let pre = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new()).unwrap();
    let full_schema = pre.schema().clone();
    let pq_indices: Vec<usize> = Q1_COLS.iter().map(|n| full_schema.index_of(n).unwrap()).collect();
    let projected_schema_utf8 = Arc::new(full_schema.project(&pq_indices).unwrap());
    let dict_fields: Vec<_> = projected_schema_utf8.fields().iter().map(|f| {
        if matches!(f.data_type(), DataType::Utf8) {
            Arc::new(arrow_schema::Field::new(
                f.name(),
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                f.is_nullable(),
            ))
        } else { f.clone() }
    }).collect();
    let projected_schema_dict = Arc::new(Schema::new(dict_fields));
    let num_rgs = {
        let f = File::open(&path).unwrap();
        let r = datafusion::parquet::file::serialized_reader::SerializedFileReader::new(f).unwrap();
        use datafusion::parquet::file::reader::FileReader;
        r.metadata().num_row_groups()
    };
    println!("  num_row_groups = {num_rgs}");
    println!("  Q1 columns = {Q1_COLS:?}");
    println!("  parquet leaf indices = {pq_indices:?}\n");

    let p = std::path::Path::new(&path);

    let mut t_bridge_utf8 = time_bridge(p, &projected_schema_utf8, &pq_indices, num_rgs, false);
    let mut t_bridge_dict = time_bridge(p, &projected_schema_dict, &pq_indices, num_rgs, true);
    let mut t_pqrs_utf8 = time_parquet_rs(&path, &pq_indices, false);
    let mut t_pqrs_utf8view = time_parquet_rs(&path, &pq_indices, true);

    println!("--- Decode-only wall-clock ---");
    print_stats("Bridge (Utf8, all RGs sequential)", &mut t_bridge_utf8);
    print_stats("Bridge (Dict, all RGs sequential)", &mut t_bridge_dict);
    print_stats("parquet-rs (Utf8, batch=65536)", &mut t_pqrs_utf8);
    print_stats("parquet-rs (Utf8View, batch=65536)", &mut t_pqrs_utf8view);

    let b_utf8 = median(&mut t_bridge_utf8);
    let b_dict = median(&mut t_bridge_dict);
    let pq_v = median(&mut t_pqrs_utf8view);
    println!();
    println!("  bridge(Utf8) − parquet-rs(Utf8View) = {:+.2} ms  ({:+.1}%)",
        b_utf8 - pq_v, 100.0 * (b_utf8 - pq_v) / pq_v);
    println!("  bridge(Dict) − parquet-rs(Utf8View) = {:+.2} ms  ({:+.1}%)",
        b_dict - pq_v, 100.0 * (b_dict - pq_v) / pq_v);
}
