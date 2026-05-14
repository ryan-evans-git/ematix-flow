//! Phase 3.5: SQL Q14-shape bench across three providers.
//!
//! All three paths run the SAME SQL against a primitive-only
//! lineitem-shaped table (l_shipdate + l_partkey + l_extendedprice +
//! l_discount). The synthetic file is built once from the real SF=1
//! lineitem and cached at `$TMP/lineitem_q14_columns.parquet`. We
//! exclude the part join here — that's identical across paths and
//! would dilute the signal. The SQL measures lineitem decode +
//! shipdate filter + revenue aggregate, which is the portion the
//! Phase 3 fused-NEON pushdown accelerates.
//!
//! Q14-shape SQL:
//!   SELECT SUM(l_extprice * (1 - l_discount)) AS rev, COUNT(*) AS m
//!   FROM lineitem
//!   WHERE l_shipdate >= DATE '1995-09-01'
//!     AND l_shipdate < DATE '1995-10-01'
//!
//! Paths timed:
//!   1. register_parquet         — DataFusion default (parquet-rs)
//!   2. FastParquetTableProvider — ematix-flow row-group-parallel
//!   3. EmatixFastParquetProvider — Phase 3 with Date32 pushdown
//!
//! Usage:
//!   TPCH_DATA_DIR=examples/tpch/data/sf1 \
//!     cargo run --release --example tpch_q14_emat_provider_bench

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

use parquet::basic::{Compression, ConvertedType, Repetition, Type as PhysicalType};
use parquet::column::reader::ColumnReader;
use parquet::column::writer::ColumnWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::Type as PType;

const WARMUPS: usize = 3;
const ITERS: usize = 15;

const SQL: &str = "SELECT \
    SUM(l_extendedprice * (1 - l_discount)) AS rev, \
    COUNT(*) AS matches \
    FROM lineitem \
    WHERE l_shipdate >= DATE '1995-09-01' \
      AND l_shipdate < DATE '1995-10-01'";

fn data_dir() -> PathBuf {
    match std::env::var("TPCH_DATA_DIR") {
        Ok(s) => PathBuf::from(s),
        Err(_) => PathBuf::from("examples/tpch/data/sf1"),
    }
}

/// Build (or reuse) a primitive-only lineitem file at /tmp containing
/// just the Q14-relevant columns. Cached so the bench's setup cost
/// doesn't pollute the first iteration of the first path.
fn ensure_synthetic_file(real_path: &PathBuf) -> PathBuf {
    let cache = std::env::temp_dir().join("lineitem_q14_columns.parquet");
    if cache.exists() {
        return cache;
    }
    eprintln!(
        "Building primitive-only synthetic file at {}",
        cache.display()
    );

    let r = SerializedFileReader::new(File::open(real_path).unwrap()).unwrap();
    let mut shipdate: Vec<i32> = Vec::new();
    let mut partkey: Vec<i64> = Vec::new();
    let mut extprice: Vec<f64> = Vec::new();
    let mut discount: Vec<f64> = Vec::new();
    for rg in 0..r.metadata().num_row_groups() {
        let rgr = r.get_row_group(rg).unwrap();
        let n = rgr.metadata().num_rows() as usize;
        {
            let mut t = match rgr.get_column_reader(10).unwrap() {
                ColumnReader::Int32ColumnReader(t) => t,
                _ => panic!(),
            };
            t.read_records(n, None, None, &mut shipdate).unwrap();
        }
        {
            let mut t = match rgr.get_column_reader(1).unwrap() {
                ColumnReader::Int64ColumnReader(t) => t,
                _ => panic!(),
            };
            t.read_records(n, None, None, &mut partkey).unwrap();
        }
        {
            let mut t = match rgr.get_column_reader(5).unwrap() {
                ColumnReader::DoubleColumnReader(t) => t,
                _ => panic!(),
            };
            t.read_records(n, None, None, &mut extprice).unwrap();
        }
        {
            let mut t = match rgr.get_column_reader(6).unwrap() {
                ColumnReader::DoubleColumnReader(t) => t,
                _ => panic!(),
            };
            t.read_records(n, None, None, &mut discount).unwrap();
        }
    }

    let schema = Arc::new(
        PType::group_type_builder("schema")
            .with_fields(vec![
                Arc::new(
                    PType::primitive_type_builder("l_shipdate", PhysicalType::INT32)
                        .with_repetition(Repetition::REQUIRED)
                        .with_converted_type(ConvertedType::DATE)
                        .build()
                        .unwrap(),
                ),
                Arc::new(
                    PType::primitive_type_builder("l_partkey", PhysicalType::INT64)
                        .with_repetition(Repetition::REQUIRED)
                        .build()
                        .unwrap(),
                ),
                Arc::new(
                    PType::primitive_type_builder("l_extendedprice", PhysicalType::DOUBLE)
                        .with_repetition(Repetition::REQUIRED)
                        .build()
                        .unwrap(),
                ),
                Arc::new(
                    PType::primitive_type_builder("l_discount", PhysicalType::DOUBLE)
                        .with_repetition(Repetition::REQUIRED)
                        .build()
                        .unwrap(),
                ),
            ])
            .build()
            .unwrap(),
    );
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build(),
    );
    let file = File::create(&cache).unwrap();
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
    // Split into 6 row groups matching the real file's shape.
    let total = shipdate.len();
    let rg_size = (total + 5) / 6;
    for chunk in (0..total).step_by(rg_size) {
        let end = (chunk + rg_size).min(total);
        let mut rg = writer.next_row_group().unwrap();
        {
            let mut col = rg.next_column().unwrap().unwrap();
            if let ColumnWriter::Int32ColumnWriter(t) = col.untyped() {
                t.write_batch(&shipdate[chunk..end], None, None).unwrap();
            }
            col.close().unwrap();
        }
        {
            let mut col = rg.next_column().unwrap().unwrap();
            if let ColumnWriter::Int64ColumnWriter(t) = col.untyped() {
                t.write_batch(&partkey[chunk..end], None, None).unwrap();
            }
            col.close().unwrap();
        }
        {
            let mut col = rg.next_column().unwrap().unwrap();
            if let ColumnWriter::DoubleColumnWriter(t) = col.untyped() {
                t.write_batch(&extprice[chunk..end], None, None).unwrap();
            }
            col.close().unwrap();
        }
        {
            let mut col = rg.next_column().unwrap().unwrap();
            if let ColumnWriter::DoubleColumnWriter(t) = col.untyped() {
                t.write_batch(&discount[chunk..end], None, None).unwrap();
            }
            col.close().unwrap();
        }
        rg.close().unwrap();
    }
    writer.close().unwrap();
    cache
}

async fn bench_one(
    label: &str,
    setup_ctx: impl Fn() -> futures_util::future::BoxFuture<'static, SessionContext>,
) -> f64 {
    for _ in 0..WARMUPS {
        let ctx = setup_ctx().await;
        let df = ctx.sql(SQL).await.unwrap();
        let _ = df.collect().await.unwrap();
    }

    let mut times: Vec<f64> = Vec::with_capacity(ITERS);
    for _ in 0..ITERS {
        let ctx = setup_ctx().await;
        let t0 = Instant::now();
        let df = ctx.sql(SQL).await.unwrap();
        let _ = df.collect().await.unwrap();
        times.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let med = times[ITERS / 2];
    let min = times[0];
    let max = times[ITERS - 1];
    println!(
        "  {label:<42} median {:>6.2} ms  min {:>6.2} ms  max {:>6.2} ms",
        med, min, max
    );
    med
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let dir = data_dir();
    let real = dir.join("lineitem.parquet");
    if !real.exists() {
        eprintln!("missing {}", real.display());
        std::process::exit(1);
    }
    let synth = ensure_synthetic_file(&real);
    println!("==> Phase 3.5: Q14-shape SQL through 3 providers");
    println!("==> data:  {}", synth.display());
    println!("==> SQL:   {}", SQL);
    println!();

    // Path 1: register_parquet (DataFusion default; uses parquet-rs)
    let p1 = synth.clone();
    let med_default = bench_one("DataFusion default (register_parquet)", move || {
        let p = p1.clone();
        Box::pin(async move {
            let ctx = SessionContext::new();
            ctx.register_parquet(
                "lineitem",
                p.to_string_lossy().to_string(),
                Default::default(),
            )
            .await
            .unwrap();
            ctx
        })
    })
    .await;

    // Path 2: FastParquetTableProvider
    let p2 = synth.clone();
    let med_fast = bench_one("FastParquetTableProvider", move || {
        let p = p2.clone();
        Box::pin(async move {
            let prov = FastParquetTableProvider::try_new(p.to_string_lossy().to_string()).unwrap();
            let ctx = SessionContext::new();
            ctx.register_table("lineitem", Arc::new(prov)).unwrap();
            ctx
        })
    })
    .await;

    // Path 3: EmatixFastParquetProvider (Phase 3 pushdown)
    let p3 = synth.clone();
    let med_emat = bench_one("EmatixFastParquetProvider (Phase 3 pushdown)", move || {
        let p = p3.clone();
        Box::pin(async move {
            let prov =
                EmatixFastParquetTableProvider::try_new(p.to_string_lossy().to_string()).unwrap();
            // Slightly larger target_partitions to match FastParquet's
            // default 14 (so partitioning isn't the variable).
            let cfg = SessionConfig::new().with_target_partitions(14);
            let ctx = SessionContext::new_with_config(cfg);
            ctx.register_table("lineitem", Arc::new(prov)).unwrap();
            ctx
        })
    })
    .await;

    println!();
    println!("==> Ratios (lower is faster)");
    let ratio = |a: f64, b: f64| a / b;
    println!(
        "  Emat vs DataFusion default: {:.2}× ({})",
        ratio(med_emat, med_default),
        if med_emat < med_default {
            format!("{:.0}% faster", 100.0 * (1.0 - med_emat / med_default))
        } else {
            format!("{:.0}% slower", 100.0 * (med_emat / med_default - 1.0))
        }
    );
    println!(
        "  Emat vs FastParquet:        {:.2}× ({})",
        ratio(med_emat, med_fast),
        if med_emat < med_fast {
            format!("{:.0}% faster", 100.0 * (1.0 - med_emat / med_fast))
        } else {
            format!("{:.0}% slower", 100.0 * (med_emat / med_fast - 1.0))
        }
    );
}
