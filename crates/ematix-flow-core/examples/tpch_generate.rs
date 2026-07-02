//! Σ.A1 PR 1: TPC-H Parquet generator.
//!
//! Bulk generates TPC-H tables at the requested scale factor and
//! writes them as Snappy-compressed Parquet files into a target
//! directory. Used by the criterion benches in Σ.A1 PR 2 + the
//! head-to-head benchmark in Σ.A1 PR 4 + Σ.C; not a runtime
//! production path.
//!
//! Usage:
//! ```sh
//! cargo run --release -p ematix-flow-core --example tpch_generate -- \
//!     --sf 1 --out examples/tpch/data/sf1
//! ```
//!
//! Idempotent: skips any table whose Parquet file already exists in
//! the output directory. Delete the directory to regenerate.
//!
//! Why CSV → arrow-csv → Parquet rather than tpchgen-arrow's direct
//! Arrow path? See the comment in `tests/tpch_smoke.rs` — workspace
//! arrow 58 vs tpchgen-arrow 2.0.2's pinned arrow 57.

use std::env;
use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_csv::ReaderBuilder;
use arrow_schema::{DataType, Field, Schema};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use tpchgen::csv::{
    CustomerCsv, LineItemCsv, NationCsv, OrderCsv, PartCsv, PartSuppCsv, RegionCsv, SupplierCsv,
};
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};

const USAGE: &str = "\
Σ.A1 TPC-H Parquet generator.

Usage:
    tpch_generate --sf <FACTOR> --out <DIR>
    tpch_generate --reemit <FILE.parquet> --rg-rows <N>

Options:
    --sf <FACTOR>   TPC-H scale factor (1, 10, 100, 1000). Required.
    --out <DIR>     Output directory. One Parquet file per TPC-H table.
                    Created if missing. Required.

    --reemit <FILE> Σ.AH.4: rewrite an existing Parquet file in place
                    with a different row-group size, through the SAME
                    arrow-rs writer stack as generation (byte-faithful
                    encodings — an external writer would not be).
                    Verifies row count before atomically replacing
                    the original.
    --rg-rows <N>   Max rows per row group for --reemit. Pick so the
                    row-group count is >= the machine's core count: a
                    2-row-group customer.parquet caps that scan at
                    2-way parallelism no matter how many cores exist.

Each table file is named `<table>.parquet` (lineitem.parquet, etc.)
and uses Snappy compression. Existing files are skipped — delete
the directory to regenerate.
";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().skip(1).collect();
    let mut sf: Option<f64> = None;
    let mut out: Option<PathBuf> = None;
    let mut reemit: Option<PathBuf> = None;
    let mut rg_rows: Option<usize> = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--sf" => {
                sf = Some(args.get(i + 1).ok_or("--sf needs a value")?.parse()?);
                i += 2;
            }
            "--out" => {
                out = Some(PathBuf::from(args.get(i + 1).ok_or("--out needs a value")?));
                i += 2;
            }
            "--reemit" => {
                reemit = Some(PathBuf::from(
                    args.get(i + 1).ok_or("--reemit needs a value")?,
                ));
                i += 2;
            }
            "--rg-rows" => {
                rg_rows = Some(args.get(i + 1).ok_or("--rg-rows needs a value")?.parse()?);
                i += 2;
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(());
            }
            other => {
                eprintln!("unknown argument: {other}\n\n{USAGE}");
                std::process::exit(2);
            }
        }
    }

    if let Some(path) = reemit {
        let rg = rg_rows.ok_or("--reemit requires --rg-rows")?;
        return reemit_with_rg_size(&path, rg);
    }

    let sf = sf.ok_or("--sf is required")?;
    let out = out.ok_or("--out is required")?;
    fs::create_dir_all(&out)?;

    println!("==> generating TPC-H SF={sf} into {}", out.display());

    // Each table generates the same way — produce CSV via tpchgen's
    // Display impls, parse with arrow-csv into our pinned arrow 58
    // ABI, write Parquet. Helper macro keeps the per-table boilerplate
    // tight.
    // Stream each table in row chunks so SF=100+ doesn't build the whole
    // table CSV as one in-memory String (SF=100 lineitem ≈ 78 GB → OOM on
    // a 36 GB box). Per chunk: accumulate CHUNK_ROWS rows of CSV, parse to
    // RecordBatches, append to one ArrowWriter; finalize once. Peak memory
    // ≈ one chunk (~260 MB for lineitem) regardless of scale factor.
    const CHUNK_ROWS: usize = 2_000_000;
    macro_rules! gen_table {
        ($name:literal, $schema:expr, $generator:expr, $csv:ident) => {{
            let path = out.join(concat!($name, ".parquet"));
            if path.exists() {
                println!("    skip {} (already exists)", path.display());
            } else {
                println!("    gen  {}", path.display());
                let schema = $schema;
                let mut writer = open_parquet_writer(&path, schema.clone())?;
                let header = format!("{}\n", $csv::header());
                let mut csv = String::with_capacity(96 << 20);
                csv.push_str(&header);
                let mut chunk_rows = 0usize;
                let mut total_rows: i64 = 0;
                for row in $generator.iter() {
                    writeln!(&mut csv, "{}", $csv::new(row))?;
                    chunk_rows += 1;
                    if chunk_rows >= CHUNK_ROWS {
                        total_rows += flush_csv_chunk(&mut writer, schema.clone(), &csv)?;
                        csv.clear();
                        csv.push_str(&header);
                        chunk_rows = 0;
                    }
                }
                if chunk_rows > 0 {
                    total_rows += flush_csv_chunk(&mut writer, schema.clone(), &csv)?;
                }
                writer.close()?;
                println!("         {total_rows} rows");
            }
        }};
    }

    gen_table!(
        "nation",
        nation_schema(),
        NationGenerator::new(sf, 1, 1),
        NationCsv
    );
    gen_table!(
        "region",
        region_schema(),
        RegionGenerator::new(sf, 1, 1),
        RegionCsv
    );
    gen_table!(
        "supplier",
        supplier_schema(),
        SupplierGenerator::new(sf, 1, 1),
        SupplierCsv
    );
    gen_table!(
        "customer",
        customer_schema(),
        CustomerGenerator::new(sf, 1, 1),
        CustomerCsv
    );
    gen_table!("part", part_schema(), PartGenerator::new(sf, 1, 1), PartCsv);
    gen_table!(
        "partsupp",
        partsupp_schema(),
        PartSuppGenerator::new(sf, 1, 1),
        PartSuppCsv
    );
    gen_table!(
        "orders",
        orders_schema(),
        OrderGenerator::new(sf, 1, 1),
        OrderCsv
    );
    gen_table!(
        "lineitem",
        lineitem_schema(),
        LineItemGenerator::new(sf, 1, 1),
        LineItemCsv
    );

    println!("==> done");
    Ok(())
}

fn open_parquet_writer(
    path: &Path,
    schema: Arc<Schema>,
) -> Result<ArrowWriter<File>, Box<dyn std::error::Error>> {
    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    Ok(ArrowWriter::try_new(file, schema, Some(props))?)
}

/// Σ.AH.4: stream-copy an existing Parquet file through the same writer
/// stack with a bounded row-group size, then atomically replace the
/// original. A dimension file with fewer row groups than the machine has
/// cores caps that scan's parallelism (customer.parquet at SF=10 shipped
/// with 2 row groups → 2-way scans on a 14-core box).
fn reemit_with_rg_size(path: &Path, rg_rows: usize) -> Result<(), Box<dyn std::error::Error>> {
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let src = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(src)?;
    let before_rgs = builder.metadata().num_row_groups();
    let before_rows: i64 = builder.metadata().file_metadata().num_rows();
    let schema = builder.schema().clone();
    let reader = builder.build()?;

    let tmp = path.with_extension("parquet.reemit-tmp");
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(rg_rows))
        .build();
    let mut writer = ArrowWriter::try_new(File::create(&tmp)?, schema, Some(props))?;

    let mut written: i64 = 0;
    for batch in reader {
        let batch: RecordBatch = batch?;
        written += batch.num_rows() as i64;
        writer.write(&batch)?;
    }
    let meta = writer.close()?;
    let after_rgs = meta.row_groups().len();

    if written != before_rows {
        fs::remove_file(&tmp)?;
        return Err(format!(
            "row-count mismatch re-emitting {}: read {before_rows}, wrote {written} — original untouched",
            path.display()
        )
        .into());
    }
    fs::rename(&tmp, path)?;
    println!(
        "==> re-emitted {}: {before_rgs} -> {after_rgs} row groups ({written} rows, <= {rg_rows} rows/rg)",
        path.display()
    );
    Ok(())
}

// Parse one in-memory CSV chunk (header line + up to CHUNK_ROWS data rows)
// into RecordBatches and append them to the open writer. Bounded memory:
// the chunk String plus one 64K-row batch at a time. Returns rows written.
fn flush_csv_chunk(
    writer: &mut ArrowWriter<File>,
    schema: Arc<Schema>,
    csv: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    let cursor = Cursor::new(csv.as_bytes().to_vec());
    let reader = ReaderBuilder::new(schema)
        .with_header(true)
        .with_batch_size(64 * 1024)
        .build(cursor)?;
    let mut rows: i64 = 0;
    for batch in reader {
        let batch: RecordBatch = batch?;
        rows += batch.num_rows() as i64;
        writer.write(&batch)?;
    }
    Ok(rows)
}

// TPC-H spec § 1.4.1 schemas. DECIMAL(15,2) / DECIMAL(12,2) columns
// land as Float64 — Q6's `SUM(l_extendedprice * l_discount)` is
// correct on f64 within 0.01 absolute (one digit past the spec's
// reference precision). Switch to Decimal128 if a query exposes
// rounding sensitivity later.

fn nation_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("n_nationkey", DataType::Int64, false),
        Field::new("n_name", DataType::Utf8, false),
        Field::new("n_regionkey", DataType::Int64, false),
        Field::new("n_comment", DataType::Utf8, false),
    ]))
}

fn region_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("r_regionkey", DataType::Int64, false),
        Field::new("r_name", DataType::Utf8, false),
        Field::new("r_comment", DataType::Utf8, false),
    ]))
}

fn supplier_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("s_suppkey", DataType::Int64, false),
        Field::new("s_name", DataType::Utf8, false),
        Field::new("s_address", DataType::Utf8, false),
        Field::new("s_nationkey", DataType::Int64, false),
        Field::new("s_phone", DataType::Utf8, false),
        Field::new("s_acctbal", DataType::Float64, false),
        Field::new("s_comment", DataType::Utf8, false),
    ]))
}

fn customer_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_address", DataType::Utf8, false),
        Field::new("c_nationkey", DataType::Int64, false),
        Field::new("c_phone", DataType::Utf8, false),
        Field::new("c_acctbal", DataType::Float64, false),
        Field::new("c_mktsegment", DataType::Utf8, false),
        Field::new("c_comment", DataType::Utf8, false),
    ]))
}

fn part_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("p_partkey", DataType::Int64, false),
        Field::new("p_name", DataType::Utf8, false),
        Field::new("p_mfgr", DataType::Utf8, false),
        Field::new("p_brand", DataType::Utf8, false),
        Field::new("p_type", DataType::Utf8, false),
        Field::new("p_size", DataType::Int32, false),
        Field::new("p_container", DataType::Utf8, false),
        Field::new("p_retailprice", DataType::Float64, false),
        Field::new("p_comment", DataType::Utf8, false),
    ]))
}

fn partsupp_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("ps_partkey", DataType::Int64, false),
        Field::new("ps_suppkey", DataType::Int64, false),
        Field::new("ps_availqty", DataType::Int32, false),
        Field::new("ps_supplycost", DataType::Float64, false),
        Field::new("ps_comment", DataType::Utf8, false),
    ]))
}

fn orders_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderstatus", DataType::Utf8, false),
        Field::new("o_totalprice", DataType::Float64, false),
        Field::new("o_orderdate", DataType::Date32, false),
        Field::new("o_orderpriority", DataType::Utf8, false),
        Field::new("o_clerk", DataType::Utf8, false),
        Field::new("o_shippriority", DataType::Int32, false),
        Field::new("o_comment", DataType::Utf8, false),
    ]))
}

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
    ]))
}
