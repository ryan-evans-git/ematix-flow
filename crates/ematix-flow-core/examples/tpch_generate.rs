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

Options:
    --sf <FACTOR>   TPC-H scale factor (1, 10, 100, 1000). Required.
    --out <DIR>     Output directory. One Parquet file per TPC-H table.
                    Created if missing. Required.

Each table file is named `<table>.parquet` (lineitem.parquet, etc.)
and uses Snappy compression. Existing files are skipped — delete
the directory to regenerate.
";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().skip(1).collect();
    let mut sf: Option<f64> = None;
    let mut out: Option<PathBuf> = None;

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

    let sf = sf.ok_or("--sf is required")?;
    let out = out.ok_or("--out is required")?;
    fs::create_dir_all(&out)?;

    println!("==> generating TPC-H SF={sf} into {}", out.display());

    // Each table generates the same way — produce CSV via tpchgen's
    // Display impls, parse with arrow-csv into our pinned arrow 58
    // ABI, write Parquet. Helper macro keeps the per-table boilerplate
    // tight.
    macro_rules! gen_table {
        ($name:literal, $schema:expr, $generator:expr, $csv:ident) => {{
            let path = out.join(concat!($name, ".parquet"));
            if path.exists() {
                println!("    skip {} (already exists)", path.display());
            } else {
                println!("    gen  {}", path.display());
                let mut csv = String::new();
                writeln!(&mut csv, "{}", $csv::header())?;
                for row in $generator.iter() {
                    writeln!(&mut csv, "{}", $csv::new(row))?;
                }
                write_parquet(&path, $schema, csv.into_bytes())?;
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

fn write_parquet(
    path: &Path,
    schema: Arc<Schema>,
    csv_bytes: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let cursor = Cursor::new(csv_bytes);
    let reader = ReaderBuilder::new(schema.clone())
        .with_header(true)
        .with_batch_size(64 * 1024)
        .build(cursor)?;

    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;

    let mut total_rows: i64 = 0;
    for batch in reader {
        let batch: RecordBatch = batch?;
        total_rows += batch.num_rows() as i64;
        writer.write(&batch)?;
    }
    writer.close()?;
    println!("         {total_rows} rows");
    Ok(())
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
