//! PARQUET_WRITER_SCOPE Phase-1 — `optimize-table`: per-column codec selection.
//!
//! Storing a table LZ4_RAW instead of Snappy makes ematix queries against it
//! 19–28% faster in absolute terms (the engine's SIMD LZ4 decode beats its
//! Snappy decode). This is a product/storage feature — faster reads on tables
//! ematix owns — not a competitive-benchmark lever (other engines decode LZ4
//! faster too; see docs/plans/PARQUET_WRITER_SCOPE.md). Codec is chosen per
//! column: LZ4_RAW for compressible columns, UNCOMPRESSED for incompressible
//! ones (ratio≈1.0, where there's no decompress to save). For TPC-H the
//! per-column refinement is ≈ a global LZ4 (compressible columns dominate);
//! it matters for tables with large incompressible columns.
//!
//! This tool reads a parquet file and re-emits it choosing, for each column,
//! LZ4_RAW (if the source column compresses below RATIO_THRESHOLD) or
//! UNCOMPRESSED (if it's effectively incompressible). It writes through the
//! arrow-rs writer (full schema fidelity: DATE/DECIMAL/STRING preserved) in V1
//! data pages (the format the emat reader decodes), streaming batch-by-batch so
//! memory stays bounded regardless of table size.
//!
//! The codec decision uses the SOURCE file's own per-column compression ratio
//! (compressed/uncompressed summed over row groups) as the compressibility
//! signal — Snappy ratio is a good proxy for LZ4 ratio (both LZ77-family) and
//! it needs no data pass.
//!
//! Run:
//!     SRC=examples/tpch/data/sf10/lineitem.parquet \
//!     DST=/tmp/sf10_opt/lineitem.parquet \
//!       cargo run --release -p ematix-flow-core --example optimize_table
//!
//! Env:
//!   SRC                required — source parquet path
//!   DST                required — destination parquet path (dirs must exist)
//!   RATIO_THRESHOLD    default 0.90 — col ratio < this → LZ4_RAW, else UNCOMPRESSED
//!   GLOBAL             optional — force ALL columns to one codec
//!                      (lz4|uncompressed|snappy|zstd); for the global-vs-per-column A/B
//!   BATCH_ROWS         default 8192 — read batch size
//!   DICT               default 1 — keep dictionary encoding (0 disables; low-card
//!                      string cols bloat hugely without it)

use std::fs::File;
use std::path::Path;

use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::basic::{Compression, Encoding, ZstdLevel};
use datafusion::parquet::file::properties::{WriterProperties, WriterVersion};
use datafusion::parquet::schema::types::ColumnPath;

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok()
}

fn codec_name(c: Compression) -> &'static str {
    match c {
        Compression::UNCOMPRESSED => "uncompressed",
        Compression::SNAPPY => "snappy",
        Compression::LZ4_RAW => "lz4_raw",
        Compression::ZSTD(_) => "zstd",
        _ => "other",
    }
}

fn parse_global(s: &str) -> Compression {
    match s.to_ascii_lowercase().as_str() {
        "lz4" | "lz4_raw" => Compression::LZ4_RAW,
        "uncompressed" | "none" => Compression::UNCOMPRESSED,
        "snappy" => Compression::SNAPPY,
        "zstd" => Compression::ZSTD(ZstdLevel::default()),
        other => panic!("unknown GLOBAL codec: {other}"),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let src = env("SRC").expect("SRC env var required");
    let dst = env("DST").expect("DST env var required");
    let threshold: f64 = env("RATIO_THRESHOLD")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.90);
    let global = env("GLOBAL").map(|s| parse_global(&s));
    let batch_rows: usize = env("BATCH_ROWS")
        .and_then(|s| s.parse().ok())
        .unwrap_or(8192);
    let dict = env("DICT").map(|s| s != "0").unwrap_or(true);

    // --- open + read metadata for the per-column compressibility decision ---
    let file = File::open(&src)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let meta = builder.metadata().clone();
    let arrow_schema = builder.schema().clone();

    let n_cols = meta.file_metadata().schema_descr().num_columns();
    let n_rgs = meta.num_row_groups();
    let rg0_rows = if n_rgs > 0 {
        meta.row_group(0).num_rows() as usize
    } else {
        1_048_576
    };

    // Sum compressed/uncompressed per leaf column across all row groups.
    let mut comp = vec![0i64; n_cols];
    let mut uncomp = vec![0i64; n_cols];
    let mut names = vec![String::new(); n_cols];
    for rg in 0..n_rgs {
        let rgm = meta.row_group(rg);
        for c in 0..n_cols {
            let col = rgm.column(c);
            comp[c] += col.compressed_size();
            uncomp[c] += col.uncompressed_size();
            if rg == 0 {
                names[c] = col.column_descr().name().to_string();
            }
        }
    }

    // --- decide codec per column ---
    let mut decisions: Vec<(String, Compression, f64)> = Vec::with_capacity(n_cols);
    for c in 0..n_cols {
        let ratio = if uncomp[c] > 0 {
            comp[c] as f64 / uncomp[c] as f64
        } else {
            1.0
        };
        let codec = match global {
            Some(g) => g,
            None => {
                if ratio < threshold {
                    Compression::LZ4_RAW
                } else {
                    Compression::UNCOMPRESSED
                }
            }
        };
        decisions.push((names[c].clone(), codec, ratio));
    }

    println!("optimize-table");
    println!("  src: {src}");
    println!("  dst: {dst}");
    if let Some(g) = global {
        println!("  mode: GLOBAL {}", codec_name(g));
    } else {
        println!("  mode: per-column (threshold ratio < {threshold} → lz4_raw, else uncompressed)");
    }
    println!("  row groups: {n_rgs}, rg0 rows: {rg0_rows}, dict: {dict}");
    println!(
        "  {:<18} {:>10} {:>10} {:>7}  codec",
        "column", "comp", "uncomp", "ratio"
    );
    for (c, (name, codec, ratio)) in decisions.iter().enumerate() {
        println!(
            "  {name:<18} {:>10} {:>10} {ratio:>7.3}  {}",
            comp[c],
            uncomp[c],
            codec_name(*codec)
        );
    }

    // --- build writer properties: V1 pages + per-column codec ---
    let mut props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_max_row_group_row_count(Some(rg0_rows.max(1)))
        .set_compression(Compression::LZ4_RAW); // default for any unlisted column
    if !dict {
        props = props.set_dictionary_enabled(false);
        props = props.set_encoding(Encoding::PLAIN);
    }
    for (name, codec, _) in &decisions {
        props = props.set_column_compression(ColumnPath::from(name.clone()), *codec);
    }
    let props = props.build();

    // --- stream rewrite ---
    let out = File::create(Path::new(&dst))?;
    let mut writer = ArrowWriter::try_new(out, arrow_schema.clone(), Some(props))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&src)?)?
        .with_batch_size(batch_rows)
        .build()?;
    let mut rows = 0usize;
    for batch in reader {
        let batch = batch?;
        rows += batch.num_rows();
        writer.write(&batch)?;
    }
    writer.close()?;

    let src_sz = std::fs::metadata(&src)?.len();
    let dst_sz = std::fs::metadata(&dst)?.len();
    println!(
        "\ndone: {rows} rows. file {} → {} bytes ({:+.1}%)",
        src_sz,
        dst_sz,
        (dst_sz as f64 - src_sz as f64) / src_sz as f64 * 100.0
    );
    Ok(())
}
