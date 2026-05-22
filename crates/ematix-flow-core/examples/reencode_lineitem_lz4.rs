//! Σ.E7 codec swap: re-encode `lineitem.parquet` with LZ4_RAW.
//!
//! Snappy on the compressed projection column (l_extendedprice) is the
//! Q06 SF=10 wall (1.73 GB/s decompress vs 12.6 GB/s memcpy). LZ4_RAW
//! is typically 2-3× faster to decompress at a small compression-ratio
//! cost. ematix-parquet has the codec wired (Π.3a).
//!
//! Reads SF=10 lineitem.parquet via DataFusion (decompresses Snappy),
//! re-writes with LZ4_RAW into the same schema. Then we re-run
//! q06_fused_proof against the new file and compare.
//!
//! Run:
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 \
//!     cargo run --release -p ematix-flow-core --example reencode_lineitem_lz4

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::var("TPCH_DATA_DIR")
        .unwrap_or_else(|_| "examples/tpch/data/sf10".to_string());
    let src = PathBuf::from(&dir).join("lineitem.parquet");
    let dst = PathBuf::from(&dir).join("lineitem.lz4.parquet");

    println!("Re-encoding {} → {}", src.display(), dst.display());
    let t = Instant::now();

    let src_file = File::open(&src)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(src_file)?;
    let schema = builder.schema().clone();
    let reader = builder.with_batch_size(64 * 1024).build()?;

    let dst_file = File::create(&dst)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::LZ4_RAW)
        .build();
    let mut writer = ArrowWriter::try_new(dst_file, Arc::new((*schema).clone()), Some(props))?;
    let mut rows: i64 = 0;
    for batch in reader {
        let b = batch?;
        rows += b.num_rows() as i64;
        writer.write(&b)?;
    }
    writer.close()?;
    let elapsed = t.elapsed().as_secs_f64();

    let src_size = std::fs::metadata(&src)?.len();
    let dst_size = std::fs::metadata(&dst)?.len();

    println!("\nWrote {rows} rows in {elapsed:.2}s");
    println!("File size:");
    println!(
        "  Snappy:    {:>10} bytes ({:.2} MB)",
        src_size,
        src_size as f64 / 1e6
    );
    println!(
        "  LZ4_RAW:   {:>10} bytes ({:.2} MB)",
        dst_size,
        dst_size as f64 / 1e6
    );
    println!(
        "  Ratio LZ4/Snappy: {:.3}× ({}{:.1}% size change)",
        dst_size as f64 / src_size as f64,
        if dst_size > src_size { "+" } else { "" },
        ((dst_size as f64 - src_size as f64) / src_size as f64) * 100.0
    );
    Ok(())
}
