//! Q06.a — one-shot utility to rewrite `lineitem.parquet` with the
//! LZ4_RAW codec instead of Snappy. The Σ.E7 / Q06 SF=10 investigation
//! ([[q06-sf10-polars-gap-wall]]) identified the `l_extendedprice`
//! Snappy stream as a 1.73 GB/s wall — hand-rolled SIMD Snappy NEG'd
//! 17%. The [[ematix-parquet-lz4-decode-bug]] memory says ematix-parquet
//! v0.14.0's fixed LZ4_RAW path hits 57.88 ms on Q06 SF=10, beating
//! Polars (62 ms) and DuckDB.
//!
//! Run once to produce `lineitem_lz4.parquet` next to `lineitem.parquet`.
//! Uses DuckDB's COPY-with-compression to keep the rewrite tooling in
//! the existing Rust dep graph (no pyarrow / external CLI). The
//! produced file is a sibling for A/B bench purposes — does NOT
//! replace the Snappy file.

use std::path::PathBuf;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::var("TPCH_DATA_DIR")
        .unwrap_or_else(|_| "examples/tpch/data/sf10".to_string());
    let src = PathBuf::from(&dir).join("lineitem.parquet");
    let dst = PathBuf::from(&dir).join("lineitem_lz4.parquet");

    if !src.exists() {
        return Err(format!("source not found: {}", src.display()).into());
    }
    if dst.exists() {
        eprintln!("destination already exists: {}", dst.display());
        eprintln!("delete it first if you want to overwrite.");
        return Ok(());
    }

    println!("Rewrite plan:");
    println!("  src: {}", src.display());
    println!("  dst: {} (LZ4_RAW)", dst.display());
    println!();

    let conn = duckdb::Connection::open_in_memory()?;
    conn.execute_batch("PRAGMA threads=14")?;

    // DuckDB's parquet COPY supports COMPRESSION 'lz4_raw'. We force
    // ROW_GROUP_SIZE to match the source's 58-row-group shape (about
    // 1.03M rows / RG) so the rewritten file has equivalent
    // page-skipping granularity. ROW_GROUPS_PER_FILE=1 keeps it a
    // single-file output.
    let src_str = src.to_string_lossy().to_string();
    let dst_str = dst.to_string_lossy().to_string();
    // PARQUET_VERSION='v1' forces V1 data pages — the V2 default
    // produces RLE_DICTIONARY indices in a format that ematix-parquet's
    // gather_dict_at_bitmap_into doesn't currently parse.
    let parquet_version = std::env::var("PARQUET_VERSION").unwrap_or_else(|_| "v1".to_string());
    let sql = format!(
        "COPY (SELECT * FROM read_parquet('{src_str}')) TO '{dst_str}' \
         (FORMAT PARQUET, COMPRESSION 'lz4_raw', PARQUET_VERSION '{parquet_version}', ROW_GROUP_SIZE 1048576);"
    );
    println!("Running: {sql}");
    let t = Instant::now();
    conn.execute_batch(&sql)?;
    let elapsed_s = t.elapsed().as_secs_f64();
    println!("done in {elapsed_s:.1}s");

    // Sanity check size.
    let src_sz = std::fs::metadata(&src)?.len();
    let dst_sz = std::fs::metadata(&dst)?.len();
    println!();
    println!(
        "src size: {} MB  ({} bytes)",
        src_sz / 1_048_576,
        src_sz
    );
    println!(
        "dst size: {} MB  ({} bytes)  Δ = {:+.1}%",
        dst_sz / 1_048_576,
        dst_sz,
        ((dst_sz as f64 - src_sz as f64) / src_sz as f64) * 100.0,
    );

    println!();
    println!("Next: run inspect_lineitem_codec with TPCH_DATA_DIR={dir} to verify codec=Lz4Raw.");
    println!("Then: cargo run --release --example q06_lz4_ab -- to A/B Q06 SF=10.");

    Ok(())
}
