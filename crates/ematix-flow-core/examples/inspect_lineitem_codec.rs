//! Quick inspect: what compression codec + encoding does each lineitem
//! column use? Helps decide if masked-decode is decompression-bound.

use ematix_parquet_io::ParquetFile;
use std::path::PathBuf;

fn main() {
    let dir =
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf10".to_string());
    // Q06.a (2026-05-24): optional filename override so the LZ4_RAW
    // sibling can be inspected without renaming the canonical file.
    let filename =
        std::env::var("TPCH_LINEITEM_FILE").unwrap_or_else(|_| "lineitem.parquet".to_string());
    let path = PathBuf::from(&dir).join(&filename);
    let file = ParquetFile::open(&path).expect("open");
    let md = file.metadata().expect("md");
    let n_rgs = md.row_groups.len();

    println!("=== lineitem.parquet ({}) ===", path.display());
    println!("Row groups: {n_rgs}\n");

    // Print per-column codec for RG0.
    let rg0 = &md.row_groups[0];
    println!("RG 0 column codecs (and total compressed/uncompressed bytes):");
    for (col_idx, c) in rg0.columns.iter().enumerate() {
        if let Some(cm) = &c.meta_data {
            println!(
                "  col {:>2} {:<20} codec={:?}  comp={:>10}  uncomp={:>10}  ratio={:>4.2}",
                col_idx,
                cm.path_in_schema
                    .iter()
                    .map(|b| std::str::from_utf8(b).unwrap_or("?"))
                    .collect::<Vec<_>>()
                    .join("."),
                cm.codec,
                cm.total_compressed_size,
                cm.total_uncompressed_size,
                cm.total_compressed_size as f64 / cm.total_uncompressed_size as f64,
            );
        }
    }
}
