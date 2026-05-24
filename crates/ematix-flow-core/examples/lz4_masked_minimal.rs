//! Q06.b — minimal reproducer of the LZ4_RAW masked-decode bug.
//! Calls `masked_decode_i32` on `l_shipdate` from the LZ4 file with an
//! all-ones mask (so the decoder must produce a value for every row).
//! If this fails the same way as the q06_lz4_probe full SQL, the bug
//! is isolated to ematix-parquet's masked path on LZ4_RAW chunks.

use ematix_flow_core::ematix_parquet_bridge::masked_decode_i32;
use ematix_parquet_io::ParquetFile;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filename = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "lineitem_lz4.parquet".to_string());
    let path = PathBuf::from("examples/tpch/data/sf10").join(filename);
    let file = ParquetFile::open(&path)?;
    let md = file.metadata()?;

    let col = md.row_groups[0]
        .columns
        .iter()
        .position(|c| {
            c.meta_data.as_ref().map(|m| {
                m.path_in_schema
                    .iter()
                    .map(|b| std::str::from_utf8(b).unwrap_or("?"))
                    .collect::<Vec<_>>()
                    .join(".")
            }) == Some("l_shipdate".to_string())
        })
        .expect("l_shipdate col");

    for rg_idx in 0..3 {
        let cm = md.row_groups[rg_idx].columns[col].meta_data.as_ref().unwrap();
        let n_rows = cm.num_values as usize;
        // All-ones mask: every bit set, full row count.
        let mask = vec![0xFFu8; n_rows.div_ceil(8)];
        println!("RG{rg_idx}: l_shipdate codec={:?}  num_rows={n_rows}", cm.codec);
        match masked_decode_i32(&file, rg_idx, col, &mask) {
            Ok(v) => println!("  OK — decoded {} values  first={:?} last={:?}",
                              v.len(),
                              v.first(),
                              v.last()),
            Err(e) => {
                println!("  FAIL — {e}");
                // Bail after first failure so we don't spam.
                return Ok(());
            }
        }
    }
    Ok(())
}
