//! Q06.b debug — walk pages of l_shipdate in lineitem_lz4.parquet to
//! see if PageHeader parsing succeeds and which page version DuckDB
//! wrote.

use ematix_parquet_io::ParquetFile;
use ematix_parquet_io::pages::PageWalker;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = PathBuf::from("examples/tpch/data/sf10/lineitem_lz4.parquet");
    let file = ParquetFile::open(&path)?;
    let md = file.metadata()?;

    for col_name in &["l_shipdate", "l_quantity", "l_extendedprice"] {
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
                }) == Some(col_name.to_string())
            })
            .ok_or_else(|| format!("col {col_name} not found"))?;
        let cm = md.row_groups[0].columns[col].meta_data.as_ref().unwrap();
        println!(
            "\n=== {col_name}  RG0  codec={:?}  comp={}  uncomp={} ===",
            cm.codec, cm.total_compressed_size, cm.total_uncompressed_size
        );
        let offset = cm.dictionary_page_offset.unwrap_or(cm.data_page_offset) as u64;
        let length = cm.total_compressed_size as u64;
        let chunk = file.read_range(offset, length)?;
        let limit = length as usize;
        let mut w = PageWalker::with_byte_limit(&chunk, limit);
        let mut page_idx = 0;
        loop {
            match w.next_page() {
                Ok(Some((hdr, body))) => {
                    let kind = if hdr.data_page_header.is_some() {
                        "V1"
                    } else if hdr.data_page_header_v2.is_some() {
                        "V2"
                    } else if hdr.dictionary_page_header.is_some() {
                        "Dict"
                    } else {
                        "?"
                    };
                    println!(
                        "  page #{page_idx}  type={:?} kind={kind}  csize={}  usize={}  body.len()={}",
                        hdr.page_type,
                        hdr.compressed_page_size,
                        hdr.uncompressed_page_size,
                        body.len()
                    );
                    if let Some(ref v2) = hdr.data_page_header_v2 {
                        println!(
                            "      v2: rep_len={} def_len={} num_values={} is_compressed={}",
                            v2.repetition_levels_byte_length,
                            v2.definition_levels_byte_length,
                            v2.num_values,
                            v2.is_compressed,
                        );
                    }
                    page_idx += 1;
                    if page_idx > 8 {
                        println!("  (truncated)");
                        break;
                    }
                }
                Ok(None) => {
                    println!("  end-of-chunk at page #{page_idx}");
                    break;
                }
                Err(e) => {
                    println!("  ERROR at page #{page_idx}: {e}");
                    break;
                }
            }
        }
    }
    Ok(())
}
