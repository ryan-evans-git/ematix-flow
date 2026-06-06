//! Q06.b debug — walk pages of l_shipdate in lineitem_lz4.parquet to
//! see if PageHeader parsing succeeds and which page version DuckDB
//! wrote.

use ematix_parquet_io::ParquetFile;
use ematix_parquet_io::pages::PageWalker;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = PathBuf::from("examples/tpch/data/sf10/lineitem.parquet");
    let file = ParquetFile::open(&path)?;
    let md = file.metadata()?;

    for col_name in &["l_extendedprice", "l_discount", "l_suppkey", "l_partkey"] {
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
        let mut data_pages = 0u64;
        let mut total_values = 0u64;
        let mut min_vals = u64::MAX;
        let mut max_vals = 0u64;
        loop {
            match w.next_page() {
                Ok(Some((hdr, _body))) => {
                    // num_values lives in V1 data_page_header or V2 header.
                    let nv = hdr
                        .data_page_header
                        .as_ref()
                        .map(|h| h.num_values as u64)
                        .or_else(|| {
                            hdr.data_page_header_v2
                                .as_ref()
                                .map(|h| h.num_values as u64)
                        });
                    if let Some(nv) = nv {
                        data_pages += 1;
                        total_values += nv;
                        min_vals = min_vals.min(nv);
                        max_vals = max_vals.max(nv);
                        if page_idx < 4 {
                            println!(
                                "  data page #{data_pages}  num_values={nv}  csize={}  usize={}",
                                hdr.compressed_page_size, hdr.uncompressed_page_size
                            );
                        }
                    }
                    page_idx += 1;
                }
                Ok(None) => break,
                Err(e) => {
                    println!("  ERROR at page #{page_idx}: {e}");
                    break;
                }
            }
        }
        let avg = if data_pages > 0 {
            total_values / data_pages
        } else {
            0
        };
        println!(
            "  >>> {col_name}: {data_pages} data pages, total_values={total_values}, \
             values/page avg={avg} min={} max={max_vals}",
            if min_vals == u64::MAX { 0 } else { min_vals }
        );
        // At Q08 selectivity (37,234 survivors of 60M = 0.062%), the chance a
        // page of P values has >=1 survivor is 1-(1-0.00062)^P. Report the
        // page-skip fraction a perfect late-mat could achieve.
        let p = avg as f64;
        let hit = 1.0 - (1.0 - 0.00062f64).powf(p);
        println!(
            "  >>> at 0.062% scatter, ~{:.1}% of pages contain a survivor => late-mat could skip ~{:.1}% of payload decompress",
            hit * 100.0,
            (1.0 - hit) * 100.0
        );
    }
    Ok(())
}
