//! Dump the first bytes of l_shipdate's data page after decompression
//! for both lineitem files. If they differ in the first 4-8 bytes, the
//! LZ4 file is using a length-prefix wire format that ematix-parquet's
//! `gather_dict_at_bitmap_into` doesn't handle.

use ematix_parquet_codec::compression::{decompress_lz4_raw_into_sized, decompress_snappy_into};
use ematix_parquet_format::types::CompressionCodec;
use ematix_parquet_io::ParquetFile;
use ematix_parquet_io::pages::PageWalker;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    for filename in &["lineitem.parquet", "lineitem_lz4.parquet"] {
        let path = PathBuf::from("examples/tpch/data/sf10").join(filename);
        if !path.exists() {
            println!("skipping (not present): {}", path.display());
            continue;
        }
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
            .expect("l_shipdate");
        let cm = md.row_groups[0].columns[col].meta_data.as_ref().unwrap();
        let offset = cm.dictionary_page_offset.unwrap_or(cm.data_page_offset) as u64;
        let length = cm.total_compressed_size as u64;
        let chunk = file.read_range(offset, length)?;
        let mut walker = PageWalker::with_byte_limit(&chunk, length as usize);

        // Skip dict page.
        let _dict = walker.next_page()?.expect("dict");
        let (hdr, body) = walker.next_page()?.expect("data page");

        let codec = cm.codec;
        let usize_hdr = hdr.uncompressed_page_size as usize;
        let mut decomp = Vec::new();
        match codec {
            CompressionCodec::Snappy => decompress_snappy_into(body, &mut decomp)?,
            CompressionCodec::Lz4Raw => {
                decompress_lz4_raw_into_sized(body, usize_hdr, &mut decomp)?
            }
            other => panic!("unexpected codec: {other:?}"),
        }
        println!(
            "\n{} : codec={:?} usize={} decomp.len()={}",
            filename,
            codec,
            usize_hdr,
            decomp.len()
        );
        println!(
            "  encoding={:?}",
            hdr.data_page_header.as_ref().unwrap().encoding
        );
        println!("  first 16 bytes: {:02x?}", &decomp[..16.min(decomp.len())]);
        // Interpret first 4 bytes as LE u32 in case it's a length prefix:
        if decomp.len() >= 4 {
            let prefix = u32::from_le_bytes([decomp[0], decomp[1], decomp[2], decomp[3]]);
            println!(
                "  first 4 as LE u32: {prefix} (decomp.len() - 4 = {})",
                decomp.len() - 4
            );
        }
    }
    Ok(())
}
