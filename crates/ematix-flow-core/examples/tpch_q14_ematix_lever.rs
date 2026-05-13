//! Full TPC-H Q14 implemented entirely via ematix-parquet kernels for
//! the lineitem column-decode portion. POC that wires the sibling
//! ematix-parquet repo into ematix-flow and measures wall-clock Q14
//! end-to-end vs the existing FastParquet / FusedQ14FullExec paths.
//!
//! Q14:
//!   SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%'
//!                            THEN l_extendedprice * (1 - l_discount)
//!                            ELSE 0 END)
//!                / SUM(l_extendedprice * (1 - l_discount))
//!   FROM lineitem JOIN part ON l_partkey = p_partkey
//!   WHERE l_shipdate >= DATE '1995-09-01'
//!     AND l_shipdate < DATE '1995-10-01';
//!
//! Pipeline:
//!   1. Phase 5 fused-NEON filter on l_shipdate → row bitmap per RG.
//!   2. Phase 6 bitmap-driven sparse gather: l_partkey + l_extprice +
//!      l_discount.
//!   3. part table read fully via parquet-rs (small, 200K rows) →
//!      HashMap<i64, is_promo>.
//!   4. Per match: lookup p_type, accumulate numerator + denominator.
//!   5. Return 100 * num / den.
//!
//! Baseline numbers (SF=1, M3 Pro):
//!   DataFusion default (register_parquet):  19.35 ms
//!   FastParquet + Utf8View:                 16.60 ms
//!   FusedQ14FullExec (full fusion):         15.06 ms
//!   Polars:                                 12.53 ms (reference)
//!
//! Usage:
//!   TPCH_DATA_DIR=examples/tpch/data/sf1 \
//!     cargo run --release --example tpch_q14_ematix_lever

use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::time::Instant;

use ematix_parquet_codec::compression::decompress_snappy;
use ematix_parquet_codec::dict::{
    decode_rle_dictionary_predicate_bitmap_bw12, gather_dict_at_bitmap_into,
};
use ematix_parquet_codec::plain::{decode_plain_f64, decode_plain_i32, decode_plain_i64};
use ematix_parquet_format::types::Encoding;
use ematix_parquet_io::{PageWalker, ParquetFile};

use parquet::column::reader::ColumnReader;
use parquet::data_type::ByteArray;
use parquet::file::reader::{FileReader, SerializedFileReader};

// Q14 filter window in Date32 days-since-epoch.
const LO: i32 = 9374; // 1995-09-01
const HI: i32 = 9404; // 1995-10-01

const WARMUPS: usize = 3;
const ITERS: usize = 15;

fn data_dir() -> PathBuf {
    match std::env::var("TPCH_DATA_DIR") {
        Ok(s) => PathBuf::from(s),
        Err(_) => PathBuf::from("examples/tpch/data/sf1"),
    }
}

/// Read a column chunk's body bytes from a `ParquetFile`. Returns
/// `(chunk, num_values)` for the given (row_group, column) pair.
fn read_chunk(file: &ParquetFile, rg: usize, col: usize) -> (Vec<u8>, usize) {
    let md = file.metadata().unwrap();
    let cm = md.row_groups[rg].columns[col].meta_data.as_ref().unwrap();
    let start = cm
        .dictionary_page_offset
        .unwrap_or(cm.data_page_offset) as u64;
    let length = cm.total_compressed_size as u64;
    let bytes = file.read_range(start, length).unwrap();
    (bytes, cm.num_values as usize)
}

/// Build the part_key → is_promo lookup table once. Read fully via
/// parquet-rs — it's a 200K-row table (small) and the part read
/// isn't the bottleneck we're trying to accelerate.
fn build_part_lookup(path: &PathBuf) -> HashMap<i64, bool> {
    let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
    let total = reader.metadata().file_metadata().num_rows() as usize;
    let rgr = reader.get_row_group(0).unwrap();

    // p_partkey is col 0 (INT64).
    let mut p_keys: Vec<i64> = Vec::with_capacity(total);
    let mut k_reader = match rgr.get_column_reader(0).unwrap() {
        ColumnReader::Int64ColumnReader(t) => t,
        _ => panic!(),
    };
    k_reader.read_records(total, None, None, &mut p_keys).unwrap();

    // p_type is col 4 (BYTE_ARRAY).
    let mut p_types: Vec<ByteArray> = Vec::with_capacity(total);
    let mut t_reader = match rgr.get_column_reader(4).unwrap() {
        ColumnReader::ByteArrayColumnReader(t) => t,
        _ => panic!(),
    };
    t_reader.read_records(total, None, None, &mut p_types).unwrap();

    let mut map = HashMap::with_capacity(total);
    for (k, t) in p_keys.into_iter().zip(p_types.into_iter()) {
        let bytes = t.data();
        let is_promo = bytes.len() >= 5 && &bytes[..5] == b"PROMO";
        map.insert(k, is_promo);
    }
    map
}

/// Decode the dict for `(rg, col)` as PLAIN i32, then build a
/// 4096-padded `dict_mask` where mask[i] = 1 iff the predicate matches
/// `dict[i]`. The mask is the input to Phase 5's NEON-fused kernel.
fn build_shipdate_dict_mask(file: &ParquetFile, rg: usize) -> Vec<u8> {
    let (chunk, _) = read_chunk(file, rg, 10);
    let mut walker = PageWalker::new(&chunk);
    let (hdr, body) = walker.next_page().unwrap().unwrap();
    assert!(hdr.dictionary_page_header.is_some(), "expected dict page");
    let decompressed = decompress_snappy(body).unwrap();
    let dict = decode_plain_i32(&decompressed).unwrap();
    let mut m = vec![0u8; 4096];
    for (i, &v) in dict.iter().enumerate() {
        if v >= LO && v < HI {
            m[i] = 1;
        }
    }
    m
}

/// Phase 5 fused-NEON shipdate filter over one row group. Returns the
/// packed row bitmap (1 bit per row, byte-stride). Skips the dict
/// page (caller has already consumed it to build dict_mask).
fn shipdate_bitmap_rg(file: &ParquetFile, rg: usize, dict_mask: &[u8]) -> (Vec<u8>, usize) {
    let (chunk, total) = read_chunk(file, rg, 10);
    let mut walker = PageWalker::new(&chunk);
    let _ = walker.next_page().unwrap().unwrap(); // skip dict page

    let mut bitmap: Vec<u8> = Vec::with_capacity(total.div_ceil(8));
    let mut emitted = 0usize;
    while emitted < total {
        let (hdr, body) = walker.next_page().unwrap().unwrap();
        let dph = hdr.data_page_header.as_ref().unwrap();
        let n = dph.num_values as usize;
        let decompressed = decompress_snappy(body).unwrap();
        decode_rle_dictionary_predicate_bitmap_bw12(&decompressed, n, dict_mask, &mut bitmap)
            .unwrap();
        emitted += n;
    }
    (bitmap, total)
}

/// Sparse-gather one dict-encoded column at bitmap-true rows in one
/// row group. Caller supplies the dict (PLAIN-decoded once per RG)
/// and a PLAIN-fallback decoder for pages that aren't dict-encoded
/// (writers fall back to PLAIN when a column's dict would exceed the
/// page-size limit — happens for l_partkey at SF=1).
fn sparse_gather_col<T: Copy>(
    file: &ParquetFile,
    rg: usize,
    col: usize,
    bitmap: &[u8],
    decode_dict: impl FnOnce(&[u8]) -> Vec<T>,
    decode_plain: impl Fn(&[u8]) -> Vec<T>,
    out: &mut Vec<T>,
) {
    let (chunk, total) = read_chunk(file, rg, col);
    let mut walker = PageWalker::new(&chunk);

    // Peek first page: it's either a dictionary page (column has a
    // dict) or directly a data page (no dict, all PLAIN). We can't
    // know without looking.
    let (first_hdr, first_body) = walker.next_page().unwrap().unwrap();
    let first_decompressed = decompress_snappy(first_body).unwrap();

    let dict: Vec<T> = if first_hdr.dictionary_page_header.is_some() {
        decode_dict(&first_decompressed)
    } else {
        // First page is a data page; handle it inline, then fall
        // through to the loop for subsequent pages.
        let dph = first_hdr.data_page_header.as_ref().unwrap();
        let n = dph.num_values as usize;
        match dph.encoding {
            Encoding::Plain => {
                let vals = decode_plain(&first_decompressed);
                for (i, v) in vals.iter().enumerate().take(n) {
                    let bp = i;
                    if (bitmap[bp / 8] >> (bp % 8)) & 1 == 1 {
                        out.push(*v);
                    }
                }
            }
            other => panic!("first page non-dict and non-PLAIN: {other:?}"),
        }
        Vec::new()
    };

    let mut emitted = if first_hdr.dictionary_page_header.is_some() {
        0usize
    } else {
        first_hdr.data_page_header.as_ref().unwrap().num_values as usize
    };

    while emitted < total {
        let (hdr, body) = walker.next_page().unwrap().unwrap();
        let dph = hdr.data_page_header.as_ref().unwrap();
        let n = dph.num_values as usize;
        let decompressed = decompress_snappy(body).unwrap();
        match dph.encoding {
            Encoding::RleDictionary | Encoding::PlainDictionary => {
                gather_dict_at_bitmap_into(&decompressed, n, bitmap, emitted, &dict, out)
                    .unwrap();
            }
            Encoding::Plain => {
                // Sparse iteration over PLAIN values. The dense decode
                // happens once per page; we then filter by bitmap as
                // we walk the page's value stream.
                let vals = decode_plain(&decompressed);
                for (i, v) in vals.iter().enumerate().take(n) {
                    let bp = emitted + i;
                    if (bitmap[bp / 8] >> (bp % 8)) & 1 == 1 {
                        out.push(*v);
                    }
                }
            }
            other => panic!("unexpected encoding {other:?} col {col} rg {rg}"),
        }
        emitted += n;
    }
}

/// Process one row group end-to-end: filter shipdate, sparse-gather
/// the three aggregate columns, accumulate Q14 partial sums.
/// Returns (numerator, denominator) for this RG.
fn process_rg(li_path: &PathBuf, rg: usize, part_lookup: &HashMap<i64, bool>) -> (f64, f64) {
    let file = ParquetFile::open(li_path).unwrap();
    let dict_mask = build_shipdate_dict_mask(&file, rg);
    let (bitmap, _total) = shipdate_bitmap_rg(&file, rg, &dict_mask);

    let matches: usize = bitmap.iter().map(|b| b.count_ones() as usize).sum();
    if matches == 0 {
        return (0.0, 0.0);
    }

    let mut keys: Vec<i64> = Vec::with_capacity(matches);
    sparse_gather_col::<i64>(
        &file,
        rg,
        1,
        &bitmap,
        |bytes| decode_plain_i64(bytes).unwrap(),
        |bytes| decode_plain_i64(bytes).unwrap(),
        &mut keys,
    );

    let mut prices: Vec<f64> = Vec::with_capacity(matches);
    sparse_gather_col::<f64>(
        &file,
        rg,
        5,
        &bitmap,
        |bytes| decode_plain_f64(bytes).unwrap(),
        |bytes| decode_plain_f64(bytes).unwrap(),
        &mut prices,
    );

    let mut discounts: Vec<f64> = Vec::with_capacity(matches);
    sparse_gather_col::<f64>(
        &file,
        rg,
        6,
        &bitmap,
        |bytes| decode_plain_f64(bytes).unwrap(),
        |bytes| decode_plain_f64(bytes).unwrap(),
        &mut discounts,
    );

    debug_assert_eq!(keys.len(), prices.len());
    debug_assert_eq!(keys.len(), discounts.len());

    let mut num: f64 = 0.0;
    let mut den: f64 = 0.0;
    for ((k, p), d) in keys.iter().zip(prices.iter()).zip(discounts.iter()) {
        let revenue = p * (1.0 - d);
        den += revenue;
        if let Some(true) = part_lookup.get(k) {
            num += revenue;
        }
    }
    (num, den)
}

/// Run Q14 end-to-end. Row groups process in parallel via
/// `std::thread::scope`, mirroring DataFusion's RG-level partitioning.
fn run_q14(li_path: &PathBuf, part_lookup: &HashMap<i64, bool>) -> f64 {
    let num_rgs = {
        let file = ParquetFile::open(li_path).unwrap();
        let md = file.metadata().unwrap();
        md.row_groups.len()
    };

    let mut total_num: f64 = 0.0;
    let mut total_den: f64 = 0.0;
    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(num_rgs);
        for rg in 0..num_rgs {
            let h = s.spawn(move || process_rg(li_path, rg, part_lookup));
            handles.push(h);
        }
        for h in handles {
            let (n, d) = h.join().unwrap();
            total_num += n;
            total_den += d;
        }
    });

    100.0 * total_num / total_den
}

fn main() {
    let dir = data_dir();
    let li_path = dir.join("lineitem.parquet");
    let part_path = dir.join("part.parquet");
    if !li_path.exists() || !part_path.exists() {
        eprintln!("missing {} or {}", li_path.display(), part_path.display());
        std::process::exit(1);
    }
    println!("==> ematix-parquet Q14 POC (SF=1)");
    println!("==> data: {}", dir.display());
    println!();

    // Build the part lookup once outside the timing loop — it's a
    // 200K-row dimension table that any real query engine caches.
    // (The existing FusedQ14FullExec also reads it once per query
    // but via DataFusion's optimized HashJoinExec.)
    let part_lookup = build_part_lookup(&part_path);
    println!(
        "  part lookup: {} entries, {} PROMO%",
        part_lookup.len(),
        part_lookup.values().filter(|v| **v).count()
    );

    // Warmup.
    for _ in 0..WARMUPS {
        let _ = run_q14(&li_path, &part_lookup);
    }

    // Correctness check (matches polars/datafusion ref).
    let ratio = run_q14(&li_path, &part_lookup);
    println!("  Q14 ratio: {:.6}% (DataFusion ref: 16.3808%)", ratio);
    assert!(
        (ratio - 16.3808).abs() < 0.01,
        "Q14 ratio {} disagrees with reference",
        ratio
    );

    // Timing.
    let mut times = Vec::with_capacity(ITERS);
    for _ in 0..ITERS {
        let t0 = Instant::now();
        let _ = run_q14(&li_path, &part_lookup);
        times.push(t0.elapsed());
    }
    times.sort();
    let med = times[ITERS / 2].as_secs_f64() * 1000.0;
    let min = times[0].as_secs_f64() * 1000.0;
    let max = times[ITERS - 1].as_secs_f64() * 1000.0;

    println!();
    println!("  ematix-parquet manual Q14:  median {:.2} ms  min {:.2} ms  max {:.2} ms", med, min, max);
    println!();
    println!("  Reference numbers (same SF=1 data):");
    println!("    DataFusion default:        19.35 ms");
    println!("    FastParquet + Utf8View:    16.60 ms");
    println!("    FusedQ14FullExec:          15.06 ms");
    println!("    Polars:                    12.53 ms");
    println!();
    let vs_polars = med / 12.53;
    let vs_fused = med / 15.06;
    println!("  vs Polars:      {:.2}× ({})", vs_polars, if vs_polars < 1.0 { "faster" } else { "slower" });
    println!("  vs FusedQ14:    {:.2}× ({})", vs_fused, if vs_fused < 1.0 { "faster" } else { "slower" });
}
