//! Σ.E7 probe: is Snappy decompress the wall on incompressible columns?
//!
//! Walks every page of l_quantity (ratio = 1.00 — Snappy provides no
//! compression) and times:
//!   1. The full `decompress_snappy_into` path (what production does)
//!   2. A raw memcpy of the body bytes (theoretical lower bound)
//!
//! If snap is close to memcpy, Snappy isn't the bottleneck. If it's
//! much slower, the literal-only fast path is a real lever.
//!
//! Run:
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 \
//!     cargo run --release -p ematix-flow-core --example snappy_decompress_probe

use std::path::PathBuf;
use std::time::Instant;

use ematix_parquet_codec::compression::decompress_snappy_into;
use ematix_parquet_format::types::{CompressionCodec, PageType};
use ematix_parquet_io::{PageWalker, ParquetFile};

const COL_QUANTITY: usize = 4; // l_quantity (F64, ratio ≈ 1.00)
const COL_EXTPRICE: usize = 5; // l_extendedprice (F64, ratio ≈ 0.73)

fn probe(file: &ParquetFile, col_idx: usize, label: &str, reps: usize) {
    let md = file.metadata().expect("md");
    let n_rgs = md.row_groups.len();
    let mut total_body_bytes: usize = 0;
    let mut total_decomp_bytes: usize = 0;
    let mut total_pages: usize = 0;

    let mut snap_times: Vec<f64> = Vec::new();
    let mut memcpy_times: Vec<f64> = Vec::new();

    for _ in 0..reps {
        // Snappy decompress timing.
        let t = Instant::now();
        let mut decomp: Vec<u8> = Vec::with_capacity(128 * 1024);
        for rg in 0..n_rgs {
            let cm = md.row_groups[rg].columns[col_idx]
                .meta_data
                .as_ref()
                .expect("cm");
            let start = cm
                .dictionary_page_offset
                .unwrap_or(cm.data_page_offset) as u64;
            let length = cm.total_compressed_size as u64;
            let chunk = file.read_range(start, length).expect("read_range");
            let mut walker = PageWalker::new(&chunk);
            while let Some((_hdr, body)) = walker.next_page().expect("next_page") {
                total_body_bytes += body.len();
                total_pages += 1;
                decompress_snappy_into(body, &mut decomp).expect("snappy");
                total_decomp_bytes += decomp.len();
            }
        }
        snap_times.push(t.elapsed().as_secs_f64() * 1000.0);

        // Raw memcpy: copy the body bytes once, no decode. Lower-bound
        // reference (what zero-decode-cost would look like).
        let t = Instant::now();
        let mut dest: Vec<u8> = Vec::with_capacity(128 * 1024);
        for rg in 0..n_rgs {
            let cm = md.row_groups[rg].columns[col_idx]
                .meta_data
                .as_ref()
                .expect("cm");
            let start = cm
                .dictionary_page_offset
                .unwrap_or(cm.data_page_offset) as u64;
            let length = cm.total_compressed_size as u64;
            let chunk = file.read_range(start, length).expect("read_range");
            let mut walker = PageWalker::new(&chunk);
            while let Some((_hdr, body)) = walker.next_page().expect("next_page") {
                dest.clear();
                dest.extend_from_slice(body);
            }
        }
        memcpy_times.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    snap_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    memcpy_times.sort_by(|a, b| a.partial_cmp(b).unwrap());

    // Per rep: total_decomp_bytes / reps.
    let decomp_per_rep = total_decomp_bytes / reps;
    let snap_median = snap_times[snap_times.len() / 2];
    let memcpy_median = memcpy_times[memcpy_times.len() / 2];
    let snap_gbs = (decomp_per_rep as f64 / 1e9) / (snap_median / 1000.0);
    let memcpy_gbs = (decomp_per_rep as f64 / 1e9) / (memcpy_median / 1000.0);
    let overhead = snap_median - memcpy_median;
    let codec = md.row_groups[0].columns[col_idx].meta_data.as_ref().unwrap().codec;

    println!("--- {label} (col {col_idx}, codec {codec:?}) ---");
    println!(
        "  pages={}/{reps} (per-rep), uncomp bytes/rep = {:.2} MB",
        total_pages / reps,
        decomp_per_rep as f64 / 1e6
    );
    println!(
        "  Snappy decompress:  median {snap_median:>7.2} ms  ({snap_gbs:.2} GB/s)"
    );
    println!(
        "  Raw memcpy (ref):   median {memcpy_median:>7.2} ms  ({memcpy_gbs:.2} GB/s)"
    );
    println!("  Snappy overhead:    {overhead:>7.2} ms  ({:.1}× memcpy)", snap_median / memcpy_median);
    println!();
}

fn main() {
    let dir = std::env::var("TPCH_DATA_DIR")
        .unwrap_or_else(|_| "examples/tpch/data/sf10".to_string());
    let path = PathBuf::from(&dir).join("lineitem.parquet");
    let file = ParquetFile::open(&path).expect("open");

    let reps: usize = std::env::var("REPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    println!("=== Snappy decompress probe ({}, {reps} reps) ===\n", path.display());

    probe(&file, COL_QUANTITY, "l_quantity (ratio ≈ 1.00)", reps);
    probe(&file, COL_EXTPRICE, "l_extendedprice (ratio ≈ 0.73)", reps);

    println!("Interpretation:");
    println!("  Snappy overhead/memcpy ≥ 2.0 → fast-path lever has real headroom");
    println!("  ≤ 1.3                       → Snappy already close to memcpy; gap is elsewhere");
}

// Unused import guard.
#[allow(dead_code)]
fn _silence(_p: PageType, _c: CompressionCodec) {}
