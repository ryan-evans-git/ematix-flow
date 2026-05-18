//! Σ.E5.2b — diagnostic 3: per-stage attribution inside ematix-parquet.
//!
//! Re-implements the body of
//! `ematix_parquet_codec::read::read_column_byte_array_dict_preserved_into`
//! inline using the public surface (`ParquetFile`, `PageWalker`,
//! `decompress_snappy_into`, `decode_plain_byte_array`,
//! `decode_rle_dictionary_indices`) — but with `Instant::now()`
//! checkpoints between each stage. This gives a verifiable per-stage
//! cost breakdown of the dict-preserved decode of `l_returnflag`
//! without touching upstream sources.
//!
//! Stages (per row group, summed across all RGs in the file):
//!   1. **footer**         — `ParquetFile::metadata()`            (per-file, amortised)
//!   2. **read_range**     — `file.read_range(...)` for the chunk (per-RG)
//!   3. **page_walk**      — `PageWalker::next_page()` thrift parse loop
//!   4. **snappy**         — `decompress_snappy_into` for dict + data pages
//!   5. **dict_plain**     — `decode_plain_byte_array` on the dict page
//!   6. **rle_indices**    — `decode_rle_dictionary_indices` per data page
//!   7. **assembly**       — accumulate `dict_bytes` / `dict_offsets` / `indices`
//!
//! Run:
//!     cargo run --release -p ematix-flow-core --example diag_dict_decode_stages

use std::path::PathBuf;
use std::time::{Duration, Instant};

use ematix_parquet_codec::compression::decompress_snappy_into;
use ematix_parquet_codec::dict::decode_rle_dictionary_indices;
use ematix_parquet_codec::plain::decode_plain_byte_array;
use ematix_parquet_format::types::{CompressionCodec, Encoding, PageType};
use ematix_parquet_io::{PageWalker, ParquetFile};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TRIALS: usize = 21;
const WARMUPS: usize = 3;
const L_RETURNFLAG_COL: usize = 8;

#[derive(Default, Debug, Clone, Copy)]
struct Stages {
    footer: Duration,
    read_range: Duration,
    page_walk: Duration,
    snappy: Duration,
    dict_plain: Duration,
    rle_indices: Duration,
    assembly: Duration,
    total: Duration,
}

impl Stages {
    fn ms(d: Duration) -> f64 {
        d.as_secs_f64() * 1000.0
    }
}

/// Walks all row groups of `path`, decoding `l_returnflag`
/// dict-preserved and returning per-stage timings.
fn decode_with_stages(path: &str) -> Stages {
    let mut s = Stages::default();
    let total_t0 = Instant::now();

    let file = ParquetFile::open(path).unwrap();
    let t0 = Instant::now();
    let md = file.metadata().unwrap();
    s.footer += t0.elapsed();

    let n_rgs = md.row_groups.len();
    for rg in 0..n_rgs {
        let cm = md.row_groups[rg].columns[L_RETURNFLAG_COL]
            .meta_data
            .as_ref()
            .unwrap();
        let codec = cm.codec;
        let start = cm
            .dictionary_page_offset
            .filter(|&d| d < cm.data_page_offset)
            .unwrap_or(cm.data_page_offset) as u64;
        let length = cm.total_compressed_size as u64;

        let t0 = Instant::now();
        let chunk_bytes = file.read_range(start, length).unwrap();
        s.read_range += t0.elapsed();

        let mut walker = PageWalker::new(&chunk_bytes);
        let mut decomp: Vec<u8> = Vec::new();
        let mut dict_bytes: Vec<u8> = Vec::new();
        let mut dict_offsets: Vec<u32> = vec![0u32];
        let mut indices: Vec<u32> = Vec::with_capacity(cm.num_values as usize);

        loop {
            let t_pw = Instant::now();
            let next = walker.next_page().unwrap();
            s.page_walk += t_pw.elapsed();
            let Some((hdr, body)) = next else { break };

            match hdr.page_type {
                PageType::DictionaryPage => {
                    let t_s = Instant::now();
                    decompress_snappy_into(body, &mut decomp).unwrap();
                    let snappy_d = t_s.elapsed();
                    // Snappy is only correct if codec is Snappy — TPC-H
                    // canonical writers (DuckDB, pyarrow) all default to
                    // Snappy, but assert for safety.
                    assert_eq!(
                        codec,
                        CompressionCodec::Snappy,
                        "this diagnostic assumes Snappy; got {codec:?}"
                    );
                    s.snappy += snappy_d;

                    let t_p = Instant::now();
                    let slices = decode_plain_byte_array(&decomp).unwrap();
                    let dict_plain_d = t_p.elapsed();
                    s.dict_plain += dict_plain_d;

                    let t_a = Instant::now();
                    for sl in &slices {
                        dict_bytes.extend_from_slice(sl);
                        dict_offsets.push(dict_bytes.len() as u32);
                    }
                    s.assembly += t_a.elapsed();
                }
                PageType::DataPage | PageType::DataPageV2 => {
                    // For DataPageV2 the body is split, but for v1
                    // (TPC-H standard) the whole body is the encoded
                    // data. The codec's own reader handles both — for
                    // diagnostic purposes we follow the v1 shape.
                    let dph = hdr.data_page_header.as_ref().unwrap();

                    let t_s = Instant::now();
                    decompress_snappy_into(body, &mut decomp).unwrap();
                    s.snappy += t_s.elapsed();

                    match dph.encoding {
                        Encoding::RleDictionary | Encoding::PlainDictionary => {
                            let t_r = Instant::now();
                            let mut page_indices =
                                decode_rle_dictionary_indices(&decomp, dph.num_values as usize)
                                    .unwrap();
                            s.rle_indices += t_r.elapsed();

                            let t_a = Instant::now();
                            indices.append(&mut page_indices);
                            s.assembly += t_a.elapsed();
                        }
                        other => panic!("unexpected encoding {other:?}"),
                    }
                }
                _ => {}
            }
            if indices.len() >= cm.num_values as usize {
                break;
            }
        }
        // Touch outputs to keep them live.
        std::hint::black_box(&dict_bytes);
        std::hint::black_box(&dict_offsets);
        std::hint::black_box(&indices);
    }

    s.total = total_t0.elapsed();
    s
}

fn stats_ms(xs: &[f64]) -> (f64, f64) {
    let mut v = xs.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = v[v.len() / 2];
    let mean = v.iter().sum::<f64>() / v.len() as f64;
    let var = v.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / v.len() as f64;
    (median, var.sqrt())
}

fn main() {
    let dir = std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf1".into());
    let path: PathBuf = PathBuf::from(&dir).join("lineitem.parquet");
    assert!(
        path.exists(),
        "lineitem.parquet not found at {}; set TPCH_DATA_DIR",
        path.display()
    );
    let path_s = path.to_string_lossy().to_string();

    println!("=== Σ.E5.2b diag 3: per-stage attribution, l_returnflag, ematix-parquet ===");
    println!("file: {}", path_s);
    println!("trials: {TRIALS} (warmups {WARMUPS})\n");

    let mut totals: Vec<f64> = Vec::new();
    let mut footers: Vec<f64> = Vec::new();
    let mut read_ranges: Vec<f64> = Vec::new();
    let mut page_walks: Vec<f64> = Vec::new();
    let mut snappys: Vec<f64> = Vec::new();
    let mut dict_plains: Vec<f64> = Vec::new();
    let mut rle_indicess: Vec<f64> = Vec::new();
    let mut assemblies: Vec<f64> = Vec::new();

    for t in 0..(TRIALS + WARMUPS) {
        let s = decode_with_stages(&path_s);
        if t >= WARMUPS {
            totals.push(Stages::ms(s.total));
            footers.push(Stages::ms(s.footer));
            read_ranges.push(Stages::ms(s.read_range));
            page_walks.push(Stages::ms(s.page_walk));
            snappys.push(Stages::ms(s.snappy));
            dict_plains.push(Stages::ms(s.dict_plain));
            rle_indicess.push(Stages::ms(s.rle_indices));
            assemblies.push(Stages::ms(s.assembly));
        }
    }

    let (t_med, t_sd) = stats_ms(&totals);
    let report = |label: &str, xs: &[f64], total_med: f64| {
        let (med, sd) = stats_ms(xs);
        let pct = 100.0 * med / total_med;
        println!("  {label:<14} median ± σ: {med:7.3} ± {sd:5.3} ms  ({pct:5.1}%)");
    };
    println!("total          median ± σ: {t_med:7.3} ± {t_sd:5.3} ms  (100.0%)");
    report("footer", &footers, t_med);
    report("read_range", &read_ranges, t_med);
    report("page_walk", &page_walks, t_med);
    report("snappy", &snappys, t_med);
    report("dict_plain", &dict_plains, t_med);
    report("rle_indices", &rle_indicess, t_med);
    report("assembly", &assemblies, t_med);

    let sum_stages = stats_ms(&footers).0
        + stats_ms(&read_ranges).0
        + stats_ms(&page_walks).0
        + stats_ms(&snappys).0
        + stats_ms(&dict_plains).0
        + stats_ms(&rle_indicess).0
        + stats_ms(&assemblies).0;
    println!(
        "  sum_stages    median:    {sum_stages:7.3} ms      ({:5.1}%)",
        100.0 * sum_stages / t_med
    );
    println!("  (difference = timing-loop overhead + uncounted micro-paths)");
}
