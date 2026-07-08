//! Σ.SC P5 — with/without-sidecar point-lookup bench (local fixture, no AWS).
//!
//! Measures the same eq lookup through both read paths on one fixture:
//!   - **scan**: `SELECT val FROM t WHERE id = K` through the production
//!     `EmatixFastParquetTableProvider` (BridgeFilter pushdown, vectorized);
//!   - **index**: the Phase 1 sidecar primitive via `sidecar_i64_eq_opt` with
//!     the P3 gate held open (threshold 1.01) — we WANT to measure the index
//!     on unselective predicates, that's where the crossover is.
//!
//! The fixture plants one dedicated key per match-fraction
//! (0.0001/0.001/0.01/0.1 of N rows), scattered at a uniform stride so the
//! masked decode cannot cheat by touching one contiguous page run. Each cell
//! is the median of `--reps` runs (default 5). Results also print what the
//! P3 gate (default `EMAT_SIDECAR_MAX_SELECTIVITY=0.05`) would have chosen,
//! so the table doubles as a sanity check of the gate's placement relative
//! to the measured crossover.
//!
//! Run:
//!   cargo run --release -p ematix-flow-core --example sidecar_bench
//!   cargo run --release -p ematix-flow-core --example sidecar_bench -- \
//!       --rows 10000000 --reps 5
//!
//! The fixture is cached under `$TMPDIR/ematix_sidecar_bench/` keyed by N —
//! delete that directory to force regeneration.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::sidecar_build::build_sorted_sidecar;
use ematix_flow_core::sidecar_index::{
    estimate_eq_selectivity, sidecar_i64_eq_opt, sidecar_max_selectivity, sidecar_path,
};
use ematix_parquet_codec::write::{ColumnData, write_table_to_path_with_row_group_size};
use ematix_parquet_format::types::CompressionCodec;

const IDX: &str = "idx_id";
/// Match fractions swept, each with its own planted key (1-based).
const FRACTIONS: &[f64] = &[0.0001, 0.001, 0.01, 0.1];
/// Filler ids start here so planted keys (1..=4) never collide.
const FILLER_BASE: i64 = 1_000;
/// Consecutive rows sharing one filler id (l_orderkey-style clustering,
/// ~4-16 rows/key). The sorted-index builder buckets one rowset bitmap per
/// (value, page); clustered duplicates keep that at ~1 triple per distinct
/// value. A fully-unique id column is the builder's worst case — one
/// page-sized bitmap per ROW, which blows past snappy's 4 GiB buffer cap
/// somewhere below 300K single-page rows.
const FILLER_DUP: usize = 16;
/// Source row-group size (== page size in this writer: one PLAIN page per
/// RG). Small pages are what give the index skip granularity — and they cap
/// each rowset bitmap at RG_ROWS/8 bytes (1 KiB here).
const RG_ROWS: usize = 8_192;

/// Plant `round(n * fraction)` occurrences of each fraction's key into an
/// otherwise-clustered-duplicate id column, scattered at a uniform stride so
/// the masked decode cannot cheat by touching one page run. Returns
/// `(ids, per-fraction match counts)`.
fn build_ids(n: usize) -> (Vec<i64>, Vec<usize>) {
    let mut ids: Vec<i64> = (0..n)
        .map(|i| FILLER_BASE + (i / FILLER_DUP) as i64)
        .collect();
    let mut counts = Vec::with_capacity(FRACTIONS.len());
    for (fi, f) in FRACTIONS.iter().enumerate() {
        let key = (fi + 1) as i64;
        let want = ((n as f64) * f).round().max(1.0) as usize;
        let stride = (n / want).max(1);
        let mut placed = 0;
        // Walk from a per-key offset; skip slots another key already took.
        let mut pos = fi;
        while placed < want && pos < n {
            if ids[pos] >= FILLER_BASE {
                ids[pos] = key;
                placed += 1;
            }
            pos += stride;
        }
        // Dense fallback for any remainder (only when strides collided).
        let mut pos = 0;
        while placed < want && pos < n {
            if ids[pos] >= FILLER_BASE {
                ids[pos] = key;
                placed += 1;
            }
            pos += 1;
        }
        counts.push(placed);
    }
    (ids, counts)
}

/// Write (or reuse) the fixture parquet + its Phase 2 sidecar; returns the
/// parquet path. Cached by (N, codec) because a 10M-row write is the slowest
/// step.
fn ensure_fixture(n: usize, codec: CompressionCodec, codec_name: &str) -> (PathBuf, Vec<usize>) {
    let dir = std::env::temp_dir().join("ematix_sidecar_bench");
    std::fs::create_dir_all(&dir).expect("create bench dir");
    let path = dir.join(format!(
        "sidecar_bench_{n}_rg{RG_ROWS}_dup{FILLER_DUP}_{codec_name}.parquet"
    ));
    let (_, counts) = {
        // Counts are deterministic for a given N; recompute cheaply even on
        // cache hit (the vector build is O(N) but allocation-only).
        let (ids, counts) = build_ids(n);
        if !path.is_file() {
            eprintln!("generating fixture ({n} rows) at {path:?} ...");
            let vals: Vec<i64> = (0..n as i64).collect();
            write_table_to_path_with_row_group_size(
                &path,
                &[
                    ("id", ColumnData::I64(&ids)),
                    ("val", ColumnData::I64(&vals)),
                ],
                codec,
                RG_ROWS,
            )
            .expect("write fixture parquet");
        }
        (ids, counts)
    };
    if !sidecar_path(&path).exists() {
        eprintln!("building sidecar ...");
        let t = Instant::now();
        build_sorted_sidecar(&path, IDX, "id", None).expect("build sidecar");
        eprintln!("sidecar built in {:.1} ms", t.elapsed().as_secs_f64() * 1e3);
    }
    (path, counts)
}

fn median(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    xs[xs.len() / 2]
}

/// One timed full-scan lookup; returns (ms, match_count, checksum).
async fn scan_once(ctx: &SessionContext, key: i64) -> (f64, usize, i64) {
    let t = Instant::now();
    let batches = ctx
        .sql(&format!("SELECT val FROM t WHERE id = {key}"))
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    let ms = t.elapsed().as_secs_f64() * 1e3;
    let mut count = 0usize;
    let mut sum = 0i64;
    for b in &batches {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("val is i64");
        count += col.len();
        sum = col.iter().flatten().fold(sum, i64::wrapping_add);
    }
    (ms, count, sum)
}

/// One timed sidecar lookup (gate held open); returns (ms, count, checksum).
fn index_once(path: &Path, key: i64) -> (f64, usize, i64) {
    let t = Instant::now();
    let vals = sidecar_i64_eq_opt(path, IDX, key, 1, 1.01)
        .expect("sidecar lookup")
        .expect("sidecar present + covering");
    let ms = t.elapsed().as_secs_f64() * 1e3;
    let sum = vals.iter().fold(0i64, |a, v| a.wrapping_add(*v));
    (ms, vals.len(), sum)
}

#[tokio::main]
async fn main() {
    let mut rows: usize = 10_000_000;
    let mut reps: usize = 5;
    let mut codec_name = "uncompressed".to_string();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--rows" => {
                rows = args[i + 1].parse().expect("--rows N");
                i += 2;
            }
            "--reps" => {
                reps = args[i + 1].parse().expect("--reps N");
                i += 2;
            }
            "--codec" => {
                codec_name = args[i + 1].to_ascii_lowercase();
                i += 2;
            }
            other => panic!(
                "unknown arg {other:?}; usage: --rows N --reps N --codec uncompressed|snappy"
            ),
        }
    }
    // The codec changes the scan side's economics: compressed data makes the
    // full scan pay decompression on every page while the index path only
    // decompresses the pages it touches.
    let codec = match codec_name.as_str() {
        "uncompressed" => CompressionCodec::Uncompressed,
        "snappy" => CompressionCodec::Snappy,
        other => panic!("unknown --codec {other:?}; expected uncompressed|snappy"),
    };

    let (path, counts) = ensure_fixture(rows, codec, &codec_name);
    let ctx = SessionContext::new_with_config(SessionConfig::new());
    ctx.register_table(
        "t",
        Arc::new(
            EmatixFastParquetTableProvider::try_new(path.to_string_lossy().to_string())
                .expect("open provider"),
        ),
    )
    .expect("register");

    // The id domain the uniform estimator sees: planted keys 1..=4 plus
    // FILLER_BASE..FILLER_BASE+N.
    let (dom_min, dom_max) = (1i64, FILLER_BASE + (rows / FILLER_DUP) as i64 - 1);
    let gate_threshold = sidecar_max_selectivity();

    println!("sidecar_bench: rows={rows} reps={reps} codec={codec_name} file={path:?}");
    println!(
        "| fraction | matches | scan ms (median) | index ms (median) | speedup | P3 gate @ {gate_threshold} |"
    );
    println!("| --- | --- | --- | --- | --- | --- |");
    let mut crossover: Option<f64> = None;
    for (fi, f) in FRACTIONS.iter().enumerate() {
        let key = (fi + 1) as i64;
        let mut scan_ms = Vec::with_capacity(reps);
        let mut index_ms = Vec::with_capacity(reps);
        let mut scan_out = (0usize, 0i64);
        let mut index_out = (0usize, 0i64);
        for _ in 0..reps {
            let (ms, c, s) = scan_once(&ctx, key).await;
            scan_ms.push(ms);
            scan_out = (c, s);
            let (ms, c, s) = index_once(&path, key);
            index_ms.push(ms);
            index_out = (c, s);
        }
        // Embedded oracle: both paths must agree before a number is reported.
        assert_eq!(
            scan_out, index_out,
            "index and scan disagree at fraction {f} — bench invalid"
        );
        assert_eq!(scan_out.0, counts[fi], "planted count mismatch at {f}");
        let (s_med, i_med) = (median(scan_ms), median(index_ms));
        let speedup = s_med / i_med;
        if speedup < 1.0 && crossover.is_none() {
            crossover = Some(*f);
        }
        // What the shipped P3 gate would decide (its estimate is domain-width
        // based, deliberately crude — print both it and the true fraction).
        let est = estimate_eq_selectivity(dom_min, dom_max, key);
        let gate = if est > gate_threshold {
            "scan"
        } else {
            "index"
        };
        println!(
            "| {f} | {} | {s_med:.2} | {i_med:.2} | {speedup:.1}x | {gate} (est {est:.2e}) |",
            scan_out.0
        );
    }
    match crossover {
        Some(f) => println!("crossover: index loses to scan from fraction {f} in this sweep"),
        None => println!("crossover: none within sweep (index won at every fraction)"),
    }
}
