//! Σ.SC — with/without-sidecar point lookups on a REAL parted TPC-H
//! table (the SF1000 showcase; works on any `lineitem/` part directory).
//!
//! Unlike `sidecar_bench` (synthetic single-file fixture), this measures
//! the production shapes end to end on real data:
//!   - **scan**: `SELECT <value> FROM lineitem WHERE l_orderkey = K`
//!     through `EmatixFastParquetMultiTableProvider::try_new_dir` — the
//!     shipped parted fast path (BridgeFilter pushdown, Σ.MW.1 width
//!     budget). A point lookup still reads every part.
//!   - **index**: per part, `sidecar_i64_eq_opt` against the sorted
//!     `.parquet.idx` built once by this harness (`flow index build`
//!     equivalent), at the SHIPPED gate threshold — binary-search the
//!     index, masked-decode only matching pages.
//!
//! Keys are sampled from the data itself (first row of each part —
//! tpchgen parts cover contiguous orderkey ranges, so this spreads keys
//! across the whole table). Both paths must agree (count + checksum)
//! before a number is reported.
//!
//! Run (box):
//!   cargo run --release -p ematix-flow-core --example sidecar_lookup_parted -- \
//!       --data-dir /opt/ematix/data/sf1000/lineitem --keys 3 --scan-reps 2 --index-reps 7
//!
//! Scan reps default LOW: on SF1000 one scan rep reads ~370 GB.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::ematix_fast_parquet_multi::EmatixFastParquetMultiTableProvider;
use ematix_flow_core::sidecar_build::build_sorted_sidecar;
use ematix_flow_core::sidecar_index::{sidecar_i64_eq_opt, sidecar_max_selectivity, sidecar_path};

const IDX: &str = "idx_orderkey";
const KEY_COLUMN: &str = "l_orderkey";

fn median(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    xs[xs.len() / 2]
}

fn list_parts(dir: &Path) -> Vec<PathBuf> {
    let mut parts: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read_dir {dir:?}: {e}"))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "parquet").unwrap_or(false))
        .collect();
    parts.sort();
    assert!(!parts.is_empty(), "no .parquet parts under {dir:?}");
    parts
}

/// Build any missing sidecars; report per-part build time + index size.
fn ensure_sidecars(parts: &[PathBuf]) {
    for p in parts {
        let idx = sidecar_path(p);
        if idx.exists() {
            continue;
        }
        let src_gb = std::fs::metadata(p).map(|m| m.len()).unwrap_or(0) as f64 / 1e9;
        eprintln!("building sidecar for {p:?} ({src_gb:.1} GB source) ...");
        let t = Instant::now();
        build_sorted_sidecar(p, IDX, KEY_COLUMN, None).expect("build sidecar");
        let idx_mb = std::fs::metadata(&idx).map(|m| m.len()).unwrap_or(0) as f64 / 1e6;
        eprintln!(
            "  built in {:.1}s -> {idx_mb:.1} MB index",
            t.elapsed().as_secs_f64()
        );
    }
}

/// One sampled key per part: the first `l_orderkey` of the part (parts
/// cover contiguous orderkey ranges, so this spreads across the table).
async fn sample_keys(parts: &[PathBuf], want: usize) -> Vec<i64> {
    let mut keys = Vec::new();
    // Spread the sampled parts across the directory.
    let stride = (parts.len() / want.max(1)).max(1);
    for p in parts.iter().step_by(stride).take(want) {
        let ctx = SessionContext::new_with_config(SessionConfig::new());
        ctx.register_table(
            "part",
            Arc::new(
                EmatixFastParquetTableProvider::try_new(p.to_string_lossy().to_string())
                    .expect("open part provider"),
            ),
        )
        .expect("register part");
        let batches = ctx
            .sql(&format!("SELECT {KEY_COLUMN} FROM part LIMIT 1"))
            .await
            .expect("plan sample")
            .collect()
            .await
            .expect("collect sample");
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .expect("l_orderkey is i64");
        keys.push(col.value(0));
    }
    keys.sort();
    keys.dedup();
    keys
}

/// One timed full-scan lookup; returns (ms, count, checksum).
async fn scan_once(ctx: &SessionContext, value_column: &str, key: i64) -> (f64, usize, i64) {
    let t = Instant::now();
    let batches = ctx
        .sql(&format!(
            "SELECT {value_column} FROM lineitem WHERE {KEY_COLUMN} = {key}"
        ))
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
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .expect("value column is i64");
        count += col.len();
        sum = col.iter().flatten().fold(sum, i64::wrapping_add);
    }
    (ms, count, sum)
}

/// One timed sidecar lookup across every part at the SHIPPED gate
/// threshold; returns (ms, count, checksum).
fn index_once(parts: &[PathBuf], value_ordinal: usize, key: i64) -> (f64, usize, i64) {
    let threshold = sidecar_max_selectivity();
    let t = Instant::now();
    let mut count = 0usize;
    let mut sum = 0i64;
    for p in parts {
        if let Some(vals) =
            sidecar_i64_eq_opt(p, IDX, key, value_ordinal, threshold).expect("sidecar lookup")
        {
            count += vals.len();
            sum = vals.iter().fold(sum, |a, v| a.wrapping_add(*v));
        } else {
            panic!("sidecar missing or gate-refused on {p:?} — bench invalid");
        }
    }
    let ms = t.elapsed().as_secs_f64() * 1e3;
    (ms, count, sum)
}

#[tokio::main]
async fn main() {
    let mut data_dir: Option<PathBuf> = None;
    let mut value_column = "l_suppkey".to_string();
    let mut want_keys: usize = 3;
    let mut scan_reps: usize = 2;
    let mut index_reps: usize = 7;
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--data-dir" => {
                data_dir = Some(PathBuf::from(&args[i + 1]));
                i += 2;
            }
            "--value-column" => {
                value_column = args[i + 1].clone();
                i += 2;
            }
            "--keys" => {
                want_keys = args[i + 1].parse().expect("--keys N");
                i += 2;
            }
            "--scan-reps" => {
                scan_reps = args[i + 1].parse().expect("--scan-reps N");
                i += 2;
            }
            "--index-reps" => {
                index_reps = args[i + 1].parse().expect("--index-reps N");
                i += 2;
            }
            other => panic!(
                "unknown arg {other:?}; usage: --data-dir DIR [--value-column c] \
                 [--keys N] [--scan-reps N] [--index-reps N]"
            ),
        }
    }
    let data_dir = data_dir.expect("--data-dir is required (a lineitem part directory)");
    let parts = list_parts(&data_dir);
    let total_gb: f64 = parts
        .iter()
        .map(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0) as f64 / 1e9)
        .sum();

    ensure_sidecars(&parts);

    let provider =
        EmatixFastParquetMultiTableProvider::try_new_dir(&data_dir).expect("open multi provider");
    let schema = datafusion::catalog::TableProvider::schema(&provider);
    let value_ordinal = schema
        .index_of(&value_column)
        .unwrap_or_else(|_| panic!("column {value_column:?} not in schema"));
    let ctx = SessionContext::new_with_config(SessionConfig::new());
    ctx.register_table("lineitem", Arc::new(provider))
        .expect("register lineitem");

    let keys = sample_keys(&parts, want_keys).await;
    println!(
        "sidecar_lookup_parted: parts={} total={total_gb:.1}GB keys={keys:?} \
         scan_reps={scan_reps} index_reps={index_reps} gate={}",
        parts.len(),
        sidecar_max_selectivity()
    );
    println!("| key | matches | scan ms (median) | index ms (median) | speedup |");
    println!("| --- | --- | --- | --- | --- |");
    for key in keys {
        let mut scan_ms = Vec::with_capacity(scan_reps);
        let mut index_ms = Vec::with_capacity(index_reps);
        let mut scan_out = (0usize, 0i64);
        let mut index_out = (0usize, 0i64);
        for _ in 0..scan_reps {
            let (ms, c, s) = scan_once(&ctx, &value_column, key).await;
            scan_ms.push(ms);
            scan_out = (c, s);
        }
        for _ in 0..index_reps {
            let (ms, c, s) = index_once(&parts, value_ordinal, key);
            index_ms.push(ms);
            index_out = (c, s);
        }
        assert_eq!(
            scan_out, index_out,
            "index and scan disagree at key {key} — bench invalid"
        );
        assert!(scan_out.0 > 0, "sampled key {key} matched no rows");
        let (s_med, i_med) = (median(scan_ms), median(index_ms));
        println!(
            "| {key} | {} | {s_med:.1} | {i_med:.2} | {:.0}x |",
            scan_out.0,
            s_med / i_med
        );
    }
}
