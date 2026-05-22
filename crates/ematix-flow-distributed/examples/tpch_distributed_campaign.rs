//! TPC-H distributed bench orchestrator for the AWS campaign.
//!
//! Runs all 22 TPC-H queries through the `DistributedBackend` peer mesh,
//! emits per-query stats as JSON. Intended to run on the coordinator
//! node of a 4-node cluster provisioned by
//! `infra/test-validation-distributed/`. Workers are `flow-worker`
//! processes listening on Arrow Flight (port 50051 by default).
//!
//! ## Env vars
//!
//!   EMATIX_PEERS            comma-separated worker URLs, e.g.
//!                           "http://10.0.1.4:50051,http://10.0.1.5:50051"
//!                           — required
//!   TPCH_DATA_DIR           local parquet root (must exist on EVERY
//!                           worker too — workers read files directly
//!                           via the same path). Default: `/opt/ematix/data/sf10`
//!   TPCH_SCALE_FACTOR       reported in JSON output. Default: 10
//!   TPCH_TRIALS             measured trials per query. Default: 5
//!   TPCH_WARMUPS            untimed warmups per query. Default: 2
//!   OUTPUT_PATH             where to write JSON. Default: stdout
//!   TPCH_QUERIES            comma-separated 1-22 subset. Default: all 22
//!
//! ## Output
//!
//! JSON conforming to the campaign schema (see
//! `docs/AWS_CAMPAIGN_2026_05_PLAN.md`). Includes both `median_ms`
//! across all trials and `median_trials_3_5_ms` so JVM/cache warmup
//! costs are comparable across engines (PySpark + Trino bench scripts
//! emit the same shape).

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::RecordBatch;
use ematix_flow_core::backend::{Backend, DistributedConfig};
use ematix_flow_distributed::DistributedBackend;
use serde::Serialize;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

#[derive(Serialize)]
struct QueryStats {
    trials_ms: Vec<f64>,
    median_ms: f64,
    p95_ms: f64,
    first_trial_ms: f64,
    median_trials_3_5_ms: f64,
    rows_returned: usize,
}

#[derive(Serialize)]
struct CampaignOutput {
    engine: &'static str,
    version: &'static str,
    scale_factor: u32,
    cluster_size: usize,
    queries: BTreeMap<String, QueryStats>,
}

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn parse_peers() -> Vec<String> {
    std::env::var("EMATIX_PEERS")
        .expect("EMATIX_PEERS env var required (comma-separated worker URLs)")
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn parse_query_subset() -> Vec<u8> {
    match std::env::var("TPCH_QUERIES") {
        Ok(raw) if !raw.trim().is_empty() => raw
            .split(',')
            .filter_map(|s| s.trim().parse::<u8>().ok())
            .filter(|n| (1..=22).contains(n))
            .collect(),
        _ => (1u8..=22).collect(),
    }
}

fn read_query(workspace_root: &Path, n: u8) -> String {
    let path = workspace_root
        .join("examples/tpch/queries")
        .join(format!("q{n:02}.sql"));
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// Median of a slice (50th percentile, linear interpolation between
/// the two middle elements when len is even).
fn median(ms: &[f64]) -> f64 {
    if ms.is_empty() {
        return 0.0;
    }
    let mut sorted: Vec<f64> = ms.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    } else {
        sorted[mid]
    }
}

/// 95th percentile (nearest-rank). Used to flag tail latency.
fn p95(ms: &[f64]) -> f64 {
    if ms.is_empty() {
        return 0.0;
    }
    let mut sorted: Vec<f64> = ms.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((sorted.len() as f64) * 0.95).ceil() as usize - 1;
    sorted[idx.min(sorted.len() - 1)]
}

/// Median of trials 3-5 inclusive (post-warmup steady-state slice).
/// Falls back to median(ms) when fewer than 5 trials exist.
fn median_trials_3_5(ms: &[f64]) -> f64 {
    if ms.len() < 5 {
        return median(ms);
    }
    median(&ms[2..5])
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let peers = parse_peers();
    let data_dir: PathBuf = std::env::var("TPCH_DATA_DIR")
        .unwrap_or_else(|_| "/opt/ematix/data/sf10".to_string())
        .into();
    let scale_factor: u32 = env_or("TPCH_SCALE_FACTOR", 10);
    let trials: usize = env_or("TPCH_TRIALS", 5);
    let warmups: usize = env_or("TPCH_WARMUPS", 2);
    let output_path = std::env::var("OUTPUT_PATH").ok();
    let query_subset = parse_query_subset();

    // Workspace root — find by walking up from this binary's runtime
    // location until we find Cargo.toml.workspace or examples/tpch/queries/q01.sql.
    let workspace_root = find_workspace_root()
        .unwrap_or_else(|| PathBuf::from("/opt/ematix/ematix-flow"));

    eprintln!("=== ematix-flow distributed campaign ===");
    eprintln!("  peers:        {} workers", peers.len());
    for p in &peers {
        eprintln!("    {p}");
    }
    eprintln!("  data_dir:     {}", data_dir.display());
    eprintln!("  scale_factor: {scale_factor}");
    eprintln!("  trials:       {trials} (after {warmups} warmups)");
    eprintln!("  queries:      {query_subset:?}");
    eprintln!("  workspace:    {}", workspace_root.display());
    eprintln!();

    // Pre-flight: confirm every table file exists on the coordinator.
    // (Workers need them too, but that's userdata's responsibility.)
    for table in TPCH_TABLES {
        let p = data_dir.join(format!("{table}.parquet"));
        if !p.exists() {
            return Err(format!("missing parquet: {}", p.display()).into());
        }
    }

    // Build the distributed backend. peers comes from the env; tls=None
    // (intra-cluster traffic is on a self-referential SG inside one VPC AZ).
    let backend = Arc::new(DistributedBackend::open(DistributedConfig {
        peers,
        tls: None,
    })?);
    let ctx = backend.session_context().await;
    for table in TPCH_TABLES {
        let p = data_dir.join(format!("{table}.parquet"));
        ctx.register_parquet(*table, p.to_str().unwrap(), Default::default())
            .await?;
    }

    let mut queries = BTreeMap::new();
    for n in query_subset {
        let sql = read_query(&workspace_root, n);
        let label = format!("Q{n:02}");
        eprintln!("--- {label} ---");

        // Warmups (untimed, but verify it runs cleanly).
        for w in 0..warmups {
            match run_query(&backend, &sql).await {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("  warmup {w} FAILED: {e}");
                    break;
                }
            }
        }

        // Measured trials.
        let mut trials_ms: Vec<f64> = Vec::with_capacity(trials);
        let mut rows_returned: usize = 0;
        for _ in 0..trials {
            let t0 = Instant::now();
            match run_query(&backend, &sql).await {
                Ok(batches) => {
                    let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
                    trials_ms.push(elapsed_ms);
                    rows_returned = batches.iter().map(|b| b.num_rows()).sum();
                }
                Err(e) => {
                    eprintln!("  trial FAILED: {e}");
                    trials_ms.push(f64::NAN);
                }
            }
        }

        let med = median(&trials_ms);
        let p95v = p95(&trials_ms);
        let first = trials_ms.first().copied().unwrap_or(f64::NAN);
        let med35 = median_trials_3_5(&trials_ms);
        eprintln!(
            "  median={med:.2}ms p95={p95v:.2}ms first={first:.2}ms med(3-5)={med35:.2}ms rows={rows_returned}"
        );

        queries.insert(
            label,
            QueryStats {
                trials_ms,
                median_ms: med,
                p95_ms: p95v,
                first_trial_ms: first,
                median_trials_3_5_ms: med35,
                rows_returned,
            },
        );
    }

    let cluster_size = backend.peers().len() + 1; // +1 for coordinator
    let out = CampaignOutput {
        engine: "ematix",
        version: env!("CARGO_PKG_VERSION"),
        scale_factor,
        cluster_size,
        queries,
    };

    let json = serde_json::to_string_pretty(&out)?;
    if let Some(path) = output_path {
        fs::write(&path, &json)?;
        eprintln!("wrote {path}");
    } else {
        println!("{json}");
    }
    Ok(())
}

async fn run_query(
    backend: &DistributedBackend,
    sql: &str,
) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
    use futures_util::TryStreamExt;
    let stream = backend.read_arrow_stream(sql).await?;
    let batches: Vec<RecordBatch> = stream.try_collect().await?;
    Ok(batches)
}

/// Walk up from CARGO_MANIFEST_DIR (or current dir at runtime) until
/// we find `examples/tpch/queries/q01.sql`. Returns the directory
/// that contains `examples/`.
fn find_workspace_root() -> Option<PathBuf> {
    let start = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut cur = start.as_path();
    loop {
        let probe = cur.join("examples/tpch/queries/q01.sql");
        if probe.exists() {
            return Some(cur.to_path_buf());
        }
        cur = cur.parent()?;
    }
}
