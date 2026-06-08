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

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::{
    CompressionType, DistributedExt, DistributedPhysicalOptimizerRule, WorkerResolver,
};
use ematix_flow_core::preset;
// Bench-parity rule set (NO_DISTRIBUTE diagnostic) — mirrors examples/explain_query.rs.
use ematix_flow_core::dedupe_aggregate_rule::DedupeAggregateForFloatDeterminism;
use ematix_flow_core::dict_aggregate_rule::EnableDictGroupCountRule;
use ematix_flow_core::force_collect_left_semi_build_rule::ForceCollectLeftForSemiBoundedBuildRule;
use ematix_flow_core::fused_aggregate_filter_multi_agg_rule::InjectFilterMultiAggRule;
use ematix_flow_core::fused_aggregate_filter_sum_rule::InjectFilterSumRule;
use ematix_flow_core::push_down_left_semi_rule::PushDownLeftSemiRule;
use ematix_flow_core::runtime_bloom_sideband_rule::EnableRuntimeBloomSidebandRule;
use ematix_flow_core::swap_emat_hash_join_rule::SwapEmatixHashJoinRule;
use ematix_flow_core::swap_semi_join_build_rule::SwapSemiJoinBuildSideRule;
use serde::Serialize;
use url::Url;

/// Inline `WorkerResolver` — mirrors `ematix_flow_distributed::StaticWorkerResolver`,
/// which is private to that crate. Same shape, returns the fixed peer list.
#[derive(Clone)]
struct StaticPeers {
    urls: Vec<Url>,
}

#[async_trait]
impl WorkerResolver for StaticPeers {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok(self.urls.clone())
    }
}

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

/// Build the campaign `SessionState`.
///
/// Rule set is chosen by `RULESET`:
///   - `bench` (DEFAULT): the production triangulation bench's EXPLICIT rule
///     chain on plain parquet — the plan that runs Q05 SF=10 in ~214ms
///     single-node (76× faster than preset). Mirrors examples/explain_query.rs.
///   - `preset`: the public-library default (`preset::with_optimizer_rules`),
///     kept as the diagnostic reference for the #315 Q05 pathology.
///
/// `distributed` appends `DistributedPhysicalOptimizerRule` LAST (the standard
/// datafusion-distributed integration: split the *fully-optimized* plan into
/// network stages) + the worker resolver + LZ4_FRAME exchange compression. The
/// old code added the distributed rule BEFORE preset's rules, so preset ran on
/// an already stage-split plan — part of what we're isolating here.
///
/// Custom emat operators (BloomEmitter via EnableRuntimeBloomSidebandRule,
/// EmatixHashJoinExec via SwapEmatixHashJoinRule) emit physical nodes that
/// datafusion-distributed must serialize across the Flight mesh. They are
/// gated OUT of the distributed `bench` arm unless DIST_CUSTOM_OPS=1, so we can
/// first establish whether the *standard*-operator rules alone survive
/// distribution and fix Q05.
fn build_session_state(distributed: bool, resolver: StaticPeers) -> SessionState {
    let use_preset = matches!(std::env::var("RULESET").as_deref(), Ok("preset"));
    // Custom-operator rules: always on single-node; opt-in for distributed.
    let custom_ops = !distributed || std::env::var_os("DIST_CUSTOM_OPS").is_some();

    // TARGET_PARTITIONS: positive int → with_target_partitions(N); "default"/"0"
    // leaves DataFusion's default (available_parallelism). Lets us A/B the
    // partition count — the suspected real Q05 lever (the old preset/distributed
    // builder never set it; the bench arm set 14). Unset ⇒ 14 (bench parity).
    let mut cfg = SessionConfig::new().with_collect_statistics(true);
    match std::env::var("TARGET_PARTITIONS").ok().as_deref() {
        Some("default") | Some("0") => {}
        Some(n) if n.parse::<usize>().is_ok() => {
            cfg = cfg.with_target_partitions(n.parse::<usize>().unwrap());
        }
        _ => cfg = cfg.with_target_partitions(14),
    }
    eprintln!(
        "  (ruleset={} distributed={} custom_ops={} target_partitions={})",
        if use_preset { "preset" } else { "bench" },
        distributed,
        custom_ops,
        cfg.target_partitions(),
    );

    let base = SessionStateBuilder::new()
        .with_config(cfg)
        .with_default_features();

    let mut builder = if use_preset {
        // Library default. Note: preset already bundles SwapSemiJoinBuildSideRule,
        // ForceCollectLeftForSemiBoundedBuildRule, PushDownLeftSemiRule, the bloom
        // sideband rule, and the FlowQueryPlanner reorder — so "add the build-side
        // subset" is a no-op against preset; the Q05 lever is elsewhere.
        preset::with_optimizer_rules_and_registry(base).0
    } else {
        // Explicit bench-parity chain (the 214ms single-node Q05 plan).
        base.with_optimizer_rule(Arc::new(PushDownLeftSemiRule))
            .with_physical_optimizer_rule(Arc::new(DedupeAggregateForFloatDeterminism::default()))
            .with_physical_optimizer_rule(Arc::new(EnableDictGroupCountRule))
            .with_physical_optimizer_rule(Arc::new(InjectFilterMultiAggRule))
            .with_physical_optimizer_rule(Arc::new(InjectFilterSumRule))
            .with_physical_optimizer_rule(Arc::new(SwapSemiJoinBuildSideRule))
            .with_physical_optimizer_rule(Arc::new(
                ForceCollectLeftForSemiBoundedBuildRule::default(),
            ))
    };

    // Custom emat operators — only in the bench arm, gated for distributed.
    if !use_preset && custom_ops {
        builder = builder
            .with_physical_optimizer_rule(Arc::new(EnableRuntimeBloomSidebandRule::default()))
            .with_physical_optimizer_rule(Arc::new(SwapEmatixHashJoinRule));
    }

    if distributed {
        builder = builder
            .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
            .with_distributed_worker_resolver(resolver);
        builder
            .with_distributed_compression(Some(CompressionType::LZ4_FRAME))
            .expect("LZ4_FRAME is a valid compression type")
            .build()
    } else {
        builder.build()
    }
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
    let workspace_root =
        find_workspace_root().unwrap_or_else(|| PathBuf::from("/opt/ematix/ematix-flow"));

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

    // Build the SessionState ourselves so we get BOTH the distributed
    // planner AND our preset rules (dedupe-for-f64-determinism +
    // multi-agg / sum / dict-group-count). Going through
    // `DistributedBackend::open` would give us only the distributed
    // planner; Q15 would non-deterministically return 0 rows because
    // the dedupe rule wouldn't fire (caught in local smoke test
    // 2026-05-22).
    let urls: Vec<Url> = peers
        .iter()
        .map(|s| Url::parse(s).unwrap_or_else(|e| panic!("bad EMATIX_PEERS url '{s}': {e}")))
        .collect();
    let resolver = StaticPeers { urls };
    // Rule chain + topology selected by RULESET (bench|preset) and NO_DISTRIBUTE
    // (single-node vs the Flight mesh). See build_session_state.
    let distributed = std::env::var_os("NO_DISTRIBUTE").is_none();
    let state = build_session_state(distributed, resolver);
    let ctx = Arc::new(SessionContext::from(state));
    for table in TPCH_TABLES {
        let p = data_dir.join(format!("{table}.parquet"));
        ctx.register_parquet(*table, p.to_str().unwrap(), Default::default())
            .await?;
    }

    // CUSTOM_SQL=<sql> — run one ad-hoc query through the distributed ctx
    // (diagnostic for DIST.1a). Prints row count + a 1-batch sample, or the
    // distributed EXPLAIN when EXPLAIN_ONLY=1. Returns after.
    if let Ok(custom) = std::env::var("CUSTOM_SQL") {
        eprintln!("--- CUSTOM_SQL ---");
        if std::env::var_os("EXPLAIN_ONLY").is_some() {
            let b = ctx
                .sql(&format!("EXPLAIN {custom}"))
                .await?
                .collect()
                .await?;
            if let Ok(t) = datafusion::arrow::util::pretty::pretty_format_batches(&b) {
                println!("{t}");
            }
        } else {
            let b = run_query(&ctx, &custom).await?;
            let rows: usize = b.iter().map(|x| x.num_rows()).sum();
            eprintln!("  rows={rows}");
            let head: Vec<_> = b.into_iter().take(1).collect();
            if let Ok(t) = datafusion::arrow::util::pretty::pretty_format_batches(&head) {
                println!("{t}");
            }
        }
        return Ok(());
    }

    let mut queries = BTreeMap::new();
    for n in query_subset {
        // #315: scale Q11's HAVING fraction (0.0001 / SF) so it isn't
        // degenerate (0 rows) at SF>=10. No-op for q!=11 / SF<=1.
        let sql = ematix_flow_core::tpch_params::apply_tpch_query_params(
            n,
            &read_query(&workspace_root, n),
            scale_factor,
        );
        let label = format!("Q{n:02}");
        eprintln!("--- {label} ---");

        // EXPLAIN_ONLY=1 — dump the distributed plan (logical + physical with
        // network/stage boundaries) and skip execution. Diagnostic for the
        // Q11/Q15 0-rows distributed correctness bug (SF100/DIST.1a).
        if std::env::var_os("EXPLAIN_ONLY").is_some() {
            match ctx.sql(&format!("EXPLAIN {sql}")).await {
                Ok(df) => match df.collect().await {
                    Ok(batches) => {
                        match datafusion::arrow::util::pretty::pretty_format_batches(&batches) {
                            Ok(t) => println!("{t}"),
                            Err(e) => eprintln!("  pretty err: {e}"),
                        }
                    }
                    Err(e) => eprintln!("  EXPLAIN collect failed: {e}"),
                },
                Err(e) => eprintln!("  EXPLAIN failed: {e}"),
            }
            continue;
        }

        // Warmups (untimed, but verify it runs cleanly).
        for w in 0..warmups {
            match run_query(&ctx, &sql).await {
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
            match run_query(&ctx, &sql).await {
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

    let cluster_size = peers.len() + 1; // +1 for coordinator
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
    ctx: &SessionContext,
    sql: &str,
) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
    let df = ctx.sql(sql).await?;
    let batches = df.collect().await?;
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
