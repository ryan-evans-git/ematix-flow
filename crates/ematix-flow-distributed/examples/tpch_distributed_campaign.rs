//! TPC-H distributed bench orchestrator for the AWS campaign.
//!
//! Runs all 22 TPC-H queries through the `DistributedBackend` peer mesh,
//! emits per-query stats as JSON. Intended to run on the coordinator
//! node of a 4-node cluster provisioned by
//! `infra/test-validation-distributed/`. Workers are `flow-worker`
//! processes listening on Arrow Flight (port 50051 by default).
//!
//! ## Session construction (2026-07-04 refresh)
//!
//! The session is built through `preset::with_optimizer_rules_overridden`
//! with default `HarnessOverrides` — the PRODUCTION rule chain (dedupe +
//! inject trio + swap-semi + force-collect-left + RANGE.AGG + push-semi +
//! rh-sum + L9 milestone sideband + `FlowQueryPlanner`), exactly what
//! `DistributedBackend::build_context` installs in production. The old
//! hand-built `RULESET=bench|preset` fork predated the rule-chain
//! unification (2026-07-02/03) and measured a STALE, non-production
//! plan; it is gone. Parity is pinned by
//! `campaign_preset_parity_tests` below (run:
//! `cargo test -p ematix-flow-distributed --example tpch_distributed_campaign`).
//!
//! Levers stay auto-gated: with no env set, `EMAT_TARGET_PARTITIONS`
//! resolves through the cross-process registry (a solo process — the
//! normal state on a dedicated cloud node — senses live=1 and gets
//! full cores), and each rule self-gates on its own `EMAT_*` var.
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
//!   PARTITIONS              explicit target_partitions override (the
//!                           strict-harness precedence; TARGET_PARTITIONS
//!                           accepted as a legacy alias). Unset ⇒ the
//!                           preset's concurrency-aware AUTO resolution.
//!   NO_DISTRIBUTE           set ⇒ single-node diagnostic run (no mesh)
//!   EMAT_MESH               tri-state gate on the stage splitter:
//!                           1=always distribute, 0=never, unset=AUTO
//!                           per query from scan-byte statistics.
//!                           Local commits localize stock parquet
//!                           leaves onto the fast provider (Σ.Q15.LS).
//!                           See `ematix_flow_distributed::mesh_gate`.
//!   EMAT_MESH_MIN_BYTES     AUTO threshold in bytes (default 4 GiB,
//!                           initial value pending campaign calibration)
//!   EMAT_MESH_BROADCAST_JOINS  opt-in (default OFF). Broadcasts a
//!                           CollectLeft join's build side to every task
//!                           so the big probe scan shards across the mesh
//!                           instead of collapsing onto one task. Mirrors
//!                           the shipped `build_context` opt-in (#32).
//!   EMAT_TWIN_ROUTE         tri-state (default ON). Σ.TW.1: a local
//!                           commit WITH a join re-plans + executes in
//!                           the native single-node twin (full native
//!                           preset + native fast providers — the
//!                           KEYS.2 downcast lever localize can't
//!                           carry). plan_mode reports "twin". 0 ⇒
//!                           today's localize-only behavior (A/B
//!                           control). See docs/ADR_NATIVE_TWIN_ROUTING.md.
//!   EMAT_MODE_MEMO          tri-state (default ON). Σ.TW.2: each join
//!                           query probes all three arms once untimed
//!                           (twin / forced-mesh / forced-mesh+bcast),
//!                           then runs its trials on the argmin —
//!                           measurement replaces the byte-threshold
//!                           prediction. Probes are row-parity-guarded
//!                           (any disagreement falls back to the gate
//!                           routing). plan_mode reports the chosen
//!                           arm. 0 ⇒ Σ.TW.1 behavior (gate + twin).
//!   CUSTOM_SQL / EXPLAIN_ONLY  diagnostics, unchanged (see below)
//!
//! ## Output
//!
//! JSON conforming to the campaign schema (see
//! `docs/AWS_CAMPAIGN_2026_05_PLAN.md`) plus a `provenance` block
//! (instance type, AZ, git SHA, EMAT_* env, engine versions) mirroring
//! the strict local protocol's env.json. Includes both `median_ms`
//! across all trials and `median_trials_3_5_ms` so JVM/cache warmup
//! costs are comparable across engines (PySpark + Trino bench scripts
//! emit the same shape).

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::{CompressionType, DistributedExt, WorkerResolver};
use ematix_flow_core::preset::{self, HarnessOverrides};
use ematix_flow_distributed::bloom_codec::{BloomExecCodec, BloomSlot, EmbeddedBloomRule};
use ematix_flow_distributed::mesh_gate::AdaptiveMeshGateRule;
use serde::Serialize;
use url::Url;

// OOM.SF1000 postmortem (2026-07-17, #223 REVERTED — read before
// "fixing" the coordinator's allocator again): the mimalloc-parity
// change (global_allocator + gated between-measurement mi_collect) was
// REFUTED by a same-box A/B at SF=1000 on the r7i.8xlarge. On the
// system allocator the suite reached Q21 before OOM (Q05 twin probe
// completed in 82.6 s; anon-RSS at kill 239 GB, ~19 queries deep). On
// mimalloc it OOM-killed on Q05's FIRST twin probe at 257 GB — earlier
// and worse — and the rss_mb_after column showed NO cross-query
// ratchet under either allocator (Q01–Q04: 1.3 / 12.5 / 4.3 / 15.2 GB,
// non-monotone). The between-query collect gate never even fired. The
// real defect is PER-QUERY peak in the twin arm inside the AUTO
// coordinator (~10× the single-leg peak for the same query — see
// issue #224); mimalloc's per-thread segment retention *within* one
// giant query only inflates that peak, and no between-query collect
// can help. Logs: s3://ematix-bench-963786/results/20260717T094236Z/
// (attempt 3, mimalloc) vs .../20260716T193957Z/ (attempts 1–2).

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
    /// What the adaptive mesh gate (`EMAT_MESH`, see
    /// `ematix_flow_distributed::mesh_gate`) decided for THIS query's
    /// physical plan: "mesh" when the optimized plan carries the
    /// datafusion-distributed stage wrapper, "single" when it stayed
    /// a single-node plan, "unknown" when planning failed before the
    /// walk (the trials will show the same failure). Detected via
    /// [`plan_mode_of`]; the flag state itself is in
    /// `provenance.emat_env` (EMAT_MESH / EMAT_MESH_MIN_BYTES).
    plan_mode: String,
    /// Summed `total_byte_size` (uncompressed, projection-narrowed) of
    /// this query's scan leaves — the exact quantity the AUTO mesh gate
    /// compares against `EMAT_MESH_MIN_BYTES`. `None` when no leaf
    /// reports a byte size. Recorded so a single+mesh run pair yields
    /// the empirical (scan_bytes → single-vs-mesh) crossover used to
    /// calibrate the AUTO threshold. See [`sum_scan_leaf_bytes`].
    scan_bytes: Option<u64>,
    /// Per-trial PLANNING time (ms): `ctx.sql` + `create_physical_plan`,
    /// i.e. the phase where the mesh gate and all physical rules run.
    /// Execution time = trials_ms − plan_ms (bloom arming rides in
    /// trials_ms only). Added after the SF1000 campaign, where AUTO
    /// totaled +20% over forced mesh with byte-identical plans — this
    /// field decomposes any future such delta for free. NaN on failed
    /// trials, mirroring trials_ms.
    plan_ms_trials: Vec<f64>,
    /// Median of `plan_ms_trials` (NaN-skipping, same as median_ms).
    median_plan_ms: f64,
    /// Coordinator-process resident set (MB) sampled after this query's
    /// last trial. OOM.SF1000 diagnostic: this column REFUTED the
    /// cross-query ratchet theory (#223 revert) — SF=1000 readings are
    /// non-monotone (Q01–Q04: 1.3 / 12.5 / 4.3 / 15.2 GB), so the OOM
    /// class is PER-QUERY peak inside the twin arm (issue #224), not
    /// accumulation between queries. Kept as the cheap always-on
    /// instrument that settles that question per run. `None` when the
    /// platform read fails.
    rss_mb_after: Option<f64>,
}

/// `plan_mode` label for [`QueryStats`] — the network-boundary walker
/// lives in [`ematix_flow_distributed::native_twin::plan_is_mesh`]
/// (single source of truth; Σ.TW.1 routes on the same predicate).
fn plan_mode_of(plan: &Arc<dyn ExecutionPlan>) -> &'static str {
    if ematix_flow_distributed::native_twin::plan_is_mesh(plan) {
        "mesh"
    } else {
        "single"
    }
}

/// Sum `total_byte_size` across the plan's scan leaves — the exact
/// quantity the AUTO mesh gate thresholds against `EMAT_MESH_MIN_BYTES`
/// (mirrors `mesh_gate::collect_scan_leaf_bytes`). `Exact`/`Inexact`
/// count; `Absent` contributes nothing. Returns `None` when NO leaf
/// reports a size (the gate's "all-unknown → distribute" path). Recorded
/// per query so a single+mesh run pair gives the empirical
/// scan_bytes→(single-vs-mesh) crossover for threshold calibration.
fn sum_scan_leaf_bytes(plan: &Arc<dyn ExecutionPlan>) -> Option<u64> {
    fn walk(plan: &Arc<dyn ExecutionPlan>, sum: &mut u64, any: &mut bool) {
        let children = plan.children();
        if children.is_empty() {
            if let Ok(s) = plan.partition_statistics(None) {
                match s.total_byte_size {
                    datafusion::common::stats::Precision::Exact(n)
                    | datafusion::common::stats::Precision::Inexact(n) => {
                        *any = true;
                        *sum = sum.saturating_add(n as u64);
                    }
                    datafusion::common::stats::Precision::Absent => {}
                }
            }
            return;
        }
        for c in children {
            walk(c, sum, any);
        }
    }
    let mut sum = 0u64;
    let mut any = false;
    walk(plan, &mut sum, &mut any);
    any.then_some(sum)
}

/// Run provenance — the campaign-side equivalent of the strict local
/// protocol's `env.json` (`scripts/bench/strict_common.sh::capture_env`).
/// Every field is best-effort: a missing IMDS endpoint (running off
/// EC2), missing git, or missing Cargo.lock degrades to `null`, never
/// to a failed run.
#[derive(Serialize)]
struct Provenance {
    captured_at_unix: u64,
    hostname: Option<String>,
    os: &'static str,
    arch: &'static str,
    cores: usize,
    /// EC2 instance type via IMDSv2 (`null` off-EC2 / local).
    instance_type: Option<String>,
    /// EC2 availability zone via IMDSv2 (`null` off-EC2 / local).
    availability_zone: Option<String>,
    git_sha: Option<String>,
    git_branch: Option<String>,
    git_dirty: Option<bool>,
    rustc: Option<String>,
    /// Key dependency versions parsed from the workspace Cargo.lock.
    engine_versions: BTreeMap<&'static str, Option<String>>,
    /// Every EMAT_* / EMATIX_* / TPCH_* / PARTITIONS / RAYON_NUM_THREADS
    /// env var visible to this process — the lever state of the run.
    emat_env: BTreeMap<String, String>,
    peers: Vec<String>,
    distributed: bool,
}

#[derive(Serialize)]
struct CampaignOutput {
    engine: &'static str,
    version: &'static str,
    scale_factor: u32,
    cluster_size: usize,
    trials: usize,
    warmups: usize,
    provenance: Provenance,
    queries: BTreeMap<String, QueryStats>,
}

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn parse_peers() -> Vec<String> {
    // A single-node diagnostic run (NO_DISTRIBUTE) has no mesh, so peers are
    // optional there; only the distributed path truly requires them.
    let raw = match std::env::var("EMATIX_PEERS") {
        Ok(v) => v,
        Err(_) if std::env::var_os("NO_DISTRIBUTE").is_some() => return Vec::new(),
        Err(_) => panic!("EMATIX_PEERS env var required (comma-separated worker URLs)"),
    };
    raw.split(',')
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

/// Explicit `PARTITIONS=N` override (`TARGET_PARTITIONS` legacy alias,
/// `PARTITIONS` wins when both are set). `None` ⇒ the preset's
/// concurrency-aware AUTO resolution owns the value.
fn explicit_partitions() -> Option<usize> {
    for key in ["PARTITIONS", "TARGET_PARTITIONS"] {
        if let Some(n) = std::env::var(key)
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n >= 1)
        {
            return Some(n);
        }
    }
    None
}

/// Build the campaign `SessionState` through the PRODUCTION preset
/// constructor (`preset::with_optimizer_rules_overridden`, the
/// rule-chain-unification single source of truth — same migration as
/// `tpch_triangulation_bench` / `stage_profiler`).
///
/// The only override is partition precedence: an explicit
/// `PARTITIONS=N` env sets `with_target_partitions(N)` and switches
/// `auto_target_partitions` OFF so the preset never second-guesses it
/// at a different instant. With no override the preset resolves
/// `target_partitions` itself (`EMAT_TARGET_PARTITIONS` tri-state +
/// cross-process AUTO sensing — a solo process on a dedicated cloud
/// node resolves to full cores) and sizes the rayon decode pool once
/// per process.
///
/// `distributed` appends the EMAT_MESH [`AdaptiveMeshGateRule`] LAST
/// (the adaptive wrapper around datafusion-distributed's stage
/// splitter: split the *fully-optimized* plan into network stages —
/// or leave it single-node per the gate's tri-state / per-query
/// AUTO decision) + the worker resolver + LZ4_FRAME exchange
/// compression — identical composition to
/// `DistributedBackend::build_context`. `from_env()` snapshots
/// EMAT_MESH / EMAT_MESH_MIN_BYTES once here at session build; the
/// gate decision itself replays per query at plan-optimization time
/// (the rule runs on every plan), reported per query as
/// `QueryStats::plan_mode`.
/// `gate`/`broadcast`: `None` = resolve from env (the historical
/// behavior); `Some` pins them — Σ.TW.2's forced-mesh probe arms are
/// separate SESSIONS (`MeshGateConfig::forced()` ± broadcast joins),
/// because both knobs are session-build-time, not per-query.
fn build_session_state(
    distributed: bool,
    resolver: StaticPeers,
    bloom_slot: BloomSlot,
    gate: Option<ematix_flow_distributed::mesh_gate::MeshGateConfig>,
    broadcast: Option<bool>,
) -> SessionState {
    let explicit = explicit_partitions();
    let mut cfg = SessionConfig::new().with_collect_statistics(true);
    if let Some(n) = explicit {
        cfg = cfg.with_target_partitions(n);
    }
    // EMAT_EXPLAIN_STATS=1: render per-node Statistics in EXPLAIN output.
    // Diagnostic for join-build-side selection — this probe exposed the
    // 15x cardinality inflation (LIKE default 0.2 + composite-key join
    // fanout) behind the Q09 SF100 page-cache cliff (Σ.JS.1).
    if std::env::var_os("EMAT_EXPLAIN_STATS").is_some() {
        cfg.options_mut().explain.show_statistics = true;
    }
    let overrides = HarnessOverrides {
        auto_target_partitions: explicit.is_none(),
        // Σ.MG.2 hang fix (2026-07-12): grace demotion must NOT run
        // in a distributed session. Its arrow-side estimates are
        // sampling-blind (LIKE=0.2 default) and over-trigger, and a
        // GraceHashJoinExec inside a stage plan is undecodable by
        // workers (no codec; per-node spill semantics don't cross the
        // mesh) — the 2026-07-11 23:07Z mesh leg hung exactly here.
        // Single-node sessions keep grace ON (its SF1000 purpose).
        grace_join: !distributed,
        ..HarnessOverrides::default()
    };

    let base = SessionStateBuilder::new()
        .with_config(cfg)
        .with_default_features();
    let (builder, _handles) = preset::with_optimizer_rules_overridden(base, &overrides);

    if distributed {
        // Σ.MG.2 (hang #3 fix, 2026-07-12): the embedded-bloom wrap
        // must run AFTER the mesh-gated stage splitter. Pre-split
        // wrapping changed the splitter's topology decisions — a
        // BloomFilterExec around a scan defeats the annotator's leaf
        // exemption under CoalescePartitionsExec, turning a local
        // CollectLeft build side into a remote stage (Q02 SF100
        // deadlocked the fleet exactly there). Post-split, the rule
        // descends through the network-boundary nodes and wraps scans
        // INSIDE the frozen stages; the codec serializes the wrap
        // into the stage protobuf (headers can't carry SF-scale
        // blooms).
        let gate_rule = match gate {
            Some(config) => AdaptiveMeshGateRule::new(config),
            None => AdaptiveMeshGateRule::from_env(),
        };
        let builder = builder
            .with_physical_optimizer_rule(Arc::new(gate_rule))
            .with_physical_optimizer_rule(Arc::new(EmbeddedBloomRule::new(bloom_slot)))
            .with_distributed_user_codec(BloomExecCodec)
            .with_distributed_worker_resolver(resolver);
        let builder = builder
            .with_distributed_compression(Some(CompressionType::LZ4_FRAME))
            .expect("LZ4_FRAME is a valid compression type")
            // Match production (`DistributedBackend::build_context`): pin
            // files_per_task=1 so the per-stage task count auto-derives as
            // min(scan file-splits, worker count) instead of the library's
            // core-count default, which collapses single-file scans to a
            // single task and silently prevents the mesh from engaging.
            .with_distributed_files_per_task(1)
            .expect("files_per_task = 1 is valid");
        // #32: EMAT_MESH_BROADCAST_JOINS mirrors the shipped
        // `DistributedBackend::build_context` opt-in. datafusion-distributed's
        // `broadcast_joins` defaults OFF, so a CollectLeft build coalesces into
        // a single partition — collapsing the whole stage, INCLUDING the big
        // probe scan, onto ONE task (measured here as lineitem single-tasked in
        // 11/17 distributed TPC-H queries). ON broadcasts the already-small
        // build side to every consumer task so the big probe shards across the
        // mesh. Same flag name + tri_state parse as production.
        let broadcast_on = broadcast.unwrap_or_else(|| {
            ematix_flow_core::flags::tri_state("EMAT_MESH_BROADCAST_JOINS").unwrap_or(false)
        });
        let builder = if broadcast_on {
            builder
                .with_distributed_broadcast_joins(true)
                .expect("broadcast_joins is a valid distributed toggle")
        } else {
            builder
        };
        builder.build()
    } else {
        builder.build()
    }
}

/// Resolve a table's on-disk parquet source, preferring the multi-file
/// parts layout that lets the distributed planner fan a scan out:
/// - `<data_dir>/<table>/` — a directory of `<table>-NNNN.parquet` parts,
///   registered as a multi-file listing so each part is its own scan
///   split (returned when the dir exists and is non-empty); else
/// - `<data_dir>/<table>.parquet` — the legacy single flat file; else
/// - `None` (pre-flight turns this into a hard error).
fn table_source(data_dir: &std::path::Path, table: &str) -> Option<PathBuf> {
    let dir = data_dir.join(table);
    let dir_has_files = std::fs::read_dir(&dir)
        .map(|mut d| d.next().is_some())
        .unwrap_or(false);
    if dir.is_dir() && dir_has_files {
        return Some(dir);
    }
    let flat = data_dir.join(format!("{table}.parquet"));
    if flat.exists() {
        return Some(flat);
    }
    None
}

// ---------------------------------------------------------------------------
// Provenance capture
// ---------------------------------------------------------------------------

/// Run a command, return trimmed stdout on success.
fn sh(cmd: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(cmd).args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if s.is_empty() { None } else { Some(s) }
}

/// IMDSv2 lookup with a hard 1s budget per call — shells out to curl
/// (present on AL2023 + macOS) so we don't grow an HTTP-client dep.
/// Returns `None` off-EC2 (curl times out / non-2xx).
fn imds(path: &str) -> Option<String> {
    let token = sh(
        "curl",
        &[
            "-fsS",
            "-m",
            "1",
            "-X",
            "PUT",
            "-H",
            "X-aws-ec2-metadata-token-ttl-seconds: 60",
            "http://169.254.169.254/latest/api/token",
        ],
    )?;
    sh(
        "curl",
        &[
            "-fsS",
            "-m",
            "1",
            "-H",
            &format!("X-aws-ec2-metadata-token: {token}"),
            &format!("http://169.254.169.254/latest/meta-data/{path}"),
        ],
    )
}

/// First `version` for `name` in the workspace Cargo.lock (registry deps).
fn lock_version(workspace_root: &Path, name: &str) -> Option<String> {
    let text = fs::read_to_string(workspace_root.join("Cargo.lock")).ok()?;
    let needle = format!("name = \"{name}\"");
    let mut lines = text.lines();
    while let Some(line) = lines.next() {
        if line.trim() == needle {
            if let Some(v) = lines.next() {
                return v
                    .trim()
                    .strip_prefix("version = \"")
                    .and_then(|s| s.strip_suffix('"'))
                    .map(str::to_string);
            }
        }
    }
    None
}

fn capture_provenance(workspace_root: &Path, peers: &[String], distributed: bool) -> Provenance {
    let git = |args: &[&str]| {
        let mut full = vec!["-C", workspace_root.to_str().unwrap_or(".")];
        full.extend_from_slice(args);
        sh("git", &full)
    };
    let emat_env: BTreeMap<String, String> = std::env::vars()
        .filter(|(k, _)| {
            k.starts_with("EMAT_")
                || k.starts_with("EMATIX_")
                || k.starts_with("TPCH_")
                || k == "PARTITIONS"
                || k == "TARGET_PARTITIONS"
                || k == "RAYON_NUM_THREADS"
                || k == "NO_DISTRIBUTE"
        })
        .collect();
    let mut engine_versions = BTreeMap::new();
    for dep in [
        "datafusion",
        "datafusion-distributed",
        "ematix-parquet-codec",
    ] {
        engine_versions.insert(dep, lock_version(workspace_root, dep));
    }
    Provenance {
        captured_at_unix: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        hostname: sh("hostname", &[]),
        os: std::env::consts::OS,
        arch: std::env::consts::ARCH,
        cores: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1),
        instance_type: imds("instance-type"),
        availability_zone: imds("placement/availability-zone"),
        git_sha: git(&["rev-parse", "HEAD"]),
        git_branch: git(&["rev-parse", "--abbrev-ref", "HEAD"]),
        git_dirty: git(&["status", "--porcelain"])
            .map(|s| !s.is_empty())
            .or(Some(false)),
        rustc: sh("rustc", &["--version"]),
        engine_versions,
        emat_env,
        peers: peers.to_vec(),
        distributed,
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

    // Pre-flight: confirm every table's data is present on the
    // coordinator. Two layouts are accepted (see `table_source`):
    // the multi-file parts dir `<data_dir>/<table>/` OR the legacy flat
    // `<data_dir>/<table>.parquet`. (Workers need them too, but that's
    // userdata's responsibility.)
    for table in TPCH_TABLES {
        if table_source(&data_dir, table).is_none() {
            let dir = data_dir.join(table);
            let flat = data_dir.join(format!("{table}.parquet"));
            return Err(format!(
                "missing parquet for {table}: neither dir {} nor file {} exists",
                dir.display(),
                flat.display()
            )
            .into());
        }
    }

    // Build the SessionState ourselves so we get BOTH the distributed
    // planner AND the production preset chain (dedupe-for-f64-determinism
    // included — without it Q15 non-deterministically returns 0 rows,
    // caught in local smoke test 2026-05-22).
    let urls: Vec<Url> = peers
        .iter()
        .map(|s| Url::parse(s).unwrap_or_else(|e| panic!("bad EMATIX_PEERS url '{s}': {e}")))
        .collect();
    let resolver = StaticPeers { urls };
    // NO_DISTRIBUTE: single-node diagnostic (same rules, no Flight mesh).
    let distributed = std::env::var_os("NO_DISTRIBUTE").is_none();
    let provenance = capture_provenance(&workspace_root, &peers, distributed);
    let bloom_slot = BloomSlot::new();
    let state = build_session_state(
        distributed,
        resolver.clone(),
        bloom_slot.clone(),
        None,
        None,
    );
    eprintln!(
        "  (distributed={distributed} target_partitions={} instance_type={:?})",
        state.config().target_partitions(),
        provenance.instance_type,
    );
    let ctx = Arc::new(SessionContext::from(state));
    // Match the SHIPPED engine: read parquet via the hand-rolled ematix-parquet
    // reader (EmatixFastParquetTableProvider — late-materialization + fused
    // NEON/AVX2 predicate kernels), which is what run_shard/work_unit use in
    // production. The generic arrow-rs `register_parquet` is used ONLY when we
    // fan out (distributed): the DistributedPhysicalOptimizerRule splits an
    // arrow-rs ListingTable across peers, and each peer's shard then reads via
    // ematix-parquet itself. So single-node (NO_DISTRIBUTE) uses ematix-parquet
    // directly — no code path benchmarks the arrow-rs reader as "ematix".
    let use_fast = !distributed;
    for table in TPCH_TABLES {
        // `table_source` resolves the multi-file parts dir `<table>/` (each
        // holding `<table>-NNNN.parquet`) when present, else the legacy flat
        // `<table>.parquet`. Registering a DIRECTORY makes DataFusion list
        // every part file as a separate scan split — the file count the
        // distributed planner divides by `files_per_task` (pinned to 1) to
        // fan the scan out across peers. A single flat file yields one split
        // (single-node), which is why the parts layout matters for the mesh.
        let p = table_source(&data_dir, table)
            .unwrap_or_else(|| panic!("pre-flight passed but {table} source vanished"));
        if use_fast {
            // Single-node via the ematix codec. `table_source` may hand back a
            // parts DIRECTORY (parted layout) or a single flat FILE. The
            // single-file provider (`try_new`) opens one file only, so a parted
            // dir needs the multi-file provider — which reads every part through
            // the same ematix codec and unions them (single-node scan
            // parallelism, no arrow-rs). This is what makes a clean single-node
            // *parted* measurement possible; before it, parted single-node
            // could only go through the arrow-rs `register_parquet` path.
            if p.is_dir() {
                let prov =
                    ematix_flow_core::ematix_fast_parquet_multi::EmatixFastParquetMultiTableProvider::try_new_dir(&p)?;
                ctx.register_table(*table, Arc::new(prov))?;
            } else {
                let prov =
                    ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider::try_new(
                        p.to_string_lossy().to_string(),
                    )?;
                ctx.register_table(*table, Arc::new(prov))?;
            }
        } else {
            ctx.register_parquet(*table, p.to_str().unwrap(), Default::default())
                .await?;
        }
    }

    // Σ.MG.2: single-node emission twin (same table providers, no
    // distributed rules) — bloom build sides must never route into
    // the mesh. Only built when shipping is live.
    let emit_ctx: Option<SessionContext> = if distributed
        && ematix_flow_core::flags::tri_state("EMAT_MESH_BLOOM_SHIP").unwrap_or(true)
    {
        match ematix_flow_distributed::bloom_emitter::single_node_emission_ctx(&ctx).await {
            Ok(twin) => Some(twin),
            Err(e) => {
                eprintln!("  bloom emission twin failed (shipping disabled): {e}");
                None
            }
        }
    } else {
        None
    };

    // Σ.TW.1 — the native twin for local-commit JOIN queries (docs/
    // ADR_NATIVE_TWIN_ROUTING.md). Distinct from `emit_ctx` above:
    // that one clones the distributed session's stock providers (it
    // only pre-executes small build sides); this one re-registers the
    // NATIVE fast providers because it runs whole queries — the KEYS.2
    // key-downcast and the rest of the planning-time levers live in
    // the provider constructors. Tri-state EMAT_TWIN_ROUTE, default
    // ON; =0 restores the localize-only behavior (the A/B control).
    let twin_ctx: Option<SessionContext> =
        if distributed && ematix_flow_core::flags::tri_state("EMAT_TWIN_ROUTE").unwrap_or(true) {
            match ematix_flow_distributed::native_twin::native_twin_ctx(&ctx).await {
                Ok(twin) => {
                    eprintln!("  twin routing: ON (local join commits run native)");
                    Some(twin)
                }
                Err(e) => {
                    eprintln!("  twin build failed (routing disabled): {e}");
                    None
                }
            }
        } else {
            None
        };

    // Σ.TW.2 — the measured mode-memo arms (see `mode_memo` module
    // docs). Two additional forced-distribute sessions: the gate is
    // pinned ON so a probe measures a REAL mesh execution even for
    // queries AUTO would commit locally (that's exactly the decision
    // bucket the memo exists to fix), one session with broadcast
    // joins off and one on. All three distributed sessions share the
    // take-once bloom slot — arms run sequentially and each mesh run
    // re-arms before planning. Tri-state EMAT_MODE_MEMO, default ON;
    // requires the twin (memo's single-node arm IS the twin).
    let memo_enabled = distributed
        && twin_ctx.is_some()
        && ematix_flow_core::flags::tri_state("EMAT_MODE_MEMO").unwrap_or(true);
    let mesh_arms: Option<(Arc<SessionContext>, Arc<SessionContext>)> = if memo_enabled {
        let mesh_ctx = Arc::new(SessionContext::from(build_session_state(
            true,
            resolver.clone(),
            bloom_slot.clone(),
            Some(ematix_flow_distributed::mesh_gate::MeshGateConfig::forced()),
            Some(false),
        )));
        let bcast_ctx = Arc::new(SessionContext::from(build_session_state(
            true,
            resolver.clone(),
            bloom_slot.clone(),
            Some(ematix_flow_distributed::mesh_gate::MeshGateConfig::forced()),
            Some(true),
        )));
        for c in [&mesh_ctx, &bcast_ctx] {
            for table in TPCH_TABLES {
                let p = table_source(&data_dir, table)
                    .unwrap_or_else(|| panic!("pre-flight passed but {table} source vanished"));
                c.register_parquet(*table, p.to_str().unwrap(), Default::default())
                    .await?;
            }
        }
        eprintln!("  mode memo: ON (probing twin / mesh / mesh+bcast per join query)");
        Some((mesh_ctx, bcast_ctx))
    } else {
        None
    };
    let memo = ematix_flow_distributed::mode_memo::ModeMemo::new();

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
            let (b, _plan_ms) = run_query(&ctx, &custom).await?;
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
    // EMAT_CACHE_STATS: cumulative counter snapshot carried across the query
    // loop so each query prints its own delta.
    let mut cache_prev: (u64, u64, u64, u64, u64, u64, u64) = (0, 0, 0, 0, 0, 0, 0);
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

        // EXPLAIN_ONLY=1 — dump the plan (logical + physical) and skip
        // execution. Default explains through the campaign's main ctx
        // (distributed when peers are set — the Q11/Q15 0-rows
        // SF100/DIST.1a diagnostic). EXPLAIN_ARM selects another arm's
        // session, so plans can be DIFFED across arms on identical
        // tables — the OOM.SF1000 instrument (same SQL, single leg
        // 21.8 s vs twin arm >250 GB peak ⇒ suspected plan divergence):
        //   EXPLAIN_ARM=twin   Σ.TW.1 native twin (requires peers)
        //   EXPLAIN_ARM=mesh   forced-mesh arm     (requires peers)
        //   EXPLAIN_ARM=bcast  mesh+broadcast arm  (requires peers)
        //   unset/other        main ctx (distributed or NO_DISTRIBUTE)
        if std::env::var_os("EXPLAIN_ONLY").is_some() {
            let arm = std::env::var("EXPLAIN_ARM").unwrap_or_default();
            let explain_ctx: &SessionContext = match arm.as_str() {
                "twin" => match twin_ctx.as_ref() {
                    Some(t) => t,
                    None => {
                        eprintln!(
                            "  EXPLAIN_ARM=twin but no twin ctx (need peers + twin routing) — skipping"
                        );
                        continue;
                    }
                },
                "mesh" | "bcast" => match mesh_arms.as_ref() {
                    Some((m, b)) => {
                        if arm == "mesh" {
                            m.as_ref()
                        } else {
                            b.as_ref()
                        }
                    }
                    None => {
                        eprintln!(
                            "  EXPLAIN_ARM={arm} but no mesh arms (need peers + memo) — skipping"
                        );
                        continue;
                    }
                },
                _ => &ctx,
            };
            eprintln!(
                "  explaining via arm: {}",
                if arm.is_empty() { "main" } else { &arm }
            );
            // Arm blooms so the dumped plan matches what a real run
            // executes (Σ.MG.2 diagnostics). The twin doesn't consume
            // embedded blooms (native sideband instead), matching the
            // trial path's skip.
            if arm != "twin" {
                emit_and_arm_blooms(emit_ctx.as_ref(), &sql, &bloom_slot).await;
            }
            match explain_ctx.sql(&format!("EXPLAIN {sql}")).await {
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

        // Σ.MG.2 plan-embedded blooms (distributed only; tri-state
        // EMAT_MESH_BLOOM_SHIP, default ON): every planning below
        // (probe, warmups, trials) re-emits + re-arms the take-once
        // slot. Emission pre-executes the small build sides, so it is
        // REAL per-query work — it runs INSIDE the trial timer,
        // exactly as production's read_arrow_stream pays it (and as
        // Trino/Spark pay their dynamic-filter builds during
        // execution). Failure is non-fatal: no blooms = no pruning,
        // never wrong results.
        let arm_blooms =
            || async { emit_and_arm_blooms(emit_ctx.as_ref(), &sql, &bloom_slot).await };

        // What did the adaptive mesh gate decide for this query?
        // Planning here is cheap relative to a trial (and each trial
        // re-plans anyway via ctx.sql); "unknown" means planning
        // itself failed — the warmups/trials below will surface the
        // same error.
        let n_blooms = arm_blooms().await;
        if n_blooms > 0 {
            eprintln!("  blooms: {n_blooms} embedded");
        }
        // Σ.TW.1: the route is decided HERE, once per query — the
        // gate's verdict is deterministic per query shape, so warmups
        // and trials below reuse it (production's plan-memo posture).
        // "twin" = the gate committed local AND the plan has a join →
        // re-plan + execute in the native twin; localized plans keep
        // running only for join-free local commits (measured faster
        // there: Q01 −706 ms / Q06 −262 ms vs native at SF100).
        let (mut plan_mode, scan_bytes, has_join): (String, Option<u64>, bool) =
            match ctx.sql(&sql).await {
                Ok(df) => match df.create_physical_plan().await {
                    Ok(plan) => {
                        let mode = if twin_ctx.is_some()
                            && ematix_flow_distributed::native_twin::should_route_to_twin(&plan)
                        {
                            "twin"
                        } else {
                            plan_mode_of(&plan)
                        };
                        (
                            mode.to_string(),
                            sum_scan_leaf_bytes(&plan),
                            ematix_flow_distributed::native_twin::plan_has_join(&plan),
                        )
                    }
                    Err(e) => {
                        eprintln!("  plan_mode probe failed (physical): {e}");
                        ("unknown".to_string(), None, false)
                    }
                },
                Err(e) => {
                    eprintln!("  plan_mode probe failed (logical): {e}");
                    ("unknown".to_string(), None, false)
                }
            };
        eprintln!(
            "  plan_mode={plan_mode} scan_bytes={}",
            scan_bytes.map_or("none".to_string(), |b| b.to_string())
        );

        // Σ.TW.2 probe-and-pick: for join queries, run each arm once
        // (untimed — the warmup budget every engine already spends),
        // remember the measured time, and run the trials on the
        // argmin. The probes double as a row-parity guard: a
        // fast-but-wrong arm (the Q15 float-veto class of bug) must
        // never win, so any row-count disagreement falls back to the
        // gate's own routing and logs loudly.
        let mut chosen_arm: Option<ematix_flow_distributed::mode_memo::Arm> = None;
        if memo_enabled && has_join {
            use ematix_flow_distributed::mode_memo::{Arm, ModeMemo};
            let (mesh_ctx, bcast_ctx) = mesh_arms.as_ref().expect("memo implies arms");
            let twin = twin_ctx.as_ref().expect("memo implies twin");
            let fp = ModeMemo::fingerprint(&sql);
            let arms: [(Arm, &SessionContext); 3] = [
                (Arm::Twin, twin),
                (Arm::Mesh, mesh_ctx.as_ref()),
                (Arm::MeshBroadcast, bcast_ctx.as_ref()),
            ];
            let mut probe_rows: Vec<(&'static str, usize)> = Vec::new();
            // Two passes per arm; the memo keeps the min, so the
            // first (cache-warming) pass can only lose to the second.
            // One pass systematically penalized whichever arm probed
            // first: the twin reads on the COORDINATOR, whose page
            // cache is cold until twin runs, while the mesh arms hit
            // worker caches warmed by every prior mesh probe —
            // measured at SF100 as twin losing Q02/Q05/Q21 probes it
            // wins by seconds once warm.
            for pass in 0..2 {
                for (arm, actx) in arms {
                    if arm != Arm::Twin {
                        arm_blooms().await;
                    }
                    let t0 = Instant::now();
                    match run_query(actx, &sql).await {
                        Ok((batches, _plan_ms)) => {
                            let ms = t0.elapsed().as_secs_f64() * 1000.0;
                            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                            if pass == 0 {
                                probe_rows.push((arm.label(), rows));
                            }
                            memo.record(fp, arm, ms);
                            eprintln!("  probe[{pass}] {}: {ms:.1} ms rows={rows}", arm.label());
                        }
                        Err(e) => eprintln!("  probe[{pass}] {} FAILED: {e}", arm.label()),
                    }
                }
            }
            if probe_rows.windows(2).any(|w| w[0].1 != w[1].1) {
                eprintln!(
                    "  PROBE ROW MISMATCH {probe_rows:?} — memo disabled for this \
                     query, falling back to the gate's routing"
                );
            } else if let Some((arm, ms)) = memo.best(fp) {
                eprintln!("  memo pick: {} ({ms:.1} ms)", arm.label());
                plan_mode = arm.label().to_string();
                chosen_arm = Some(arm);
            }
        }

        // Σ.TW.1 routing (also the memo's fallback): twin-routed
        // queries execute in the native twin — code path identical to
        // a NO_DISTRIBUTE run, including skipping the embedded-bloom
        // emit+arm (the twin's preset carries the native runtime-bloom
        // sideband instead; arming the mesh's plan-embedded blooms
        // would be dead work).
        use ematix_flow_distributed::mode_memo::Arm;
        let exec_ctx: &SessionContext = match chosen_arm {
            Some(Arm::Twin) => twin_ctx.as_ref().expect("twin arm implies twin ctx"),
            Some(Arm::Mesh) => mesh_arms.as_ref().expect("arm implies arms").0.as_ref(),
            Some(Arm::MeshBroadcast) => mesh_arms.as_ref().expect("arm implies arms").1.as_ref(),
            None if plan_mode == "twin" => twin_ctx.as_ref().expect("twin mode implies twin ctx"),
            None => &ctx,
        };
        let needs_bloom_arm = match chosen_arm {
            Some(Arm::Twin) => false,
            Some(_) => true,
            None => plan_mode != "twin",
        };

        // Warmups (untimed, but verify it runs cleanly). When the memo
        // probed, the chosen arm already had two untimed runs (one per
        // probe pass) — spend only the remaining warmup budget on it
        // so total untimed runs per trial-measured configuration match
        // the protocol (2).
        let remaining_warmups = if chosen_arm.is_some() {
            warmups.saturating_sub(2)
        } else {
            warmups
        };
        for w in 0..remaining_warmups {
            if needs_bloom_arm {
                arm_blooms().await;
            }
            match run_query(exec_ctx, &sql).await {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("  warmup {w} FAILED: {e}");
                    break;
                }
            }
        }

        // Measured trials.
        let mut trials_ms: Vec<f64> = Vec::with_capacity(trials);
        let mut plan_ms_trials: Vec<f64> = Vec::with_capacity(trials);
        let mut rows_returned: usize = 0;
        for _ in 0..trials {
            // Timer starts BEFORE emit+arm: the bloom build is part
            // of the measured query, not free setup. (Twin trials pay
            // their own full native planning inside the timer instead
            // — exactly what the native leg pays.)
            let t0 = Instant::now();
            if needs_bloom_arm {
                arm_blooms().await;
            }
            match run_query(exec_ctx, &sql).await {
                Ok((batches, plan_ms)) => {
                    let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
                    trials_ms.push(elapsed_ms);
                    plan_ms_trials.push(plan_ms);
                    rows_returned = batches.iter().map(|b| b.num_rows()).sum();
                }
                Err(e) => {
                    eprintln!("  trial FAILED: {e}");
                    trials_ms.push(f64::NAN);
                    plan_ms_trials.push(f64::NAN);
                }
            }
        }

        let med = median(&trials_ms);
        let p95v = p95(&trials_ms);
        let first = trials_ms.first().copied().unwrap_or(f64::NAN);
        let med35 = median_trials_3_5(&trials_ms);
        let med_plan = median(&plan_ms_trials);
        let rss_mb_after = ematix_flow_core::heap_pressure::current_rss_mb();
        eprintln!(
            "  median={med:.2}ms p95={p95v:.2}ms first={first:.2}ms med(3-5)={med35:.2}ms plan={med_plan:.2}ms rows={rows_returned} rss={}MB",
            rss_mb_after.map_or("?".to_string(), |r| format!("{r:.0}"))
        );

        // EMAT_CACHE_STATS=1 diagnostic: per-query DELTA of the process-global
        // RG decode-cache counters (hits/misses/entries + Σ.AI.6e retention
        // admits/rejects/protected-evictions + Σ.AI.6f ghost hits/demotions).
        // Answers "where did this query's cache hits go" without a profiler —
        // cumulative counters are read before/after each query via the public
        // probes.
        if std::env::var_os("EMAT_CACHE_STATS").is_some() {
            if let Some(c) = ematix_flow_core::emat_arrow_reader::process_rg_decode_cache() {
                let (h, m, entries) = c.stats();
                let (ad, rj, pe) = c.retention_stats();
                let (gh, gd) = c.retention_ghost_stats();
                let (ph, pm, pad, prj, ppe, pgh, pgd) = cache_prev;
                eprintln!(
                    "  cache: hits+{} misses+{} entries={} | retention(admit+{} reject+{} prot_evict+{} ghost_hit+{} demote+{}) enabled={}",
                    h - ph,
                    m - pm,
                    entries,
                    ad - pad,
                    rj - prj,
                    pe - ppe,
                    gh - pgh,
                    gd - pgd,
                    c.retention_enabled()
                );
                cache_prev = (h, m, ad, rj, pe, gh, gd);
            }
        }

        queries.insert(
            label,
            QueryStats {
                trials_ms,
                median_ms: med,
                p95_ms: p95v,
                first_trial_ms: first,
                median_trials_3_5_ms: med35,
                rows_returned,
                plan_mode,
                scan_bytes,
                plan_ms_trials,
                median_plan_ms: med_plan,
                rss_mb_after,
            },
        );
    }

    let cluster_size = peers.len() + 1; // +1 for coordinator
    let out = CampaignOutput {
        engine: "ematix",
        version: env!("CARGO_PKG_VERSION"),
        scale_factor,
        cluster_size,
        trials,
        warmups,
        provenance,
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

/// Σ.MG.2: emit plan-embedded blooms for `sql` and arm the take-once
/// slot; clears the slot when disabled / non-distributed / nothing
/// eligible. Returns the number of blooms armed.
async fn emit_and_arm_blooms(
    emit_ctx: Option<&SessionContext>,
    sql: &str,
    slot: &BloomSlot,
) -> usize {
    // `emit_ctx` is the SINGLE-NODE twin (Σ.MG.2 hang fix) — never
    // the distributed ctx, whose splitter would route the build-side
    // pre-execution into the Flight mesh and deadlock (2026-07-12
    // 00:28Z: coordinator parked on futexes, workers idle). None ⇒
    // shipping disabled / non-distributed run.
    let Some(ctx) = emit_ctx else {
        slot.clear();
        return 0;
    };
    // Σ.MG.3: from_env = key cap + emission cost gate (build-scan
    // cap / probe floor / probe:build ratio). Q02 SF100 paid ~8s of
    // partsupp scanning per trial for blooms worth nothing — only
    // lineitem-class probes with small builds emit now.
    let opts = ematix_flow_distributed::bloom_emitter::BloomEmitterOptions::from_env();
    // Defense in depth: a wedged emission degrades to "no blooms"
    // (pruning lost, correctness + liveness kept), never a hung leg.
    let emitted = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        match ctx.sql(sql).await {
            Ok(df) => match df.into_optimized_plan() {
                Ok(lp) => {
                    ematix_flow_distributed::bloom_emitter::emit_build_side_blooms(ctx, &lp, &opts)
                        .await
                        .unwrap_or_default()
                }
                Err(_) => Default::default(),
            },
            Err(_) => Default::default(),
        }
    })
    .await;
    let blooms = match emitted {
        Ok(b) => b,
        Err(_) => {
            eprintln!("  bloom emit timed out (30s) — running unbloomd");
            Default::default()
        }
    };
    let n = blooms.len();
    if n == 0 {
        slot.clear();
    } else {
        slot.fill(blooms);
    }
    n
}

/// Run one query, returning its batches plus the PLANNING time
/// (logical plan + physical optimization — the phase where the
/// adaptive mesh gate and every physical rule run) in ms. Splitting
/// plan from execution is what lets a mesh-vs-AUTO total delta be
/// attributed: identical physical plans with differing totals and flat
/// plan_ms ⇒ the delta is environmental (run order, cache state), not
/// the gate's decision cost. Execution goes through
/// `physical_plan::collect` on the SAME plan `create_physical_plan`
/// returned — byte-identical to what `df.collect()` would have built.
async fn run_query(
    ctx: &SessionContext,
    sql: &str,
) -> Result<(Vec<RecordBatch>, f64), Box<dyn std::error::Error>> {
    let t_plan = Instant::now();
    let df = ctx.sql(sql).await?;
    let plan = df.create_physical_plan().await?;
    let plan_ms = t_plan.elapsed().as_secs_f64() * 1000.0;
    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok((batches, plan_ms))
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

/// Bench↔preset parity pins (2026-07-04 refresh, mirrors
/// `bench_preset_parity_tests` in tpch_triangulation_bench and
/// `profiler_preset_parity_tests` in stage_profiler): the campaign's
/// single-node session must carry EXACTLY the production preset chain,
/// and the distributed session must be that chain + the
/// datafusion-distributed stage-splitter appended LAST.
///
/// Run: `cargo test -p ematix-flow-distributed --example tpch_distributed_campaign`
// NOTE: no `alloc_guard` here, deliberately — the campaign coordinator
// runs on the SYSTEM allocator, unlike flow-worker/CLI. That asymmetry
// is measured, not accidental: see the OOM.SF1000 postmortem at the top
// of this file (#223 reverted — mimalloc moved the SF=1000 OOM from
// Q21 to Q05 and raised peak RSS 239→257 GB).

#[cfg(test)]
mod campaign_preset_parity_tests {
    use super::*;

    fn no_peers() -> StaticPeers {
        StaticPeers { urls: vec![] }
    }

    #[test]
    fn single_node_rule_chain_equals_production_preset() {
        let state = build_session_state(false, no_peers(), BloomSlot::new(), None, None);
        let (physical, logical) = preset::ematix_rule_names(&state);
        assert_eq!(
            physical,
            preset::PRODUCTION_PHYSICAL_RULE_NAMES,
            "campaign PHYSICAL rule chain diverged from the production preset"
        );
        assert_eq!(
            logical,
            preset::PRODUCTION_LOGICAL_RULE_NAMES,
            "campaign LOGICAL rule chain diverged from the production preset"
        );
        assert_eq!(
            format!("{:?}", state.query_planner()),
            "FlowQueryPlanner",
            "campaign must ship the production FlowQueryPlanner"
        );
    }

    #[test]
    fn distributed_appends_stage_splitter_last() {
        let state = build_session_state(
            true,
            StaticPeers {
                urls: vec![Url::parse("http://127.0.0.1:50051").unwrap()],
            },
            BloomSlot::new(),
            None,
            None,
        );
        let (physical, logical) = preset::ematix_rule_names(&state);
        assert_eq!(
            logical,
            preset::PRODUCTION_LOGICAL_RULE_NAMES,
            "distributed LOGICAL chain diverged from the production preset"
        );
        // The distributed chain = production preset MINUS grace
        // demotion (single-node-only: a GraceHashJoinExec inside a
        // stage plan is undecodable by workers and its spill is
        // per-node — the 2026-07-11 mesh hang) + the mesh-gated
        // splitter + the Σ.MG.2 embedded-bloom wrap LAST (hang #3:
        // wrapping pre-split perturbs the splitter's topology and
        // deadlocks the fleet — blooms wrap INSIDE frozen stages).
        let expected_prefix: Vec<&str> = preset::PRODUCTION_PHYSICAL_RULE_NAMES
            .iter()
            .copied()
            .filter(|n| *n != "ematix_flow_grace_join_demotion")
            .collect();
        let n = expected_prefix.len();
        assert_eq!(
            &physical[..n],
            &expected_prefix[..],
            "distributed PHYSICAL chain must be the production preset \
             minus grace demotion"
        );
        assert_eq!(
            physical.len(),
            n + 2,
            "distributed chain adds the mesh-gated stage splitter then \
             the Σ.MG.2 embedded-bloom wrap: {physical:?}"
        );
        // 2026-07 adaptive mesh gate: the splitter slot is the
        // EMAT_MESH tri-state gate wrapping it
        // (ematix_flow_distributed::mesh_gate). Pin the exact name
        // so a silent revert to the ungated splitter trips here.
        assert_eq!(
            physical[n], "ematix_adaptive_distributed_mesh_gate",
            "the mesh-gated stage splitter must run before the bloom wrap"
        );
        assert_eq!(
            physical[n + 1],
            "ematix_embedded_bloom",
            "the bloom wrap must run AFTER the splitter — pre-split \
             wrapping changes stage topology (hang #3, Q02 SF100)"
        );
    }

    /// `plan_mode` detection: a session with no stage splitter can
    /// never produce a mesh plan, so the walker must report "single".
    /// (The mesh-positive side — a real `DistributedExec` root — is
    /// pinned by `tests/mesh_gate_plan.rs` against an in-process
    /// worker mesh.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn plan_mode_walker_reports_single_for_non_distributed_session() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use datafusion::datasource::MemTable;

        let state = build_session_state(false, no_peers(), BloomSlot::new(), None, None);
        let ctx = SessionContext::from(state);
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "n",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("nums", Arc::new(mem)).unwrap();

        let plan = ctx
            .sql("SELECT SUM(n) FROM nums")
            .await
            .expect("plan")
            .create_physical_plan()
            .await
            .expect("physical plan");
        assert_eq!(plan_mode_of(&plan), "single");
    }
}
