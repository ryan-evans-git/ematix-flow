//! TPC-H 22 triangulation bench: run every query through ematix-flow,
//! DuckDB, and Polars in the same process on the same SF=1 parquet
//! data, with warmup + N timed trials, and emit BENCHMARKS.md.
//!
//! Rationale: published Photon numbers vs our local SF=1 numbers
//! aren't apples-to-apples (different hardware, different scale,
//! different runtime warm-up). DuckDB and Polars are the open
//! competitors closest to Photon on SF=1-10 — beating them on the
//! shapes we've targeted (Q1/Q6/Q14 etc.) anchors us to the
//! Photon-class performance envelope without claiming Photon parity.
//!
//! Run:
//!     cargo run --release -p ematix-flow-core \
//!         --example tpch_triangulation_bench --features triangulation
//!
//! Env knobs:
//!   - TPCH_DATA_DIR   path to SF=1 parquet (default examples/tpch/data/sf1)
//!   - TPCH_TRIALS     measured trials per (query, engine) — default 5
//!   - TPCH_WARMUPS    untimed warmups before measured trials — default 2
//!   - TPCH_OUT        BENCHMARKS.md output path — default repo-root/BENCHMARKS.md
//!   - TPCH_QUERIES    comma-separated subset, e.g. "1,6,14" (default all 22)

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::bloom::ContextBlooms;
use ematix_flow_core::dedupe_aggregate_rule::DedupeAggregateForFloatDeterminism;
use ematix_flow_core::dict_aggregate_rule::EnableDictGroupCountRule;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::force_collect_left_semi_build_rule::ForceCollectLeftForSemiBoundedBuildRule;
use ematix_flow_core::fused_aggregate_filter_multi_agg_rule::InjectFilterMultiAggRule;
use ematix_flow_core::fused_aggregate_filter_sum_rule::InjectFilterSumRule;
use ematix_flow_core::inbloom_scan_pushdown_rule::EnableInBloomScanPushdownRule;
use ematix_flow_core::local_bloom_emitter::{LocalBloomOptions, emit_build_side_blooms_local};
use ematix_flow_core::push_down_left_semi_rule::PushDownLeftSemiRule;
use ematix_flow_core::robin_hood_sum_f64_exec::EnableRobinHoodSumF64Rule;
use ematix_flow_core::runtime_bloom_cascading_rule::EnableCascadingBloomRule;
use ematix_flow_core::runtime_bloom_sideband_rule::EnableRuntimeBloomSidebandRule;
use ematix_flow_core::swap_semi_join_build_rule::SwapSemiJoinBuildSideRule;
use futures_util::TryStreamExt;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Σ.AG.4 (2026-05-26): process-global PlanCache shared across trials.
/// Opt-in via `EMAT_PLAN_CACHE=1`. The cache's `is_cacheable` walker
/// refuses to cache plans with `BuildSideBloomEmitterExec` or
/// `HashJoinExec(mode=CollectLeft, join_type=LeftSemi|LeftAnti)`,
/// which carry execute-state not reset by `with_new_children`.
/// Uncacheable plans fall back to full re-physicalize per trial.
static PLAN_CACHE: OnceLock<ematix_flow_core::plan_cache::PlanCache> = OnceLock::new();

fn plan_cache() -> &'static ematix_flow_core::plan_cache::PlanCache {
    PLAN_CACHE.get_or_init(ematix_flow_core::plan_cache::PlanCache::new)
}

/// Σ.AJ.1 Lever C bench-harness fix (2026-05-27): when an opt-in
/// pre-plan walker (EMAT_AGG_SEMI / EMAT_DIM_PUSH / EMAT_REORDER) is
/// enabled, the trial code path bypasses the plan cache for ALL 22
/// queries — even those where the walker doesn't actually rewrite
/// anything. That uniformly regresses non-matching queries by ~3-8%
/// (plan cache miss every trial), masking the matching queries' wins.
///
/// `FIRES_CACHE` memoizes per-(rule, SQL) "does this rule actually
/// change the plan?" so subsequent trials short-circuit. First trial
/// pays one logical-opt + one TreeNode walk per enabled rule; warmup
/// discards this. Only used to gate the `any_rewrite` flag.
type FiresKey = (RewriteRule, String);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum RewriteRule {
    AggSemi,
    DimPush,
    Reorder,
    ReorderShapeGated,
    Q20Semi,
}

static FIRES_CACHE: OnceLock<Mutex<HashMap<FiresKey, bool>>> = OnceLock::new();

fn fires_cache() -> &'static Mutex<HashMap<FiresKey, bool>> {
    FIRES_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Returns true iff applying `rule` to the optimized plan of `sql`
/// produces a structurally different plan. Caches per (rule, SQL).
async fn rewrite_fires_for_sql(ctx: &SessionContext, sql: &str, rule: RewriteRule) -> bool {
    let key: FiresKey = (rule, sql.to_string());
    {
        let guard = fires_cache().lock().unwrap();
        if let Some(&fires) = guard.get(&key) {
            return fires;
        }
    }
    let df = match ctx.sql(sql).await {
        Ok(d) => d,
        Err(_) => return false,
    };
    let optimized = match df.into_optimized_plan() {
        Ok(p) => p,
        Err(_) => return false,
    };
    let pre = format!("{}", optimized.display_indent());
    let rewritten = match rule {
        RewriteRule::AggSemi => {
            ematix_flow_core::agg_filter_pushdown::push_filter_into_agg(optimized.clone())
        }
        RewriteRule::DimPush => {
            ematix_flow_core::dim_join_pushdown::push_dim_join_into_chain(optimized.clone())
        }
        RewriteRule::Reorder => {
            ematix_flow_core::join_reorder::reorder_inner_joins(optimized.clone())
        }
        RewriteRule::ReorderShapeGated => {
            ematix_flow_core::join_reorder::reorder_inner_joins_shape_gated(optimized.clone())
        }
        RewriteRule::Q20Semi => {
            ematix_flow_core::agg_filter_pushdown::push_transitive_semi_into_agg(optimized.clone())
        }
    };
    let rewritten = match rewritten {
        Ok(p) => p,
        Err(_) => return false,
    };
    let post = format!("{}", rewritten.display_indent());
    let fires = pre != post;
    fires_cache().lock().unwrap().insert(key, fires);
    fires
}

/// Convenience wrapper preserved for the agg-semi call site.
async fn agg_semi_fires_for_sql(ctx: &SessionContext, sql: &str) -> bool {
    rewrite_fires_for_sql(ctx, sql, RewriteRule::AggSemi).await
}

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Engine {
    EmatixFlow,
    DuckDb,
    Polars,
}

impl Engine {
    fn label(self) -> &'static str {
        match self {
            Engine::EmatixFlow => "ematix-flow",
            Engine::DuckDb => "DuckDB",
            Engine::Polars => "Polars",
        }
    }
    fn all() -> &'static [Engine] {
        &[Engine::EmatixFlow, Engine::DuckDb, Engine::Polars]
    }
    /// Filter `all()` by env vars:
    ///   TPCH_SKIP_POLARS=1   — drop Polars (Q21 SF=10 takes ~41s/trial,
    ///                          Q05 SF=10 panics with bigidx).
    ///   TPCH_SKIP_DUCKDB=1   — drop DuckDB.
    ///   TPCH_SKIP_EMATIX=1   — drop ematix-flow.
    /// At least one engine must remain; an empty set falls back to all().
    fn selected() -> Vec<Engine> {
        let skip_polars = std::env::var("TPCH_SKIP_POLARS")
            .map(|v| v != "0")
            .unwrap_or(false);
        let skip_duckdb = std::env::var("TPCH_SKIP_DUCKDB")
            .map(|v| v != "0")
            .unwrap_or(false);
        let skip_ematix = std::env::var("TPCH_SKIP_EMATIX")
            .map(|v| v != "0")
            .unwrap_or(false);
        let kept: Vec<Engine> = Self::all()
            .iter()
            .copied()
            .filter(|e| match e {
                Engine::Polars => !skip_polars,
                Engine::DuckDb => !skip_duckdb,
                Engine::EmatixFlow => !skip_ematix,
            })
            .collect();
        if kept.is_empty() {
            Self::all().to_vec()
        } else {
            kept
        }
    }
}

#[derive(Debug, Clone)]
enum Trial {
    Pass { elapsed_ms: f64, rows: usize },
    Skip(String), // dialect / unsupported feature
    Fail(String), // genuine error
}

#[derive(Debug, Default, Clone)]
struct EngineResult {
    trials: Vec<Trial>,
}

impl EngineResult {
    fn summarize(&self) -> Option<(f64, f64, usize)> {
        let mut times: Vec<f64> = self
            .trials
            .iter()
            .filter_map(|t| match t {
                Trial::Pass { elapsed_ms, .. } => Some(*elapsed_ms),
                _ => None,
            })
            .collect();
        if times.is_empty() {
            return None;
        }
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = times.len();
        let median = if n % 2 == 1 {
            times[n / 2]
        } else {
            (times[n / 2 - 1] + times[n / 2]) / 2.0
        };
        let mean = times.iter().sum::<f64>() / n as f64;
        let var = if n > 1 {
            times.iter().map(|t| (t - mean).powi(2)).sum::<f64>() / (n - 1) as f64
        } else {
            0.0
        };
        let rows = self
            .trials
            .iter()
            .find_map(|t| match t {
                Trial::Pass { rows, .. } => Some(*rows),
                _ => None,
            })
            .unwrap_or(0);
        Some((median, var.sqrt(), rows))
    }

    fn failure_reason(&self) -> Option<String> {
        self.trials.iter().find_map(|t| match t {
            Trial::Skip(m) | Trial::Fail(m) => Some(m.clone()),
            _ => None,
        })
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .ok_or("workspace root not found")?
        .to_path_buf();
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| workspace.join("examples/tpch/data/sf1"));
    let queries_dir = workspace.join("examples/tpch/queries");
    // #315: Q11's HAVING fraction is 0.0001 / SF — scale it so SF>=10 isn't
    // degenerate (0 rows for every engine). Applied to all engines' SQL below.
    let scale_factor = ematix_flow_core::tpch_params::scale_factor_from_data_dir(&data_dir);
    let out_path = std::env::var("TPCH_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| workspace.join("BENCHMARKS.md"));
    let trials: usize = std::env::var("TPCH_TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let warmups: usize = std::env::var("TPCH_WARMUPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);
    let query_subset: Vec<u8> = std::env::var("TPCH_QUERIES")
        .ok()
        .map(|s| {
            s.split(',')
                .filter_map(|t| t.trim().parse::<u8>().ok())
                .collect()
        })
        .unwrap_or_else(|| (1..=22u8).collect());

    for t in TPCH_TABLES {
        let p = data_dir.join(format!("{t}.parquet"));
        if !p.exists() {
            return Err(format!(
                "missing {}\nGenerate first:\n  cargo run --release -p ematix-flow-core \
                 --example tpch_generate -- --sf 1 --out {}",
                p.display(),
                data_dir.display()
            )
            .into());
        }
    }

    // Σ.AΩ Phase 1.4: `EMAT_PREFILL=1` populates the workload log's
    // `aggregate_observations` table by executing each query once at
    // the session-default partition count, walking the physical plan
    // for AggregateExec metrics, and recording (input_rows,
    // output_groups) under the recommender's shape hash. Then exits.
    // Strict-A/B benches should call this before the timed runs so
    // mode B's per-SQL partition count is stable across invocations.
    let prefill_mode = std::env::var("EMAT_PREFILL")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if prefill_mode {
        prefill_observations(&data_dir, &queries_dir, &query_subset).await?;
        return Ok(());
    }

    // Σ.AΩ Phase 1.6: race prefill mode. Runs each query twice at
    // the formula partition count and twice at session cores, takes
    // min on each side, records the verdict to
    // `partition_race_outcomes`. The recommender then consults the
    // race log first via `recommend_target_partitions_via_race`.
    let race_prefill_mode = std::env::var("EMAT_RACE_PREFILL")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if race_prefill_mode {
        race_prefill_observations(&data_dir, &queries_dir, &query_subset).await?;
        return Ok(());
    }

    // Σ.AΩ Phase 2.1: batch-size race prefill mode.
    let batch_race_prefill_mode = std::env::var("EMAT_BATCH_RACE_PREFILL")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if batch_race_prefill_mode {
        batch_race_prefill_observations(&data_dir, &queries_dir, &query_subset).await?;
        return Ok(());
    }

    println!("=== TPC-H 22 triangulation bench ===");
    println!("    data:    {}", data_dir.display());
    println!("    queries: {} ({:?})", query_subset.len(), query_subset);
    println!("    trials:  {trials} (after {warmups} warmups)");
    println!("    output:  {}", out_path.display());
    println!();

    let mut results: BTreeMap<u8, BTreeMap<Engine, EngineResult>> = BTreeMap::new();

    for &q in &query_subset {
        println!("--- Q{q:02} ---");
        let sql_path = queries_dir.join(format!("q{q:02}.sql"));
        let polars_sql_path = queries_dir.join(format!("q{q:02}.polars.sql"));
        let sql = ematix_flow_core::tpch_params::apply_tpch_query_params(
            q,
            &std::fs::read_to_string(&sql_path)?,
            scale_factor,
        );
        let polars_sql = std::fs::read_to_string(&polars_sql_path)
            .ok()
            .map(|s| ematix_flow_core::tpch_params::apply_tpch_query_params(q, &s, scale_factor));

        let mut per_engine: BTreeMap<Engine, EngineResult> = BTreeMap::new();
        let selected = Engine::selected();
        for &engine in &selected {
            let mut res = EngineResult::default();
            let engine_sql = match engine {
                Engine::Polars => polars_sql.as_deref().unwrap_or(&sql),
                _ => &sql,
            };
            for trial_idx in 0..(trials + warmups) {
                let trial = run_one(engine, &data_dir, engine_sql).await;
                if trial_idx >= warmups {
                    res.trials.push(trial);
                }
            }
            let tag = match res.summarize() {
                Some((med, sd, rows)) => format!("{med:7.2} ms ± {sd:5.2}  ({rows} rows)"),
                None => format!("FAIL: {}", res.failure_reason().unwrap_or_default()),
            };
            println!("  {:12} {tag}", engine.label());
            per_engine.insert(engine, res);
        }
        results.insert(q, per_engine);
        println!();
    }

    write_benchmarks_md(&out_path, &results, trials, warmups, &data_dir)?;
    println!("wrote {}", out_path.display());

    Ok(())
}

async fn run_one(engine: Engine, data_dir: &Path, sql: &str) -> Trial {
    match engine {
        Engine::EmatixFlow => run_ematix_flow(data_dir, sql).await,
        Engine::DuckDb => {
            // DuckDB uses sync IO; run on the blocking pool so we don't
            // block the tokio runtime worker threads.
            let dir = data_dir.to_path_buf();
            let sql = sql.to_string();
            tokio::task::spawn_blocking(move || run_duckdb(&dir, &sql))
                .await
                .unwrap_or_else(|e| Trial::Fail(format!("duckdb join: {e}")))
        }
        Engine::Polars => {
            // Polars's parquet reader uses tokio internally and panics
            // if invoked from within an outer multi-thread runtime. Drop
            // to a dedicated thread to give it a clean process state.
            let dir = data_dir.to_path_buf();
            let sql = sql.to_string();
            tokio::task::spawn_blocking(move || run_polars(&dir, &sql))
                .await
                .unwrap_or_else(|e| Trial::Fail(format!("polars join: {e}")))
        }
    }
}

// ---------- ematix-flow ----------

/// Σ.AΩ Phase 1.2 + 1.4 cache for the plan-time `target_partitions`
/// recommender. Keys are SQL strings; values pair the routed partition
/// count (Some when boost is desired, None for cores) with the
/// LogicalPlan-derived shape hash (Some when a qualifying aggregate
/// exists, None otherwise). The shape hash carries through to the
/// post-execution observation recorder so Phase 1.4 can match
/// observed cardinalities to the SQL the recommender saw.
///
/// First call per SQL builds a minimal SessionContext just to parse
/// the SQL into a LogicalPlan, runs the recommender, caches the
/// result. Subsequent calls hit the cache.
#[derive(Clone, Debug, Default)]
struct AutoPartitionsRec {
    target_partitions: Option<usize>,
    shape_hash: Option<String>,
    /// Σ.AΩ Phase 2.1: per-shape batch-size override from
    /// `batch_size_race_outcomes`. None means "use DEFAULT_BATCH_SIZE
    /// (or whatever `EMAT_BATCH_SIZE` env var says)". When
    /// `EMAT_AUTO_BATCH_SIZE=1`, run_ematix_flow sets
    /// `EMAT_BATCH_SIZE` to this value just before constructing the
    /// providers so they pick it up via `env_batch_size()`.
    batch_size_override: Option<u32>,
}

static AUTO_PARTITIONS_CACHE: OnceLock<Mutex<HashMap<String, AutoPartitionsRec>>> = OnceLock::new();

fn auto_partitions_cache() -> &'static Mutex<HashMap<String, AutoPartitionsRec>> {
    AUTO_PARTITIONS_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Σ.AΩ Phase 1.4 — single process-wide WorkloadLog handle. Opened
/// lazily on first use; persists at `~/.ematix/workload.db` (override
/// via `EMATIX_WORKLOAD_DB`). Read at recommend time, written at
/// EMAT_PREFILL time (or always when EMAT_AUTO_TARGET_PARTITIONS=1
/// and the executed plan exposes an AggregateExec).
static WORKLOAD_LOG: OnceLock<Option<Arc<ematix_flow_core::workload_log::WorkloadLog>>> =
    OnceLock::new();

/// Σ.AΩ Phase 2.2 — snapshot of `EMAT_BATCH_SIZE` at process start so
/// queries without a race verdict can restore it instead of clobbering
/// to "unset" when the auto-batch-size lever is on.
static ORIGINAL_BATCH_SIZE: OnceLock<Option<String>> = OnceLock::new();

fn original_batch_size_env() -> &'static Option<String> {
    ORIGINAL_BATCH_SIZE.get_or_init(|| std::env::var("EMAT_BATCH_SIZE").ok())
}

fn workload_log() -> Option<&'static Arc<ematix_flow_core::workload_log::WorkloadLog>> {
    WORKLOAD_LOG
        .get_or_init(|| {
            ematix_flow_core::workload_log::WorkloadLog::open_default()
                .ok()
                .map(Arc::new)
        })
        .as_ref()
}

async fn auto_target_partitions_lookup(data_dir: &Path, sql: &str) -> AutoPartitionsRec {
    {
        let guard = auto_partitions_cache().lock().unwrap();
        if let Some(v) = guard.get(sql) {
            return v.clone();
        }
    }
    let session_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(14);
    let recommendation = compute_auto_target_partitions(data_dir, sql, session_cores).await;
    auto_partitions_cache()
        .lock()
        .unwrap()
        .insert(sql.to_string(), recommendation.clone());
    recommendation
}

async fn compute_auto_target_partitions(
    data_dir: &Path,
    sql: &str,
    session_cores: usize,
) -> AutoPartitionsRec {
    // Build a minimal SessionContext just to parse SQL.
    let ctx = SessionContext::new();
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        if !path.exists() {
            return AutoPartitionsRec::default();
        }
        if *t == "lineitem" {
            let Ok(prov) = EmatixFastParquetTableProvider::try_new(path.to_string_lossy()) else {
                return AutoPartitionsRec::default();
            };
            if ctx.register_table(*t, Arc::new(prov)).is_err() {
                return AutoPartitionsRec::default();
            }
        } else {
            let Ok(prov) = FastParquetTableProvider::try_new(path.to_string_lossy()) else {
                return AutoPartitionsRec::default();
            };
            if ctx.register_table(*t, Arc::new(prov)).is_err() {
                return AutoPartitionsRec::default();
            }
        }
    }
    let Ok(df) = ctx.sql(sql).await else {
        return AutoPartitionsRec::default();
    };
    let Ok(plan) = df.into_optimized_plan() else {
        return AutoPartitionsRec::default();
    };
    let log = workload_log().map(|a| a.as_ref());
    // Σ.AΩ Phase 1.6: race-aware recommender consults
    // partition_race_outcomes first; falls through to Phase 1.5's
    // observation-aware path on cold start (no race verdict yet).
    let n = ematix_flow_core::auto_target_partitions::recommend_target_partitions_via_race(
        &plan,
        session_cores,
        log,
    );
    let shape_hash =
        ematix_flow_core::auto_target_partitions::qualifying_aggregate_shape_hash(&plan);
    // Σ.AΩ Phase 2.1: per-shape batch-size override from race log.
    let batch_size_override =
        ematix_flow_core::auto_target_partitions::recommend_batch_size_via_race(&plan, log);
    AutoPartitionsRec {
        target_partitions: if n > session_cores { Some(n) } else { None },
        shape_hash,
        batch_size_override,
    }
}

/// Σ.AΩ Phase 1.4 — populate `aggregate_observations` for every
/// query in `query_subset` by executing each once at the session-
/// default partition count and walking the physical plan for the
/// largest-group-by `AggregateExec(Final|FinalPartitioned)`'s
/// (input_rows, output_groups) metrics. Records under the shape
/// hash the Phase 1.4 recommender uses, so subsequent strict-A/B
/// invocations land in the observation-aware branch.
///
/// Skips queries with no qualifying aggregate (no GROUP BY, or
/// failed Phase 1.3 safety gates).
async fn prefill_observations(
    data_dir: &Path,
    queries_dir: &Path,
    query_subset: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(log) = workload_log() else {
        println!("EMAT_PREFILL: workload_log unavailable — skip");
        return Ok(());
    };
    println!("=== Σ.AΩ Phase 1.4 prefill ===");
    println!("    data:    {}", data_dir.display());
    println!("    queries: {} ({:?})", query_subset.len(), query_subset);
    println!();
    for &q in query_subset {
        let sql_path = queries_dir.join(format!("q{q:02}.sql"));
        let Ok(sql) = std::fs::read_to_string(&sql_path) else {
            println!("  Q{q:02}: missing SQL — skip");
            continue;
        };
        let rec = auto_target_partitions_lookup(data_dir, &sql).await;
        let Some(hash) = rec.shape_hash.clone() else {
            println!("  Q{q:02}: no qualifying aggregate — skip");
            continue;
        };
        // Always prefill at session default (no Phase 1.4 boost).
        // The recommendation IS the observation we're trying to make.
        let (ctx, bloom_rule) = match build_ematix_ctx(data_dir, None).await {
            Ok(c) => c,
            Err(e) => {
                println!("  Q{q:02}: ctx build fail: {}", short(&e.to_string()));
                continue;
            }
        };
        let df = match ctx.sql(&sql).await {
            Ok(d) => d,
            Err(e) => {
                println!("  Q{q:02}: sql fail: {}", short(&e.to_string()));
                continue;
            }
        };
        // Apply the same default-on logical rewrites as run_ematix_flow's
        // hot path so observed cardinalities match what the bench will
        // actually execute. Σ.U PushDownLeftSemi (`agg_semi_on`) is
        // critical here: without it Q17's inner AVG agg sees 60M rows
        // / 2M groups instead of the ~30K / ~200 the routed bench gets.
        let logical_plan = match df.into_optimized_plan() {
            Ok(p) => p,
            Err(e) => {
                println!("  Q{q:02}: opt fail: {}", short(&e.to_string()));
                continue;
            }
        };
        let logical_plan =
            match ematix_flow_core::agg_filter_pushdown::push_filter_into_agg(logical_plan) {
                Ok(p) => p,
                Err(e) => {
                    println!("  Q{q:02}: agg_semi fail: {}", short(&e.to_string()));
                    continue;
                }
            };
        let logical_plan =
            match ematix_flow_core::dim_join_pushdown::push_dim_join_into_chain(logical_plan) {
                Ok(p) => p,
                Err(e) => {
                    println!("  Q{q:02}: dim_push fail: {}", short(&e.to_string()));
                    continue;
                }
            };
        let logical_plan =
            match ematix_flow_core::agg_filter_pushdown::push_transitive_semi_into_agg(logical_plan)
            {
                Ok(p) => p,
                Err(e) => {
                    println!("  Q{q:02}: q20_semi fail: {}", short(&e.to_string()));
                    continue;
                }
            };
        if std::env::var_os("EMAT_DUMP_LOGICAL").is_some() {
            println!(
                "  Q{q:02} optimized logical plan (post agg_semi+dim_push):\n{}",
                logical_plan.display_indent()
            );
        }
        let df = match ctx.execute_logical_plan(logical_plan).await {
            Ok(d) => d,
            Err(e) => {
                println!("  Q{q:02}: exec_logical fail: {}", short(&e.to_string()));
                continue;
            }
        };
        let plan = match df.create_physical_plan().await {
            Ok(p) => p,
            Err(e) => {
                println!("  Q{q:02}: plan fail: {}", short(&e.to_string()));
                continue;
            }
        };
        let plan: Arc<dyn datafusion::physical_plan::ExecutionPlan> =
            if plan.output_partitioning().partition_count() > 1 {
                Arc::new(CoalescePartitionsExec::new(plan))
            } else {
                plan
            };
        let mut stream = match plan.execute(0, ctx.task_ctx()) {
            Ok(s) => s,
            Err(e) => {
                println!("  Q{q:02}: exec fail: {}", short(&e.to_string()));
                continue;
            }
        };
        loop {
            match stream.try_next().await {
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(e) => {
                    println!("  Q{q:02}: drain fail: {}", short(&e.to_string()));
                    break;
                }
            }
        }
        if std::env::var("EMAT_PREFILL_DUMP")
            .ok()
            .map(|v| v == "1")
            .unwrap_or(false)
        {
            println!("  Q{q:02} physical plan after execute:");
            dump_plan_metrics(plan.as_ref(), 2);
        }
        match ematix_flow_core::auto_target_partitions::record_observation_from_physical_plan(
            plan.as_ref(),
            &hash,
            log,
        ) {
            Ok(true) => {
                let (input_rows, output_groups) =
                    ematix_flow_core::auto_target_partitions::largest_groupby_agg_cardinalities(
                        plan.as_ref(),
                    )
                    .unwrap_or((0, 0));
                println!(
                    "  Q{q:02}: recorded {} input rows / {} groups (hash={})",
                    input_rows, output_groups, hash
                );
            }
            Ok(false) => {
                println!("  Q{q:02}: no group-by terminal aggregate found — skip");
            }
            Err(e) => println!("  Q{q:02}: record fail: {e}"),
        }
        if let Some(rule) = bloom_rule.as_ref() {
            rule.set(ContextBlooms::default());
        }
    }
    println!();
    Ok(())
}

/// Σ.AΩ Phase 2.1 — 3-way batch-size race. Runs each query twice at
/// 32K, 64K, and 128K batch sizes (taking the min on each side),
/// picks the winner via `pick_batch_size_winner`, records to
/// `batch_size_race_outcomes`. Uses the same shape hash as Phase
/// 1.6's partition race so the same prefill DB covers both arcs.
async fn batch_race_prefill_observations(
    data_dir: &Path,
    queries_dir: &Path,
    query_subset: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    use ematix_flow_core::workload_log::pick_batch_size_winner;
    let Some(log) = workload_log() else {
        println!("EMAT_BATCH_RACE_PREFILL: workload_log unavailable — skip");
        return Ok(());
    };
    println!("=== Σ.AΩ Phase 2.1 batch-size race prefill ===");
    println!("    data:    {}", data_dir.display());
    println!("    queries: {} ({:?})", query_subset.len(), query_subset);
    println!();
    for &q in query_subset {
        let sql_path = queries_dir.join(format!("q{q:02}.sql"));
        let Ok(sql) = std::fs::read_to_string(&sql_path) else {
            continue;
        };
        let shape_hash = match compute_shape_hash(data_dir, &sql).await {
            Some(h) => h,
            None => {
                println!("  Q{q:02}: no qualifying aggregate — skip");
                continue;
            }
        };
        // Three candidate batch sizes; take the min of two reps each
        // (Σ.L.1 pattern). Default (64K) goes first so OS cache is
        // warmed on the side most likely to win.
        let mut ms_by_size: [(u32, f64); 3] = [(65_536, f64::INFINITY); 3];
        ms_by_size[0] = (65_536, f64::INFINITY);
        ms_by_size[1] = (32_768, f64::INFINITY);
        ms_by_size[2] = (131_072, f64::INFINITY);
        let mut bad = false;
        for slot in &mut ms_by_size {
            let size = slot.0;
            unsafe { std::env::set_var("EMAT_BATCH_SIZE", size.to_string()) };
            // Three reps × take min. The 3rd rep is critical for the
            // wider-noise-floor case where 2 reps occasionally let an
            // outlier slip through; min-of-3 cuts that tail.
            let r1 = time_one(data_dir, &sql, None).await;
            let r2 = time_one(data_dir, &sql, None).await;
            let r3 = time_one(data_dir, &sql, None).await;
            slot.1 = [r1, r2, r3]
                .into_iter()
                .flatten()
                .fold(f64::INFINITY, f64::min);
            if !slot.1.is_finite() {
                bad = true;
                break;
            }
        }
        unsafe { std::env::remove_var("EMAT_BATCH_SIZE") };
        if bad {
            println!("  Q{q:02}: batch-size timing failed — skip");
            continue;
        }
        let ms_32k = ms_by_size.iter().find(|p| p.0 == 32_768).unwrap().1;
        let ms_64k = ms_by_size.iter().find(|p| p.0 == 65_536).unwrap().1;
        let ms_128k = ms_by_size.iter().find(|p| p.0 == 131_072).unwrap().1;
        let winner = pick_batch_size_winner(ms_32k, ms_64k, ms_128k);
        match log.record_batch_size_race(&shape_hash, ms_32k, ms_64k, ms_128k, winner) {
            Ok(()) => println!(
                "  Q{q:02}: 32K={ms_32k:.1}ms 64K={ms_64k:.1}ms 128K={ms_128k:.1}ms → winner={winner} (hash={shape_hash})"
            ),
            Err(e) => println!("  Q{q:02}: record batch race fail: {e}"),
        }
    }
    println!();
    Ok(())
}

/// Σ.AΩ Phase 1.6 — Σ.L.1-style 2-way partition race. For each query
/// with a qualifying aggregate shape, runs the query twice at the
/// plan-time formula partition count and twice at session cores,
/// takes the min wall time on each side, picks a winner via the
/// `pick_partition_race_winner` 5%-margin rule, and records the
/// verdict to the `partition_race_outcomes` table.
///
/// After race prefill, mode B in a strict A/B run consults the
/// race verdict directly and bypasses Phase 1.4's heuristic — Q17's
/// cores-wins verdict and Q18's formula-wins verdict are decided by
/// empirical wall time rather than by the
/// distinct-large-table predicate.
async fn race_prefill_observations(
    data_dir: &Path,
    queries_dir: &Path,
    query_subset: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    use ematix_flow_core::workload_log::pick_partition_race_winner;
    let Some(log) = workload_log() else {
        println!("EMAT_RACE_PREFILL: workload_log unavailable — skip");
        return Ok(());
    };
    println!("=== Σ.AΩ Phase 1.6 race prefill ===");
    println!("    data:    {}", data_dir.display());
    println!("    queries: {} ({:?})", query_subset.len(), query_subset);
    println!();

    let session_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(14);

    for &q in query_subset {
        let sql_path = queries_dir.join(format!("q{q:02}.sql"));
        let Ok(sql) = std::fs::read_to_string(&sql_path) else {
            continue;
        };
        // Phase 1.3 formula recommendation for this SQL. Compute it
        // without the log to get the plan-time-only answer (so the
        // race is genuinely formula-vs-cores, not log-vs-cores).
        let formula_partitions =
            match compute_formula_partitions(data_dir, &sql, session_cores).await {
                Some(n) => n,
                None => {
                    println!("  Q{q:02}: no qualifying aggregate — skip");
                    continue;
                }
            };
        if formula_partitions == session_cores {
            println!("  Q{q:02}: formula == cores ({session_cores}) — no race needed");
            continue;
        }
        // Need the shape hash to key the race outcome. Recompute via
        // the same mini-ctx the recommender uses (one parse, cheap).
        let shape_hash = match compute_shape_hash(data_dir, &sql).await {
            Some(h) => h,
            None => {
                println!("  Q{q:02}: shape hash unavailable — skip");
                continue;
            }
        };

        // Two reps at each partition count; take the min (Σ.L.1
        // pattern). Default first to warm OS cache, then formula —
        // matches the order in `dict_routing::probe_dict_vs_default`.
        let cores_ms_1 = time_one(data_dir, &sql, Some(session_cores)).await;
        let formula_ms_1 = time_one(data_dir, &sql, Some(formula_partitions)).await;
        let cores_ms_2 = time_one(data_dir, &sql, Some(session_cores)).await;
        let formula_ms_2 = time_one(data_dir, &sql, Some(formula_partitions)).await;
        let cores_ms = match (cores_ms_1, cores_ms_2) {
            (Some(a), Some(b)) => a.min(b),
            (Some(x), None) | (None, Some(x)) => x,
            (None, None) => {
                println!("  Q{q:02}: cores-side timing failed — skip");
                continue;
            }
        };
        let formula_ms = match (formula_ms_1, formula_ms_2) {
            (Some(a), Some(b)) => a.min(b),
            (Some(x), None) | (None, Some(x)) => x,
            (None, None) => {
                println!("  Q{q:02}: formula-side timing failed — skip");
                continue;
            }
        };
        let winner = pick_partition_race_winner(
            formula_partitions as u32,
            session_cores as u32,
            formula_ms,
            cores_ms,
        );
        if let Err(e) = log.record_partition_race(
            &shape_hash,
            formula_partitions as u32,
            session_cores as u32,
            formula_ms,
            cores_ms,
            winner,
        ) {
            println!("  Q{q:02}: record race fail: {e}");
        } else {
            println!(
                "  Q{q:02}: formula({formula_partitions})={formula_ms:.2}ms vs cores({session_cores})={cores_ms:.2}ms → winner={winner} (hash={shape_hash})"
            );
        }
    }
    println!();
    Ok(())
}

async fn compute_formula_partitions(
    data_dir: &Path,
    sql: &str,
    session_cores: usize,
) -> Option<usize> {
    let ctx = SessionContext::new();
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        if !path.exists() {
            return None;
        }
        if *t == "lineitem" {
            let prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy()).ok()?;
            ctx.register_table(*t, Arc::new(prov)).ok()?;
        } else {
            let prov = FastParquetTableProvider::try_new(path.to_string_lossy()).ok()?;
            ctx.register_table(*t, Arc::new(prov)).ok()?;
        }
    }
    let df = ctx.sql(sql).await.ok()?;
    let plan = df.into_optimized_plan().ok()?;
    let n =
        ematix_flow_core::auto_target_partitions::recommend_target_partitions(&plan, session_cores);
    Some(n)
}

async fn compute_shape_hash(data_dir: &Path, sql: &str) -> Option<String> {
    let ctx = SessionContext::new();
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        if !path.exists() {
            return None;
        }
        if *t == "lineitem" {
            let prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy()).ok()?;
            ctx.register_table(*t, Arc::new(prov)).ok()?;
        } else {
            let prov = FastParquetTableProvider::try_new(path.to_string_lossy()).ok()?;
            ctx.register_table(*t, Arc::new(prov)).ok()?;
        }
    }
    let df = ctx.sql(sql).await.ok()?;
    let plan = df.into_optimized_plan().ok()?;
    ematix_flow_core::auto_target_partitions::qualifying_aggregate_shape_hash(&plan)
}

/// Σ.AΩ Phase 1.6 — runs the query end-to-end at the requested
/// partition count and returns wall-ms. Applies the same default-on
/// logical rewrites as `run_ematix_flow` so race timings reflect
/// what the bench will actually execute.
async fn time_one(data_dir: &Path, sql: &str, partitions: Option<usize>) -> Option<f64> {
    let (ctx, bloom_rule) = build_ematix_ctx(data_dir, partitions).await.ok()?;
    let df = ctx.sql(sql).await.ok()?;
    let logical_plan = df.into_optimized_plan().ok()?;
    let logical_plan =
        ematix_flow_core::agg_filter_pushdown::push_filter_into_agg(logical_plan).ok()?;
    let logical_plan =
        ematix_flow_core::dim_join_pushdown::push_dim_join_into_chain(logical_plan).ok()?;
    let logical_plan =
        ematix_flow_core::agg_filter_pushdown::push_transitive_semi_into_agg(logical_plan).ok()?;
    let df = ctx.execute_logical_plan(logical_plan).await.ok()?;
    let t0 = Instant::now();
    let plan = df.create_physical_plan().await.ok()?;
    let plan: Arc<dyn datafusion::physical_plan::ExecutionPlan> =
        if plan.output_partitioning().partition_count() > 1 {
            Arc::new(CoalescePartitionsExec::new(plan))
        } else {
            plan
        };
    let mut stream = plan.execute(0, ctx.task_ctx()).ok()?;
    loop {
        match stream.try_next().await {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_) => return None,
        }
    }
    let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
    if let Some(rule) = bloom_rule.as_ref() {
        rule.set(ContextBlooms::default());
    }
    Some(elapsed_ms)
}

/// Σ.AΩ Phase 1.4 — dump every operator's (name, output_rows) in a
/// tree so the prefill mode can confirm where the aggregate
/// cardinality actually lives. Activated when `EMAT_PREFILL_DUMP=1`.
fn dump_plan_metrics(plan: &dyn datafusion::physical_plan::ExecutionPlan, depth: usize) {
    let n = plan.name();
    let output_rows = plan
        .metrics()
        .and_then(|m| m.output_rows())
        .map(|r| r.to_string())
        .unwrap_or_else(|| "—".to_string());
    println!(
        "    {}{} (output_rows={})",
        "  ".repeat(depth),
        n,
        output_rows
    );
    for child in plan.children() {
        dump_plan_metrics(child.as_ref(), depth + 1);
    }
}

/// KEYS.3 dig-in — dump the executed physical plan once per process when
/// `EMAT_DUMP_PLAN` is set, labelled with the downcast arm so off-vs-on dumps
/// are self-identifying. Once-guarded so it fires on the first trial only.
fn maybe_dump_plan(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>) {
    use std::sync::Once;
    static DUMPED: Once = Once::new();
    if std::env::var_os("EMAT_DUMP_PLAN").is_some() {
        DUMPED.call_once(|| {
            let arm = if std::env::var_os("EMAT_DOWNCAST_KEYS").is_some() {
                "downcast=ON"
            } else {
                "downcast=OFF"
            };
            eprintln!(
                "=== EMAT PHYSICAL PLAN ({arm}) ===\n{}=== END PLAN ===",
                datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
            );
        });
    }
}

async fn run_ematix_flow(data_dir: &Path, sql: &str) -> Trial {
    // Σ.AΩ Phase 1.6 (2026-05-28): default ON. The race-aware
    // recommender (`recommend_target_partitions_via_race`) consults
    // `partition_race_outcomes` first, falls through to Phase 1.5's
    // Σ.AΩ re-assessment (2026-05-28): DEFAULT FLIPPED BACK TO OFF.
    // The Phase 1.7/2.2 default-on flips were premised on prefilled
    // race verdicts (EMAT_RACE_PREFILL / EMAT_BATCH_RACE_PREFILL), but
    // (a) nothing populates the log in normal operation — empty log →
    // recommender returns None → no-op, so the "default-on win" never
    // materialised for real users; and (b) the verdicts go stale:
    // re-prefilled against the current binary (post metadata-cache +
    // dict-distinct flip + ematix-parquet 0.16.3) the batch race now
    // picks 64K (default) for every shape — the Q17→128K win decayed.
    // A fresh strict 22q SF=10 A/B (autotune OFF vs ON, freshly
    // prefilled) was net +0.94% (slightly slower), 0 clear wins:
    // the isolated per-shape race doesn't predict full-query wall time
    // (Q18/Q20/Q21 got *slower* with the 112-partition routing the
    // race "won"). Same trap as Σ.L.3.c / Σ.N.e. Static defaults win.
    // See `[[project_sigma_aomega_reassessment]]`. Opt in with
    // EMAT_AUTO_TARGET_PARTITIONS=1 (+ a prefill) for experiments.
    let auto_target_partitions = std::env::var("EMAT_AUTO_TARGET_PARTITIONS")
        .ok()
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(false);
    // Σ.AΩ re-assessment (2026-05-28): default OFF — see the
    // target_partitions note above. The batch-size race verdict
    // decayed to "pick 64K" on the current binary, so this lever is
    // a no-op even when prefilled. Opt in with EMAT_AUTO_BATCH_SIZE=1.
    let auto_batch_size = std::env::var("EMAT_AUTO_BATCH_SIZE")
        .ok()
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(false);
    let auto_rec = if auto_target_partitions || auto_batch_size {
        auto_target_partitions_lookup(data_dir, sql).await
    } else {
        AutoPartitionsRec::default()
    };
    let target_partitions_override = if auto_target_partitions {
        auto_rec.target_partitions
    } else {
        None
    };
    // Σ.AΩ Phase 2.1: set EMAT_BATCH_SIZE just before constructing
    // the providers so `env_batch_size()` in the readers picks up
    // the per-shape verdict. SAFETY: bench coordinator is single-
    // threaded at this point (next trial of this SQL waits for
    // this one), so the env-var write doesn't race with reader-
    // side reads in this or other queries.
    //
    // Original-batch-size preservation: a query without a per-shape
    // verdict resets to whatever the user set at process start
    // (captured in ORIGINAL_BATCH_SIZE), not to "unset". This way a
    // user-supplied `EMAT_BATCH_SIZE=8192` survives queries that
    // have no race verdict.
    if auto_batch_size {
        let original = original_batch_size_env();
        if let Some(b) = auto_rec.batch_size_override {
            unsafe { std::env::set_var("EMAT_BATCH_SIZE", b.to_string()) };
        } else {
            match original {
                Some(v) => unsafe { std::env::set_var("EMAT_BATCH_SIZE", v) },
                None => unsafe { std::env::remove_var("EMAT_BATCH_SIZE") },
            }
        }
    }
    let (ctx, bloom_rule) = match build_ematix_ctx(data_dir, target_partitions_override).await {
        Ok(c) => c,
        Err(e) => return Trial::Fail(format!("ctx build: {e}")),
    };
    // Σ.Q.L4′: when EMAT_BLOOM_PUSHDOWN=1, pre-execute small Inner-
    // equijoin build sides via the local emitter and install the
    // resulting ContextBlooms into the shared rule slot before timing
    // begins. Emission cost IS counted in the timed window so the
    // bench measures the lever's net effect.
    let bloom_pushdown = std::env::var("EMAT_BLOOM_PUSHDOWN")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    // Σ.AG.4 (2026-05-26): plan cache short-circuit. Active only for
    // the "vanilla" path (no per-trial logical rewriting), where the
    // cached LogicalPlan + PhysicalPlan template is functionally
    // identical across trials. Bypassed for plans with stateful
    // operators via is_cacheable in PlanCache::get_or_plan.
    //
    // Σ.AG.6 (2026-05-26): default ON at milestone config. SharedSubtreeExec
    // gate closed the last stale-data hole — the cache only stores plan
    // structure, every hit rebuilds a fresh executable tree via
    // `with_new_children`, and TableProviders re-read on every execute.
    // Set `EMAT_PLAN_CACHE=0` to disable for A/B benching.
    let plan_cache_on = std::env::var("EMAT_PLAN_CACHE")
        .ok()
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true);
    let reorder_on_check = std::env::var("EMAT_REORDER")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    // Σ.AJ.1 Lever C (2026-05-27): default ON. 22q SF=10 strict
    // interleaved A/B (commit pending) shows net -225.91 ms (-6.64%)
    // with 3 wins (Q08 -11, Q17 -104, Q18 -27) and zero >2σ
    // regressions. Set EMAT_AGG_SEMI=0 to disable for A/B benching.
    let agg_semi_on_check = std::env::var("EMAT_AGG_SEMI")
        .ok()
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true);
    // Σ.AK (2026-05-27): default ON. Σ.AD's dim-join pushdown gated
    // by the Q10-shape predicate (fact_side_has_competing_filtered_dim
    // in dim_join_pushdown.rs). Strict A/B vs current defaults shows
    // net -74.98 ms (-2.39%), Q10 -66 ms WIN, no regression > 5%. Set
    // EMAT_DIM_PUSH=0 to disable for A/B benching.
    let dim_push_on_check = std::env::var("EMAT_DIM_PUSH")
        .ok()
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true);
    // Σ.AJ.1 Lever C harness fix (2026-05-27): only bypass the plan
    // cache for queries where the pre-plan rewrite actually changes
    // the plan. Without per-SQL detection, all 22 queries pay the
    // plan-cache-bypass cost when ANY rewrite env var is set, masking
    // wins under the noise of uniform non-matching regressions.
    //
    // Each rule's actual-fires check runs the rule once on a cloned
    // optimized plan during the first trial for that SQL, then
    // memoizes the result. See `rewrite_fires_for_sql` above.
    let reorder_shape_gated_check = std::env::var("EMAT_REORDER_SHAPE_GATED")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let agg_semi_actually_fires = agg_semi_on_check && agg_semi_fires_for_sql(&ctx, sql).await;
    let dim_push_actually_fires =
        dim_push_on_check && rewrite_fires_for_sql(&ctx, sql, RewriteRule::DimPush).await;
    let reorder_actually_fires = reorder_on_check
        && rewrite_fires_for_sql(
            &ctx,
            sql,
            if reorder_shape_gated_check {
                RewriteRule::ReorderShapeGated
            } else {
                RewriteRule::Reorder
            },
        )
        .await;
    // Σ.Q20: bypass the plan cache when the transitive semi-pushdown
    // actually fires (Q20 only), so the timed path applies it instead of
    // serving the vanilla cached plan. Default-on; EMAT_Q20_TRANSITIVE_SEMI=0 off.
    let q20_semi_on_check = std::env::var("EMAT_Q20_TRANSITIVE_SEMI")
        .ok()
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true);
    let q20_semi_actually_fires =
        q20_semi_on_check && rewrite_fires_for_sql(&ctx, sql, RewriteRule::Q20Semi).await;
    let any_rewrite = reorder_actually_fires
        || agg_semi_actually_fires
        || dim_push_actually_fires
        || q20_semi_actually_fires
        || bloom_pushdown;

    if plan_cache_on && !any_rewrite {
        let t0 = Instant::now();
        let plan = match plan_cache().get_or_plan(&ctx, sql).await {
            Ok(p) => p,
            Err(e) => return Trial::Fail(format!("plan cache: {}", short(&e.to_string()))),
        };
        // Match `df.execute_stream()` parallelism: wrap multi-partition
        // plans in CoalescePartitionsExec so partition drains happen
        // concurrently inside DataFusion's coordinator, not sequentially
        // in a `for p in 0..N` loop on the bench side (which serialised
        // Q21 SF=10 and gave +30% wall — Q21's 14-partition CollectLeft
        // pipelines need to overlap to keep cores warm).
        let plan: Arc<dyn datafusion::physical_plan::ExecutionPlan> =
            if plan.output_partitioning().partition_count() > 1 {
                Arc::new(CoalescePartitionsExec::new(plan))
            } else {
                plan
            };
        maybe_dump_plan(&plan);
        let mut s = match plan.execute(0, ctx.task_ctx()) {
            Ok(s) => s,
            Err(e) => return Trial::Fail(format!("execute: {}", short(&e.to_string()))),
        };
        let mut total_rows = 0usize;
        loop {
            match s.try_next().await {
                Ok(Some(b)) => total_rows += b.num_rows(),
                Ok(None) => break,
                Err(e) => return Trial::Fail(format!("collect: {}", short(&e.to_string()))),
            }
        }
        let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
        if let Some(rule) = bloom_rule.as_ref() {
            rule.set(ContextBlooms::default());
        }
        // Σ.AΩ Phase 1.5: record observed aggregate cardinality AFTER
        // elapsed_ms is captured, so the recording cost (~µs) doesn't
        // leak into the timed window. The Σ.L.2 EWMA smooths across
        // trials/invocations; within a single process the per-SQL
        // recommendation cache is stable, so changing observations
        // mid-process doesn't alter target_partitions for this trial.
        if let (true, Some(shape_hash), Some(log)) = (
            auto_target_partitions,
            auto_rec.shape_hash.as_deref(),
            workload_log(),
        ) {
            let _ = ematix_flow_core::auto_target_partitions::record_observation_from_physical_plan(
                plan.as_ref(),
                shape_hash,
                log,
            );
        }
        return Trial::Pass {
            elapsed_ms,
            rows: total_rows,
        };
    }

    let t0 = Instant::now();
    if let (true, Some(rule)) = (bloom_pushdown, bloom_rule.as_ref()) {
        match ctx.sql(sql).await {
            Ok(df) => match df.into_optimized_plan() {
                Ok(plan) => {
                    let opts = LocalBloomOptions::default();
                    match emit_build_side_blooms_local(&ctx, &plan, &opts).await {
                        Ok(map) => rule.set(ContextBlooms::new(map)),
                        Err(e) => {
                            return Trial::Fail(format!("bloom emit: {}", short(&e.to_string())));
                        }
                    }
                }
                Err(e) => return Trial::Fail(format!("plan: {}", short(&e.to_string()))),
            },
            Err(e) => return Trial::Fail(format!("plan: {}", short(&e.to_string()))),
        }
    }
    let df = match ctx.sql(sql).await {
        Ok(d) => d,
        Err(e) => return Trial::Fail(format!("plan: {}", short(&e.to_string()))),
    };
    // Σ.T Phase 3 (2026-05-25): optional join-reorder pre-plan walker.
    // Opt-in via `EMAT_REORDER=1`. Runs DataFusion's logical
    // optimizer to expand `Cross Join + Filter` → Inner Join, then
    // applies the connectivity-aware cardinality-minimizing greedy
    // reorder, then hands the rewritten plan to `execute_logical_plan`
    // (which skips logical optimization — predicate pushdown /
    // projection pruning ran in the first pass).
    //
    // Σ.U Phase 1 (2026-05-26) + Σ.AJ.1 Lever C (2026-05-27): agg-side
    // LeftSemi pushdown. Default ON. Pushes a Filter subtree as
    // LeftSemi into an Aggregate's input so the agg only sees rows
    // whose group key survives the outer filter. Targets correlated-
    // subquery shapes (Q17 -104 ms, Q08 -11 ms, Q18 -27 ms at SF=10).
    // Set EMAT_AGG_SEMI=0 to disable for A/B benching.
    let reorder_on = std::env::var("EMAT_REORDER")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let agg_semi_on = std::env::var("EMAT_AGG_SEMI")
        .ok()
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true);
    // Σ.AD (2026-05-25) + Σ.AK (2026-05-27): dim-join pushdown gated
    // by Q10-shape predicate. Default ON. Targets Q10's customer×orders
    // chain (-23% / -66 ms at SF=10). The Σ.AK predicate blocks the
    // rule on Q07/Q21/Q03 shapes (competing filtered dim in fact_side
    // would force CollectLeft-after-Coalesce inversion). Set
    // EMAT_DIM_PUSH=0 to disable for A/B benching.
    let dim_push_on = std::env::var("EMAT_DIM_PUSH")
        .ok()
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true);
    let df = if reorder_on || agg_semi_on || dim_push_on {
        match df.into_optimized_plan() {
            Ok(plan) => {
                let plan = if agg_semi_on {
                    match ematix_flow_core::agg_filter_pushdown::push_filter_into_agg(plan) {
                        Ok(p) => p,
                        Err(e) => {
                            return Trial::Fail(format!("agg_semi: {}", short(&e.to_string())));
                        }
                    }
                } else {
                    plan
                };
                let plan = if dim_push_on {
                    match ematix_flow_core::dim_join_pushdown::push_dim_join_into_chain(plan) {
                        Ok(p) => p,
                        Err(e) => {
                            return Trial::Fail(format!("dim_push: {}", short(&e.to_string())));
                        }
                    }
                } else {
                    plan
                };
                // Σ.Q20: transitive semi-pushdown (forest-parts → lineitem
                // correlated-agg). Default-on; EMAT_Q20_TRANSITIVE_SEMI=0 to disable.
                let plan = if std::env::var("EMAT_Q20_TRANSITIVE_SEMI")
                    .ok()
                    .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
                    .unwrap_or(true)
                {
                    match ematix_flow_core::agg_filter_pushdown::push_transitive_semi_into_agg(plan)
                    {
                        Ok(p) => p,
                        Err(e) => {
                            return Trial::Fail(format!("q20_semi: {}", short(&e.to_string())));
                        }
                    }
                } else {
                    plan
                };
                if std::env::var_os("EMAT_DUMP_LOGICAL").is_some() {
                    eprintln!(
                        "=== Q timed logical plan (post agg_semi+dim_push+q20_semi) ===\n{}",
                        plan.display_indent()
                    );
                }
                let plan = if reorder_on {
                    // Σ.AH.X Lever G (closed 2026-05-27): the shape-gated
                    // entry point exists and works on focused subsets
                    // (Q10 -20 ms, Q09 -18 ms reliably), but the 22q SF=10
                    // bench doesn't clear the noise floor. Default here
                    // remains the permissive `reorder_inner_joins` to
                    // preserve Σ.T's original behavior. Set
                    // `EMAT_REORDER_SHAPE_GATED=1` to use the gated entry
                    // (max 4 leaves + reject string-LIKE + reject
                    // aggregate-result join keys + jump-on-reject).
                    let gated = std::env::var("EMAT_REORDER_SHAPE_GATED")
                        .ok()
                        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                        .unwrap_or(false);
                    // Σ.AΩ Phase 2.5 (C.1): override max_leaves on the
                    // shape-gated path only. Default 4; sweep {3,4,5,6}.
                    let max_leaves_env: Option<usize> = std::env::var("EMAT_REORDER_MAX_LEAVES")
                        .ok()
                        .and_then(|s| s.parse().ok());
                    let reorder_result = if gated {
                        let mut opts = ematix_flow_core::join_reorder::ReorderOpts::default();
                        if let Some(n) = max_leaves_env {
                            opts.max_leaves = n;
                        }
                        ematix_flow_core::join_reorder::reorder_inner_joins_with_opts(plan, opts)
                    } else {
                        ematix_flow_core::join_reorder::reorder_inner_joins(plan)
                    };
                    match reorder_result {
                        Ok(p) => p,
                        Err(e) => {
                            return Trial::Fail(format!("reorder: {}", short(&e.to_string())));
                        }
                    }
                } else {
                    plan
                };
                match ctx.execute_logical_plan(plan).await {
                    Ok(d) => d,
                    Err(e) => {
                        return Trial::Fail(format!("rewrite exec: {}", short(&e.to_string())));
                    }
                }
            }
            Err(e) => return Trial::Fail(format!("optimize: {}", short(&e.to_string()))),
        }
    } else {
        df
    };
    // Σ.AΩ Phase 1.5: replace `df.execute_stream()` with explicit
    // `create_physical_plan() + execute(0)` so we keep a handle to
    // the executed plan after drain. The post-drain handle is what
    // `record_observation_from_physical_plan` walks for the largest
    // group-by aggregate's metrics. Behaviour matches DataFusion's
    // `execute_stream`: wrap multi-partition plans in
    // `CoalescePartitionsExec` and execute partition 0.
    let plan = match df.create_physical_plan().await {
        Ok(p) => p,
        Err(e) => return Trial::Fail(format!("plan: {}", short(&e.to_string()))),
    };
    let plan: Arc<dyn datafusion::physical_plan::ExecutionPlan> =
        if plan.output_partitioning().partition_count() > 1 {
            Arc::new(CoalescePartitionsExec::new(plan))
        } else {
            plan
        };
    maybe_dump_plan(&plan);
    let stream = match plan.execute(0, ctx.task_ctx()) {
        Ok(s) => s,
        Err(e) => return Trial::Fail(format!("execute: {}", short(&e.to_string()))),
    };
    let batches: Result<Vec<_>, _> = stream.try_collect().await;
    let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
    // Clear the bloom slot so the next trial starts cold (matters for
    // the warmup vs timed-trial distinction; emit only inside timed
    // windows above).
    if let Some(rule) = bloom_rule.as_ref() {
        rule.set(ContextBlooms::default());
    }
    // Σ.AΩ Phase 1.5: record observed cardinality AFTER elapsed_ms,
    // before returning. See the plan-cache branch above for rationale.
    if let (true, Some(shape_hash), Some(log)) = (
        auto_target_partitions,
        auto_rec.shape_hash.as_deref(),
        workload_log(),
    ) {
        let _ = ematix_flow_core::auto_target_partitions::record_observation_from_physical_plan(
            plan.as_ref(),
            shape_hash,
            log,
        );
    }
    match batches {
        Ok(b) => Trial::Pass {
            elapsed_ms,
            rows: b.iter().map(|rb| rb.num_rows()).sum(),
        },
        Err(e) => Trial::Fail(format!("collect: {}", short(&e.to_string()))),
    }
}

async fn build_ematix_ctx(
    data_dir: &Path,
    target_partitions_override: Option<usize>,
) -> Result<(SessionContext, Option<Arc<EnableInBloomScanPushdownRule>>), Box<dyn std::error::Error>>
{
    // EMAT_RULES env knob for A/B benching. Defaults to "all" (production
    // configuration). Other values enable subsets for isolating which rule
    // accounts for which slice of the geomean perf vs v0.4.0.
    //   "all" / unset           — dedupe + dict + multi + sum
    //   "none"                  — no flow rules, default DataFusion only
    //   "v040"                  — dict + multi + sum (matches v0.4.0)
    //   "dedupe"                — dedupe only
    let rules = std::env::var("EMAT_RULES").unwrap_or_else(|_| "all".to_string());
    let session_partitions: usize = std::env::var("PARTITIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(14)
        });
    // Σ.AΩ Phase 1.2 (2026-05-28): per-query target_partitions override.
    // When the plan-time recommender (auto_target_partitions) detects a
    // high-cardinality GROUP BY aggregate, the override raises this
    // query's target_partitions above the session default so DataFusion's
    // EnforceDistribution naturally propagates the higher count.
    let partitions = target_partitions_override.unwrap_or(session_partitions);
    let mut session_config = SessionConfig::new().with_target_partitions(partitions);
    // REV.17.2 (2026-05-30): optionally raise DataFusion's collect-left
    // threshold so its OWN JoinSelection broadcasts provably-small dim
    // builds (Q17 j1/j2 ~4M, Q16 j1 ~200K) that the default 128K-row
    // threshold otherwise leaves hash-Partitioned (= a fact-side shuffle).
    // Default unset = stock DataFusion behavior.
    if let Some(rows) = std::env::var("EMAT_COLLECT_LEFT_THRESHOLD_ROWS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
    {
        let opts = session_config.options_mut();
        opts.optimizer.hash_join_single_partition_threshold_rows = rows;
        // Make the row count the binding constraint: lift the byte cap
        // (default 1 MB) well above a small-row build so it isn't rejected
        // on bytes. 64 B/row is a generous upper bound for TPC-H keys.
        opts.optimizer.hash_join_single_partition_threshold = rows.saturating_mul(64);
    }
    let mut builder = SessionStateBuilder::new()
        .with_config(session_config)
        .with_default_features();
    if matches!(rules.as_str(), "all" | "dedupe") {
        builder = builder
            .with_physical_optimizer_rule(Arc::new(DedupeAggregateForFloatDeterminism::default()));
    }
    if matches!(rules.as_str(), "all" | "v040") {
        builder = builder
            .with_physical_optimizer_rule(Arc::new(EnableDictGroupCountRule))
            .with_physical_optimizer_rule(Arc::new(InjectFilterMultiAggRule))
            .with_physical_optimizer_rule(Arc::new(InjectFilterSumRule));
    }
    // Σ.Q L2: opt-in semi-join build-side swap. ON by default in "all"
    // and "swap"; OFF for "v040" / "none" / "dedupe" so A/B benches can
    // isolate its impact.
    let swap_enabled = std::env::var("EMAT_SWAP_SEMI")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or_else(|| matches!(rules.as_str(), "all" | "swap"));
    if swap_enabled {
        builder = builder.with_physical_optimizer_rule(Arc::new(SwapSemiJoinBuildSideRule));
    }
    // REV.3: force CollectLeft on Inner joins with a semi-bounded build
    // (Q18-class) so the 60M/600M-row probe streams with NO hash
    // exchange (re-run EnforceDistribution drops the probe repartition +
    // coalesces the tiny build). Runs after SwapSemi so it sees the
    // RightSemi. DEFAULT-ON to match the production preset (REV.15 force
    // CollectLeft + REV.17.3 relative broadcast, both via ::default()) so a
    // bare bench run mirrors production and future regressions surface.
    // Opt-out via EMAT_FORCE_COLLECT_LEFT=0 (for A/B isolation).
    let force_collect_left_enabled = std::env::var("EMAT_FORCE_COLLECT_LEFT")
        .ok()
        .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
        .unwrap_or(true);
    if force_collect_left_enabled {
        builder = builder.with_physical_optimizer_rule(Arc::new(
            ForceCollectLeftForSemiBoundedBuildRule::default(),
        ));
    }
    // Σ.Q.L10: logical-plan rewrite — push LeftSemi past Inner joins
    // down to its target table. Closes the Q18-shape structural gap
    // to DuckDB (semi-filter pushed to wrap orders directly,
    // eliminating the 60M-row intermediate). **Default ON at the
    // milestone config (0.738 / 17 wins SF=10);** set
    // `EMAT_PUSH_SEMI=0` to disable for A/B benching.
    let push_semi_enabled = std::env::var("EMAT_PUSH_SEMI")
        .ok()
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true);
    if push_semi_enabled {
        // Σ.Q.M (synthetic LeftSemi producer) MUST run BEFORE Σ.Q.L10
        // (semi pushdown consumer). DataFusion runs custom rules in
        // registration order, so add M first.
        let synth_semi_enabled = std::env::var("EMAT_SYNTHETIC_LEFT_SEMI")
            .ok()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        if synth_semi_enabled {
            builder = builder.with_optimizer_rule(Arc::new(
                ematix_flow_core::synthetic_left_semi_rule::SyntheticLeftSemiRule,
            ));
        }
        builder = builder.with_optimizer_rule(Arc::new(PushDownLeftSemiRule));
    }
    // Σ.Q.L1b: routes SUM(Float64) GROUP BY Int64 through
    // RobinHoodSumF64Exec ([[sigma-nf3-beats-stock]]). **Default ON
    // at milestone config** (closes Q22 −18 ms, Q04 −26 ms); set
    // `EMAT_RH_SUM_F64=0` to disable for A/B.
    let rh_sum_f64_enabled = std::env::var("EMAT_RH_SUM_F64")
        .ok()
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true);
    // REV.8: single-pass radix agg replaces the WHOLE Partial→Repartition→
    // Final for high-card SUM(f64) GROUP BY i64 (Q18 SF=100 subquery, ~150M
    // groups). Gate measured 1.71× vs the two-phase. It must match an
    // AggregateExec(Partial), so it is mutually exclusive with the
    // RobinHood-sum rewrite (which would replace the Partial first) — when
    // EMAT_SINGLE_PASS_RADIX=1 it pre-empts rh_sum. Opt-in for A/B.
    let single_pass_radix_enabled = std::env::var("EMAT_SINGLE_PASS_RADIX")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if single_pass_radix_enabled {
        builder = builder.with_physical_optimizer_rule(Arc::new(
            ematix_flow_core::single_pass_radix_sum_exec::EnableSinglePassRadixSumRule::default(),
        ));
    } else if rh_sum_f64_enabled {
        builder =
            builder.with_physical_optimizer_rule(Arc::new(EnableRobinHoodSumF64Rule::default()));
    }
    // REV.18c: COUNT(*) GROUP BY i64 and AVG(f64) GROUP BY i64 RobinHood
    // kernels are OPT-IN. The operator crossover sweep shows all three only
    // beat stock in the 200K–256K group band; no SF=10 TPC-H query lands a
    // matching shape there (Q18 SUM/l_orderkey ≈15M groups, Q13 COUNT/o_custkey
    // ≈1M — both gated out at 256K). Env-gated here for the default-on A/B.
    if std::env::var("EMAT_RH_COUNT").ok().as_deref() == Some("1") {
        builder = builder.with_physical_optimizer_rule(Arc::new(
            ematix_flow_core::robin_hood_agg_rule::EnableRobinHoodAggregateRule::default(),
        ));
    }
    if std::env::var("EMAT_RH_AVG").ok().as_deref() == Some("1") {
        builder = builder.with_physical_optimizer_rule(Arc::new(
            ematix_flow_core::robin_hood_avg_f64_exec::EnableRobinHoodAvgF64Rule::default(),
        ));
    }
    // Σ.AN.1 (2026-05-28): per-operator partition routing for
    // high-cardinality FinalPartitioned aggregates. Opt-in via
    // EMAT_AGG_PARTITION_BOOST=1. Q18 SF=10 target: -20 to -30 ms
    // by routing the 15M-group SUM aggregate's repartition from
    // cores (=14) to 112 (8× cores). See PHASE_SIGMA_AN_1_DESIGN.md.
    let agg_partition_boost_enabled = std::env::var("EMAT_AGG_PARTITION_BOOST")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if agg_partition_boost_enabled {
        builder = builder.with_physical_optimizer_rule(Arc::new(
            ematix_flow_core::agg_partition_boost::AggPartitionBoostRule,
        ));
    }
    // Σ.Q.L9: threads a sideband between HashJoinExec build and
    // probe-side EmatixFastParquetExec so the build-side bloom is
    // captured as a side-effect of the regular HashJoin build phase.
    // **Default ON at milestone config**; set `EMAT_RT_BLOOM_SIDEBAND=0`
    // to disable for A/B.
    let rt_bloom_enabled = std::env::var("EMAT_RT_BLOOM_SIDEBAND")
        .ok()
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true);
    if rt_bloom_enabled {
        // Σ.Q.L15: tighter selectivity ratio + Inner-join L9 default
        // ON at milestone config. ratio=1024 gates out the L4'-style
        // net-negative s⋈l firing while still firing on small-dim →
        // fact pushdowns. Set `EMAT_RT_BLOOM_RATIO=64` and
        // `EMAT_RT_BLOOM_INNER_JOIN=0` to revert to pre-L15.
        let ratio = std::env::var("EMAT_RT_BLOOM_RATIO")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1024);
        let allow_inner = std::env::var("EMAT_RT_BLOOM_INNER_JOIN")
            .ok()
            .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
            .unwrap_or(true);
        let require_filtered_build = std::env::var("EMAT_L9_REQUIRE_FILTERED_BUILD")
            .ok()
            .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
            .unwrap_or(true);
        // Σ.S.B: install cascading variant when EMAT_L9_CASCADE=1.
        // Strict superset — same gates plus FK-chain extras.
        let cascade = std::env::var_os("EMAT_L9_CASCADE").is_some();
        if cascade {
            let max_extras = std::env::var("EMAT_L9_CASCADE_MAX")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(4);
            builder = builder.with_physical_optimizer_rule(Arc::new(EnableCascadingBloomRule {
                min_probe_to_build_ratio: ratio,
                allow_inner_join: allow_inner,
                require_filtered_build,
                max_extras_per_emitter: max_extras,
            }));
        } else {
            // Σ.AH.3 Story 2a: per-partition build-size ceiling, opt-in.
            let max_keys: usize = std::env::var("EMAT_L9_MAX_EXPECTED_KEYS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            builder =
                builder.with_physical_optimizer_rule(Arc::new(EnableRuntimeBloomSidebandRule {
                    min_probe_to_build_ratio: ratio,
                    allow_inner_join: allow_inner,
                    require_filtered_build,
                    max_expected_keys_per_partition: max_keys,
                }));
        }
    }
    // Σ.AE: opt-in late physical-optimizer rule that surgically drops
    // redundant FilterExec conjuncts already evaluated exactly by
    // BridgeFilter. Default OFF — enable via
    // `EMAT_DROP_REDUNDANT_FILTER=1` to A/B against the Inexact
    // baseline.
    builder =
        ematix_flow_core::drop_redundant_filter_rule::install_drop_redundant_filter_rule(builder);
    // Σ.AJ.1 Lever B POC: opt-in via EMAT_L9_BROADCAST_SIBLINGS=1.
    // Runs after the L9 sideband rule to broadcast existing bloom
    // emitters to sibling same-parquet-path scans elsewhere in the
    // plan (specifically the Q17 part⋈lineitem bloom → subquery
    // lineitem.l_partkey scan pattern).
    if std::env::var_os("EMAT_L9_BROADCAST_SIBLINGS").is_some() {
        builder =
            ematix_flow_core::broadcast_sibling_blooms_rule::install_broadcast_sibling_blooms_rule(
                builder,
            );
    }
    // Σ.Q.L4′: install the in-scan bloom pushdown rule with an empty
    // shared bloom slot. `run_ematix_flow` swaps the slot's contents
    // before each timed query when EMAT_BLOOM_PUSHDOWN=1.
    let bloom_pushdown = std::env::var("EMAT_BLOOM_PUSHDOWN")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let bloom_rule: Option<Arc<EnableInBloomScanPushdownRule>> = if bloom_pushdown {
        let rule = Arc::new(EnableInBloomScanPushdownRule::default());
        builder = builder.with_physical_optimizer_rule(rule.clone());
        Some(rule)
    } else {
        None
    };
    let state = builder.build();
    let ctx = SessionContext::new_with_state(state);
    for t in TPCH_TABLES {
        let path = data_dir
            .join(format!("{t}.parquet"))
            .to_string_lossy()
            .into_owned();
        // Σ.Q.L9 extension probe — register orders as Emat too so
        // the descent into the inner HashJoinExec can reach an Emat
        // scan carrying o_orderkey. Enabled via env for measurement;
        // not default since Emat for orders hasn't been broadly
        // benched on the full 22 queries.
        let orders_as_emat = std::env::var("EMAT_REGISTER_ORDERS_AS_EMAT")
            .ok()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        // Σ.Q.L15: register every TPC-H table via
        // EmatixFastParquetTableProvider. Lets the L9 runtime bloom
        // sideband target supplier/customer/etc. scans (otherwise
        // those land on FastParquet and L9's
        // find_probe_scan_for_column skips them). **Default ON at
        // milestone config;** set `EMAT_ALL_TABLES_EMAT=0` to revert
        // to lineitem-only Emat.
        let all_emat = std::env::var("EMAT_ALL_TABLES_EMAT")
            .ok()
            .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
            .unwrap_or(true);
        let use_emat = all_emat || *t == "lineitem" || (orders_as_emat && *t == "orders");
        if use_emat {
            // Emat for lineitem with late-mat ON (default since
            // 2026-05-16 + the misaligned-bitmap-offset fix in
            // ematix-parquet 0.4.1).
            let prov = EmatixFastParquetTableProvider::try_new(path)?;
            ctx.register_table(*t, Arc::new(prov))?;
        } else {
            // FastParquet without dict preservation by default. Probe
            // (`probe_dict_arrival`) confirms `with_dict_preservation(
            // true)` does surface Dictionary(UInt32, Utf8) at the
            // Arrow boundary — but enabling it globally regresses Q10
            // (38→77ms) and Q13 (43→109ms) because downstream operators
            // (filter on Utf8, multi-col GROUP BY mixing dict + plain
            // strings) without dict-fast-paths materialize per batch.
            // Future work: per-column or rule-driven opt-in.
            let prov = FastParquetTableProvider::try_new(path)?;
            ctx.register_table(*t, Arc::new(prov))?;
        }
    }
    Ok((ctx, bloom_rule))
}

// ---------- DuckDB ----------

fn run_duckdb(data_dir: &Path, sql: &str) -> Trial {
    use duckdb::Connection;
    let conn = match Connection::open_in_memory() {
        Ok(c) => c,
        Err(e) => return Trial::Fail(format!("duckdb open: {e}")),
    };
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        let stmt = format!(
            "CREATE VIEW {t} AS SELECT * FROM read_parquet('{}')",
            path.display()
        );
        if let Err(e) = conn.execute_batch(&stmt) {
            return Trial::Fail(format!("duckdb register {t}: {}", short(&e.to_string())));
        }
    }
    let t0 = Instant::now();
    let mut stmt = match conn.prepare(sql) {
        Ok(s) => s,
        Err(e) => {
            return Trial::Skip(format!("duckdb prepare: {}", short(&e.to_string())));
        }
    };
    let mut rows_iter = match stmt.query([]) {
        Ok(r) => r,
        Err(e) => return Trial::Fail(format!("duckdb query: {}", short(&e.to_string()))),
    };
    let mut row_count = 0usize;
    loop {
        match rows_iter.next() {
            Ok(Some(_)) => row_count += 1,
            Ok(None) => break,
            Err(e) => return Trial::Fail(format!("duckdb iter: {}", short(&e.to_string()))),
        }
    }
    let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
    Trial::Pass {
        elapsed_ms,
        rows: row_count,
    }
}

// ---------- Polars ----------

fn run_polars(data_dir: &Path, sql: &str) -> Trial {
    use polars::prelude::*;
    use polars::sql::SQLContext;
    let mut ctx = SQLContext::new();
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        let pl_path = polars::prelude::PlPath::new(path.to_str().unwrap_or_default());
        let lf = match LazyFrame::scan_parquet(pl_path, ScanArgsParquet::default()) {
            Ok(lf) => lf,
            Err(e) => {
                return Trial::Fail(format!("polars scan {t}: {}", short(&e.to_string())));
            }
        };
        ctx.register(t, lf);
    }
    let t0 = Instant::now();
    let lf = match ctx.execute(sql) {
        Ok(lf) => lf,
        Err(e) => {
            return Trial::Skip(format!("polars sql: {}", short(&e.to_string())));
        }
    };
    let df = match lf.collect() {
        Ok(df) => df,
        Err(e) => {
            return Trial::Fail(format!("polars collect: {}", short(&e.to_string())));
        }
    };
    let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
    Trial::Pass {
        elapsed_ms,
        rows: df.height(),
    }
}

// ---------- output ----------

fn write_benchmarks_md(
    out_path: &Path,
    results: &BTreeMap<u8, BTreeMap<Engine, EngineResult>>,
    trials: usize,
    warmups: usize,
    data_dir: &Path,
) -> std::io::Result<()> {
    use std::fmt::Write;
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.to_path_buf())
        .unwrap_or_default();
    let data_rel = data_dir
        .strip_prefix(&workspace)
        .map(|p| p.display().to_string())
        .unwrap_or_else(|_| data_dir.display().to_string());

    // Derive the SF label from the data directory name (sf1, sf10, sf100,
    // ...) so the report title doesn't lie when running against larger
    // factors. Falls back to the bare data_rel when the directory doesn't
    // match the `sf<N>` convention.
    let sf_label = data_rel
        .rsplit('/')
        .next()
        .and_then(|name| name.strip_prefix("sf"))
        .map(|n| format!("SF={n}"))
        .unwrap_or_else(|| data_rel.clone());

    let mut s = String::new();
    writeln!(s, "# TPC-H {sf_label} triangulation").unwrap();
    writeln!(s).unwrap();
    writeln!(
        s,
        "Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on \
         {sf_label} parquet data, {trials} timed trials after {warmups} warmups, single-machine."
    )
    .unwrap();
    writeln!(s).unwrap();
    writeln!(
        s,
        "Source: `crates/ematix-flow-core/examples/tpch_triangulation_bench.rs` — \
         feature-gated behind `--features triangulation`."
    )
    .unwrap();
    writeln!(s).unwrap();
    writeln!(s, "Data: `{}`.", data_rel).unwrap();
    writeln!(s).unwrap();
    writeln!(
        s,
        "Each cell is **median ms ± σ** across {trials} trials. \"—\" means the engine \
         couldn't parse / execute the query (dialect gap)."
    )
    .unwrap();
    writeln!(s).unwrap();
    writeln!(s, "| Query | ematix-flow | DuckDB | Polars | Best |").unwrap();
    writeln!(s, "|------:|------------:|-------:|-------:|:-----|").unwrap();
    let mut wins: BTreeMap<Engine, usize> = BTreeMap::new();
    for (q, per_engine) in results {
        #[allow(clippy::type_complexity)] // (mean, stddev, trials) summary tuple per engine
        let cells: Vec<(Engine, Option<(f64, f64, usize)>)> = Engine::all()
            .iter()
            .map(|e| (*e, per_engine.get(e).and_then(|r| r.summarize())))
            .collect();
        let render = |cell: &Option<(f64, f64, usize)>| -> String {
            match cell {
                Some((m, sd, _)) => format!("{m:.2} ± {sd:.2}"),
                None => "—".to_string(),
            }
        };
        let best = cells
            .iter()
            .filter_map(|(e, c)| c.map(|(m, _, _)| (*e, m)))
            .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let best_label = best.map(|(e, _)| e.label()).unwrap_or("—");
        if let Some((e, _)) = best {
            *wins.entry(e).or_insert(0) += 1;
        }
        writeln!(
            s,
            "| Q{q:02}  | {} | {} | {} | {} |",
            render(&cells[0].1),
            render(&cells[1].1),
            render(&cells[2].1),
            best_label
        )
        .unwrap();
    }

    writeln!(s).unwrap();
    writeln!(s, "## Wins").unwrap();
    writeln!(s).unwrap();
    for e in Engine::all() {
        let w = wins.get(e).copied().unwrap_or(0);
        writeln!(s, "- **{}**: {w}", e.label()).unwrap();
    }

    writeln!(s).unwrap();
    writeln!(s, "## Caveats").unwrap();
    writeln!(s).unwrap();
    writeln!(
        s,
        "- ematix-flow's late-materialization path (Π.10 `read_column_*_masked_into`) \
         is enabled for `lineitem`. Late-mat helps queries with a selective filter on \
         a dict/PLAIN-decodable scalar column (notably Q14); on aggregate-heavy queries \
         with low filter selectivity (Q1) it's effectively a no-op."
    )
    .unwrap();
    writeln!(
        s,
        "- Polars's SQL frontend rejects several TPC-H canonical shapes: implicit \
         cross-join in FROM, bare-column equi-joins, EXISTS subqueries, scalar-subquery \
         comparisons, `SUBSTRING ... FROM ... FOR`, HAVING against unprojected columns. \
         We ship hand-translated `q??.polars.sql` variants alongside the canonical \
         `q??.sql` files (under `examples/tpch/queries/`); the bench feeds Polars the \
         polars variant when present. Translations are semantically equivalent: \
         explicit JOIN ON with qualified columns; scalar subqueries materialized as \
         CTE + CROSS JOIN; EXISTS rewritten as semi-join via DISTINCT + INNER JOIN; \
         SUBSTRING rewritten as SUBSTR(x, start, len)."
    )
    .unwrap();
    writeln!(
        s,
        "- DuckDB runs at default settings (in-memory `read_parquet` views). \
         ematix-flow runs with `target_partitions = std::thread::available_parallelism()` \
         (override via `PARTITIONS=N`) and the InjectFusedQ1/Q3/Q5/Q6/Q12 + \
         EnableDictGroupCount physical-optimizer rules registered."
    )
    .unwrap();

    writeln!(s).unwrap();
    writeln!(s, "## Failures and dialect gaps").unwrap();
    writeln!(s).unwrap();
    let mut any_failure = false;
    for (q, per_engine) in results {
        for (engine, r) in per_engine {
            if let Some(reason) = r.failure_reason() {
                any_failure = true;
                writeln!(
                    s,
                    "- **Q{q:02} / {}**: {}",
                    engine.label(),
                    summarize_failure(&reason)
                )
                .unwrap();
            }
        }
    }
    if !any_failure {
        writeln!(s, "_None — every engine ran every query._").unwrap();
    }
    std::fs::write(out_path, s)
}

/// Trim error text to the first sentence-ish chunk so the failures
/// section reads cleanly. We don't need the full PEST trace.
fn summarize_failure(reason: &str) -> String {
    let head = reason.split(';').next().unwrap_or(reason);
    let head = head.split(',').next().unwrap_or(head);
    let head = head.trim();
    if head.len() <= 110 {
        head.to_string()
    } else {
        format!("{}…", &head[..107])
    }
}

fn short(msg: &str) -> String {
    msg.lines()
        .next()
        .unwrap_or(msg)
        .chars()
        .take(140)
        .collect()
}
