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
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::inbloom_scan_pushdown_rule::EnableInBloomScanPushdownRule;
use ematix_flow_core::local_bloom_emitter::{LocalBloomOptions, emit_build_side_blooms_local};
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

/// Σ.AJ.1 Lever C bench-harness fix (2026-05-27): when the opt-in
/// EMAT_REORDER pre-plan walker is enabled, the trial code path
/// bypasses the plan cache for ALL 22 queries — even those where the
/// walker doesn't actually rewrite anything. That uniformly regresses
/// non-matching queries by ~3-8% (plan cache miss every trial),
/// masking the matching queries' wins.
///
/// `FIRES_CACHE` memoizes per-(rule, SQL) "does this rule actually
/// change the plan?" so subsequent trials short-circuit. First trial
/// pays one logical-opt + one TreeNode walk per enabled rule; warmup
/// discards this. Only used to gate the `any_rewrite` flag.
///
/// (2026-07-02 hardening: the agg_semi / dim_push / q20_semi / q05_semi
/// variants are gone — those walkers now run inside the session's
/// production `FlowQueryPlanner`, so cached plan templates already
/// carry them and no bypass is needed. Only the legacy EMAT_REORDER
/// manual walker still bypasses.)
type FiresKey = (RewriteRule, String);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum RewriteRule {
    Reorder,
    ReorderShapeGated,
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
        RewriteRule::Reorder => {
            ematix_flow_core::join_reorder::reorder_inner_joins(optimized.clone())
        }
        RewriteRule::ReorderShapeGated => {
            ematix_flow_core::join_reorder::reorder_inner_joins_shape_gated(optimized.clone())
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
    // Legacy lever-name compatibility (2026-07-02 hardening): the Q20
    // transitive semi-pushdown used to be applied manually here, gated
    // on `EMAT_Q20_TRANSITIVE_SEMI`; it now runs inside the production
    // `FlowQueryPlanner`, whose gate is spelled `EMAT_Q20_SEMI`.
    // Forward the historical spelling so documented A/B invocations
    // keep working. SAFETY: first statement of main — no query has
    // been planned yet, so no other thread is reading EMAT_* env
    // (same coordination argument as the EMAT_BATCH_SIZE writes below).
    if let (Ok(v), Err(_)) = (
        std::env::var("EMAT_Q20_TRANSITIVE_SEMI"),
        std::env::var("EMAT_Q20_SEMI"),
    ) {
        unsafe { std::env::set_var("EMAT_Q20_SEMI", v) };
    }
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
    // Σ.AI.3: single-invocation numbers under-estimate cross-invocation
    // variance by 5-10x and reuse in-process session state that the
    // competitor engines don't get. Verdict-grade comparisons must go
    // through the strict wrappers.
    println!(
        "    NOTE: single-invocation output is SMOKE-GRADE only. For win/loss\n\
         \x20   claims use scripts/bench/strict_22q.sh / strict_ab.sh /\n\
         \x20   strict_throughput.sh (see scripts/bench/README.md)."
    );
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
        // The production pre-plan walkers (agg_semi / dim_push /
        // q20_semi / transitive dim-semi / shape-gated reorder) run
        // inside the session's `FlowQueryPlanner` at physical-planning
        // time (2026-07-02 hardening), so observed cardinalities match
        // what the bench actually executes without manual
        // re-application. Σ.U agg_semi is the critical one: without it
        // Q17's inner AVG agg sees 60M rows / 2M groups instead of the
        // ~30K / ~200 the routed bench gets.
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
/// partition count and returns wall-ms.
async fn time_one(data_dir: &Path, sql: &str, partitions: Option<usize>) -> Option<f64> {
    let (ctx, bloom_rule) = build_ematix_ctx(data_dir, partitions).await.ok()?;
    // Pre-plan walkers run inside the session's FlowQueryPlanner
    // (2026-07-02 hardening) — race timings reflect what the bench
    // actually executes without manual re-application.
    let df = ctx.sql(sql).await.ok()?;
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
    // Scalar-final-agg partition oversubscription (morsel-engine
    // down-payment, default ON; opt-out EMAT_SCALAR_AGG_BOOST=0): since
    // the 2026-07-02 hardening the bench session installs the production
    // `FlowQueryPlanner` (via `preset::with_optimizer_rules_overridden`),
    // which applies the 2× boost natively at physical-planning time —
    // the old bench-side mirror (`scalar_agg_partitions_lookup`) was
    // removed so the boost isn't applied twice.
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
    // 2026-07-02 hardening: the production pre-plan walkers (agg_semi /
    // dim_push / q20_semi / transitive dim-semi / shape-gated reorder)
    // now run inside the session's `FlowQueryPlanner` — the SAME code
    // path production users execute — instead of being re-applied by
    // hand here. Their A/B levers are the planner's own env gates
    // (EMAT_AGG_SEMI / EMAT_DIM_PUSH / EMAT_Q20_SEMI /
    // EMAT_TRANSITIVE_DIM_SEMI / EMAT_REORDER_QP, all default ON;
    // main() forwards the legacy EMAT_Q20_TRANSITIVE_SEMI spelling).
    // The plan cache's `get_or_plan` plans through
    // `SessionState::create_physical_plan`, which invokes the planner,
    // so cached templates already carry the rewrites — the old
    // per-rule cache-bypass machinery is no longer needed.
    //
    // Σ.T Phase 3 legacy lever: `EMAT_REORDER=1` additionally
    // pre-applies the PERMISSIVE `reorder_inner_joins` walker (the
    // production planner only ever runs the shape-gated variant, via
    // EMAT_REORDER_QP). Kept for Σ.T-style experiments; combine with
    // `EMAT_REORDER_QP=0` to isolate it from the planner's reorder.
    let reorder_on_check = std::env::var("EMAT_REORDER")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let reorder_shape_gated_check = std::env::var("EMAT_REORDER_SHAPE_GATED")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    // Plan-cache bypass (Σ.AJ.1 Lever C harness fix): only bypass for
    // queries where the manual rewrite actually changes the plan —
    // memoized per SQL via `rewrite_fires_for_sql`.
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
    let any_rewrite = reorder_actually_fires || bloom_pushdown;

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
    // Σ.T Phase 3 (2026-05-25) legacy lever: optional PERMISSIVE
    // join-reorder pre-plan walker, opt-in via `EMAT_REORDER=1` (the
    // production planner only ships the shape-gated variant). Runs
    // DataFusion's logical optimizer to expand `Cross Join + Filter` →
    // Inner Join, applies the connectivity-aware cardinality-minimizing
    // greedy reorder, then hands the rewritten plan to
    // `execute_logical_plan` — whose physical planning re-runs the
    // session's FlowQueryPlanner pipeline on top. Combine with
    // `EMAT_REORDER_QP=0` to isolate the permissive reorder from the
    // planner's own shape-gated one.
    let df = if reorder_actually_fires {
        match df.into_optimized_plan() {
            Ok(plan) => {
                if std::env::var_os("EMAT_DUMP_LOGICAL").is_some() {
                    eprintln!(
                        "=== Q timed logical plan (pre EMAT_REORDER walker) ===\n{}",
                        plan.display_indent()
                    );
                }
                let plan = {
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
    // Concurrency-aware partitions (campaign-2026-07-01 §3): explicit
    // `PARTITIONS=N` keeps absolute precedence (back-compat with every
    // recorded bench invocation); otherwise resolve through the
    // library's `EMAT_TARGET_PARTITIONS` tri-state (`=N` force, `=0`
    // legacy available_parallelism, unset = AUTO cross-process
    // sensing — a solo process still resolves to full cores). The
    // harness resolves HERE, so the preset's own auto hook is disabled
    // below (`auto_target_partitions: false`) and nothing can
    // second-guess the PARTITIONS override.
    let session_partitions: usize = std::env::var("PARTITIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n >= 1)
        .unwrap_or_else(ematix_flow_core::partition_registry::resolve_target_partitions);
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
    // Σ.Q.L10: **Default ON at the milestone config (0.738 / 17 wins
    // SF=10);** set `EMAT_PUSH_SEMI=0` to disable for A/B benching.
    let push_semi_enabled = std::env::var("EMAT_PUSH_SEMI")
        .ok()
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true);
    // 2026-07-02 hardening (docs/PERF_Q18.md § Hardening): the chain is
    // now assembled by the PRODUCTION preset via
    // `preset::with_optimizer_rules_overridden` — this harness only maps
    // its documented EMAT_* A/B levers onto the preset's explicit, named
    // `HarnessOverrides`. With no lever env set, the session is
    // EXACTLY the production preset session (pinned by
    // `bench_preset_parity_tests` below + the preset's own
    // `production_chain_matches_pinned_names`). This closes the Σ.V /
    // RANGE.AGG / Q05-walker class of copy-drift for good — and means
    // the bench now ALSO ships the production `FlowQueryPlanner`
    // (agg_semi → dim_push → q20_semi → transitive dim-semi →
    // shape-gated reorder + scalar-agg boost), which the old
    // hand-rolled chain only partially mirrored (the Q05 transitive
    // dim-semi + shape-gated reorder were missing by default —
    // bench-measured Q05 plans lacked the pass-1 L9 lineitem wrap
    // production produces).
    let overrides = ematix_flow_core::preset::HarnessOverrides {
        // Partitions already resolved above (PARTITIONS > tri-state);
        // the preset must not re-resolve at a different instant.
        auto_target_partitions: false,
        // EMAT_RULES subsets ("all"/"none"/"v040"/"dedupe"/"swap").
        dedupe_aggregate: matches!(rules.as_str(), "all" | "dedupe"),
        inject_fused_rules: matches!(rules.as_str(), "all" | "v040"),
        // Σ.Q L2: EMAT_SWAP_SEMI overrides; else ON for "all"/"swap".
        swap_semi_join_build: std::env::var("EMAT_SWAP_SEMI")
            .ok()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or_else(|| matches!(rules.as_str(), "all" | "swap")),
        // REV.3: EMAT_FORCE_COLLECT_LEFT=0 for A/B isolation.
        force_collect_left: std::env::var("EMAT_FORCE_COLLECT_LEFT")
            .ok()
            .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
            .unwrap_or(true),
        // Σ.JS.1 stays installed; the rule self-gates on
        // EMAT_JOIN_SIDE_FIX (default ON), so A/B via the env var.
        sampled_join_side: true,
        // Σ.SP Phase 1b stays installed; the rule self-gates on
        // EMAT_GRACE_JOIN + an honest oversize estimate.
        grace_join: true,
        // RANGE.AGG stays installed; A/B via its own EMAT_RANGE_AGG gate.
        clustered_single_phase_agg: true,
        // Σ.Q.L10: EMAT_PUSH_SEMI=0 to disable for A/B benching.
        push_down_left_semi: push_semi_enabled,
        // Σ.Q.M producer must precede the Σ.Q.L10 consumer, so it is
        // only meaningful when push-semi is on (historical bench gate).
        synthetic_left_semi: push_semi_enabled
            && std::env::var("EMAT_SYNTHETIC_LEFT_SEMI")
                .ok()
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
        // Σ.Q.L1b: EMAT_RH_SUM_F64=0 to disable for A/B.
        robin_hood_sum_f64: std::env::var("EMAT_RH_SUM_F64")
            .ok()
            .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
            .unwrap_or(true),
        // REV.8: EMAT_SINGLE_PASS_RADIX=1 pre-empts the rh_sum rewrite.
        single_pass_radix_replaces_rh_sum: std::env::var("EMAT_SINGLE_PASS_RADIX")
            .ok()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false),
        // REV.18c opt-in RobinHood COUNT/AVG kernels.
        robin_hood_count: std::env::var("EMAT_RH_COUNT").ok().as_deref() == Some("1"),
        robin_hood_avg: std::env::var("EMAT_RH_AVG").ok().as_deref() == Some("1"),
        // Σ.AN.1 opt-in partition routing.
        agg_partition_boost: std::env::var("EMAT_AGG_PARTITION_BOOST")
            .ok()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false),
        // Σ.Q.L9: EMAT_RT_BLOOM_SIDEBAND=0 to disable for A/B; the
        // milestone fields (ratio=1024 / inner=on / filtered-build=on)
        // are overridable via EMAT_RT_BLOOM_RATIO /
        // EMAT_RT_BLOOM_INNER_JOIN / EMAT_L9_REQUIRE_FILTERED_BUILD.
        runtime_bloom_sideband: std::env::var("EMAT_RT_BLOOM_SIDEBAND")
            .ok()
            .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
            .unwrap_or(true),
        l9_min_probe_to_build_ratio: std::env::var("EMAT_RT_BLOOM_RATIO")
            .ok()
            .and_then(|s| s.parse().ok()),
        l9_allow_inner_join: std::env::var("EMAT_RT_BLOOM_INNER_JOIN")
            .ok()
            .map(|v| v != "0" && !v.eq_ignore_ascii_case("false")),
        l9_require_filtered_build: std::env::var("EMAT_L9_REQUIRE_FILTERED_BUILD")
            .ok()
            .map(|v| v != "0" && !v.eq_ignore_ascii_case("false")),
        // Σ.S.B legacy stem-fanout cascade — EMAT_L9_CASCADE_STEM=1
        // swaps the base L9 rule out (Σ.Q05.CHAIN repurposed
        // EMAT_L9_CASCADE as the tri-state gate INSIDE the base rule).
        l9_cascade_stem_max_extras: ematix_flow_core::flags::opt_in("EMAT_L9_CASCADE_STEM").then(
            || {
                std::env::var("EMAT_L9_CASCADE_MAX")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(4)
            },
        ),
        l9_stock_defaults: false,
        // Σ.AE: the installer itself no-ops unless
        // EMAT_DROP_REDUNDANT_FILTER=1 (historical bench behavior).
        drop_redundant_filter: true,
        // Σ.Q.L4′: install the in-scan bloom pushdown rule with an
        // empty shared slot; `run_ematix_flow` swaps the contents in
        // per query when EMAT_BLOOM_PUSHDOWN=1.
        in_bloom_scan_pushdown: std::env::var("EMAT_BLOOM_PUSHDOWN")
            .ok()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false),
        swap_emat_hash_join: false,
        flow_query_planner: true,
    };
    let (builder, handles) = ematix_flow_core::preset::with_optimizer_rules_overridden(
        SessionStateBuilder::new()
            .with_config(session_config)
            .with_default_features(),
        &overrides,
    );
    let bloom_rule = handles.bloom_pushdown;
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
        "> **SMOKE-GRADE OUTPUT.** Single-invocation, same-process numbers: \
         cross-invocation variance is 5-10x what the ± column suggests, and \
         ematix reuses in-process session state competitors don't get. \
         Verdict-grade win/loss claims must come from the strict protocol \
         (`scripts/bench/README.md`)."
    )
    .unwrap();
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
         ematix-flow's `target_partitions` resolves as: explicit `PARTITIONS=N`, \
         else the `EMAT_TARGET_PARTITIONS` tri-state (`=N` force, `=0` legacy \
         `available_parallelism()`, unset = AUTO cross-process sensing — solo \
         processes get full cores). The InjectFusedQ1/Q3/Q5/Q6/Q12 + \
         EnableDictGroupCount physical-optimizer rules are registered."
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

/// Bench↔preset parity pins (2026-07-02, perf/q18-sf100-dig; extended
/// same day by the chain-unification hardening).
///
/// History: the strict harness used to construct its rule chain
/// manually instead of calling the preset, and the duplication bit
/// once in each direction — Σ.V (2026-05-26) found three rules
/// default-on in the bench but missing from the preset; the Q18 SF=100
/// dig found the inverse (RANGE.AGG preset-default since f15d2fc but
/// never installed here, corrupting a campaign's Q18 SF=100 verdicts);
/// the Q05 cascade work found the bench still missing the production
/// FlowQueryPlanner walkers (transitive dim-semi + shape-gated
/// reorder). `build_ematix_ctx` now goes through
/// `preset::with_optimizer_rules_overridden`, and this module pins
/// FULL name-set equality (modulo the documented harness-only
/// allowlist) so any re-fork trips immediately.
///
/// Run: `cargo test -p ematix-flow-core --example
/// tpch_triangulation_bench --features triangulation`.
#[cfg(test)]
mod bench_preset_parity_tests {
    use super::*;

    /// Harness-only diagnostic rules the bench may install ON TOP of
    /// the production chain — every one is opt-in via an env lever and
    /// therefore ABSENT at default env (which is how these tests run).
    /// Anything else appearing in the bench chain but not the preset
    /// chain (or vice versa) is a parity bug.
    const HARNESS_ONLY_PHYSICAL_RULES: &[&str] = &[
        // EMAT_SINGLE_PASS_RADIX=1 (REPLACES the rh_sum rewrite)
        "ematix_flow_enable_single_pass_radix_sum",
        // EMAT_L9_CASCADE_STEM=1 (REPLACES the base L9 sideband rule)
        "ematix_flow_enable_cascading_bloom",
        // EMAT_RH_COUNT=1 / EMAT_RH_AVG=1
        "ematix_flow_enable_robin_hood_agg",
        "ematix_flow_enable_robin_hood_avg_f64",
        // EMAT_AGG_PARTITION_BOOST=1
        "AggPartitionBoostRule",
        // EMAT_DROP_REDUNDANT_FILTER=1
        "ematix_flow_drop_redundant_bridge_filter",
        // EMAT_BLOOM_PUSHDOWN=1
        "EnableInBloomScanPushdownRule",
    ];
    const HARNESS_ONLY_LOGICAL_RULES: &[&str] = &[
        // EMAT_SYNTHETIC_LEFT_SEMI=1
        "ematix_flow_synthetic_left_semi",
    ];

    /// THE drift tripwire: the strict bench session's ematix rule chain
    /// (physical + logical, in registration order) must EQUAL the
    /// production preset's, modulo the documented harness-only
    /// allowlist. Adding a rule to `preset.rs` without going through
    /// `with_optimizer_rules_overridden` — or re-forking the bench
    /// chain — fails here.
    #[tokio::test(flavor = "multi_thread")]
    async fn bench_rule_chain_name_set_equals_production_preset() {
        use ematix_flow_core::preset;

        let dir = write_fixture_dir();
        let (ctx, _bloom) = build_ematix_ctx(&dir, Some(4)).await.unwrap();
        let bench_state = ctx.state();
        let preset_state = preset::with_optimizer_rules(
            SessionStateBuilder::new()
                .with_config(SessionConfig::new().with_target_partitions(4))
                .with_default_features(),
        )
        .build();

        let (bench_phys, bench_logi) = preset::ematix_rule_names(&bench_state);
        let (preset_phys, preset_logi) = preset::ematix_rule_names(&preset_state);
        let bench_phys: Vec<String> = bench_phys
            .into_iter()
            .filter(|n| !HARNESS_ONLY_PHYSICAL_RULES.contains(&n.as_str()))
            .collect();
        let bench_logi: Vec<String> = bench_logi
            .into_iter()
            .filter(|n| !HARNESS_ONLY_LOGICAL_RULES.contains(&n.as_str()))
            .collect();
        assert_eq!(
            bench_phys, preset_phys,
            "strict bench PHYSICAL rule chain diverged from the production \
             preset (allowlist-filtered). If you changed the preset chain, \
             it flows through with_optimizer_rules_overridden automatically \
             — this failing means a rule was registered OUTSIDE the unified \
             constructor, or a harness-only rule is missing from the \
             documented allowlist."
        );
        assert_eq!(
            bench_logi, preset_logi,
            "strict bench LOGICAL rule chain diverged from the production preset"
        );
        // The preset also pins the chain content itself; re-assert here
        // so a bench-session drift can never pass silently even if the
        // preset test module is skipped.
        assert_eq!(preset_phys, preset::PRODUCTION_PHYSICAL_RULE_NAMES);
        assert_eq!(preset_logi, preset::PRODUCTION_LOGICAL_RULE_NAMES);
        // Query planner parity: the bench must ship the production
        // FlowQueryPlanner (the Q05 transitive-dim-semi / shape-gated
        // reorder / scalar-agg-boost carrier).
        assert_eq!(
            format!("{:?}", bench_state.query_planner()),
            format!("{:?}", preset_state.query_planner()),
            "bench session's query planner differs from the production preset's"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Plan-identity pin: with identical providers + partitions, the
    /// bench session and a pure-preset session must produce
    /// byte-identical physical plans for the fixture's Q18 subquery
    /// shape (covers rule CONFIG parity — e.g. the L9 milestone fields
    /// — that name-set equality alone can't see).
    #[tokio::test(flavor = "multi_thread")]
    async fn bench_fixture_plan_identical_to_preset_session() {
        use ematix_flow_core::preset;

        let dir = write_fixture_dir();
        let sql = "select l_orderkey from lineitem \
                   group by l_orderkey having sum(l_quantity) > 300";

        let (bench_ctx, _bloom) = build_ematix_ctx(&dir, Some(4)).await.unwrap();

        let preset_ctx = SessionContext::new_with_state(
            preset::with_optimizer_rules(
                SessionStateBuilder::new()
                    .with_config(SessionConfig::new().with_target_partitions(4))
                    .with_default_features(),
            )
            .build(),
        );
        for t in TPCH_TABLES {
            let path = dir
                .join(format!("{t}.parquet"))
                .to_string_lossy()
                .into_owned();
            let prov = EmatixFastParquetTableProvider::try_new(path).unwrap();
            preset_ctx.register_table(*t, Arc::new(prov)).unwrap();
        }

        let render = |ctx: &SessionContext| {
            let ctx = ctx.clone();
            let sql = sql.to_string();
            async move {
                let plan = ctx
                    .sql(&sql)
                    .await
                    .unwrap()
                    .create_physical_plan()
                    .await
                    .unwrap();
                datafusion::physical_plan::displayable(plan.as_ref())
                    .indent(true)
                    .to_string()
            }
        };
        let bench_plan = render(&bench_ctx).await;
        let preset_plan = render(&preset_ctx).await;
        assert_eq!(
            bench_plan, preset_plan,
            "bench session plans the fixture query differently from the \
             production preset session — a rule CONFIG (not membership) \
             diverged.\nbench:\n{bench_plan}\npreset:\n{preset_plan}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Write one tiny valid parquet and copy it to every TPC-H table
    /// name so `build_ematix_ctx` can register a full catalog. The
    /// lineitem file is Q18-shaped and physically clustered on
    /// `l_orderkey` with strict row-group key gaps (8 RGs × 125 rows,
    /// 5 rows/key ⇒ every RG boundary is a strict gap), so RANGE.AGG
    /// is eligible to fire on the Q18 inner-subquery shape.
    fn write_fixture_dir() -> std::path::PathBuf {
        use arrow_array::{Float64Array, Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::parquet::file::properties::WriterProperties;

        let dir = std::env::temp_dir().join(format!(
            "tpch_bench_parity_{}_{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();

        let n = 1000i64;
        let keys: Vec<i64> = (0..n).map(|i| i / 5).collect();
        let quantities: Vec<f64> = (0..n).map(|_| 1.0).collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_quantity", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Float64Array::from(quantities)),
            ],
        )
        .unwrap();
        let lineitem = dir.join("lineitem.parquet");
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(125))
            .build();
        let f = std::fs::File::create(&lineitem).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        for t in TPCH_TABLES {
            if *t != "lineitem" {
                std::fs::copy(&lineitem, dir.join(format!("{t}.parquet"))).unwrap();
            }
        }
        dir
    }

    /// Parity pin: the strict bench session must carry the production
    /// preset's RANGE.AGG rule. Guards against the Q18 SF=100 harness
    /// artifact recurring (and its inverse — the rule leaving the
    /// preset without leaving here would trip the preset's own docs).
    #[tokio::test(flavor = "multi_thread")]
    async fn strict_bench_session_carries_range_agg_rule() {
        let dir = write_fixture_dir();
        let (ctx, _bloom) = build_ematix_ctx(&dir, None).await.unwrap();
        let names: Vec<String> = ctx
            .state()
            .physical_optimizers()
            .iter()
            .map(|r| r.name().to_string())
            .collect();
        assert!(
            names
                .iter()
                .any(|n| n == "ematix_flow_clustered_single_phase_agg"),
            "strict bench session is missing the production-preset \
             ClusteredSinglePhaseAggRule (RANGE.AGG, preset.rs since \
             f15d2fc) — the harness would re-measure Q18 SF=100 on a \
             plan production never runs. Installed rules: {names:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Plan-diff pin: the Q18 inner-subquery shape (single-key GROUP
    /// BY over cluster-keyed lineitem + HAVING) must plan as ONE
    /// SinglePartitioned aggregate through the bench session — the
    /// same rewrite `preset::with_optimizer_rules` produces — instead
    /// of the Partial → hash-shuffle → FinalPartitioned triple.
    #[tokio::test(flavor = "multi_thread")]
    async fn q18_subquery_shape_plans_single_phase_through_bench_session() {
        let dir = write_fixture_dir();
        let (ctx, _bloom) = build_ematix_ctx(&dir, None).await.unwrap();
        let sql = "select l_orderkey from lineitem \
                   group by l_orderkey having sum(l_quantity) > 300";
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let rendered = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();
        assert!(
            rendered.contains("SinglePartitioned"),
            "Q18 subquery shape must take the RANGE.AGG single-phase \
             plan through the strict bench session (clustered \
             l_orderkey, strict RG gaps).\nPlan:\n{rendered}"
        );
        assert!(
            !rendered.contains("mode=FinalPartitioned"),
            "two-phase agg survived — RANGE.AGG did not fire.\n\
             Plan:\n{rendered}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
