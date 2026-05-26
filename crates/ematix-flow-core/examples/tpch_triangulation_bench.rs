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

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
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
        let sql = std::fs::read_to_string(&sql_path)?;
        let polars_sql = std::fs::read_to_string(&polars_sql_path).ok();

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

async fn run_ematix_flow(data_dir: &Path, sql: &str) -> Trial {
    let (ctx, bloom_rule) = match build_ematix_ctx(data_dir).await {
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
    let agg_semi_on_check = std::env::var("EMAT_AGG_SEMI")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let dim_push_on_check = std::env::var("EMAT_DIM_PUSH")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let any_rewrite =
        reorder_on_check || agg_semi_on_check || dim_push_on_check || bloom_pushdown;

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
    // Σ.U Phase 1 (2026-05-26): optional agg-side LeftSemi
    // pushdown. Opt-in via `EMAT_AGG_SEMI=1`. Pushes a Filter
    // subtree as LeftSemi into an Aggregate's input so the agg only
    // sees rows whose group key survives the outer filter. Targets
    // Q17's correlated-subquery shape.
    let reorder_on = std::env::var("EMAT_REORDER")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let agg_semi_on = std::env::var("EMAT_AGG_SEMI")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    // Σ.AD (2026-05-25): pushes simple dim-join (no extra filter slot,
    // single equi-key) down adjacent to the FK table's scan inside an
    // Inner-join chain. Targets Q07's `Inner(s_nationkey = n1.n_nationkey)`
    // sitting above supplier⋈lineitem. Opt-in via `EMAT_DIM_PUSH=1`.
    let dim_push_on = std::env::var("EMAT_DIM_PUSH")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let df = if reorder_on || agg_semi_on || dim_push_on {
        match df.into_optimized_plan() {
            Ok(plan) => {
                let plan = if agg_semi_on {
                    match ematix_flow_core::agg_filter_pushdown::push_filter_into_agg(plan) {
                        Ok(p) => p,
                        Err(e) => {
                            return Trial::Fail(format!(
                                "agg_semi: {}",
                                short(&e.to_string())
                            ));
                        }
                    }
                } else {
                    plan
                };
                let plan = if dim_push_on {
                    match ematix_flow_core::dim_join_pushdown::push_dim_join_into_chain(plan) {
                        Ok(p) => p,
                        Err(e) => {
                            return Trial::Fail(format!(
                                "dim_push: {}",
                                short(&e.to_string())
                            ));
                        }
                    }
                } else {
                    plan
                };
                let plan = if reorder_on {
                    match ematix_flow_core::join_reorder::reorder_inner_joins(plan) {
                        Ok(p) => p,
                        Err(e) => {
                            return Trial::Fail(format!(
                                "reorder: {}",
                                short(&e.to_string())
                            ));
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
    let stream = match df.execute_stream().await {
        Ok(s) => s,
        Err(e) => return Trial::Fail(format!("execute_stream: {}", short(&e.to_string()))),
    };
    let batches: Result<Vec<_>, _> = stream.try_collect().await;
    let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
    // Clear the bloom slot so the next trial starts cold (matters for
    // the warmup vs timed-trial distinction; emit only inside timed
    // windows above).
    if let Some(rule) = bloom_rule.as_ref() {
        rule.set(ContextBlooms::default());
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
    let partitions: usize = std::env::var("PARTITIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(14)
        });
    let mut builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(partitions))
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
    if rh_sum_f64_enabled {
        builder = builder.with_physical_optimizer_rule(Arc::new(EnableRobinHoodSumF64Rule));
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
            builder =
                builder.with_physical_optimizer_rule(Arc::new(EnableRuntimeBloomSidebandRule {
                    min_probe_to_build_ratio: ratio,
                    allow_inner_join: allow_inner,
                    require_filtered_build,
                }));
        }
    }
    // Σ.AE: opt-in late physical-optimizer rule that surgically drops
    // redundant FilterExec conjuncts already evaluated exactly by
    // BridgeFilter. Default OFF — enable via
    // `EMAT_DROP_REDUNDANT_FILTER=1` to A/B against the Inexact
    // baseline.
    builder = ematix_flow_core::drop_redundant_filter_rule::install_drop_redundant_filter_rule(builder);
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
