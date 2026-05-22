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
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::dedupe_aggregate_rule::DedupeAggregateForFloatDeterminism;
use ematix_flow_core::dict_aggregate_rule::EnableDictGroupCountRule;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fused_aggregate_filter_multi_agg_rule::InjectFilterMultiAggRule;
use ematix_flow_core::fused_aggregate_filter_sum_rule::InjectFilterSumRule;
use futures_util::TryStreamExt;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

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
        for &engine in Engine::all() {
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
    let ctx = match build_ematix_ctx(data_dir).await {
        Ok(c) => c,
        Err(e) => return Trial::Fail(format!("ctx build: {e}")),
    };
    let t0 = Instant::now();
    let df = match ctx.sql(sql).await {
        Ok(d) => d,
        Err(e) => return Trial::Fail(format!("plan: {}", short(&e.to_string()))),
    };
    let stream = match df.execute_stream().await {
        Ok(s) => s,
        Err(e) => return Trial::Fail(format!("execute_stream: {}", short(&e.to_string()))),
    };
    let batches: Result<Vec<_>, _> = stream.try_collect().await;
    let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
    match batches {
        Ok(b) => Trial::Pass {
            elapsed_ms,
            rows: b.iter().map(|rb| rb.num_rows()).sum(),
        },
        Err(e) => Trial::Fail(format!("collect: {}", short(&e.to_string()))),
    }
}

async fn build_ematix_ctx(data_dir: &Path) -> Result<SessionContext, Box<dyn std::error::Error>> {
    // EMAT_RULES env knob for A/B benching. Defaults to "all" (production
    // configuration). Other values enable subsets for isolating which rule
    // accounts for which slice of the geomean perf vs v0.4.0.
    //   "all" / unset           — dedupe + dict + multi + sum
    //   "none"                  — no flow rules, default DataFusion only
    //   "v040"                  — dict + multi + sum (matches v0.4.0)
    //   "dedupe"                — dedupe only
    let rules = std::env::var("EMAT_RULES").unwrap_or_else(|_| "all".to_string());
    let mut builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
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
    let state = builder.build();
    let ctx = SessionContext::new_with_state(state);
    for t in TPCH_TABLES {
        let path = data_dir
            .join(format!("{t}.parquet"))
            .to_string_lossy()
            .into_owned();
        if *t == "lineitem" {
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
    Ok(ctx)
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

    let mut s = String::new();
    writeln!(s, "# TPC-H SF=1 triangulation").unwrap();
    writeln!(s).unwrap();
    writeln!(
        s,
        "Same-process bench: ematix-flow vs DuckDB vs Polars over all 22 TPC-H queries on \
         SF=1 parquet data, {trials} timed trials after {warmups} warmups, single-machine."
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
         ematix-flow runs with `target_partitions=14` and the InjectFusedQ1/Q3/Q5/Q6/Q12 \
         + EnableDictGroupCount physical-optimizer rules registered."
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
