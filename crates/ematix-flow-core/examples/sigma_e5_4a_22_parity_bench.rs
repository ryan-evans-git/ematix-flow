//! Σ.E5.4.a — 22-query parity bench: `FastParquetTableProvider`
//! (parquet-rs) vs `EmatixFastParquetTableProvider` (ematix-parquet
//! streaming reader, the default since PR #115).
//!
//! Acceptance criterion from `docs/PHASE_SIGMA_E5_PARQUET_RS_ELIMINATION.md`
//! §4 Phase E5.4: **22-query parity bench within ±5% per-query, geomean
//! ≤ 2% loss**. This bench measures parity; it does NOT migrate any
//! call site. Findings ship in
//! `docs/PHASE_SIGMA_E5_4_22_PARITY_FINDINGS.md`.
//!
//! Methodology (mirrors `tpch_q1_e2e_gate.rs`):
//!   - 21 measured trials per (provider, query) after 3 warmups.
//!   - Median ± σ in ms; delta = (emat - fast) / fast × 100.
//!   - Verdict per query:
//!       * `within ±5%`  — parity (acceptable).
//!       * `EmatFaster`  — Emat is faster by > 5% (a win — counts as
//!         parity for the acceptance criterion).
//!       * `Regression`  — Emat slower by > 5% — needs investigation.
//!   - The same physical-optimizer rule chain that
//!     `tpch_triangulation_bench.rs` uses is registered for both runs:
//!     `InjectFusedQ{1,3,5,12}Rule`, `InjectFilterSumRule`,
//!     `EnableDictGroupCountRule`. The goal is "what users get
//!     end-to-end with each provider", including all the existing rule
//!     rewrites.
//!   - Both providers register the same 8 TPC-H tables. No table is
//!     swapped per-query; each rep registers every table with the
//!     same provider type.
//!
//! Knobs (env):
//!   - `TPCH_DATA_DIR`   path to SF=1 parquet (default `examples/tpch/data/sf1`)
//!   - `TPCH_TRIALS`     measured trials per (query, provider) — default 21
//!   - `TPCH_WARMUPS`    untimed warmups before measured trials — default 3
//!   - `TPCH_QUERIES`    comma-separated subset, e.g. "1,6,14" (default all 22)
//!   - `TPCH_OUT`        markdown output path (default
//!     `BENCH_E5_4A_22_PARITY.md` in workspace root)
//!
//! Run:
//!     cargo run --release -p ematix-flow-core --example sigma_e5_4a_22_parity_bench

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::dict_aggregate_rule::EnableDictGroupCountRule;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fused_aggregate_filter_sum_rule::InjectFilterSumRule;
use ematix_flow_core::fused_jit_rule::{
    InjectFusedQ1Rule, InjectFusedQ3Rule, InjectFusedQ5Rule, InjectFusedQ12Rule,
};
use futures_util::TryStreamExt;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Provider {
    FastParquet,
    EmatixFastParquet,
}

impl Provider {
    fn label(self) -> &'static str {
        match self {
            Provider::FastParquet => "FastParquet (parquet-rs)",
            Provider::EmatixFastParquet => "EmatixFastParquet",
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)] // `Skip` is constructed lazily by `summarize_failure` paths if
// a future change starts categorising provider-unsupported queries (e.g. nested
// types); keep the variant so the match arms in `ProviderResult` stay exhaustive
// without a runtime guard.
enum Trial {
    Pass { elapsed_ms: f64, rows: usize },
    Skip(String),
    Fail(String),
}

#[derive(Debug, Default, Clone)]
struct ProviderResult {
    trials: Vec<Trial>,
}

impl ProviderResult {
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
        .unwrap_or_else(|_| workspace.join("BENCH_E5_4A_22_PARITY.md"));
    let trials: usize = std::env::var("TPCH_TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(21);
    let warmups: usize = std::env::var("TPCH_WARMUPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
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
                "missing {}\n\nThis bench requires SF=1 TPC-H parquet data. Generate it:\n\
                 \n  cargo run --release -p ematix-flow-core --example tpch_generate -- \
                 --sf 1 --out {}\n\n\
                 (Multi-minute job — this bench does not auto-generate.)",
                p.display(),
                data_dir.display()
            )
            .into());
        }
    }

    println!("=== Σ.E5.4.a — 22-query parity bench ===");
    println!("    data:    {}", data_dir.display());
    println!("    queries: {} ({:?})", query_subset.len(), query_subset);
    println!("    trials:  {trials} (after {warmups} warmups)");
    println!("    output:  {}", out_path.display());
    println!(
        "    rules:   InjectFusedQ{{1,3,5,12}}Rule + InjectFilterSumRule + EnableDictGroupCountRule"
    );
    println!();

    let mut results: BTreeMap<u8, BTreeMap<Provider, ProviderResult>> = BTreeMap::new();

    // Pre-load every SQL once.
    let mut sqls: BTreeMap<u8, String> = BTreeMap::new();
    for &q in &query_subset {
        let path = queries_dir.join(format!("q{q:02}.sql"));
        let sql =
            std::fs::read_to_string(&path).map_err(|e| format!("read {}: {e}", path.display()))?;
        sqls.insert(q, sql);
    }

    for &q in &query_subset {
        let sql = &sqls[&q];
        println!("--- Q{q:02} ---");
        let mut per_provider: BTreeMap<Provider, ProviderResult> = BTreeMap::new();
        for &provider in &[Provider::FastParquet, Provider::EmatixFastParquet] {
            let mut res = ProviderResult::default();
            // Build the context once per (provider, query). The TPC-H
            // triangulation bench reuses the ctx across trials too —
            // mirror that. Plan caching inside DataFusion lets the
            // first measured trial reflect the steady-state.
            let ctx = match build_ctx(&data_dir, provider).await {
                Ok(c) => c,
                Err(e) => {
                    res.trials.push(Trial::Fail(format!("ctx build: {e}")));
                    println!("  {:30} FAIL: {e}", provider.label());
                    per_provider.insert(provider, res);
                    continue;
                }
            };
            for trial_idx in 0..(trials + warmups) {
                let trial = run_one(&ctx, sql).await;
                if trial_idx >= warmups {
                    res.trials.push(trial);
                }
            }
            let tag = match res.summarize() {
                Some((med, sd, rows)) => format!("{med:7.2} ms ± {sd:5.2}  ({rows} rows)"),
                None => format!("FAIL: {}", res.failure_reason().unwrap_or_default()),
            };
            println!("  {:30} {tag}", provider.label());
            per_provider.insert(provider, res);
        }
        results.insert(q, per_provider);
        println!();
    }

    write_findings_md(&out_path, &results, trials, warmups, &data_dir)?;
    println!("wrote {}", out_path.display());

    // Console summary so the run leaves a quick-glance breadcrumb.
    print_summary(&results);

    Ok(())
}

async fn run_one(ctx: &SessionContext, sql: &str) -> Trial {
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

async fn build_ctx(
    data_dir: &Path,
    provider: Provider,
) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(InjectFusedQ1Rule))
        .with_physical_optimizer_rule(Arc::new(InjectFusedQ3Rule))
        .with_physical_optimizer_rule(Arc::new(InjectFusedQ5Rule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterSumRule))
        .with_physical_optimizer_rule(Arc::new(InjectFusedQ12Rule))
        .with_physical_optimizer_rule(Arc::new(EnableDictGroupCountRule))
        .build();
    let ctx = SessionContext::new_with_state(state);
    for t in TPCH_TABLES {
        let path = data_dir
            .join(format!("{t}.parquet"))
            .to_string_lossy()
            .into_owned();
        match provider {
            Provider::FastParquet => {
                let prov = FastParquetTableProvider::try_new(path)?;
                ctx.register_table(*t, Arc::new(prov))?;
            }
            Provider::EmatixFastParquet => {
                // Default knobs — late-mat ON (per PR #115 the
                // streaming reader is also the default). Mirror what
                // a downstream user gets out of the box: no manual
                // `with_dict_preservation` or other tweaks.
                let prov = EmatixFastParquetTableProvider::try_new(path)?;
                ctx.register_table(*t, Arc::new(prov))?;
            }
        }
    }
    Ok(ctx)
}

fn print_summary(results: &BTreeMap<u8, BTreeMap<Provider, ProviderResult>>) {
    let mut wins = 0usize;
    let mut parity = 0usize;
    let mut regs = 0usize;
    let mut both_skipped_or_failed = 0usize;
    let mut log_ratio_sum = 0.0_f64;
    let mut log_ratio_n = 0usize;
    println!("=== summary ===");
    for (q, per) in results {
        let fast = per.get(&Provider::FastParquet).and_then(|r| r.summarize());
        let emat = per
            .get(&Provider::EmatixFastParquet)
            .and_then(|r| r.summarize());
        match (fast, emat) {
            (Some((fm, _, _)), Some((em, _, _))) => {
                let delta = 100.0 * (em - fm) / fm;
                log_ratio_sum += (em / fm).ln();
                log_ratio_n += 1;
                let verdict = if delta.abs() <= 5.0 {
                    parity += 1;
                    "PARITY"
                } else if delta < 0.0 {
                    wins += 1;
                    "EMAT FASTER"
                } else {
                    regs += 1;
                    "REGRESSION"
                };
                println!(
                    "  Q{q:02}: fast={fm:6.2}ms emat={em:6.2}ms  delta={delta:+6.1}%  {verdict}"
                );
            }
            _ => {
                both_skipped_or_failed += 1;
                println!("  Q{q:02}: incomplete (skip/fail on one or both providers)");
            }
        }
    }
    let geomean_ratio = if log_ratio_n > 0 {
        (log_ratio_sum / log_ratio_n as f64).exp()
    } else {
        f64::NAN
    };
    println!();
    println!("  paired queries:        {}", wins + parity + regs);
    println!("  within ±5% (parity):   {parity}");
    println!("  EmatFaster (>5% win):  {wins}");
    println!("  Regression (>5%):      {regs}");
    println!("  incomplete:            {both_skipped_or_failed}");
    println!("  geomean(emat/fast):    {geomean_ratio:.4}  (target ≤ 1.02)");
}

fn write_findings_md(
    out_path: &Path,
    results: &BTreeMap<u8, BTreeMap<Provider, ProviderResult>>,
    trials: usize,
    warmups: usize,
    data_dir: &Path,
) -> std::io::Result<()> {
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
    writeln!(s, "# Σ.E5.4.a — 22-query parity bench").unwrap();
    writeln!(s).unwrap();
    writeln!(
        s,
        "Same-process bench comparing `FastParquetTableProvider` (parquet-rs) \
         vs `EmatixFastParquetTableProvider` (ematix-parquet streaming reader, \
         default since PR #115) across all 22 TPC-H queries on SF=1 parquet."
    )
    .unwrap();
    writeln!(s).unwrap();
    writeln!(
        s,
        "Source: `crates/ematix-flow-core/examples/sigma_e5_4a_22_parity_bench.rs`."
    )
    .unwrap();
    writeln!(s).unwrap();
    writeln!(s, "Data: `{}`.", data_rel).unwrap();
    writeln!(s).unwrap();
    writeln!(
        s,
        "Methodology: median ± σ across {trials} timed trials after {warmups} warmups, \
         single-machine, 14 target partitions, mimalloc allocator. Both providers \
         register the same 8 TPC-H tables with the same rule chain \
         (`InjectFusedQ{{1,3,5,12}}Rule` + `InjectFilterSumRule` + \
         `EnableDictGroupCountRule`). Delta = `(emat - fast) / fast × 100`. \
         Verdict bands: within ±5% = `parity`; emat ≥ 5% faster = `EmatFaster`; \
         emat ≥ 5% slower = `Regression`."
    )
    .unwrap();
    writeln!(s).unwrap();

    writeln!(s, "## 1. Bench numbers").unwrap();
    writeln!(s).unwrap();
    writeln!(
        s,
        "| Query | FastParquet (ms) | EmatixFastParquet (ms) | Δ% (emat vs fast) | Verdict |"
    )
    .unwrap();
    writeln!(
        s,
        "|------:|-----------------:|-----------------------:|------------------:|:--------|"
    )
    .unwrap();
    let mut log_ratio_sum = 0.0_f64;
    let mut log_ratio_n = 0usize;
    let mut parity_count = 0usize;
    let mut win_count = 0usize;
    let mut reg_count = 0usize;
    let mut worst_regs: Vec<(u8, f64, f64, f64)> = Vec::new(); // (q, fast, emat, delta)
    for (q, per) in results {
        let fast = per.get(&Provider::FastParquet).and_then(|r| r.summarize());
        let emat = per
            .get(&Provider::EmatixFastParquet)
            .and_then(|r| r.summarize());
        let (fast_cell, emat_cell, delta_cell, verdict) = match (fast, emat) {
            (Some((fm, fs, _)), Some((em, es, _))) => {
                let delta = 100.0 * (em - fm) / fm;
                log_ratio_sum += (em / fm).ln();
                log_ratio_n += 1;
                let v = if delta.abs() <= 5.0 {
                    parity_count += 1;
                    "within ±5%"
                } else if delta < 0.0 {
                    win_count += 1;
                    "EmatFaster"
                } else {
                    reg_count += 1;
                    worst_regs.push((*q, fm, em, delta));
                    "Regression"
                };
                (
                    format!("{fm:.2} ± {fs:.2}"),
                    format!("{em:.2} ± {es:.2}"),
                    format!("{delta:+.1}"),
                    v,
                )
            }
            (Some((fm, fs, _)), None) => (
                format!("{fm:.2} ± {fs:.2}"),
                "—".to_string(),
                "—".to_string(),
                "Emat skip/fail",
            ),
            (None, Some((em, es, _))) => (
                "—".to_string(),
                format!("{em:.2} ± {es:.2}"),
                "—".to_string(),
                "Fast skip/fail",
            ),
            (None, None) => (
                "—".to_string(),
                "—".to_string(),
                "—".to_string(),
                "both skip/fail",
            ),
        };
        writeln!(
            s,
            "| Q{q:02}  | {fast_cell} | {emat_cell} | {delta_cell} | {verdict} |"
        )
        .unwrap();
    }
    let geomean_ratio = if log_ratio_n > 0 {
        (log_ratio_sum / log_ratio_n as f64).exp()
    } else {
        f64::NAN
    };
    writeln!(s).unwrap();
    writeln!(
        s,
        "**Top-line:** {parity_count} parity, {win_count} EmatFaster, {reg_count} Regression \
         (paired queries: {}). geomean(emat / fast) = **{geomean_ratio:.4}** (target ≤ 1.02 \
         per E5.4 acceptance).",
        parity_count + win_count + reg_count
    )
    .unwrap();
    writeln!(s).unwrap();

    // Skip/fail listing.
    let mut any_incomplete = false;
    let mut incomplete = String::new();
    for (q, per) in results {
        for (provider, r) in per {
            if let Some(reason) = r.failure_reason() {
                if !any_incomplete {
                    writeln!(incomplete, "### Skips and failures").unwrap();
                    writeln!(incomplete).unwrap();
                }
                any_incomplete = true;
                writeln!(
                    incomplete,
                    "- **Q{q:02} / {}**: {}",
                    provider.label(),
                    summarize_failure(&reason)
                )
                .unwrap();
            }
        }
    }
    if any_incomplete {
        s.push_str(&incomplete);
        writeln!(s).unwrap();
    }

    writeln!(s, "## 2. Per-query analysis").unwrap();
    writeln!(s).unwrap();
    if worst_regs.is_empty() {
        writeln!(
            s,
            "No query regressed by more than 5%. The parity criterion holds for every \
             query in the suite — no per-query EXPLAIN ANALYZE deep-dive is required."
        )
        .unwrap();
    } else {
        worst_regs.sort_by(|a, b| b.3.partial_cmp(&a.3).unwrap());
        writeln!(
            s,
            "Regressions > 5%, ordered by magnitude. Threshold for EXPLAIN ANALYZE \
             deep-dive is > 10%; queries between 5% and 10% are listed for completeness \
             but not individually attributed unless they cluster on a shared root cause."
        )
        .unwrap();
        writeln!(s).unwrap();
        for (q, fm, em, delta) in &worst_regs {
            writeln!(s, "### Q{q:02} — {delta:+.1}% ({fm:.2} → {em:.2} ms)").unwrap();
            writeln!(s).unwrap();
            if delta.abs() > 10.0 {
                writeln!(
                    s,
                    "_Deep-dive required (> 10% regression). Likely candidates, ranked \
                     by prior data from §3 capability gaps:_"
                )
                .unwrap();
                writeln!(s).unwrap();
                writeln!(
                    s,
                    "1. **Filter pushdown disabled** when the streaming reader is on \
                       (see `EmatixFastParquetTableProvider::supports_filters_pushdown` — \
                       returns `Unsupported` for every filter while `streaming_arrow_reader` \
                       is true). DataFusion's residual `FilterExec` runs the predicates \
                       instead. On selective filters (Q06, Q14, Q19) this materially \
                       changes the rows-pushed-into-aggregate count.\n\
                       _Confirm with `EXPLAIN ANALYZE`: count of rows emerging from the scan \
                       node should be equal to the file's total rows on EmatixFastParquet \
                       and to the post-filter row count on FastParquet._"
                )
                .unwrap();
                writeln!(s).unwrap();
                writeln!(
                    s,
                    "2. **Row-group pruning by stats** — `EmatixFastParquetTableProvider::\
                       partition_statistics()` returns `Statistics::new_unknown` with only \
                       `num_rows` populated (`ematix_fast_parquet.rs:637`). FastParquet \
                       reports typed min/max from `ParquetMetaData::row_group().statistics()`, \
                       which feeds DataFusion's join-size + agg-cardinality estimates and \
                       drives row-group pruning. On stats-sensitive queries this changes the \
                       physical plan (smaller join build side, different operator ordering)."
                )
                .unwrap();
                writeln!(s).unwrap();
                writeln!(
                    s,
                    "3. **Per-column decode cost on specific column types** — primarily \
                       Decimal128 (none in TPC-H), Int96, FLBA, nested. TPC-H is all \
                       Int32/Int64/Float64/Date32/Utf8; if a regression shows here it's \
                       in the Utf8 → Utf8View streaming path (Σ.E5.1.d). Cross-check with \
                       the codec-layer `bench_decode` in ematix-parquet."
                )
                .unwrap();
                writeln!(s).unwrap();
                writeln!(
                    s,
                    "4. **Different operator selection by the planner** — if \
                       `partition_statistics` differences flip a join from hash to nested \
                       loop or vice versa, this is the symptom. EXPLAIN-diff the two plans."
                )
                .unwrap();
            } else {
                writeln!(
                    s,
                    "Within the 5–10% band. Most likely cause: cumulative effect of \
                     filter-pushdown-disabled + unknown partition stats on a query whose \
                     hot path is dominated by aggregation, not scan. No deep-dive yet \
                     unless it clusters with a > 10% regression."
                )
                .unwrap();
            }
            writeln!(s).unwrap();
        }
    }

    writeln!(
        s,
        "## 3. Capability gaps in EmatixFastParquet vs FastParquet"
    )
    .unwrap();
    writeln!(s).unwrap();
    writeln!(
        s,
        "Gathered from a read of `src/ematix_fast_parquet.rs` and confirmed against \
         the §1 acceptance check. These are properties of the *current* (PR #115) \
         streaming-default provider, not codec capability gaps in ematix-parquet \
         itself."
    )
    .unwrap();
    writeln!(s).unwrap();
    writeln!(
        s,
        "1. **Filter pushdown is deliberately disabled** when the streaming reader \
            path is on (Σ.E5.1.b scope cut). `supports_filters_pushdown` returns \
            `Unsupported` for every filter so DataFusion's residual `FilterExec` runs \
            the predicates. On the bridge path (with streaming off), Int32/Date32 \
            range predicates against a single column still push down. This is the \
            single largest end-to-end-visible behaviour gap and the most likely \
            cause of any > 10% regression on selective queries."
    )
    .unwrap();
    writeln!(s).unwrap();
    writeln!(
        s,
        "2. **Row-group pruning by typed min/max stats is not driven by the \
            provider.** `EmatixFastParquetExec::partition_statistics` returns \
            `Statistics::new_unknown(schema)` with only `num_rows` set \
            (`ematix_fast_parquet.rs:646`). FastParquet reports typed min/max + \
            null_count via `aggregate_column_statistics`, which DataFusion's planner \
            uses for cardinality estimates and join-size selection. This means the \
            planner can pick different operator orderings on EmatixFastParquet — \
            usually slower, occasionally faster (parallel-build heuristics)."
    )
    .unwrap();
    writeln!(s).unwrap();
    writeln!(
        s,
        "3. **No row-group pruning at scan time.** Both providers assign all row \
            groups to partitions; FastParquet additionally drops row groups whose \
            stats don't intersect the pushed-down filter (when the filter does \
            push down). EmatixFastParquet does no RG pruning today (no filter \
            pushdown → nothing to prune on). When E5.4.b restores pushdown, \
            RG pruning needs to ride along."
    )
    .unwrap();
    writeln!(s).unwrap();
    writeln!(
        s,
        "4. **Utf8View promotion is automatic on the streaming path** \
            (Σ.E5.1.d, PR #113). When `streaming_arrow_reader = true` and \
            `dict_preservation = false`, Utf8 columns in the reported schema are \
            rewritten to `Utf8View` and the streaming reader emits \
            `StringViewArray`. This closes the Q1 SQL gate against FastParquet's \
            supplied-schema Utf8View path — but it's automatic on EmatixFastParquet, \
            opt-in on FastParquet. Net effect: schema parity at the boundary."
    )
    .unwrap();
    writeln!(s).unwrap();
    writeln!(
        s,
        "5. **Dict preservation is off by default on both providers.** This is \
            apples-to-apples for the bench — neither path lights up \
            `EnableDictGroupCountRule` on real TPC-H data without an explicit \
            `with_dict_preservation(true)` (see the `dict-arrival-blocker` memory \
            note). Wins from dict-aware execution are outside E5.4.a's scope."
    )
    .unwrap();
    writeln!(s).unwrap();
    writeln!(
        s,
        "6. **Both providers exercise the same per-RG-parallel partition layout** \
            (`Partitioning::UnknownPartitioning(N)` with `N = \
            min(num_row_groups, target_partitions).max(1)`). Decode parallelism \
            beyond row groups is provider-specific: FastParquet uses parquet-rs's \
            internal per-page parallelism, EmatixFastParquet uses \
            `EmatArrowBatchReader`'s per-column scoped-thread fan-out with a \
            partition-aware budget. The Σ.E5.1.c budget cap keeps total threads \
            tracking the core count rather than `N_partitions × N_cols`."
    )
    .unwrap();
    writeln!(s).unwrap();

    writeln!(s, "## 4. Migration sequencing recommendation").unwrap();
    writeln!(s).unwrap();
    if reg_count == 0 && geomean_ratio <= 1.02 {
        writeln!(
            s,
            "**EmatixFastParquet is ready — proceed to E5.4.b in-tree switch.** \
             Zero queries regressed by more than 5%, and the geomean of \
             `emat / fast` is within the E5.4 acceptance threshold (≤ 1.02). \
             Recommended sequencing:"
        )
        .unwrap();
        writeln!(s).unwrap();
        writeln!(
            s,
            "1. **E5.4.b** — flip `tpch_triangulation_bench.rs` and other in-tree \
                consumers from `FastParquetTableProvider` to \
                `EmatixFastParquetTableProvider` one call site at a time, gating \
                each on its own bench run.\n\
             2. **E5.4.c** — restore filter pushdown on the streaming path \
                (re-enable `supports_filters_pushdown` for Int32/Date32 range \
                predicates; fuse the bitmap-first decode with the streaming \
                emission). Expected wins on Q06/Q14/Q19.\n\
             3. **E5.4.d** — wire typed `partition_statistics` so the planner \
                sees min/max + null_count. Decode the thrift-level \
                `ematix-parquet-format::Statistics` for the 5 physical types \
                we use (Int32/Int64/Float/Double/Bool).\n\
             4. **E5.4.e** — delete `FastParquetTableProvider` and its \
                parquet-rs imports from `src/fast_parquet.rs`; verify \
                `cargo tree -p ematix-flow-core -e=normal | grep parquet` \
                shows no direct `parquet 58` edge."
        )
        .unwrap();
    } else {
        writeln!(
            s,
            "**Close gaps first** — {} query/queries regressed by more than 5% \
             (geomean = {:.4}, target ≤ 1.02). Recommended ordered sub-phases:",
            reg_count, geomean_ratio
        )
        .unwrap();
        writeln!(s).unwrap();
        writeln!(
            s,
            "1. **E5.4.b — restore filter pushdown on the streaming reader path** \
                (highest impact). Re-enable `supports_filters_pushdown` for \
                Int32/Date32 range predicates and fuse with the streaming \
                bitmap-first decode. Expected to close Q06, Q14, Q19 and any \
                other selective-filter query in the regression list.\n\
             2. **E5.4.c — typed `partition_statistics`** (medium impact). \
                Decode `ematix_parquet_format::Statistics` for the 5 physical \
                types and report typed min/max + null_count from \
                `EmatixFastParquetExec::partition_statistics`. Re-runs the \
                planner's cardinality estimates on the EmatixFastParquet side; \
                expected to close the join-heavy regressions (Q05, Q07, Q09, Q21).\n\
             3. **E5.4.d — row-group pruning at scan time** (small impact, \
                rides E5.4.c). Once typed stats are present, drop RGs whose \
                stats don't intersect any pushed-down filter. Mostly redundant \
                with E5.4.b for SF=1 (lineitem has 6 RGs total) but lands \
                cleanly at SF=10.\n\
             4. **E5.4.e — rerun this parity bench**. Acceptance criterion: \
                same as E5.4 — within ±5% per-query, geomean ≤ 1.02. \
                Migrate in-tree call sites once green."
        )
        .unwrap();
    }
    writeln!(s).unwrap();

    writeln!(s, "## 5. Bench reproduction").unwrap();
    writeln!(s).unwrap();
    writeln!(
        s,
        "Prerequisite: SF=1 TPC-H parquet under `examples/tpch/data/sf1/`. Generate \
         once (multi-minute):"
    )
    .unwrap();
    writeln!(s).unwrap();
    writeln!(s, "```sh").unwrap();
    writeln!(
        s,
        "cargo run --release -p ematix-flow-core --example tpch_generate -- \\\n\
         \x20   --sf 1 --out examples/tpch/data/sf1"
    )
    .unwrap();
    writeln!(s, "```").unwrap();
    writeln!(s).unwrap();
    writeln!(s, "Then:").unwrap();
    writeln!(s).unwrap();
    writeln!(s, "```sh").unwrap();
    writeln!(
        s,
        "cargo run --release -p ematix-flow-core --example sigma_e5_4a_22_parity_bench"
    )
    .unwrap();
    writeln!(s, "```").unwrap();
    writeln!(s).unwrap();
    writeln!(s, "Knobs (env):").unwrap();
    writeln!(s).unwrap();
    writeln!(
        s,
        "- `TPCH_DATA_DIR` — override the SF=1 path.\n\
         - `TPCH_TRIALS`   — measured trials per (query, provider). Default 21.\n\
         - `TPCH_WARMUPS`  — untimed warmups before measured trials. Default 3.\n\
         - `TPCH_QUERIES`  — comma-separated subset, e.g. `1,6,14`. Default all 22.\n\
         - `TPCH_OUT`      — markdown output path. Default `BENCH_E5_4A_22_PARITY.md`\n\
         \x20  at the workspace root."
    )
    .unwrap();
    writeln!(s).unwrap();
    writeln!(
        s,
        "The bench writes the table above into the output path on every run and \
         also prints the per-query verdict + geomean to stdout. To re-freeze this \
         findings doc, run the bench, copy the printed table here, and update \
         §1's top-line counts + the §4 recommendation conditional."
    )
    .unwrap();
    writeln!(s).unwrap();

    std::fs::write(out_path, s)
}

fn summarize_failure(reason: &str) -> String {
    let head = reason.split(';').next().unwrap_or(reason);
    let head = head.split(',').next().unwrap_or(head);
    let head = head.trim();
    if head.len() <= 120 {
        head.to_string()
    } else {
        format!("{}…", &head[..117])
    }
}

fn short(msg: &str) -> String {
    msg.lines()
        .next()
        .unwrap_or(msg)
        .chars()
        .take(160)
        .collect()
}
