//! Σ.K.2 gate — 22-query TPC-H bench measuring whether the
//! `dict_routing::analyse_dict_arrival_for_sql` decision rule produces
//! a net geomean improvement vs the default (Utf8View everywhere).
//!
//! For each query 1..=22:
//!   1. Load SQL from `examples/tpch/queries/qNN.sql`
//!   2. Build an analysis context (no dict) → ask `analyse_dict_arrival_for_sql`
//!      for per-table verdict
//!   3. Build the OFF context (everything Utf8View, current default)
//!   4. Build the ROUTED context with each table's dict-preservation
//!      set per the verdict
//!   5. Run each 3 reps, take min (more stable than median on small N)
//!   6. Report per-query delta + overall geomean
//!
//! Pass criteria:
//!   - geomean(ROUTED / OFF) ≤ 0.985 (≥1.5% improvement over the 3pp
//!     run-to-run noise floor, per [[sigma-e5-geomean-ceiling]]).
//!   - No single query regresses >5%. (Acceptable noise band.)
//!
//! Run:
//!   cargo run --release -p ematix-flow-core --example dict_arrival_22q_gate

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::dict_routing::analyse_dict_arrival_with_sizes;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use futures_util::TryStreamExt;

/// Read row count by running `SELECT COUNT(*)` on the table — cheap
/// when the table is already registered with our parquet provider
/// (provider reads the footer for stats).
async fn count_rows(ctx: &SessionContext, table: &str) -> Option<u64> {
    let df = ctx
        .sql(&format!("SELECT COUNT(*) FROM {table}"))
        .await
        .ok()?;
    let batches = df.collect().await.ok()?;
    let batch = batches.first()?;
    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()?;
    Some(arr.value(0) as u64)
}

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];
const REPS: usize = 3;

fn target_partitions() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8)
}

fn build_ctx(dir: &str, dict_overrides: &HashMap<String, bool>) -> SessionContext {
    let cfg = SessionConfig::new().with_target_partitions(target_partitions());
    let ctx = SessionContext::new_with_config(cfg);
    for t in TPCH_TABLES {
        let path = format!("{dir}/{t}.parquet");
        if !std::path::Path::new(&path).exists() {
            continue;
        }
        let want_dict = dict_overrides.get(*t).copied().unwrap_or(false);
        let prov = if want_dict {
            EmatixFastParquetTableProvider::try_new(&path)
                .unwrap()
                .with_dict_preservation(true)
        } else {
            EmatixFastParquetTableProvider::try_new(&path).unwrap()
        };
        ctx.register_table(*t, Arc::new(prov)).unwrap();
    }
    ctx
}

async fn run_one(ctx: &SessionContext, sql: &str) -> Result<Duration, String> {
    let t = Instant::now();
    let df = ctx.sql(sql).await.map_err(|e| e.to_string())?;
    let plan = df.create_physical_plan().await.map_err(|e| e.to_string())?;
    let mut n = 0usize;
    for p in 0..plan.output_partitioning().partition_count() {
        let mut s = plan.execute(p, ctx.task_ctx()).map_err(|e| e.to_string())?;
        while let Some(b) = s.try_next().await.map_err(|e| e.to_string())? {
            n += b.num_rows();
        }
    }
    std::hint::black_box(n);
    Ok(t.elapsed())
}

async fn measure(ctx: &SessionContext, sql: &str) -> Option<Duration> {
    // warmup
    let _ = run_one(ctx, sql).await;
    let mut times = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        match run_one(ctx, sql).await {
            Ok(d) => times.push(d),
            Err(_) => return None,
        }
    }
    times.sort();
    Some(times[0]) // min — more stable than median for small N
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let dir =
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf1".to_string());
    let queries_dir = PathBuf::from(
        std::env::var("TPCH_QUERIES_DIR").unwrap_or_else(|_| "examples/tpch/queries".to_string()),
    );

    println!("=== Σ.K.2 22-query gate: OFF vs ROUTED ({}) ===\n", dir);

    // One-shot table row counts via COUNT(*).
    let sizing_ctx = build_ctx(&dir, &HashMap::new());
    let mut row_counts: HashMap<String, u64> = HashMap::new();
    for t in TPCH_TABLES {
        if let Some(n) = count_rows(&sizing_ctx, t).await {
            row_counts.insert((*t).to_string(), n);
        }
    }
    println!("Row counts: {:?}\n", row_counts);

    println!(
        "{:<5} {:>10} {:>10} {:>10} {}",
        "Q", "OFF (ms)", "ROUTED", "Δ%", "routing"
    );
    println!("{}", "-".repeat(72));

    let mut all_ratios: Vec<f64> = Vec::new();
    let mut wins = 0;
    let mut regressions = 0;
    let mut skipped = 0;

    for q in 1..=22u8 {
        let sql_path = queries_dir.join(format!("q{q:02}.sql"));
        let sql = match std::fs::read_to_string(&sql_path) {
            Ok(s) => s,
            Err(_) => {
                println!("{:<5} (no sql file)", format!("Q{q:02}"));
                skipped += 1;
                continue;
            }
        };

        // Decide routing for this query.
        let analysis_ctx = build_ctx(&dir, &HashMap::new());
        let decision = match analyse_dict_arrival_with_sizes(&analysis_ctx, &sql, &row_counts).await
        {
            Ok(d) => d,
            Err(_) => {
                println!(
                    "{:<5}  (analysis failed — falling back to OFF)",
                    format!("Q{q:02}")
                );
                HashMap::new()
            }
        };
        let routing: HashMap<String, bool> = decision
            .iter()
            .filter_map(|(k, v)| if *v { Some((k.clone(), true)) } else { None })
            .collect();

        let off_ctx = build_ctx(&dir, &HashMap::new());
        let routed_ctx = build_ctx(&dir, &routing);

        let off = match measure(&off_ctx, &sql).await {
            Some(t) => t.as_secs_f64() * 1000.0,
            None => {
                println!("{:<5}  (OFF failed — skipping)", format!("Q{q:02}"));
                skipped += 1;
                continue;
            }
        };
        let routed = match measure(&routed_ctx, &sql).await {
            Some(t) => t.as_secs_f64() * 1000.0,
            None => {
                println!("{:<5}  (ROUTED failed — skipping)", format!("Q{q:02}"));
                skipped += 1;
                continue;
            }
        };

        let delta = (routed - off) / off * 100.0;
        let ratio = routed / off;
        all_ratios.push(ratio);
        if delta < -1.0 {
            wins += 1;
        } else if delta > 5.0 {
            regressions += 1;
        }

        let mut dec_keys: Vec<_> = routing.keys().cloned().collect();
        dec_keys.sort();
        let dec_str = if dec_keys.is_empty() {
            "(none)".to_string()
        } else {
            format!("dict:{}", dec_keys.join(","))
        };

        println!(
            "{:<5} {:>10.2} {:>10.2} {:>9.1}% {}",
            format!("Q{q:02}"),
            off,
            routed,
            delta,
            dec_str,
        );
    }

    let n = all_ratios.len();
    let log_sum: f64 = all_ratios.iter().map(|r| r.ln()).sum();
    let geomean = (log_sum / n as f64).exp();
    println!();
    println!("Paired queries: {n}  Skipped: {skipped}");
    println!("Wins (ROUTED ≥1% faster): {wins}");
    println!("Regressions (ROUTED >5% slower): {regressions}");
    println!("geomean(ROUTED / OFF) = {:.4}", geomean);
    println!();
    let passed = geomean <= 0.985 && regressions == 0;
    println!(
        "Gate: {}  (target geomean ≤ 0.985 AND regressions == 0)",
        if passed { "PASS" } else { "FAIL" }
    );
}
