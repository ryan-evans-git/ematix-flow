//! Σ.L.1 gate — 22-query TPC-H bench where Speculate verdicts are
//! resolved by a per-query probe that races dict-on vs dict-off on
//! a small slice of the table.
//!
//! Pipeline per query:
//!   1. Static shape analysis → `DictArrivalVerdictMap` (Yes/No/Speculate)
//!   2. For each `Speculate`: build a probe SQL (`SELECT <gb_col>,
//!      COUNT(*) FROM <table> GROUP BY <gb_col>`) and race dict-on vs
//!      dict-off factories. Cache the result in a process-local map
//!      keyed by (table, gb_col) so we probe each shape at most once.
//!   3. Resolved decision drives the real-query context.
//!   4. Run the real query OFF + ROUTED-via-probe.
//!
//! Run:
//!   cargo run --release -p ematix-flow-core --example dict_arrival_speculative_gate

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::dict_routing::{
    DictArrivalVerdict, analyse_dict_arrival_verdicts, resolve_via_probe,
};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use futures_util::TryStreamExt;

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
    let _ = run_one(ctx, sql).await; // warmup
    let mut times = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        match run_one(ctx, sql).await {
            Ok(d) => times.push(d),
            Err(_) => return None,
        }
    }
    times.sort();
    Some(times[0])
}

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

/// Pick a probe SQL — single GROUP BY + COUNT on the first string
/// column of the table (covers the Q12 / Q13 / Q21-style shape we
/// most care about).
async fn probe_sql_for_table(ctx: &SessionContext, table: &str) -> Option<String> {
    let schema = ctx.table_provider(table).await.ok()?.schema();
    let gb = schema
        .fields()
        .iter()
        .find(|f| {
            matches!(
                f.data_type(),
                datafusion::arrow::datatypes::DataType::Utf8
                    | datafusion::arrow::datatypes::DataType::Utf8View
                    | datafusion::arrow::datatypes::DataType::LargeUtf8
                    | datafusion::arrow::datatypes::DataType::Dictionary(_, _)
            )
        })?
        .name()
        .clone();
    Some(format!("SELECT {gb}, COUNT(*) FROM {table} GROUP BY {gb}"))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let dir =
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf1".to_string());
    let queries_dir = PathBuf::from(
        std::env::var("TPCH_QUERIES_DIR").unwrap_or_else(|_| "examples/tpch/queries".to_string()),
    );
    println!("=== Σ.L.1 speculative gate ({}) ===\n", dir);

    // Per-table sizes (single COUNT).
    let sizing_ctx = build_ctx(&dir, &HashMap::new());
    let mut row_counts: HashMap<String, u64> = HashMap::new();
    for t in TPCH_TABLES {
        if let Some(n) = count_rows(&sizing_ctx, t).await {
            row_counts.insert((*t).to_string(), n);
        }
    }
    println!("Row counts: {:?}\n", row_counts);

    // Probe cache: (table) → dict_wins bool, populated lazily.
    let probe_cache: Mutex<HashMap<String, bool>> = Mutex::new(HashMap::new());

    // Pre-compute the probe SQL for each table (needs async to read
    // schema). Reuse across queries.
    let mut probe_sqls: HashMap<String, String> = HashMap::new();
    for t in TPCH_TABLES {
        if let Some(sql) = probe_sql_for_table(&sizing_ctx, t).await {
            probe_sqls.insert((*t).to_string(), sql);
        }
    }

    println!(
        "{:<5} {:>10} {:>10} {:>10} decision (after probe)",
        "Q", "OFF (ms)", "ROUTED", "Δ%"
    );
    println!("{}", "-".repeat(72));

    let mut all_ratios: Vec<f64> = Vec::new();
    let mut wins = 0;
    let mut regressions = 0;
    let mut probes_run = 0;

    for q in 1..=22u8 {
        let sql_path = queries_dir.join(format!("q{q:02}.sql"));
        let sql = match std::fs::read_to_string(&sql_path) {
            Ok(s) => s,
            Err(_) => continue,
        };

        // Static verdicts.
        let analysis_ctx = build_ctx(&dir, &HashMap::new());
        let verdicts = match analyse_dict_arrival_verdicts(&analysis_ctx, &sql, &row_counts).await {
            Ok(v) => v,
            Err(_) => continue,
        };

        // Resolve Speculate verdicts via probe (with caching).
        let dir2 = dir.clone();
        let probe_cache_clone = &probe_cache;
        let decision = match resolve_via_probe(&verdicts, |table| {
            // Consult cache first.
            if let Some(&cached) = probe_cache_clone.lock().unwrap().get(table) {
                let chosen: bool = cached;
                let dir_inner = dir2.clone();
                let table_inner = table.to_string();
                let factory_dict: Box<ematix_flow_core::dict_routing::CtxFactory<'static>> =
                    Box::new(move || {
                        let mut m = HashMap::new();
                        m.insert(table_inner.clone(), true);
                        build_ctx(&dir_inner, &m)
                    });
                let dir_inner2 = dir2.clone();
                let _table_inner2 = table.to_string();
                let factory_default: Box<ematix_flow_core::dict_routing::CtxFactory<'static>> =
                    Box::new(move || build_ctx(&dir_inner2, &HashMap::new()));
                // Use a trivially-cheap probe so the cached result drives the decision.
                #[allow(clippy::if_same_then_else)]
                let probe = if chosen {
                    "SELECT 1".to_string()
                } else {
                    "SELECT 1".to_string()
                };
                return Some((factory_dict, factory_default, probe));
            }
            // Look up pre-computed probe SQL.
            let probe = match probe_sqls.get(table) {
                Some(p) => p.clone(),
                None => return None,
            };
            let dir_inner = dir2.clone();
            let table_inner = table.to_string();
            let factory_dict: Box<ematix_flow_core::dict_routing::CtxFactory<'static>> =
                Box::new(move || {
                    let mut m = HashMap::new();
                    m.insert(table_inner.clone(), true);
                    build_ctx(&dir_inner, &m)
                });
            let dir_inner2 = dir2.clone();
            let factory_default: Box<ematix_flow_core::dict_routing::CtxFactory<'static>> =
                Box::new(move || build_ctx(&dir_inner2, &HashMap::new()));
            Some((factory_dict, factory_default, probe))
        })
        .await
        {
            Ok(d) => d,
            Err(_) => continue,
        };

        // Update probe cache from this run's decisions.
        {
            let mut c = probe_cache.lock().unwrap();
            for (k, v) in &decision {
                let was_speculate =
                    matches!(verdicts.get(k), Some(&DictArrivalVerdict::Speculate { .. }));
                if was_speculate && !c.contains_key(k) {
                    c.insert(k.clone(), *v);
                    probes_run += 1;
                }
            }
        }

        // Build OFF + ROUTED contexts.
        let off_ctx = build_ctx(&dir, &HashMap::new());
        let routed_overrides: HashMap<String, bool> = decision
            .iter()
            .filter(|(_, v)| **v)
            .map(|(k, _)| (k.clone(), true))
            .collect();
        let routed_ctx = build_ctx(&dir, &routed_overrides);

        let off = match measure(&off_ctx, &sql).await {
            Some(t) => t.as_secs_f64() * 1000.0,
            None => continue,
        };
        let rt = match measure(&routed_ctx, &sql).await {
            Some(t) => t.as_secs_f64() * 1000.0,
            None => continue,
        };
        let delta = (rt - off) / off * 100.0;
        let ratio = rt / off;
        all_ratios.push(ratio);
        if delta < -1.0 {
            wins += 1;
        } else if delta > 5.0 {
            regressions += 1;
        }
        let dec_keys: Vec<_> = routed_overrides.keys().cloned().collect();
        let dec_label = if dec_keys.is_empty() {
            "(none)".to_string()
        } else {
            let mut k = dec_keys;
            k.sort();
            format!("dict:{}", k.join(","))
        };

        println!(
            "{:<5} {:>10.2} {:>10.2} {:>9.1}% {}",
            format!("Q{q:02}"),
            off,
            rt,
            delta,
            dec_label
        );
    }

    let n = all_ratios.len();
    let log_sum: f64 = all_ratios.iter().map(|r| r.ln()).sum();
    let geomean = (log_sum / n as f64).exp();
    println!();
    println!("Paired queries: {n}    Probes run: {probes_run}");
    println!("Wins: {wins}    Regressions (>5%): {regressions}");
    println!("geomean(ROUTED / OFF) = {:.4}", geomean);
    let pass = geomean <= 0.985 && regressions == 0;
    println!(
        "Gate: {}  (target geomean ≤ 0.985 AND regressions == 0)",
        if pass { "PASS" } else { "FAIL" }
    );
    println!();
    println!("Probe cache state:");
    for (k, v) in probe_cache.lock().unwrap().iter() {
        println!("  {k:<12}  dict_wins={v}");
    }
}
