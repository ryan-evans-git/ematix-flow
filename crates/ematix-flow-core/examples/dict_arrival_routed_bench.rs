//! Σ.K.2 — A/B/C bench: OFF baseline vs ON-everywhere vs ROUTED
//! (per-query shape-aware decision via `dict_routing::analyse_dict_arrival_for_sql`).
//!
//! Σ.K.1 proved ON-everywhere regresses badly on Q01/Q13/Q19. This
//! bench validates that ROUTED — which flips dict-preservation per
//! table only when the query shape is dict-friendly — picks ON for
//! Q12 and OFF for the rest, capturing the Q12 win without the
//! regressions.
//!
//! Success criterion: for each query, ROUTED median ≤ min(OFF, ON) +
//! 1ms noise, AND ROUTED on Q12 ≥ 20% faster than OFF.
//!
//! Run:
//!   cargo run --release -p ematix-flow-core --example dict_arrival_routed_bench

use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::dict_routing::analyse_dict_arrival_for_sql;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use futures_util::TryStreamExt;

const QUERIES: &[(&str, &str)] = &[
    (
        "Q01",
        "SELECT l_returnflag, l_linestatus, COUNT(*), SUM(l_quantity), \
                MIN(l_quantity), MAX(l_quantity), AVG(l_quantity), \
                SUM(l_extendedprice) \
           FROM lineitem WHERE l_shipdate <= DATE '1998-09-02' \
           GROUP BY l_returnflag, l_linestatus",
    ),
    (
        "Q12",
        "SELECT l_shipmode, COUNT(*) FROM lineitem \
          WHERE l_shipmode IN ('MAIL', 'SHIP') \
            AND l_receiptdate >= DATE '1994-01-01' \
            AND l_receiptdate <  DATE '1995-01-01' \
          GROUP BY l_shipmode",
    ),
    (
        "Q13",
        "SELECT o_orderpriority, COUNT(*) FROM orders \
           WHERE o_comment LIKE '%special%' \
           GROUP BY o_orderpriority",
    ),
    (
        "Q16",
        "SELECT p_brand, p_type, COUNT(*) FROM part \
           WHERE p_size IN (49, 14, 23, 45, 19, 3, 36, 9) \
           GROUP BY p_brand, p_type",
    ),
    (
        "Q19",
        "SELECT SUM(l_extendedprice) FROM lineitem \
           JOIN part ON l_partkey = p_partkey \
          WHERE (p_brand = 'Brand#12' AND l_quantity BETWEEN 1 AND 11) \
             OR (p_brand = 'Brand#23' AND l_quantity BETWEEN 10 AND 20) \
             OR (p_brand = 'Brand#34' AND l_quantity BETWEEN 20 AND 30)",
    ),
];

const TABLES: &[&str] = &["lineitem", "orders", "part"];
const REPS: usize = 5;

fn target_partitions() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8)
}

fn build_ctx(dir: &str, dict_overrides: &std::collections::HashMap<String, bool>) -> SessionContext {
    let cfg = SessionConfig::new().with_target_partitions(target_partitions());
    let ctx = SessionContext::new_with_config(cfg);
    for t in TABLES {
        let path = format!("{dir}/{t}.parquet");
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
    let mut total = 0usize;
    for p in 0..plan.output_partitioning().partition_count() {
        let mut s = plan.execute(p, ctx.task_ctx()).map_err(|e| e.to_string())?;
        while let Some(b) = s.try_next().await.map_err(|e| e.to_string())? {
            total += b.num_rows();
        }
    }
    std::hint::black_box(total);
    Ok(t.elapsed())
}

fn no_dict() -> std::collections::HashMap<String, bool> {
    std::collections::HashMap::new()
}

fn all_dict() -> std::collections::HashMap<String, bool> {
    let mut m = std::collections::HashMap::new();
    for t in TABLES {
        m.insert((*t).to_string(), true);
    }
    m
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let dir = std::env::var("TPCH_DATA_DIR")
        .unwrap_or_else(|_| "examples/tpch/data/sf1".to_string());
    println!("=== Σ.K.2: OFF vs ON-everywhere vs ROUTED ({}) ===\n", dir);
    println!(
        "{:<6} {:>10} {:>10} {:>10} {:>10} {:>10} {:>22}",
        "Query", "OFF (ms)", "ON (ms)", "ROUTED", "Δrouted%", "Δon%", "routing decision"
    );
    println!("{}", "-".repeat(86));

    let mut routed_wins = 0;
    let mut routed_regressions = 0;
    let mut headline_q12_delta_pct: Option<f64> = None;

    for (label, sql) in QUERIES {
        // 1. Decide routing for this query.
        let analysis_ctx = build_ctx(&dir, &no_dict());
        let decision = analyse_dict_arrival_for_sql(&analysis_ctx, sql)
            .await
            .unwrap();

        // 2. Build the three contexts.
        let off_ctx = build_ctx(&dir, &no_dict());
        let on_ctx = build_ctx(&dir, &all_dict());
        let routed_ctx = build_ctx(
            &dir,
            &decision
                .iter()
                .filter_map(|(k, v)| if *v { Some((k.clone(), true)) } else { None })
                .collect(),
        );

        // 3. Warm each, then 5-rep median. ON-everywhere may fail
        //    outright (e.g. Q13 on `o_comment` with PLAIN-fallback row
        //    group) — that's a *feature* of the result, not a bug.
        async fn measure(ctx: &SessionContext, sql: &str) -> Option<f64> {
            // warmup (best-effort)
            let _ = run_one(ctx, sql).await;
            let mut times = Vec::with_capacity(REPS);
            for _ in 0..REPS {
                match run_one(ctx, sql).await {
                    Ok(d) => times.push(d),
                    Err(_) => return None,
                }
            }
            times.sort();
            Some(times[REPS / 2].as_secs_f64() * 1000.0)
        }

        let off = measure(&off_ctx, sql).await.expect("OFF must succeed");
        let on = measure(&on_ctx, sql).await;
        let rt = measure(&routed_ctx, sql)
            .await
            .expect("ROUTED must succeed by construction");
        let drt = (rt - off) / off * 100.0;
        let don = on.map(|on| (on - off) / off * 100.0);

        if drt < -1.0 {
            routed_wins += 1;
        } else if drt > 5.0 {
            routed_regressions += 1;
        }
        if *label == "Q12" {
            headline_q12_delta_pct = Some(drt);
        }

        // Format decision compactly.
        let mut dec_pairs: Vec<_> = decision.iter().collect();
        dec_pairs.sort_by_key(|(k, _)| (*k).clone());
        let dec_str = dec_pairs
            .iter()
            .filter(|(_, v)| **v)
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>()
            .join(",");
        let dec_label = if dec_str.is_empty() {
            "(none — Utf8View)".to_string()
        } else {
            format!("dict:{dec_str}")
        };

        let on_str = on
            .map(|x| format!("{:>10.2}", x))
            .unwrap_or_else(|| format!("{:>10}", "FAIL"));
        let don_str = don
            .map(|x| format!("{:>9.1}%", x))
            .unwrap_or_else(|| format!("{:>10}", "FAIL"));
        println!(
            "{:<6} {:>10.2} {} {:>10.2} {:>9.1}% {} {:>22}",
            label, off, on_str, rt, drt, don_str, dec_label,
        );
    }

    println!();
    println!("ROUTED wins (≥1% faster than OFF): {routed_wins}");
    println!("ROUTED regressions (>5% slower than OFF): {routed_regressions}");
    if let Some(q12) = headline_q12_delta_pct {
        println!("Q12 headline Δ vs OFF: {:.1}%", q12);
    }
    println!();
    println!("Pass criteria: ROUTED regressions == 0 AND Q12 Δ ≤ -20%.");
}
