//! v2 S2.0 — window-function measurement gate probe.
//!
//! Decides whether a native window operator is worth building at all, by
//! measuring what fraction of each TPC-DS window query's execution time is
//! actually spent in DataFusion's window operators
//! (`WindowAggExec` / `BoundedWindowAggExec`). This is the S1 discipline
//! applied up front: measure before building (S1 built a grouping-set
//! operator that was 1.8× slower than DF-native — see
//! `PHASE_V2_S1_GROUPING_SETS.md`).
//!
//! For each of the 9 TPC-DS queries that use `OVER(...)`:
//!   1. Translate Spark SQL -> DataFusion, build the physical plan on the
//!      shared v2 session (`preset::session_context()`).
//!   2. Execute it (populates per-operator metrics), timing the wall clock.
//!   3. Walk the physical tree; sum `elapsed_compute` per operator; report
//!      the window operators' share of total compute.
//!
//! Gate (see `PHASE_V2_S2_WINDOW_FUNCTIONS.md` §3): a query "opens the
//! gate" only if window compute is >=15% of the query's total operator
//! compute AND >=50 ms absolute. Prints a per-query table + verdict.
//!
//! Run over the SF=1 data (default) or point elsewhere:
//! ```sh
//! cargo run --release -p ematix-flow-core --example win_gate_probe
//! TPCDS_DATA_DIR=examples/tpcds/data/sf10 cargo run --release \
//!     -p ematix-flow-core --example win_gate_probe
//! ```
//! Gated: if the data dir is absent it prints a skip line and exits 0.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use ematix_flow_core::dialect::{Dialect, translate};
use ematix_flow_core::preset;

const TPCDS_TABLES: &[&str] = &[
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
];

/// The 9 TPC-DS queries that use SQL window functions (audit 2026-07-18).
const WINDOW_QUERIES: &[&str] = &[
    "q36", "q44", "q49", "q51", "q53", "q63", "q67", "q70", "q86",
];

/// Gate thresholds (`PHASE_V2_S2_WINDOW_FUNCTIONS.md` §3).
const GATE_SHARE: f64 = 0.15; // >=15% of total operator compute
const GATE_ABS_MS: f64 = 50.0; // AND >=50 ms absolute in window ops

#[derive(Default)]
struct Walk {
    total_elapsed_ns: u128,
    window_elapsed_ns: u128,
    window_ops: Vec<(String, u128, i64)>, // (name, elapsed_ns, output_rows)
    all_ops: Vec<(String, u128, i64)>,    // every operator, for OPEN breakdowns
}

/// Recursively accumulate `elapsed_compute` per operator, flagging the
/// window operators.
fn walk(plan: &Arc<dyn ExecutionPlan>, acc: &mut Walk) {
    let name = plan.name().to_string();
    let (elapsed_ns, rows) = plan
        .metrics()
        .map(|m| {
            (
                m.elapsed_compute().unwrap_or(0) as u128,
                m.output_rows().map(|r| r as i64).unwrap_or(-1),
            )
        })
        .unwrap_or((0, -1));
    acc.total_elapsed_ns += elapsed_ns;
    acc.all_ops.push((name.clone(), elapsed_ns, rows));
    if name.contains("Window") {
        acc.window_elapsed_ns += elapsed_ns;
        acc.window_ops.push((name, elapsed_ns, rows));
    }
    for child in plan.children() {
        walk(child, acc);
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .ok_or("workspace root not found")?
        .to_path_buf();
    let data_dir = std::env::var("TPCDS_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| workspace.join("examples/tpcds/data/sf1"));
    let queries_dir = workspace.join("examples/tpcds/queries/spark");

    let first_table = data_dir.join(format!("{}.parquet", TPCDS_TABLES[0]));
    if !first_table.exists() {
        println!(
            "skip: TPC-DS data not found at {}\n      generate it first (tpcds_generate).",
            data_dir.display()
        );
        return Ok(());
    }

    let ctx = preset::session_context();
    for t in TPCDS_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        ctx.register_parquet(*t, path.to_str().unwrap(), Default::default())
            .await?;
    }

    println!(
        "=== window measurement gate — data: {} ===",
        data_dir.display()
    );
    println!(
        "gate: window compute >= {:.0}% of total AND >= {:.0} ms absolute\n",
        GATE_SHARE * 100.0,
        GATE_ABS_MS
    );
    println!(
        "{:<6} {:>10} {:>12} {:>10} {:>8}  window operators",
        "query", "wall(ms)", "win_cmp(ms)", "win_share", "gate"
    );

    let mut any_open = false;
    let mut shapes: Vec<(String, String)> = Vec::new();
    for q in WINDOW_QUERIES {
        let path = queries_dir.join(format!("{q}.sql"));
        let raw = std::fs::read_to_string(&path)?;
        let spark_sql = raw.trim().trim_end_matches(';').trim();
        let df_sql = match translate(spark_sql, Dialect::Spark) {
            Ok(s) => s,
            Err(e) => {
                println!("{q:<6} TRANSLATE_FAIL {e}");
                continue;
            }
        };

        // Build physical plan (records the shape), then execute (populates
        // metrics), timing the wall clock.
        let plan = match ctx.sql(&df_sql).await {
            Ok(df) => match df.create_physical_plan().await {
                Ok(p) => p,
                Err(e) => {
                    println!("{q:<6} PLAN_FAIL {e}");
                    continue;
                }
            },
            Err(e) => {
                println!("{q:<6} PLAN_FAIL {e}");
                continue;
            }
        };
        // Record the operator shape (names + window frame) once.
        let tree = format!("{}", displayable(plan.as_ref()).indent(false));
        let win_lines: Vec<String> = tree
            .lines()
            .filter(|l| l.contains("WindowAggExec") || l.contains("BoundedWindowAggExec"))
            .map(|l| l.trim().to_string())
            .collect();
        shapes.push((q.to_string(), win_lines.join("\n         ")));

        let start = Instant::now();
        let _ = collect(plan.clone(), ctx.task_ctx()).await?;
        let wall_ms = start.elapsed().as_secs_f64() * 1e3;

        let mut acc = Walk::default();
        walk(&plan, &mut acc);
        let win_ms = acc.window_elapsed_ns as f64 / 1e6;
        let share = if acc.total_elapsed_ns > 0 {
            acc.window_elapsed_ns as f64 / acc.total_elapsed_ns as f64
        } else {
            0.0
        };
        let open = share >= GATE_SHARE && win_ms >= GATE_ABS_MS;
        any_open |= open;

        let op_names: Vec<String> = acc
            .window_ops
            .iter()
            .map(|(n, e, r)| format!("{n}({:.1}ms,{}rows)", *e as f64 / 1e6, r))
            .collect();
        println!(
            "{:<6} {:>10.1} {:>12.1} {:>9.1}% {:>8}  {}",
            q,
            wall_ms,
            win_ms,
            share * 100.0,
            if open { "OPEN" } else { "shut" },
            op_names.join(", ")
        );

        // For OPEN queries, dump where compute actually goes — the S1
        // lesson: a window hotspot may be the SORT feeding the window or
        // the arithmetic, not the window kernel a bespoke operator replaces.
        if open {
            acc.all_ops.sort_by_key(|o| std::cmp::Reverse(o.1));
            println!("       ── {q} operator breakdown (top by elapsed_compute) ──");
            for (n, e, r) in acc.all_ops.iter().take(8) {
                let pct = *e as f64 / acc.total_elapsed_ns as f64 * 100.0;
                println!(
                    "         {:>7.1}ms {:>5.1}%  {:<28} out={}",
                    *e as f64 / 1e6,
                    pct,
                    n,
                    r
                );
            }
        }
    }

    println!("\n=== window operator shapes ===");
    for (q, s) in &shapes {
        println!("{q}: {}", if s.is_empty() { "(none found)" } else { s });
    }

    println!("\n=== GATE VERDICT ===");
    if any_open {
        println!(
            "OPEN for >=1 query — a scoped window operator is authorized for the frame(s) marked OPEN.\n\
             Record the numbers in PHASE_V2_S2_WINDOW_FUNCTIONS.md §3 and proceed to S2.5."
        );
    } else {
        println!(
            "SHUT for all 9 — no query spends a material fraction of compute in window execution.\n\
             DF-native window execution is retained; S2 delivers parity coverage (WIN.1-3) only.\n\
             Record this verdict in PHASE_V2_S2_WINDOW_FUNCTIONS.md §3, exactly as S1 recorded its verdict."
        );
    }
    Ok(())
}
