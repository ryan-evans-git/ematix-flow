//! v2 S2.5 — q51 A/B: fused cumulative-window operator vs DF-native.
//!
//! One process, one session; toggles `EMAT_WINDOW_FUSED` (read at plan
//! time) between runs. First asserts row-for-row correctness parity, then
//! benchmarks both modes and prints the speedup (or refutation).
//!
//! `TPCDS_DATA_DIR=examples/tpcds/data/sf10 cargo run --release \
//!     -p ematix-flow-core --example q51_ab`

use std::path::PathBuf;
use std::time::Instant;

use datafusion::arrow::array::Array;
use datafusion::physical_plan::{collect, displayable};
use ematix_flow_core::dialect::{Dialect, translate};
use ematix_flow_core::preset;

const TABLES: &[&str] = &[
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

fn set_fused(on: bool) {
    // SAFETY: single-threaded harness; the rule reads EMAT_WINDOW_FUSED at
    // plan time, so toggling between sequential plans is well-defined.
    // The rule is default-ON, so "off" must be explicit `=0`.
    unsafe {
        std::env::set_var("EMAT_WINDOW_FUSED", if on { "1" } else { "0" });
    }
}

/// Render all result rows as canonical strings (order-preserving; q51 has
/// ORDER BY + LIMIT 100 so the result is deterministic).
fn rows(batches: &[datafusion::arrow::array::RecordBatch]) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        for r in 0..b.num_rows() {
            let mut cells = Vec::with_capacity(b.num_columns());
            for c in 0..b.num_columns() {
                let col = b.column(c);
                cells.push(if col.is_null(r) {
                    "∅".to_string()
                } else {
                    datafusion::common::ScalarValue::try_from_array(col, r)
                        .map(|s| s.to_string())
                        .unwrap_or_else(|_| "?".into())
                });
            }
            out.push(cells.join("|"));
        }
    }
    out
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
        .unwrap_or_else(|_| workspace.join("examples/tpcds/data/sf10"));
    if !data_dir.join("web_sales.parquet").exists() {
        println!("skip: TPC-DS data not found at {}", data_dir.display());
        return Ok(());
    }

    let ctx = preset::session_context();
    for t in TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        ctx.register_parquet(*t, path.to_str().unwrap(), Default::default())
            .await?;
    }
    let raw = std::fs::read_to_string(workspace.join("examples/tpcds/queries/spark/q51.sql"))?;
    let df_sql = translate(raw.trim().trim_end_matches(';').trim(), Dialect::Spark)?;

    println!("=== q51 A/B — data: {} ===\n", data_dir.display());

    // --- Correctness: fused plan must use the operator + match DF rows ---
    set_fused(true);
    let fused_plan = ctx.sql(&df_sql).await?.create_physical_plan().await?;
    let fused_tree = format!("{}", displayable(fused_plan.as_ref()).indent(false));
    let uses_op = fused_tree.contains("FusedCumulativeWindowExec");
    let n_bounded_left = fused_tree.matches("BoundedWindowAggExec").count();
    println!(
        "fused plan uses FusedCumulativeWindowExec: {uses_op}  (BoundedWindowAggExec remaining: {n_bounded_left})"
    );
    let fused_rows = rows(&collect(fused_plan, ctx.task_ctx()).await?);

    set_fused(false);
    let df_plan = ctx.sql(&df_sql).await?.create_physical_plan().await?;
    let df_rows = rows(&collect(df_plan, ctx.task_ctx()).await?);

    if fused_rows == df_rows {
        println!("correctness: PASS — {} rows identical\n", fused_rows.len());
    } else {
        println!(
            "correctness: FAIL — fused {} rows vs DF {} rows",
            fused_rows.len(),
            df_rows.len()
        );
        for (i, (a, b)) in fused_rows.iter().zip(df_rows.iter()).enumerate() {
            if a != b {
                println!("  first diff at row {i}:\n    fused: {a}\n    df:    {b}");
                break;
            }
        }
        return Ok(());
    }

    // --- Benchmark both modes (warm) ---
    let iters = 9usize;
    let mut results = Vec::new();
    for (label, fused) in [("DF-native", false), ("fused", true)] {
        set_fused(fused);
        let mut walls = Vec::new();
        for i in 0..(iters + 2) {
            let plan = ctx.sql(&df_sql).await?.create_physical_plan().await?;
            let start = Instant::now();
            let _ = collect(plan, ctx.task_ctx()).await?;
            let ms = start.elapsed().as_secs_f64() * 1e3;
            if i >= 2 {
                walls.push(ms);
            }
        }
        walls.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let min = walls[0];
        let median = walls[walls.len() / 2];
        results.push((label, min, median));
        println!("{label:<10} min {min:6.1} ms · median {median:6.1} ms");
    }
    set_fused(false);

    let df_med = results[0].2;
    let fused_med = results[1].2;
    let speedup = df_med / fused_med;
    println!("\n=== VERDICT ===");
    println!(
        "median: DF {df_med:.1} ms vs fused {fused_med:.1} ms → {speedup:.2}× ({:+.1} ms)",
        df_med - fused_med
    );
    if speedup > 1.05 {
        println!("fused WINS (>5%). Ship: make the rule default-on, add parity test, keep.");
    } else if speedup < 0.95 {
        println!("fused LOSES. Revert the operator (S1 discipline).");
    } else {
        println!("neutral (within ±5%). Not worth the operator — revert.");
    }
    Ok(())
}
