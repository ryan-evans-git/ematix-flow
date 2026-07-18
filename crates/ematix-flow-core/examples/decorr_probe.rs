//! v2 S3 — subquery-decorrelation probe.
//!
//! The S3 question is plan quality, not correctness (the dialect audit
//! says all 99 plan; DataFusion decorrelates). The decisive signal: does
//! DF decorrelate TPC-DS's correlated subqueries into **hash-based
//! Semi/Anti/Mark joins** — which ride ematix's well-developed semi/anti
//! join rules (see the set-op probe finding) — or into a
//! **`NestedLoopJoinExec` carrying a Semi/Anti/Mark join type**, i.e. a
//! quadratic re-evaluation of the subquery per outer row (the real gap
//! S3 would fix). This probe dumps, per correlated-subquery query, the
//! join-type histogram, HashJoin vs NestedLoopJoin counts, a quadratic
//! red-flag, and each join operator's `elapsed_compute` share — so the
//! S3 scope is grounded in real plans + timings, not assumption.
//!
//! Run over SF=1 (default) or via `TPCDS_DATA_DIR`. Gated: skips (exit 0)
//! if the data is absent.
//! `cargo run --release -p ematix-flow-core --example decorr_probe`

use std::collections::BTreeMap;
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

/// TPC-DS queries with CORRELATED subqueries: EXISTS/NOT EXISTS
/// (q10/q16/q35/q69/q94) + a correlated scalar-aggregate subquery (q41,
/// the hardest case). (Uncorrelated IN-subqueries are covered by the
/// set-op probe.)
const DECORR_QUERIES: &[&str] = &["q10", "q16", "q35", "q41", "q69", "q94"];

fn is_correlated_mode(mode: &str) -> bool {
    mode.contains("Semi") || mode.contains("Anti") || mode.contains("Mark")
}

struct Walk {
    join_types: BTreeMap<String, usize>,
    hash_joins: usize,
    nested_loop_joins: usize,
    /// NestedLoopJoins whose mode is Semi/Anti/Mark — the quadratic
    /// correlated-subquery red flag.
    quadratic_correlated: usize,
    join_elapsed_ns: u128,
    total_elapsed_ns: u128,
}

fn walk(plan: &Arc<dyn ExecutionPlan>, tree_line_of: &BTreeMap<usize, String>, acc: &mut Walk) {
    let name = plan.name().to_string();
    let elapsed = plan
        .metrics()
        .and_then(|m| m.elapsed_compute())
        .unwrap_or(0) as u128;
    acc.total_elapsed_ns += elapsed;
    if name.contains("Join") {
        acc.join_elapsed_ns += elapsed;
        if name.contains("HashJoin") {
            acc.hash_joins += 1;
        }
        if name.contains("NestedLoopJoin") {
            acc.nested_loop_joins += 1;
        }
    }
    let _ = tree_line_of;
    for child in plan.children() {
        walk(child, tree_line_of, acc);
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

    if !data_dir
        .join(format!("{}.parquet", TPCDS_TABLES[0]))
        .exists()
    {
        println!(
            "skip: TPC-DS data not found at {} — generate with tpcds_generate.",
            data_dir.display()
        );
        return Ok(());
    }

    // EMAT_PROBE_VANILLA=1 → plain DataFusion session (no ematix preset
    // rules), to tell a DF-53 decorrelation limitation from an
    // ematix-rule regression.
    let vanilla = std::env::var("EMAT_PROBE_VANILLA").is_ok();
    let ctx = if vanilla {
        datafusion::prelude::SessionContext::new()
    } else {
        preset::session_context()
    };
    println!(
        "(session: {})",
        if vanilla {
            "VANILLA DataFusion"
        } else {
            "ematix preset"
        }
    );
    for t in TPCDS_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        ctx.register_parquet(*t, path.to_str().unwrap(), Default::default())
            .await?;
    }

    println!(
        "=== decorrelation probe — data: {} ===\n",
        data_dir.display()
    );
    println!(
        "{:<6} {:>9} {:>7} {:>5} {:>5} {:>9}  join_types / QUADRATIC?",
        "query", "wall(ms)", "jn_cmp", "HJ", "NLJ", "jn_share"
    );

    let mut any_quadratic = false;
    for q in DECORR_QUERIES {
        let raw = std::fs::read_to_string(queries_dir.join(format!("{q}.sql")))?;
        let spark_sql = raw.trim().trim_end_matches(';').trim();
        let df_sql = match translate(spark_sql, Dialect::Spark) {
            Ok(s) => s,
            Err(e) => {
                println!("{q:<6} TRANSLATE_FAIL {e}");
                continue;
            }
        };
        let df = match ctx.sql(&df_sql).await {
            Ok(df) => df,
            Err(e) => {
                println!("{q:<6} SQL_FAIL(logical build) {e}");
                continue;
            }
        };
        // Split logical-opt failure from physical-planning failure, to
        // localize a preset-rule regression to the right optimizer stage.
        if let Err(e) = df.clone().into_optimized_plan() {
            println!("{q:<6} LOGICAL_OPT_FAIL {e}");
            continue;
        }
        let plan = match df.create_physical_plan().await {
            Ok(p) => p,
            Err(e) => {
                println!("{q:<6} PHYSICAL_PLAN_FAIL {e}");
                continue;
            }
        };

        // Parse join_type modes + detect NLJ carrying a correlated mode,
        // by pairing each join operator's line with its join_type in the
        // indented tree text.
        let tree = format!("{}", displayable(plan.as_ref()).indent(false));
        let mut join_types: BTreeMap<String, usize> = BTreeMap::new();
        let mut quadratic_correlated = 0usize;
        for line in tree.lines() {
            if let Some(idx) = line.find("join_type=") {
                let mode: String = line[idx + "join_type=".len()..]
                    .chars()
                    .take_while(|c| c.is_alphanumeric())
                    .collect();
                if !mode.is_empty() {
                    *join_types.entry(mode.clone()).or_default() += 1;
                    if line.contains("NestedLoopJoin") && is_correlated_mode(&mode) {
                        quadratic_correlated += 1;
                    }
                }
            }
        }

        let start = Instant::now();
        let _ = collect(plan.clone(), ctx.task_ctx()).await?;
        let wall_ms = start.elapsed().as_secs_f64() * 1e3;

        let mut acc = Walk {
            join_types,
            hash_joins: 0,
            nested_loop_joins: 0,
            quadratic_correlated,
            join_elapsed_ns: 0,
            total_elapsed_ns: 0,
        };
        walk(&plan, &BTreeMap::new(), &mut acc);
        any_quadratic |= acc.quadratic_correlated > 0;

        let share = if acc.total_elapsed_ns > 0 {
            acc.join_elapsed_ns as f64 / acc.total_elapsed_ns as f64
        } else {
            0.0
        };
        let jt: Vec<String> = acc
            .join_types
            .iter()
            .map(|(m, c)| format!("{m}×{c}"))
            .collect();
        println!(
            "{:<6} {:>9.1} {:>7.1} {:>5} {:>5} {:>8.1}%  {}{}",
            q,
            wall_ms,
            acc.join_elapsed_ns as f64 / 1e6,
            acc.hash_joins,
            acc.nested_loop_joins,
            share * 100.0,
            jt.join(", "),
            if acc.quadratic_correlated > 0 {
                format!("  ⚠ QUADRATIC×{}", acc.quadratic_correlated)
            } else {
                String::new()
            }
        );
    }

    println!("\n=== VERDICT ===");
    if any_quadratic {
        println!(
            "GAP: >=1 query decorrelates to a NestedLoopJoin with a Semi/Anti/Mark mode\n\
             — a per-outer-row subquery re-evaluation. An ematix decorrelation pass (or\n\
             a rule forcing hash-join lowering) would remove the quadratic. Proceed to S3."
        );
    } else {
        println!(
            "NO quadratic decorrelation: every correlated subquery lowered to HASH-based\n\
             Semi/Anti/Mark joins, which ride ematix's semi/anti join + bloom rules (same\n\
             path the set-op probe found well-developed). Like S1/S2, the 'gap' is then a\n\
             plan-quality confirmation, not a missing pass — verify ematix rules FIRE on\n\
             these joins, else it's a targeted rule-predicate fix, not a new operator."
        );
    }
    Ok(())
}
