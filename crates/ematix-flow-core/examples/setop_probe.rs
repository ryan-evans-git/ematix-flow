//! v2 S2 (set-ops half) — physical-lowering probe.
//!
//! The scoping question for `INTERSECT` / `EXCEPT` / large `IN` is *not*
//! "does it run" (the dialect audit says all 99 plan) but "what physical
//! operators does DataFusion lower it to, and are those already on
//! ematix's accelerated path (joins / aggregation) or a generic operator
//! that loses the benchmark?" — the same "is there actually a gap"
//! discipline the S2.0 window gate applied. This probe dumps, per query,
//! the physical operator histogram + whether an `InList` (IN-list) node
//! survives, so the scope is grounded in real plans, not assumption.
//!
//! Run over SF=1 (default) or point elsewhere via `TPCDS_DATA_DIR`.
//! Gated: prints a skip line + exits 0 if the data is absent.
//! `cargo run --release -p ematix-flow-core --example setop_probe`

use std::collections::BTreeMap;
use std::path::PathBuf;

use datafusion::physical_plan::{ExecutionPlan, displayable};
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

/// TPC-DS set-op queries: INTERSECT (q8/q14a/q14b/q38), EXCEPT (q87),
/// and the UNION-heavy / large-IN queries (q8's ~400-elem literal IN,
/// q33/q56/q60 channel unions).
const SETOP_QUERIES: &[&str] = &["q8", "q14a", "q14b", "q38", "q87", "q33", "q56", "q60"];

/// Operator names that mean "ematix already accelerates this" — set-ops
/// that lower to joins/aggregation ride the existing join/bloom/reorder +
/// fused-aggregate paths.
fn is_accelerated(name: &str) -> bool {
    name.contains("HashJoinExec")
        || name.contains("AggregateExec")
        || name.contains("SortMergeJoinExec")
}

fn walk(plan: &dyn ExecutionPlan, hist: &mut BTreeMap<String, usize>) {
    *hist.entry(plan.name().to_string()).or_default() += 1;
    for child in plan.children() {
        walk(child.as_ref(), hist);
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

    let ctx = preset::session_context();
    for t in TPCDS_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        ctx.register_parquet(*t, path.to_str().unwrap(), Default::default())
            .await?;
    }

    println!(
        "=== set-op physical-lowering probe — data: {} ===\n",
        data_dir.display()
    );
    for q in SETOP_QUERIES {
        let raw = std::fs::read_to_string(queries_dir.join(format!("{q}.sql")))?;
        let spark_sql = raw.trim().trim_end_matches(';').trim();
        let df_sql = match translate(spark_sql, Dialect::Spark) {
            Ok(s) => s,
            Err(e) => {
                println!("{q}: TRANSLATE_FAIL {e}\n");
                continue;
            }
        };
        let plan = match ctx.sql(&df_sql).await {
            Ok(df) => match df.create_physical_plan().await {
                Ok(p) => p,
                Err(e) => {
                    println!("{q}: PLAN_FAIL {e}\n");
                    continue;
                }
            },
            Err(e) => {
                println!("{q}: PLAN_FAIL {e}\n");
                continue;
            }
        };

        let mut hist = BTreeMap::new();
        walk(plan.as_ref(), &mut hist);
        let tree = format!("{}", displayable(plan.as_ref()).indent(false));

        // Is there any dedicated set-op operator, or is it all join/agg?
        let has_dedicated_setop = hist
            .keys()
            .any(|n| n.contains("Intersect") || n.contains("Except") || n.contains("SetOp"));
        let accel: usize = hist
            .iter()
            .filter(|(n, _)| is_accelerated(n))
            .map(|(_, c)| c)
            .sum();
        let has_inlist = tree.contains("InList") || tree.contains(" IN (");
        let joins: Vec<String> = hist
            .iter()
            .filter(|(n, _)| n.contains("Join"))
            .map(|(n, c)| format!("{n}×{c}"))
            .collect();

        // Extract the join_type modes DF chose (INTERSECT→LeftSemi,
        // EXCEPT→LeftAnti expected) — the set-op lowering signature, and
        // the input to "do ematix's join rules fire on these modes?".
        let mut jt: BTreeMap<String, usize> = BTreeMap::new();
        for line in tree.lines() {
            if let Some(idx) = line.find("join_type=") {
                let rest = &line[idx + "join_type=".len()..];
                let mode: String = rest.chars().take_while(|c| c.is_alphanumeric()).collect();
                if !mode.is_empty() {
                    *jt.entry(mode).or_default() += 1;
                }
            }
        }
        let jt_s: Vec<String> = jt.iter().map(|(m, c)| format!("{m}×{c}")).collect();

        println!(
            "{q}: dedicated_setop_op={} accel_ops={} joins=[{}] join_types=[{}] inlist_node={}",
            has_dedicated_setop,
            accel,
            joins.join(", "),
            jt_s.join(", "),
            has_inlist
        );
        // Compact histogram (operator×count), most frequent first.
        let mut items: Vec<(&String, &usize)> = hist.iter().collect();
        items.sort_by(|a, b| b.1.cmp(a.1));
        let compact: Vec<String> = items
            .iter()
            .take(10)
            .map(|(n, c)| format!("{n}×{c}"))
            .collect();
        println!("     ops: {}\n", compact.join(", "));
    }

    println!(
        "Read: set-ops with NO dedicated operator whose work is all join/agg are\n\
         already on ematix's accelerated path — the 'native set-op operator' gap\n\
         is then a non-issue (like S1/grouping-sets). A surviving Intersect/Except\n\
         physical node, or a large IN-list that expands to OR-chains, would be the\n\
         real gap."
    );
    Ok(())
}
