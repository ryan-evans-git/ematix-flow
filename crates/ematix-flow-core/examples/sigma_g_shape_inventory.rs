//! Σ.G — broader-query shape inventory.
//!
//! Plan-only analysis (no data, no execution). For each TPC-H and
//! TPC-DS query:
//!
//! - Register all tables of the suite as empty `MemTable`s with the
//!   correct schema (via DataFusion's native `CREATE TABLE ddl`).
//! - Try to plan the query via `SessionContext::sql` +
//!   `DataFrame::create_physical_plan`. Capture parse / plan errors.
//! - For each planned query, identify the **top operator class** of
//!   the physical plan (what DataFusion would execute outermost).
//! - For each current catalog rule (`EnableDictFilterRule`,
//!   `EnableDictGroupCountRule`, `InjectFilterSumRule`,
//!   `InjectFilterMultiAggRule`), run it on the plan and detect
//!   whether it produces any rewrite (pointer-diff the result).
//! - Emit a markdown table summarising every query and an
//!   aggregate "shape demand" section.
//!
//! Run:
//!     cargo run --release -p ematix-flow-core \
//!         --example sigma_g_shape_inventory > docs/SIGMA_G_INVENTORY.md
//!
//! Why this matters: the Σ.F shape catalog has 4 entries today
//! covering ~5 TPC-H queries. To know which shapes to build next,
//! we need to see (a) which queries currently fall through to
//! DataFusion's default and (b) which plan shapes appear most
//! often across a broad query set. This is the inventory that
//! anchors that decision.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

use ematix_flow_core::dict_aggregate_rule::EnableDictGroupCountRule;
use ematix_flow_core::dict_filter_rule::EnableDictFilterRule;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fused_aggregate_filter_multi_agg_rule::InjectFilterMultiAggRule;
use ematix_flow_core::fused_aggregate_filter_sum_rule::InjectFilterSumRule;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

#[derive(Debug, Clone)]
struct QueryResult {
    query_id: String,
    /// `Ok((top_op_class, all_ops_in_tree))` on success,
    /// `Err(reason)` on plan failure. `all_ops_in_tree` is the
    /// set of distinct `ExecutionPlan::name()` values appearing
    /// anywhere in the tree.
    plan: Result<(String, Vec<String>), String>,
    /// Names of catalog rules that produced a rewrite (pointer-diff).
    rules_fired: Vec<&'static str>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let repo_root: PathBuf = std::env::current_dir()?;
    let mut sections: Vec<(String, Vec<QueryResult>)> = Vec::new();

    // --- Suite 1: TPC-H 22 queries ----------------------------
    // Wire real parquet via EmatixFastParquetTableProvider so the
    // plan shapes match what production sees (with the partitioning,
    // CoalescePartitionsExec, RepartitionHash, etc. that the rules
    // depend on). The schema-only fallback runs if data is missing.
    let tpch_data_dir = repo_root.join("examples/tpch/data/sf1");
    let tpch_ctx = if tpch_data_dir.is_dir() {
        setup_tpch_ctx_from_parquet(&tpch_data_dir).await?
    } else {
        eprintln!(
            "TPC-H data dir {} not found; falling back to schema-only \
             registration (rule-firing data will be incomplete)",
            tpch_data_dir.display()
        );
        setup_ctx(&build_tpch_schema_ddl()).await?
    };
    let tpch_queries = read_queries(
        repo_root.join("examples/tpch/queries").as_path(),
        |name| name.ends_with(".sql") && !name.ends_with(".polars.sql"),
        |stem| stem.trim_start_matches('q').to_string(),
    )?;
    let tpch_results = audit_suite(&tpch_ctx, tpch_queries).await;
    sections.push(("TPC-H (22 queries)".to_string(), tpch_results));

    // --- Suite 2: TPC-DS 99-ish queries ----------------------
    let tpcds_schema = fs::read_to_string(repo_root.join("examples/tpcds/schema.sql"))?;
    let tpcds_ctx = setup_ctx(&tpcds_schema).await?;
    let tpcds_queries = read_queries(
        repo_root.join("examples/tpcds/queries/spark").as_path(),
        |name| name.ends_with(".sql"),
        |stem| stem.trim_start_matches('q').to_string(),
    )?;
    let tpcds_results = audit_suite(&tpcds_ctx, tpcds_queries).await;
    sections.push(("TPC-DS (Spark dialect)".to_string(), tpcds_results));

    // --- Suite 3: ClickBench (43 queries, single-table hits) -----
    let cb_schema_path = repo_root.join("examples/clickbench/schema.sql");
    let cb_queries_path = repo_root.join("examples/clickbench/queries.sql");
    if cb_schema_path.is_file() && cb_queries_path.is_file() {
        let cb_schema = fs::read_to_string(&cb_schema_path)?;
        let cb_ctx = setup_ctx(&cb_schema).await?;
        let cb_queries = read_queries_inline(&cb_queries_path)?;
        let cb_results = audit_suite(&cb_ctx, cb_queries).await;
        sections.push(("ClickBench (43 queries)".to_string(), cb_results));
    } else {
        eprintln!(
            "ClickBench files not found at {} / {}; skipping suite",
            cb_schema_path.display(),
            cb_queries_path.display()
        );
    }

    // --- Report ---------------------------------------------
    emit_markdown(&sections);
    Ok(())
}

/// Build TPC-H DDL from hand-known schema. The bench harness has the
/// types; the audit doesn't have access to parquet data, so we
/// declare the columns as `CREATE TABLE ... (cols)`.
fn build_tpch_schema_ddl() -> String {
    // Types match the canonical TPC-H spec / what DataFusion emits
    // when reading the parquet fixtures under examples/tpch/data/sf1/.
    // Keep the columns identical to what each query references.
    [
        "CREATE TABLE region (
            r_regionkey BIGINT, r_name VARCHAR, r_comment VARCHAR
        )",
        "CREATE TABLE nation (
            n_nationkey BIGINT, n_name VARCHAR, n_regionkey BIGINT,
            n_comment VARCHAR
        )",
        "CREATE TABLE supplier (
            s_suppkey BIGINT, s_name VARCHAR, s_address VARCHAR,
            s_nationkey BIGINT, s_phone VARCHAR, s_acctbal DOUBLE,
            s_comment VARCHAR
        )",
        "CREATE TABLE customer (
            c_custkey BIGINT, c_name VARCHAR, c_address VARCHAR,
            c_nationkey BIGINT, c_phone VARCHAR, c_acctbal DOUBLE,
            c_mktsegment VARCHAR, c_comment VARCHAR
        )",
        "CREATE TABLE part (
            p_partkey BIGINT, p_name VARCHAR, p_mfgr VARCHAR,
            p_brand VARCHAR, p_type VARCHAR, p_size INT,
            p_container VARCHAR, p_retailprice DOUBLE, p_comment VARCHAR
        )",
        "CREATE TABLE partsupp (
            ps_partkey BIGINT, ps_suppkey BIGINT, ps_availqty INT,
            ps_supplycost DOUBLE, ps_comment VARCHAR
        )",
        "CREATE TABLE orders (
            o_orderkey BIGINT, o_custkey BIGINT, o_orderstatus VARCHAR,
            o_totalprice DOUBLE, o_orderdate DATE, o_orderpriority VARCHAR,
            o_clerk VARCHAR, o_shippriority INT, o_comment VARCHAR
        )",
        "CREATE TABLE lineitem (
            l_orderkey BIGINT, l_partkey BIGINT, l_suppkey BIGINT,
            l_linenumber INT, l_quantity DOUBLE, l_extendedprice DOUBLE,
            l_discount DOUBLE, l_tax DOUBLE, l_returnflag VARCHAR,
            l_linestatus VARCHAR, l_shipdate DATE, l_commitdate DATE,
            l_receiptdate DATE, l_shipinstruct VARCHAR, l_shipmode VARCHAR,
            l_comment VARCHAR
        )",
    ]
    .join(";\n")
        + ";"
}

/// Set up a `SessionContext` and run every `CREATE TABLE` statement
/// in `schema_sql`. Strips line comments BEFORE splitting on `;` so
/// semicolons inside comments don't break the parse.
async fn setup_ctx(schema_sql: &str) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    // Strip line comments first (avoids a `tables; column types`
    // comment breaking the statement split).
    let no_comments: String = schema_sql
        .lines()
        .filter(|l| !l.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    for stmt in no_comments.split(';') {
        let cleaned = stmt.trim();
        if cleaned.is_empty() {
            continue;
        }
        match ctx.sql(cleaned).await {
            Ok(df) => {
                df.collect().await.ok();
            }
            Err(e) => {
                eprintln!("schema setup: skipping stmt: {e}");
            }
        }
    }
    Ok(ctx)
}

/// Set up a TPC-H `SessionContext` backed by real parquet via the
/// production `EmatixFastParquetTableProvider`. This is the only way
/// to get plan shapes (partitioning, CoalescePartitionsExec, etc.)
/// that match what the catalog rules see in benches.
async fn setup_tpch_ctx_from_parquet(
    data_dir: &Path,
) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        let path_str = path
            .to_str()
            .ok_or("TPC-H parquet path is not UTF-8")?
            .to_string();
        let prov = EmatixFastParquetTableProvider::try_new(path_str)?.with_dict_preservation(true);
        ctx.register_table(*t, Arc::new(prov))?;
    }
    Ok(ctx)
}

/// Read every `.sql` file in `dir` whose name passes `name_filter`,
/// and assign each a query ID via `stem_to_id`.
fn read_queries(
    dir: &Path,
    name_filter: impl Fn(&str) -> bool,
    stem_to_id: impl Fn(&str) -> String,
) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
    let mut entries: Vec<(String, String)> = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();
        if !name_filter(&name) {
            continue;
        }
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();
        let id = stem_to_id(&stem);
        let sql = fs::read_to_string(&path)?;
        entries.push((id, sql));
    }
    // Natural-sort by ID. Strip trailing letters first for tpcds
    // (q14a, q14b, ...).
    entries.sort_by(|a, b| {
        let (na, sa) = split_id(&a.0);
        let (nb, sb) = split_id(&b.0);
        na.cmp(&nb).then_with(|| sa.cmp(&sb))
    });
    Ok(entries)
}

/// Read queries from a single file with one statement per line (the
/// ClickBench `queries.sql` format). Blank lines and lines starting
/// with `--` are skipped. The query ID is the 1-based line number.
fn read_queries_inline(path: &Path) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
    let raw = fs::read_to_string(path)?;
    let mut out: Vec<(String, String)> = Vec::new();
    let mut idx = 0usize;
    for line in raw.lines() {
        let stripped = line.trim();
        if stripped.is_empty() || stripped.starts_with("--") {
            continue;
        }
        idx += 1;
        let sql = stripped.trim_end_matches(';').to_string();
        out.push((format!("{:02}", idx), sql));
    }
    Ok(out)
}

fn truncate(s: &str, n: usize) -> String {
    let s = s.replace('|', "\\|").replace('\n', " ");
    if s.chars().count() <= n {
        s
    } else {
        let truncated: String = s.chars().take(n).collect();
        format!("{truncated}…")
    }
}

fn split_id(s: &str) -> (u32, String) {
    let digit_end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (digits, rest) = s.split_at(digit_end);
    let n = digits.parse::<u32>().unwrap_or(u32::MAX);
    (n, rest.to_string())
}

async fn audit_suite(ctx: &SessionContext, queries: Vec<(String, String)>) -> Vec<QueryResult> {
    let mut out = Vec::new();
    for (id, sql) in queries {
        out.push(audit_query(ctx, &id, &sql).await);
    }
    out
}

async fn audit_query(ctx: &SessionContext, id: &str, sql: &str) -> QueryResult {
    let plan: Result<Arc<dyn ExecutionPlan>, String> = match ctx.sql(sql).await {
        Ok(df) => df
            .create_physical_plan()
            .await
            .map_err(|e| format!("{e}").lines().next().unwrap_or("plan error").to_string()),
        Err(e) => Err(format!("{e}").lines().next().unwrap_or("parse error").to_string()),
    };

    match plan {
        Err(reason) => QueryResult {
            query_id: id.to_string(),
            plan: Err(reason),
            rules_fired: vec![],
        },
        Ok(physical) => {
            let top_op = exec_class(physical.as_ref());
            let mut all_ops: Vec<String> = Vec::new();
            collect_ops(&physical, &mut all_ops);
            all_ops.sort();
            all_ops.dedup();
            let rules_fired = check_rules(&physical);
            QueryResult {
                query_id: id.to_string(),
                plan: Ok((top_op, all_ops)),
                rules_fired,
            }
        }
    }
}

/// Short type-class name for an `ExecutionPlan` node. Uses the
/// trait's own `name()` method which every impl provides (returns
/// `"FilterExec"`, `"AggregateExec"`, etc).
fn exec_class(plan: &dyn ExecutionPlan) -> String {
    plan.name().to_string()
}

/// DFS the plan tree, collecting `name()` of every node.
fn collect_ops(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<String>) {
    out.push(plan.name().to_string());
    for child in plan.children() {
        let owned: Arc<dyn ExecutionPlan> = (*child).clone();
        collect_ops(&owned, out);
    }
}

/// Run each catalog rule against `plan`; return the names of any
/// that produced a rewrite (detected via `Arc::ptr_eq`).
fn check_rules(plan: &Arc<dyn ExecutionPlan>) -> Vec<&'static str> {
    let cfg = ConfigOptions::default();
    let mut fired: Vec<&'static str> = Vec::new();

    let rules: Vec<(&'static str, Box<dyn PhysicalOptimizerRule>)> = vec![
        ("dict_filter", Box::new(EnableDictFilterRule)),
        ("dict_group_count", Box::new(EnableDictGroupCountRule)),
        ("filter_sum", Box::new(InjectFilterSumRule)),
        ("filter_multi_agg", Box::new(InjectFilterMultiAggRule)),
    ];

    for (name, rule) in rules {
        if let Ok(new) = rule.optimize(plan.clone(), &cfg) {
            if !Arc::ptr_eq(plan, &new) {
                fired.push(name);
            }
        }
    }
    fired
}

fn emit_markdown(sections: &[(String, Vec<QueryResult>)]) {
    println!("# Σ.G — shape inventory");
    println!();
    println!(
        "Plan-only audit of every query in TPC-H and TPC-DS against \
         the current Σ.F shape catalog (4 rules). For each query: \
         did it plan, what is the top-of-plan operator class, and \
         which (if any) of the catalog rules produce a rewrite."
    );
    println!();
    println!(
        "Rules: `dict_filter`, `dict_group_count`, `filter_sum`, \
         `filter_multi_agg`."
    );
    println!();

    for (name, results) in sections {
        println!("## {name}\n");
        println!("| Q | Plans? | Top op | Rules fired |");
        println!("|---|---|---|---|");
        for r in results {
            let (planned_str, top_str) = match &r.plan {
                Ok((top, _)) => ("yes", top.clone()),
                Err(reason) => ("no", format!("❌ `{}`", truncate(reason, 60))),
            };
            let rules_cell = if r.rules_fired.is_empty() {
                "—".to_string()
            } else {
                r.rules_fired.join(", ")
            };
            println!(
                "| Q{} | {} | {} | {} |",
                r.query_id, planned_str, top_str, rules_cell
            );
        }
        println!();

        // Aggregates per section.
        let total = results.len();
        let planned = results.iter().filter(|r| r.plan.is_ok()).count();
        let unplanned = total - planned;
        let rules_total: usize = results.iter().map(|r| r.rules_fired.len()).sum();
        let any_rule = results.iter().filter(|r| !r.rules_fired.is_empty()).count();

        let mut top_op_counts: BTreeMap<String, usize> = BTreeMap::new();
        let mut tree_op_counts: BTreeMap<String, usize> = BTreeMap::new();
        for r in results {
            if let Ok((top, all_ops)) = &r.plan {
                *top_op_counts.entry(top.clone()).or_default() += 1;
                for op in all_ops {
                    *tree_op_counts.entry(op.clone()).or_default() += 1;
                }
            }
        }
        let mut rule_counts: BTreeMap<&'static str, usize> = BTreeMap::new();
        for r in results {
            for rule in &r.rules_fired {
                *rule_counts.entry(*rule).or_default() += 1;
            }
        }

        println!("### Aggregate — {name}\n");
        println!("- **Queries**: {total}");
        println!("- **Planned**: {planned} / {total}  ({unplanned} failed to plan)");
        println!(
            "- **Any catalog rule fired**: {any_rule} / {planned}  \
             (total rule activations: {rules_total})"
        );
        println!();
        println!("**Top-op distribution (root of plan):**\n");
        for (op, n) in &top_op_counts {
            println!("- `{op}` — {n}");
        }
        println!();
        println!("**Operator-class footprint across the tree (queries that mention each):**\n");
        let mut sorted: Vec<(&String, &usize)> = tree_op_counts.iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
        for (op, n) in sorted {
            println!("- `{op}` — {n}");
        }
        println!();
        if !rule_counts.is_empty() {
            println!("**Rule activation:**\n");
            for (rule, n) in &rule_counts {
                println!("- `{rule}` — {n}");
            }
            println!();
        }
    }

    // Final summary across both suites.
    let all: Vec<&QueryResult> = sections.iter().flat_map(|(_, rs)| rs.iter()).collect();
    let total = all.len();
    let planned = all.iter().filter(|r| r.plan.is_ok()).count();
    let any_rule = all.iter().filter(|r| !r.rules_fired.is_empty()).count();

    println!("## Combined headline\n");
    println!("- **{total}** queries across TPC-H + TPC-DS.");
    println!("- **{planned}** plan successfully ({:.0}%).", 100.0 * planned as f64 / total as f64);
    println!(
        "- **{any_rule}** hit at least one current catalog rule \
         ({:.0}% of total, {:.0}% of planned).",
        100.0 * any_rule as f64 / total as f64,
        100.0 * any_rule as f64 / planned as f64
    );
}
