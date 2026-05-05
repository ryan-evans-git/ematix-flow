//! Σ.A2 PR 4 / PR 5: TPC-DS dialect-translator audit.
//!
//! For each of the 99 official Apache Spark TPC-DS queries, run them
//! through the chosen dialect translator + DataFusion's planner
//! (no execution — TPC-DS data isn't needed). Categorize per-query:
//!   - PASS: translator succeeded + DataFusion planned successfully
//!   - TRANSLATE_FAIL: translator returned `DialectError`
//!   - PLAN_FAIL: translator succeeded but DataFusion couldn't plan
//!     the result (typically: function-name gap, syntax DataFusion
//!     doesn't yet implement, type-coercion mismatch)
//!
//! Acceptance gates per `docs/PHASE_SIGMA_PLAN.md`:
//!   - Σ.A2 PR 4 (Spark dialect): ≥80% PASS
//!   - Σ.A2 PR 5 (DuckDB dialect): ≥90% PASS on TPC-H + curated set
//!     (TPC-DS isn't a primary acceptance surface for DuckDB but
//!     useful as a signal — DuckDB's SQL is closer to the Spark
//!     queries' shape than its own canonical TPC-DS)
//!
//! Output: a markdown summary table on stdout. Per-query failures
//! logged to stderr with the underlying error so individual gaps
//! are actionable.
//!
//! Run:
//!     cargo run --release -p ematix-flow-core --example tpcds_dialect_audit
//!     cargo run --release -p ematix-flow-core --example tpcds_dialect_audit -- duckdb
//!
//! Defaults to `spark` if no argument is given.

use std::collections::BTreeMap;
use std::path::PathBuf;

use datafusion::prelude::SessionContext;
use ematix_flow_core::dialect::{Dialect, translate};

const SCHEMA_DDL: &str = include_str!("../../../examples/tpcds/schema.sql");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Pass,
    TranslateFail,
    PlanFail,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    // Parse the dialect from argv. Default to Spark since the queries
    // under `examples/tpcds/queries/spark/` are Spark-canonical.
    let dialect_name: String = std::env::args().nth(1).unwrap_or_else(|| "spark".into());
    let dialect: Dialect = dialect_name
        .parse()
        .unwrap_or_else(|e| panic!("--{dialect_name}: {e}"));
    println!("==> dialect: {dialect:?}");

    let ctx = SessionContext::new();
    register_schema(&ctx).await;

    let queries_dir = workspace_root().join("examples/tpcds/queries/spark");
    let mut entries: Vec<_> = std::fs::read_dir(&queries_dir)
        .unwrap_or_else(|e| panic!("read {queries_dir:?}: {e}"))
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|x| x == "sql"))
        .collect();
    // Sort by query name (string-lex order, fine for "q1" → "q99")
    // for deterministic output.
    entries.sort_by_key(|e| {
        e.path()
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default()
    });

    let mut results: BTreeMap<String, (Outcome, String)> = BTreeMap::new();
    for entry in &entries {
        let path = entry.path();
        let name = path.file_stem().unwrap().to_string_lossy().into_owned();
        let sql = std::fs::read_to_string(&path).unwrap();
        // Strip trailing semicolons + whitespace; sqlparser's
        // multi-statement path is exercised in unit tests, not the
        // audit. Single-query-per-file makes failure attribution
        // unambiguous.
        let sql = sql.trim().trim_end_matches(';').trim();

        let outcome = match translate(sql, dialect) {
            Err(e) => {
                eprintln!("  {name}: TRANSLATE_FAIL — {e}");
                (Outcome::TranslateFail, e.to_string())
            }
            Ok(translated) => match ctx.sql(&translated).await {
                Ok(df) => match df.create_physical_plan().await {
                    Ok(_) => (Outcome::Pass, String::new()),
                    Err(e) => {
                        eprintln!("  {name}: PLAN_FAIL — {e}");
                        (Outcome::PlanFail, e.to_string())
                    }
                },
                Err(e) => {
                    eprintln!("  {name}: PLAN_FAIL — {e}");
                    (Outcome::PlanFail, e.to_string())
                }
            },
        };
        results.insert(name, outcome);
    }

    // Summary.
    let total = results.len();
    let pass = results
        .values()
        .filter(|(o, _)| *o == Outcome::Pass)
        .count();
    let translate_fail = results
        .values()
        .filter(|(o, _)| *o == Outcome::TranslateFail)
        .count();
    let plan_fail = results
        .values()
        .filter(|(o, _)| *o == Outcome::PlanFail)
        .count();
    let pass_rate = (pass as f64) / (total as f64) * 100.0;

    println!();
    println!("=== Σ.A2 dialect audit ({dialect:?}) ===");
    println!("total:           {total}");
    println!("PASS:            {pass} ({pass_rate:.1}%)");
    println!("TRANSLATE_FAIL:  {translate_fail}");
    println!("PLAN_FAIL:       {plan_fail}");
    println!();
    println!(
        "acceptance gate: ≥80% PASS — {}",
        if pass_rate >= 80.0 { "MET" } else { "NOT YET" }
    );

    // Group failures by error message prefix for triage. Many queries
    // typically share the same root cause.
    if pass_rate < 100.0 {
        println!("\n=== failure clusters ===");
        let mut clusters: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (name, (outcome, msg)) in &results {
            if *outcome == Outcome::Pass {
                continue;
            }
            let key = msg
                .lines()
                .next()
                .unwrap_or("")
                .chars()
                .take(120)
                .collect::<String>();
            clusters.entry(key).or_default().push(name.clone());
        }
        let mut clusters: Vec<_> = clusters.into_iter().collect();
        clusters.sort_by_key(|(_, qs)| std::cmp::Reverse(qs.len()));
        for (msg, qs) in clusters {
            println!("  [{:>2} queries] {msg}", qs.len());
            // Show up to 5 affected queries for triage scope.
            let names = qs.iter().take(5).cloned().collect::<Vec<_>>().join(", ");
            let suffix = if qs.len() > 5 {
                format!(", … +{} more", qs.len() - 5)
            } else {
                String::new()
            };
            println!("              → {names}{suffix}");
        }
    }
}

async fn register_schema(ctx: &SessionContext) {
    // Strip whole-line comments before splitting on `;`; otherwise
    // a stray semicolon inside a header comment confuses the splitter.
    let cleaned: String = SCHEMA_DDL
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");

    for (idx, stmt) in cleaned.split(';').enumerate() {
        let stmt = stmt.trim();
        if stmt.is_empty() {
            continue;
        }
        ctx.sql(stmt)
            .await
            .unwrap_or_else(|e| panic!("schema stmt #{idx} failed: {e}\nstmt: {stmt}"))
            .collect()
            .await
            .unwrap_or_else(|e| panic!("schema stmt #{idx} execute: {e}"));
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(PathBuf::from)
        .expect("workspace root")
}
