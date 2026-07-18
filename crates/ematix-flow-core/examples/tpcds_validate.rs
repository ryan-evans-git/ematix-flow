//! v2 S0.2 — TPC-DS validation harness (`tpcds_validate`).
//!
//! For each TPC-DS query under `examples/tpcds/queries/spark/`:
//!   1. Translate the Spark-canonical SQL to DataFusion via the ematix
//!      dialect (`dialect::translate(.., Dialect::Spark)`).
//!   2. Run it on **`preset::session_context()`** — the shared v2 session
//!      (dogfoods S0.1) — over the SF=1 Parquet tables.
//!   3. Row-parity oracle: run the same translated SQL on an in-process
//!      DuckDB over the identical Parquet (`read_parquet`) and compare row
//!      counts. Where DuckDB can't run the translated SQL, record
//!      `ORACLE_SKIP` (honest degradation — never a false pass).
//!
//! Emits a per-query pass/fail matrix + summary. This is the S0 exit
//! artifact (`docs/plans/V2_S0_FOUNDATIONS.md`): "runs 99 queries at SF=1,
//! pass/fail matrix, not yet all-green." Row-*value* parity (beyond count)
//! is a documented follow-on refinement.
//!
//! Gated: if the data directory is absent it prints a skip line and exits
//! 0, so it is safe to wire into CI ahead of a data-gen step.
//!
//! Usage:
//! ```sh
//! # generate once (see tpcds_generate):
//! cargo run --release -p ematix-flow-core --example tpcds_generate -- --sf 1 --out examples/tpcds/data/sf1
//! # then:
//! cargo run --release -p ematix-flow-core --example tpcds_validate
//! # or point at another dir:
//! TPCDS_DATA_DIR=/path/to/sf1 cargo run --release -p ematix-flow-core --example tpcds_validate
//! ```

use std::collections::BTreeMap;
use std::path::PathBuf;

use ematix_flow_core::dialect::{translate, Dialect};
use ematix_flow_core::preset;
use futures_util::TryStreamExt;

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

/// ematix-side outcome for one query.
enum Emat {
    Pass { rows: usize },
    TranslateFail(String),
    PlanFail(String),
    ExecFail(String),
}

/// Oracle-side row-parity verdict.
enum Parity {
    Match(i64),
    Mismatch { emat: usize, duck: i64 },
    OracleSkip(String),
    Unchecked, // ematix itself failed — nothing to compare
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

    // --- Gate: skip cleanly (exit 0) if the data isn't generated yet. ---
    let first_table = data_dir.join(format!("{}.parquet", TPCDS_TABLES[0]));
    if !first_table.exists() {
        println!(
            "skip: TPC-DS SF=1 data not found at {}\n      generate it first:\n        \
             cargo run --release -p ematix-flow-core --example tpcds_generate -- --sf 1 --out {}",
            data_dir.display(),
            data_dir.display()
        );
        return Ok(());
    }

    // --- ematix side: register on the SHARED v2 session (dogfoods S0.1). ---
    let ctx = preset::session_context();
    for t in TPCDS_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        if !path.exists() {
            return Err(format!("missing table parquet: {}", path.display()).into());
        }
        ctx.register_parquet(*t, path.to_str().unwrap(), Default::default())
            .await?;
    }

    // --- DuckDB oracle: views over the same Parquet. ---
    let duck = duckdb::Connection::open_in_memory()?;
    for t in TPCDS_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        duck.execute_batch(&format!(
            "CREATE VIEW {t} AS SELECT * FROM read_parquet('{}');",
            path.display()
        ))?;
    }

    // --- Collect query files (q1.sql .. q99.sql + variants), sorted. ---
    let mut files: Vec<PathBuf> = std::fs::read_dir(&queries_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .collect();
    files.sort_by_key(|p| {
        // numeric-aware-ish: pad the leading number so q2 < q10.
        let stem = p.file_stem().unwrap().to_string_lossy().into_owned();
        let num: String = stem.chars().skip_while(|c| !c.is_ascii_digit()).collect();
        let n: u32 = num
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect::<String>()
            .parse()
            .unwrap_or(0);
        (n, stem)
    });

    println!("=== TPC-DS validate (SF=1) — data: {} ===\n", data_dir.display());

    let mut results: BTreeMap<String, (Emat, Parity)> = BTreeMap::new();
    for path in &files {
        let name = path.file_stem().unwrap().to_string_lossy().into_owned();
        let raw = std::fs::read_to_string(path)?;
        let spark_sql = raw.trim().trim_end_matches(';').trim();

        // ematix: translate Spark -> DataFusion, run on the shared context.
        let (emat, translated) = match translate(spark_sql, Dialect::Spark) {
            Err(e) => (Emat::TranslateFail(e.to_string()), None),
            Ok(df_sql) => match run_emat(&ctx, &df_sql).await {
                Ok(rows) => (Emat::Pass { rows }, Some(df_sql)),
                Err(RunErr::Plan(e)) => (Emat::PlanFail(e), Some(df_sql)),
                Err(RunErr::Exec(e)) => (Emat::ExecFail(e), Some(df_sql)),
            },
        };

        // Oracle: run the SAME translated SQL on DuckDB, compare row count.
        let parity = match (&emat, &translated) {
            (Emat::Pass { rows }, Some(df_sql)) => match duck_count(&duck, df_sql) {
                Ok(duck_rows) if duck_rows == *rows as i64 => Parity::Match(duck_rows),
                Ok(duck_rows) => Parity::Mismatch {
                    emat: *rows,
                    duck: duck_rows,
                },
                Err(e) => Parity::OracleSkip(e),
            },
            _ => Parity::Unchecked,
        };

        let line = match (&emat, &parity) {
            (Emat::Pass { rows }, Parity::Match(_)) => format!("PASS  rows={rows}  parity=OK"),
            (Emat::Pass { rows }, Parity::Mismatch { duck, .. }) => {
                format!("PASS  rows={rows}  parity=MISMATCH(duck={duck})")
            }
            (Emat::Pass { rows }, Parity::OracleSkip(e)) => {
                format!("PASS  rows={rows}  parity=ORACLE_SKIP({})", short(e))
            }
            (Emat::TranslateFail(e), _) => format!("TRANSLATE_FAIL {}", short(e)),
            (Emat::PlanFail(e), _) => format!("PLAN_FAIL {}", short(e)),
            (Emat::ExecFail(e), _) => format!("EXEC_FAIL {}", short(e)),
            (Emat::Pass { .. }, Parity::Unchecked) => "PASS  parity=?".to_string(),
        };
        println!("{name:<8} {line}");
        results.insert(name, (emat, parity));
    }

    // --- Summary matrix. ---
    let total = results.len();
    let emat_pass = results
        .values()
        .filter(|(e, _)| matches!(e, Emat::Pass { .. }))
        .count();
    let parity_ok = results
        .values()
        .filter(|(_, p)| matches!(p, Parity::Match(_)))
        .count();
    let parity_mismatch = results
        .values()
        .filter(|(_, p)| matches!(p, Parity::Mismatch { .. }))
        .count();
    let oracle_skip = results
        .values()
        .filter(|(_, p)| matches!(p, Parity::OracleSkip(_)))
        .count();

    println!("\n=== summary ===");
    println!("  queries:          {total}");
    println!("  ematix executes:  {emat_pass}/{total}");
    println!("  row-parity OK:    {parity_ok}/{emat_pass} (of ematix-passing)");
    println!("  parity MISMATCH:  {parity_mismatch}");
    println!("  oracle skipped:   {oracle_skip}");
    if emat_pass < total {
        println!("  (failing queries above are the SQL-surface gaps — see V2_SQL_SURFACE_GAPS.md)");
    }
    Ok(())
}

enum RunErr {
    Plan(String),
    Exec(String),
}

async fn run_emat(ctx: &datafusion::prelude::SessionContext, sql: &str) -> Result<usize, RunErr> {
    let df = ctx.sql(sql).await.map_err(|e| RunErr::Plan(e.to_string()))?;
    let stream = df
        .execute_stream()
        .await
        .map_err(|e| RunErr::Exec(e.to_string()))?;
    let batches: Vec<_> = stream
        .try_collect()
        .await
        .map_err(|e| RunErr::Exec(e.to_string()))?;
    Ok(batches.iter().map(|b| b.num_rows()).sum())
}

fn duck_count(conn: &duckdb::Connection, sql: &str) -> Result<i64, String> {
    let wrapped = format!("SELECT count(*) FROM ({sql}) AS _t");
    let mut stmt = conn.prepare(&wrapped).map_err(|e| e.to_string())?;
    stmt.query_row([], |r| r.get::<_, i64>(0))
        .map_err(|e| e.to_string())
}

fn short(s: &str) -> String {
    let one = s.replace('\n', " ");
    if one.len() > 90 {
        format!("{}…", &one[..90])
    } else {
        one
    }
}
