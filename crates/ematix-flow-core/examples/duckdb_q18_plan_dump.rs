//! Dump DuckDB's physical plan for Q18 on SF=10, so we can see
//! what techniques DuckDB applies that our DataFusion+ematix path
//! doesn't.
//!
//! Usage:
//!   cargo run --release -p ematix-flow-core \
//!     --example duckdb_q18_plan_dump --features triangulation -- 18
//!
//! (Pass a different number to dump another query.)

use std::path::PathBuf;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let q: u8 = args
        .get(1)
        .map(|s| s.parse().unwrap_or(18))
        .unwrap_or(18);

    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf10"));
    let queries_dir = PathBuf::from("examples/tpch/queries");
    let sql_path = queries_dir.join(format!("q{q:02}.sql"));
    let sql = std::fs::read_to_string(&sql_path)?;

    let conn = duckdb::Connection::open_in_memory()?;
    // Make DuckDB use the same number of threads ematix gets in the
    // triangulation bench (target_partitions=14).
    conn.execute_batch("PRAGMA threads=14")?;
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        let stmt = format!(
            "CREATE VIEW {t} AS SELECT * FROM read_parquet('{}')",
            path.display()
        );
        conn.execute_batch(&stmt)?;
    }

    // EXPLAIN — logical + physical plan.
    println!("==== DuckDB EXPLAIN Q{q} (logical + physical) ====\n");
    let explain_sql = format!("EXPLAIN {sql}");
    let mut stmt = conn.prepare(&explain_sql)?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        // EXPLAIN returns one row with one or more text columns.
        // Print every value as text.
        let n_cols = row.as_ref().column_count();
        for i in 0..n_cols {
            if let Ok(v) = row.get::<_, String>(i) {
                println!("{v}");
            }
        }
    }

    println!();
    println!("==== DuckDB EXPLAIN ANALYZE Q{q} (with timing) ====\n");
    let explain_sql = format!("EXPLAIN ANALYZE {sql}");
    let mut stmt = conn.prepare(&explain_sql)?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let n_cols = row.as_ref().column_count();
        for i in 0..n_cols {
            if let Ok(v) = row.get::<_, String>(i) {
                println!("{v}");
            }
        }
    }

    Ok(())
}
