//! One-off helper: convert TPC-H parquet → pipe-delimited CSV for
//! Postgres `\copy`, and (optionally) print each table's schema so the
//! Postgres DDL column order/types match the parquet exactly.
//!
//! Uses the bundled `duckdb` crate — `read_parquet` is built in, and the
//! triangulation bench already reads the same parquet through it, so this
//! needs no extra features.
//!
//! Run:
//!   TPCH_DATA_DIR=examples/tpch/data/sf1 PG_CSV_OUT=/tmp/tpch_csv_sf1 \
//!   PRINT_SCHEMA=1 \
//!     cargo run --release -p ematix-flow-core --example parquet_to_csv
//!
//! Env:
//!   TPCH_DATA_DIR  input parquet dir (default examples/tpch/data/sf1)
//!   PG_CSV_OUT     output CSV dir     (default /tmp/tpch_csv)
//!   PRINT_SCHEMA   if set, DESCRIBE each table to stdout before COPY
//!
//! Delimiter is `|` (the canonical TPC-H field separator — never appears
//! in dbgen text), FORMAT CSV so DuckDB quotes any field that would need
//! it and Postgres `\copy ... (format csv, delimiter '|')` reads it back
//! identically. TPC-H base tables have no NULLs, so no null-token dance.

use std::path::PathBuf;

use duckdb::Connection;

const TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir =
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf1".to_string());
    let out = std::env::var("PG_CSV_OUT").unwrap_or_else(|_| "/tmp/tpch_csv".to_string());
    let print_schema = std::env::var("PRINT_SCHEMA").is_ok();
    std::fs::create_dir_all(&out)?;

    let conn = Connection::open_in_memory()?;

    for t in TABLES {
        let pq = PathBuf::from(&dir).join(format!("{t}.parquet"));
        let pq = pq.to_string_lossy().into_owned();

        if print_schema {
            println!("== {t} ==");
            let mut stmt = conn.prepare(&format!("DESCRIBE SELECT * FROM read_parquet('{pq}')"))?;
            let mut rows = stmt.query([])?;
            while let Some(r) = rows.next()? {
                let name: String = r.get(0)?;
                let ty: String = r.get(1)?;
                println!("  {name:<18} {ty}");
            }
        }

        let csv = PathBuf::from(&out).join(format!("{t}.csv"));
        let csv = csv.to_string_lossy().into_owned();
        let copy = format!(
            "COPY (SELECT * FROM read_parquet('{pq}')) TO '{csv}' \
             (FORMAT CSV, DELIMITER '|', HEADER false)"
        );
        conn.execute_batch(&copy)?;
        let bytes = std::fs::metadata(&csv).map(|m| m.len()).unwrap_or(0);
        println!("wrote {csv}  ({bytes} bytes)");
    }

    Ok(())
}
