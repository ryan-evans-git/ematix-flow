//! v2 S0.2 — TPC-DS Parquet generator.
//!
//! Mirrors the role of `tpch_generate.rs` for TPC-DS. There is no
//! Rust-native TPC-DS generator (unlike `tpchgen` for TPC-H), so this
//! uses DuckDB's `tpcds` extension (`dsdgen`) via the already-bundled
//! `duckdb` crate and exports each of the 24 tables to Snappy Parquet.
//!
//! Requires the DuckDB `tpcds` extension. `INSTALL tpcds` fetches it over
//! the network on first use (then it is cached); `LOAD tpcds` is enough
//! afterwards. If your environment is offline, pre-install the extension
//! or generate the data on a connected machine and copy the directory.
//!
//! Usage:
//! ```sh
//! cargo run --release -p ematix-flow-core --example tpcds_generate -- \
//!     --sf 1 --out examples/tpcds/data/sf1
//! ```
//!
//! Idempotent-ish: overwrites existing Parquet in `--out`. Delete the
//! directory to start clean.

use std::path::PathBuf;

/// The 24 TPC-DS tables (matches `examples/tpcds/schema.sql`).
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut sf: Option<f64> = None;
    let mut out: Option<PathBuf> = None;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--sf" => {
                sf = Some(args.get(i + 1).ok_or("--sf needs a value")?.parse()?);
                i += 2;
            }
            "--out" => {
                out = Some(PathBuf::from(args.get(i + 1).ok_or("--out needs a value")?));
                i += 2;
            }
            other => return Err(format!("unknown arg {other:?}").into()),
        }
    }
    let sf = sf.ok_or("--sf is required")?;
    let out = out.ok_or("--out is required")?;
    std::fs::create_dir_all(&out)?;

    let conn = duckdb::Connection::open_in_memory()?;
    // The tpcds extension: INSTALL fetches on first use (network), LOAD
    // activates. dsdgen materialises the 24 tables into the connection.
    conn.execute_batch("INSTALL tpcds; LOAD tpcds;")
        .map_err(|e| format!("load tpcds extension (needs network on first install): {e}"))?;
    println!("generating TPC-DS SF={sf} …");
    conn.execute_batch(&format!("CALL dsdgen(sf = {sf});"))?;

    for t in TPCDS_TABLES {
        let path = out.join(format!("{t}.parquet"));
        conn.execute_batch(&format!(
            "COPY {t} TO '{}' (FORMAT PARQUET, COMPRESSION SNAPPY);",
            path.display()
        ))?;
        println!("  wrote {}", path.display());
    }
    println!("done → {}", out.display());
    Ok(())
}
