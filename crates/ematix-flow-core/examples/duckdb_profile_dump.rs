//! Dump DuckDB's per-operator JSON profile for a TPC-H query on SF=10.
//!
//! Unlike EXPLAIN ANALYZE (whose text rendering was flaky via the Rust
//! crate's row iteration), this enables JSON profiling to a FILE, runs
//! the query normally, then parses the file. Gives per-operator timing
//! AND `operator_cardinality` (rows each operator emitted) AND the
//! TABLE_SCAN `extra_info` (pushed filters/projections) — which tells us
//! whether DuckDB skips lineitem rows we decode.
//!
//! Usage:
//!   cargo run --release -p ematix-flow-core \
//!     --example duckdb_profile_dump --features triangulation -- 8
//!
//! Env: TPCH_DATA_DIR (default examples/tpch/data/sf10).

use std::path::PathBuf;
use std::time::Instant;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

fn walk(node: &serde_json::Value, depth: usize, agg: &mut std::collections::BTreeMap<String, f64>) {
    let ty = node
        .get("operator_type")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let timing = node
        .get("operator_timing")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let card = node
        .get("operator_cardinality")
        .and_then(|v| v.as_u64())
        .or_else(|| node.get("operator_rows_scanned").and_then(|v| v.as_u64()));
    if !ty.is_empty() {
        let indent = "  ".repeat(depth);
        let extra = node.get("extra_info");
        // Pull a few interesting extra_info keys (Table, Filters, Projections).
        let mut hint = String::new();
        if let Some(ei) = extra {
            for k in [
                "Table",
                "Filters",
                "Projections",
                "__estimated_cardinality__",
            ] {
                if let Some(v) = ei.get(k) {
                    let s = match v {
                        serde_json::Value::Array(a) => a
                            .iter()
                            .filter_map(|x| x.as_str())
                            .collect::<Vec<_>>()
                            .join(","),
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    if !s.is_empty() {
                        hint.push_str(&format!(" {k}={s}"));
                    }
                }
            }
        }
        println!(
            "{indent}{ty:<24} {:>9.2} ms  card={:<12}{hint}",
            timing * 1000.0,
            card.map(|c| c.to_string()).unwrap_or_else(|| "-".into())
        );
        *agg.entry(ty.to_string()).or_insert(0.0) += timing * 1000.0;
    }
    if let Some(children) = node.get("children").and_then(|v| v.as_array()) {
        for c in children {
            walk(c, depth + 1, agg);
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let q: u8 = args.get(1).map(|s| s.parse().unwrap_or(8)).unwrap_or(8);

    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf10"));
    let queries_dir = PathBuf::from("examples/tpch/queries");
    let sql = std::fs::read_to_string(queries_dir.join(format!("q{q:02}.sql")))?;

    let conn = duckdb::Connection::open_in_memory()?;
    conn.execute_batch("PRAGMA threads=14")?;
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        conn.execute_batch(&format!(
            "CREATE VIEW {t} AS SELECT * FROM read_parquet('{}')",
            path.display()
        ))?;
    }

    let prof_path = format!("/tmp/duck_q{q:02}_profile.json");
    // Warm + timed runs. Iterate every row to force full materialization.
    let mut walls = Vec::new();
    for i in 0..7 {
        // Re-arm profiling each run; the file is overwritten with the last run.
        conn.execute_batch(&format!(
            "SET enable_profiling='json'; SET profiling_output='{prof_path}';"
        ))?;
        let t0 = Instant::now();
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        let mut n = 0usize;
        while let Some(_r) = rows.next()? {
            n += 1;
        }
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        walls.push(ms);
        if i == 0 {
            println!("Q{q}: {n} result rows");
        }
    }
    walls.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = walls[walls.len() / 2];
    println!(
        "Q{q} DuckDB wall: median {median:.1} ms  (min {:.1}, max {:.1}, n={})",
        walls[0],
        walls[walls.len() - 1],
        walls.len()
    );

    // Parse the last profile.
    let json: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&prof_path)?)?;
    // Top-level total time field varies by version.
    for k in ["latency", "operator_timing", "cpu_time"] {
        if let Some(v) = json.get(k).and_then(|v| v.as_f64()) {
            println!("top-level {k}: {:.2} ms", v * 1000.0);
        }
    }
    println!("\n==== per-operator tree (warm run) ====");
    let mut agg = std::collections::BTreeMap::new();
    walk(&json, 0, &mut agg);

    println!("\n==== time summed by operator_type ====");
    let mut rows: Vec<_> = agg.into_iter().collect();
    rows.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    for (ty, ms) in rows {
        println!("  {ty:<26} {ms:>9.2} ms");
    }

    Ok(())
}
