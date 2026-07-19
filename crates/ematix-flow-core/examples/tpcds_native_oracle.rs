//! TPC-DS native-engine ORACLE GATE.
//!
//! For every canonical Spark TPC-DS query the clean-room engine
//! (`ematix-flow-engine`) can execute, run it AND run the same query on an
//! in-process DuckDB over the identical SF=1 Parquet, then compare the
//! full result sets value-for-value (sorted multiset, FP tolerance,
//! trimmed strings — the `tpch_validate` comparison contract). This is the
//! parity half of the TPC-DS breadth campaign: coverage says a query runs;
//! this says it runs *correctly*.
//!
//! Queries the native engine can't yet execute are reported `NATIVE_SKIP`
//! (they're the coverage frontier, not a parity failure). Queries DuckDB
//! can't parse are `ORACLE_SKIP` (honest degradation — never a false
//! pass). The process exits non-zero iff a native-executing query
//! MISMATCHES the oracle, so it gates.
//!
//! Usage:
//! ```sh
//! cargo run --release -p ematix-flow-core --example tpcds_native_oracle              # sf1 sweep
//! cargo run --release -p ematix-flow-core --example tpcds_native_oracle -- q65       # one query, verbose
//! cargo run --release -p ematix-flow-core --example tpcds_native_oracle -- sf10      # sf10 sweep
//! cargo run --release -p ematix-flow-core --example tpcds_native_oracle -- sf10 q65  # sf10, one query
//! ```

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::expr::ScalarValue;
use ematix_flow_engine::plan::execute;

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
    "web_site",
    "web_sales",
];

/// A comparable cell (the `tpch_validate` contract).
#[derive(Debug, Clone)]
enum Cell {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Date(i32),
    Str(String),
}

impl Cell {
    fn sort_key(&self) -> String {
        match self {
            Cell::Null => "\x00".into(),
            Cell::Bool(b) => format!("\x01{b}"),
            Cell::Int(i) => format!("\x02{i:020}"),
            Cell::Float(f) => format!("\x03{f:.9e}"),
            Cell::Date(d) => format!("\x02{d:020}"), // sort with ints (cross-eq)
            Cell::Str(s) => format!("\x05{}", s.trim()),
        }
    }
}

fn fp_eq(a: f64, b: f64) -> bool {
    if a == b || (a.is_nan() && b.is_nan()) {
        return true;
    }
    let mag = a.abs().max(b.abs()).max(1.0);
    (a - b).abs() / mag <= 1e-6
}

fn cell_eq(a: &Cell, b: &Cell) -> bool {
    use Cell::*;
    match (a, b) {
        (Null, Null) => true,
        (Bool(x), Bool(y)) => x == y,
        (Int(x), Int(y)) => x == y,
        (Date(x), Date(y)) => x == y,
        (Str(x), Str(y)) => x.trim() == y.trim(),
        (Float(x), Float(y)) => fp_eq(*x, *y),
        // Cross-type numerics: a scaled-decimal SUM comes back Float on
        // one side and Int/Decimal on the other; a raw date column is
        // Int(days) from the native engine (Date32 collapses to i64 in
        // its evaluator) but Date(days) from DuckDB.
        (Int(x), Float(y)) | (Float(y), Int(x)) => fp_eq(*x as f64, *y),
        (Int(x), Date(y)) | (Date(y), Int(x)) => *x == *y as i64,
        (Float(x), Date(y)) | (Date(y), Float(x)) => fp_eq(*x, *y as f64),
        // A NULL on one side vs a type-default 0 / "" on the other is a
        // real difference — do NOT equate.
        _ => false,
    }
}

fn row_lt(a: &[Cell], b: &[Cell]) -> std::cmp::Ordering {
    for (ca, cb) in a.iter().zip(b.iter()) {
        let o = ca.sort_key().cmp(&cb.sort_key());
        if o != std::cmp::Ordering::Equal {
            return o;
        }
    }
    a.len().cmp(&b.len())
}

fn fmt_row(r: &[Cell]) -> String {
    r.iter()
        .map(|c| match c {
            Cell::Null => "NULL".into(),
            Cell::Bool(b) => b.to_string(),
            Cell::Int(i) => i.to_string(),
            Cell::Float(f) => format!("{f:.4}"),
            Cell::Date(d) => format!("d{d}"),
            Cell::Str(s) => format!("\"{}\"", s.trim()),
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

fn native_cell(v: &ScalarValue) -> Cell {
    match v {
        ScalarValue::Null => Cell::Null,
        ScalarValue::Boolean(b) => Cell::Bool(*b),
        ScalarValue::Int32(i) => Cell::Int(*i as i64),
        ScalarValue::Int64(i) => Cell::Int(*i),
        ScalarValue::Date32(d) => Cell::Date(*d),
        ScalarValue::Float64(f) => Cell::Float(*f),
        ScalarValue::Utf8(s) => Cell::Str(s.to_string()),
    }
}

fn duck_cell(row: &duckdb::Row, idx: usize) -> Cell {
    use duckdb::types::ValueRef;
    match row.get_ref(idx) {
        Err(_) => Cell::Str("ERR".into()),
        Ok(v) => match v {
            ValueRef::Null => Cell::Null,
            ValueRef::Boolean(b) => Cell::Bool(b),
            ValueRef::TinyInt(i) => Cell::Int(i as i64),
            ValueRef::SmallInt(i) => Cell::Int(i as i64),
            ValueRef::Int(i) => Cell::Int(i as i64),
            ValueRef::BigInt(i) => Cell::Int(i),
            ValueRef::HugeInt(i) => Cell::Int(i as i64),
            ValueRef::UTinyInt(i) => Cell::Int(i as i64),
            ValueRef::USmallInt(i) => Cell::Int(i as i64),
            ValueRef::UInt(i) => Cell::Int(i as i64),
            ValueRef::UBigInt(i) => Cell::Int(i as i64),
            ValueRef::Float(f) => Cell::Float(f as f64),
            ValueRef::Double(f) => Cell::Float(f),
            ValueRef::Text(b) => Cell::Str(String::from_utf8_lossy(b).into_owned()),
            ValueRef::Date32(d) => Cell::Date(d),
            ValueRef::Decimal(d) => Cell::Float(d.to_string().parse().unwrap_or(0.0)),
            other => Cell::Str(format!("{other:?}")),
        },
    }
}

fn duck_rows(conn: &duckdb::Connection, sql: &str) -> Result<Vec<Vec<Cell>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let mut it = stmt.query([]).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    while let Some(row) = it.next().map_err(|e| e.to_string())? {
        let nc = row.as_ref().column_count();
        out.push((0..nc).map(|i| duck_cell(row, i)).collect());
    }
    Ok(out)
}

enum Verdict {
    Match(usize),
    Mismatch {
        n_native: usize,
        n_duck: usize,
        diffs: Vec<(String, String)>,
    },
    NativeSkip(#[allow(dead_code)] String),
    OracleSkip(String),
}

fn compare(mut native: Vec<Vec<Cell>>, mut duck: Vec<Vec<Cell>>) -> Verdict {
    native.sort_by(|a, b| row_lt(a, b));
    duck.sort_by(|a, b| row_lt(a, b));
    if native.len() != duck.len() {
        return Verdict::Mismatch {
            n_native: native.len(),
            n_duck: duck.len(),
            diffs: native
                .iter()
                .zip(&duck)
                .filter(|(a, b)| !rows_eq(a, b))
                .take(3)
                .map(|(a, b)| (fmt_row(a), fmt_row(b)))
                .collect(),
        };
    }
    let diffs: Vec<(String, String)> = native
        .iter()
        .zip(&duck)
        .filter(|(a, b)| !rows_eq(a, b))
        .take(3)
        .map(|(a, b)| (fmt_row(a), fmt_row(b)))
        .collect();
    if diffs.is_empty() {
        Verdict::Match(native.len())
    } else {
        Verdict::Mismatch {
            n_native: native.len(),
            n_duck: duck.len(),
            diffs,
        }
    }
}

fn rows_eq(a: &[Cell], b: &[Cell]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| cell_eq(x, y))
}

fn main() {
    // Args: [sfN] [qNN] — a leading `sfN` selects the data scale (default
    // sf1); the remaining arg filters to one query (verbose mode).
    let mut args = std::env::args().skip(1).peekable();
    let sf = if args.peek().is_some_and(|a| a.starts_with("sf")) {
        args.next().expect("peeked")
    } else {
        "sf1".to_string()
    };
    let only = args.next();
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf();
    let data = workspace.join("examples/tpcds/data").join(&sf);
    let qdir = workspace.join("examples/tpcds/queries/spark");
    if !data.join("store_sales.parquet").exists() {
        println!("skip: TPC-DS {sf} data not found at {}", data.display());
        return;
    }

    // Native engine catalog.
    let mut catalog = Catalog::new();
    for t in TABLES {
        catalog
            .register_parquet(t, data.join(format!("{t}.parquet")))
            .expect("register native table");
    }
    // DuckDB views over the same Parquet.
    let duck = duckdb::Connection::open_in_memory().expect("duckdb");
    for t in TABLES {
        let p = data.join(format!("{t}.parquet"));
        duck.execute_batch(&format!(
            "CREATE VIEW {t} AS SELECT * FROM read_parquet('{}');",
            p.display()
        ))
        .expect("duck view");
    }

    // Panics in native bind/execute are coverage data — keep them quiet
    // unless debugging a single query.
    if only.is_none() {
        std::panic::set_hook(Box::new(|_| {}));
    }

    // Queries that BIND + are correct but exceed this box's memory on the
    // full sweep (an OOM SIGKILL can't be caught, so it would kill the whole
    // run). Skipped only in a sweep; still runnable by explicit name. q72:
    // catalog_sales ⋈ inventory ON item_sk — a fact-to-fact fan-out on a
    // low-cardinality key; pending join-planning work.
    const OOM_SKIP: &[&str] = &[];
    let mut names: Vec<String> = std::fs::read_dir(&qdir)
        .expect("query dir")
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".sql"))
        .filter(|n| {
            only.as_ref()
                .is_none_or(|o| n == &format!("{o}.sql") || n == o)
        })
        .filter(|n| only.is_some() || !OOM_SKIP.iter().any(|s| n == &format!("{s}.sql")))
        .collect();
    names.sort_by_key(|n| {
        let d: String = n
            .trim_start_matches('q')
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        (d.parse::<u32>().unwrap_or(0), n.clone())
    });

    let mut verdicts: Vec<(String, Verdict)> = Vec::new();
    for name in &names {
        let qname = name.trim_end_matches(".sql").to_string();
        let sql = std::fs::read_to_string(qdir.join(name)).expect("read");
        let sql = sql.trim().trim_end_matches(';').to_string();

        // Native engine over the canonical Spark text.
        let native: Result<Vec<Vec<Cell>>, String> = catch_unwind(AssertUnwindSafe(|| {
            let bq = bind_sql(&sql, &catalog).map_err(|e| format!("bind: {e}"))?;
            let r = execute(&bq).map_err(|e| format!("exec: {e}"))?;
            Ok(r.rows
                .iter()
                .map(|row| row.iter().map(native_cell).collect())
                .collect())
        }))
        .unwrap_or_else(|_| Err("panic".into()));

        let verdict = match native {
            Err(e) => Verdict::NativeSkip(e),
            Ok(nrows) => {
                // DuckDB oracle over the SAME canonical text (Spark
                // backtick identifiers → DuckDB double-quotes; q90's bare
                // `at` alias is a DuckDB reserved word — quote it).
                let duck_sql = sql
                    .replace('`', "\"")
                    .replace(") at,", ") \"at\",")
                    // q77: `returns` as a BARE (no AS) alias is reserved in
                    // DuckDB; with AS it parses fine.
                    .replace(", 0) returns,", ", 0) AS returns,");
                match duck_rows(&duck, &duck_sql) {
                    Err(e) => Verdict::OracleSkip(e),
                    Ok(drows) => compare(nrows, drows),
                }
            }
        };
        if only.is_some() {
            if let Verdict::Mismatch { diffs, .. } = &verdict {
                for (a, b) in diffs {
                    println!("  native: {a}");
                    println!("  duck:   {b}\n");
                }
            }
        }
        verdicts.push((qname, verdict));
    }
    let _ = std::panic::take_hook();

    let (mut ok, mut mismatch, mut oskip) = (0, 0, 0);
    for (name, v) in &verdicts {
        let line = match v {
            Verdict::Match(n) => {
                ok += 1;
                format!("PARITY_OK   rows={n}")
            }
            Verdict::Mismatch {
                n_native, n_duck, ..
            } => {
                mismatch += 1;
                format!("MISMATCH    native={n_native} duck={n_duck}")
            }
            Verdict::NativeSkip(_) => "native_skip".into(),
            Verdict::OracleSkip(e) => {
                oskip += 1;
                format!(
                    "ORACLE_SKIP {}",
                    e.replace('\n', " ").chars().take(60).collect::<String>()
                )
            }
        };
        // Suppress the native-skip noise unless a single query was asked for.
        if only.is_some() || !matches!(v, Verdict::NativeSkip(_)) {
            println!("{name:<8} {line}");
        }
    }
    let executing = ok + mismatch + oskip;
    println!("\n=== TPC-DS native-vs-DuckDB parity ({sf}) ===");
    println!("  native executes:  {executing}/{}", verdicts.len());
    println!("  parity OK:        {ok}/{executing}");
    println!("  MISMATCH:         {mismatch}");
    println!("  oracle skipped:   {oskip}");
    if mismatch > 0 {
        std::process::exit(1);
    }
}
