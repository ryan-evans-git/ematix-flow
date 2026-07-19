//! TPC-DS front-end coverage sweep: register the 24-table catalog from the
//! parquet footers, push every canonical query text through `bind_sql`
//! (and optionally `execute`), and print a failure taxonomy — the gap map
//! that drives which front-end capabilities to build next.
//!
//! Usage: `cargo run --release -p ematix-flow-engine --example
//! tpcds_coverage [bind|exec] [sf1|sf10] [qNN]`
//!
//! With a third `qNN` argument only that query runs and panics stay
//! loud (backtraces on) — the single-failure debugging mode.

use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
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
    "web_sales",
    "web_site",
];

/// Collapse an error message into a taxonomy bucket: strip
/// query-specific identifiers so the same missing capability groups.
fn bucket(msg: &str) -> String {
    let msg = msg.replace('\n', " ");
    let mut m: &str = &msg;
    if let Some(i) = m.find(" at Line") {
        m = &m[..i];
    }
    let mut out = String::new();
    let mut in_quote = false;
    for ch in m.chars() {
        match ch {
            '\'' | '"' | '`' => in_quote = !in_quote,
            _ if in_quote => {}
            _ => out.push(ch),
        }
        if out.len() >= 90 {
            break;
        }
    }
    out
}

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "bind".into());
    let sf = std::env::args().nth(2).unwrap_or_else(|| "sf1".into());
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpcds");
    let data = root.join("data").join(&sf);

    let mut catalog = Catalog::new();
    for t in TABLES {
        catalog
            .register_parquet(t, data.join(format!("{t}.parquet")))
            .expect("register table");
    }

    let only: Option<String> = std::env::args().nth(3);
    // Panics inside bind/execute are coverage data, not crashes — capture
    // them like errors and keep the hook quiet (unless debugging one).
    if only.is_none() {
        std::panic::set_hook(Box::new(|_| {}));
    }

    // Binds + correct, but exceeds this box's memory on execution — an OOM
    // SIGKILL can't be caught and would kill the whole sweep. Skipped unless
    // named explicitly. q72: catalog_sales ⋈ inventory ON item_sk fan-out.
    const OOM_SKIP: &[&str] = &["q72"];
    let qdir = root.join("queries/spark");
    let mut names: Vec<String> = std::fs::read_dir(&qdir)
        .expect("query dir")
        .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".sql"))
        .filter(|n| only.as_ref().is_none_or(|o| n == &format!("{o}.sql")))
        .filter(|n| only.is_some() || !OOM_SKIP.iter().any(|s| n == &format!("{s}.sql")))
        .collect();
    names.sort_by_key(|n| {
        let stem = n.trim_end_matches(".sql").trim_start_matches('q');
        let digits: String = stem.chars().take_while(|c| c.is_ascii_digit()).collect();
        (digits.parse::<u32>().unwrap_or(0), stem.to_string())
    });

    let mut ok = 0usize;
    let mut failures: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for name in &names {
        let sql = std::fs::read_to_string(qdir.join(name)).expect("read query");
        let sql = sql.trim().trim_end_matches(';');
        let qname = name.trim_end_matches(".sql").to_string();

        let outcome: Result<String, String> = catch_unwind(AssertUnwindSafe(|| {
            let bound = bind_sql(sql, &catalog).map_err(|e| format!("bind: {e}"))?;
            if mode == "exec" {
                let r = execute(&bound).map_err(|e| format!("exec: {e}"))?;
                Ok(format!("OK ({} rows)", r.rows.len()))
            } else {
                Ok("BINDS".to_string())
            }
        }))
        .unwrap_or_else(|p| {
            let msg = p
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "unknown panic".into());
            Err(format!("panic: {msg}"))
        });

        match outcome {
            Ok(status) => {
                ok += 1;
                println!("{qname:8} {status}");
            }
            Err(e) => {
                failures.entry(bucket(&e)).or_default().push(qname.clone());
                println!("{qname:8} FAIL {}", bucket(&e));
            }
        }
    }
    let _ = std::panic::take_hook();

    println!("\n==== {ok}/{} {} ====", names.len(), mode);
    let mut cats: Vec<(usize, &String, &Vec<String>)> =
        failures.iter().map(|(k, v)| (v.len(), k, v)).collect();
    cats.sort_by_key(|c| std::cmp::Reverse(c.0));
    for (n, cat, qs) in cats {
        println!("{n:3}x {cat}");
        println!("      {}", qs.join(" "));
    }
}
