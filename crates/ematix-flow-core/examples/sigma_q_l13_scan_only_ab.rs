//! Σ.Q.L13 SPIKE — scan-only A/B: lineitem SF=10, ematix vs DuckDB.
//!
//! The per-stage profile of Q05/Q07/Q08 SF=10 showed lineitem scan
//! dominates ematix's cumulative CPU. This spike isolates whether the
//! gap is in:
//!   (a) raw parquet decode itself,
//!   (b) the bloom/predicate-apply path inside EmatixFastParquetExec,
//!   (c) some interaction with the downstream operator coalesce/repart.
//!
//! Test queries (run on lineitem.parquet at SF=10):
//!   T1 (decode-only):      SELECT count(l_orderkey), count(l_extendedprice), ...
//!                          forces decode of Q07's 5 cols
//!   T2 (decode + filter):  + WHERE l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
//!                          adds a column predicate
//!   T3 (decode + IN-list): + AND l_suppkey IN (100 keys)
//!                          simulates the bloom membership cost
//!
//! Same SQL run on both engines; wall-time A/B.
//!
//! Usage:
//!   cargo run --release -p ematix-flow-core --example sigma_q_l13_scan_only_ab \
//!     --features triangulation

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

const TRIALS: usize = 9;
const WARMUPS: usize = 3;

/// 100 fake supplier keys distributed across the keyspace. Simulates a
/// nation-filtered supplier list for Q07-shape bloom predicate cost.
const SIM_BLOOM_KEYS: &[i64] = &[
    33, 1001, 2017, 3041, 4079, 5101, 6113, 7129, 8147, 9173,
    10193, 11213, 12239, 13259, 14281, 15307, 16333, 17359, 18389, 19421,
    20443, 21467, 22501, 23533, 24571, 25601, 26633, 27659, 28687, 29717,
    30743, 31769, 32801, 33829, 34861, 35893, 36919, 37951, 38981, 40009,
    41039, 42061, 43093, 44119, 45151, 46181, 47207, 48239, 49267, 50291,
    51319, 52343, 53371, 54401, 55429, 56459, 57487, 58513, 59539, 60571,
    61601, 62629, 63659, 64687, 65713, 66743, 67771, 68803, 69829, 70859,
    71887, 72917, 73943, 74971, 76001, 77027, 78053, 79081, 80111, 81139,
    82171, 83197, 84229, 85253, 86281, 87311, 88339, 89369, 90397, 91423,
    92453, 93481, 94511, 95537, 96569, 97597, 98621, 99649, 99990, 99000,
];

/// T1: decode-only. Sum of computed expressions touching all 5 cols.
/// `sum(col % small_prime)` avoids stats-based short-circuit (DuckDB's
/// `count(col)` and `max(col)` can be answered from parquet stats).
fn t1_sql() -> String {
    "SELECT \
       sum(l_extendedprice * (1.0 - l_discount)), \
       sum(l_orderkey % 7) + sum(l_suppkey % 11), \
       sum(extract(year FROM l_shipdate)) \
     FROM lineitem".to_string()
}

/// T2: T1 + date-range filter. Adds column predicate evaluation. The
/// shipdate filter is the same as Q07's. Row-group stats may prune
/// some groups; the filter still needs per-row evaluation for the
/// surviving groups' boundary pages.
fn t2_sql() -> String {
    "SELECT \
       sum(l_extendedprice * (1.0 - l_discount)), \
       sum(l_orderkey % 7) + sum(l_suppkey % 11) \
     FROM lineitem \
     WHERE l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'".to_string()
}

/// T3: T2 + IN-list on l_suppkey. Simulates the cost of L9's runtime
/// bloom: a per-row membership test against ~100 keys. If T3 - T2 is
/// large for ematix but small for DuckDB, the IN-list / bloom-apply
/// path is the lever (not raw decode).
fn t3_sql() -> String {
    let in_list = SIM_BLOOM_KEYS
        .iter()
        .map(|k| k.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "SELECT \
           sum(l_extendedprice * (1.0 - l_discount)), \
           sum(l_orderkey % 7) \
         FROM lineitem \
         WHERE l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' \
           AND l_suppkey IN ({in_list})"
    )
}

fn median(xs: &mut [f64]) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    xs[xs.len() / 2]
}

fn stddev(xs: &[f64], mean: f64) -> f64 {
    let var: f64 = xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / xs.len() as f64;
    var.sqrt()
}

async fn time_ematix_sql(ctx: &SessionContext, sql: &str) -> f64 {
    let t = Instant::now();
    let _ = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    t.elapsed().as_secs_f64() * 1000.0
}

fn time_duckdb_sql(conn: &duckdb::Connection, sql: &str) -> f64 {
    let t = Instant::now();
    let mut stmt = conn.prepare(sql).unwrap();
    let mut rows = stmt.query([]).unwrap();
    while let Some(_row) = rows.next().unwrap() {
        // consume
    }
    t.elapsed().as_secs_f64() * 1000.0
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::var("TPCH_DATA_DIR")
        .unwrap_or_else(|_| "examples/tpch/data/sf10".to_string());
    let lineitem_path = PathBuf::from(&dir).join("lineitem.parquet");

    // ----- ematix-flow + EmatixFastParquet (our full stack) -----
    let state_emat = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features()
        .build();
    let ctx_emat = SessionContext::new_with_state(state_emat);
    let prov_emat = EmatixFastParquetTableProvider::try_new(lineitem_path.to_string_lossy())?;
    ctx_emat.register_table("lineitem", Arc::new(prov_emat))?;

    // ----- ematix-flow + FastParquet (DataFusion's native reader) -----
    // Same DataFusion machinery, but parquet decode goes through the
    // standard datafusion-parquet path instead of ematix-parquet.
    // Compare-to-DuckDB shows whether ematix-parquet's filter path is
    // the gap, vs the datafusion-parquet path that uses standard
    // arrow-rs parquet reader.
    let state_fp = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features()
        .build();
    let ctx_fp = SessionContext::new_with_state(state_fp);
    let prov_fp = FastParquetTableProvider::try_new(lineitem_path.to_string_lossy())?;
    ctx_fp.register_table("lineitem", Arc::new(prov_fp))?;

    // ----- DuckDB -----
    let duckdb_conn = duckdb::Connection::open_in_memory()?;
    duckdb_conn.execute_batch("PRAGMA threads=14")?;
    duckdb_conn.execute_batch(&format!(
        "CREATE VIEW lineitem AS SELECT * FROM read_parquet('{}')",
        lineitem_path.display()
    ))?;

    let tests: &[(&str, String, &str)] = &[
        ("T1 decode-only (5 cols)", t1_sql(), "All 5 Q07 cols, no filter"),
        ("T2 decode+date filter",   t2_sql(), "4 cols + l_shipdate predicate"),
        ("T3 decode+date+IN(100)",  t3_sql(), "3 cols + date + 100-key IN-list"),
    ];

    println!(
        "=== Σ.Q.L13 — scan-only A/B, lineitem SF=10, {TRIALS} trials × {WARMUPS} warmups ===\n"
    );
    println!(
        "{:<28} {:>9} {:>9} {:>9} {:>11} {:>11}",
        "Test", "Emat p50", "FastP p50", "Duck p50", "Emat/Duck", "FastP/Duck"
    );
    println!("{}", "-".repeat(82));

    for (label, sql, _desc) in tests {
        // Warmups (each engine).
        for _ in 0..WARMUPS {
            let _ = time_ematix_sql(&ctx_emat, sql).await;
            let _ = time_ematix_sql(&ctx_fp, sql).await;
            let _ = time_duckdb_sql(&duckdb_conn, sql);
        }

        let mut emat: Vec<f64> = Vec::with_capacity(TRIALS);
        let mut fp: Vec<f64> = Vec::with_capacity(TRIALS);
        let mut duck: Vec<f64> = Vec::with_capacity(TRIALS);
        for _ in 0..TRIALS {
            emat.push(time_ematix_sql(&ctx_emat, sql).await);
            fp.push(time_ematix_sql(&ctx_fp, sql).await);
            duck.push(time_duckdb_sql(&duckdb_conn, sql));
        }
        let e_med = median(&mut emat.clone());
        let f_med = median(&mut fp.clone());
        let d_med = median(&mut duck.clone());
        let er = e_med / d_med;
        let fr = f_med / d_med;
        println!(
            "{:<28} {:>9.1} {:>9.1} {:>9.1} {:>10.2}× {:>10.2}×",
            label, e_med, f_med, d_med, er, fr
        );
    }

    println!();
    println!("Interpretation:");
    println!("  Emat = ematix-parquet (our optimised reader)");
    println!("  FastP = DataFusion's native parquet reader (datafusion-parquet)");
    println!("  Both run inside the same DataFusion session.");
    println!();
    println!("  If Emat ≫ FastP for T2/T3:  ematix-parquet's filter path is the lever");
    println!("  If FastP ≈ Emat ≫ Duck:    the gap is in DataFusion-level integration");
    println!("                              (e.g. predicate not pushed into scan)");
    println!("  If Emat ≈ FastP ≈ Duck:    no gap; numbers are noise");

    Ok(())
}
