//! Scan-only comparison: read all columns of a query's input tables
//! and count rows. Isolates decode cost from downstream filter / join /
//! aggregate / coalesce overhead.
//!
//! If Emat WINS the scan-only test but LOSES the full query, the gap
//! is in downstream interaction (orchestration / batch shape / view
//! coalesce). If Emat LOSES the scan-only test, decode itself is slow.
//!
//! Run:
//!   cargo run --release --example sigma_e5_scan_only_compare

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use futures_util::TryStreamExt;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

/// (query name, table → columns to scan).
const QUERY_SCANS: &[(&str, &[(&str, &[&str])])] = &[
    // Regressions
    (
        "q16",
        &[
            ("part", &["p_partkey", "p_brand", "p_type", "p_size"]),
            ("partsupp", &["ps_partkey", "ps_suppkey"]),
            ("supplier", &["s_suppkey", "s_comment"]),
        ],
    ),
    (
        "q13",
        &[
            ("customer", &["c_custkey"]),
            ("orders", &["o_orderkey", "o_custkey", "o_comment"]),
        ],
    ),
    (
        "q19",
        &[(
            "lineitem",
            &[
                "l_partkey",
                "l_quantity",
                "l_extendedprice",
                "l_discount",
                "l_shipmode",
                "l_shipinstruct",
            ],
        ),
            ("part", &["p_partkey", "p_brand", "p_size", "p_container"])
        ],
    ),
    (
        "q22",
        &[
            ("customer", &["c_custkey", "c_phone", "c_acctbal"]),
            ("orders", &["o_custkey"]),
        ],
    ),
    // Wins, for comparison
    (
        "q06",
        &[(
            "lineitem",
            &["l_shipdate", "l_discount", "l_quantity", "l_extendedprice"],
        )],
    ),
    (
        "q14",
        &[(
            "lineitem",
            &["l_partkey", "l_shipdate", "l_extendedprice", "l_discount"],
        ), ("part", &["p_partkey", "p_type"])],
    ),
];

fn data_dir() -> String {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    match std::env::var("TPCH_DATA_DIR") {
        Ok(s) => s,
        Err(_) => manifest
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("examples/tpch/data/sf1")
            .to_string_lossy()
            .into_owned(),
    }
}

fn make_ctx() -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features()
        .build();
    SessionContext::new_with_state(state)
}

fn register_emat(ctx: &SessionContext, dir: &str) {
    for t in TABLES {
        let path = format!("{dir}/{t}.parquet");
        let prov = EmatixFastParquetTableProvider::try_new(path).unwrap();
        ctx.register_table(*t, Arc::new(prov)).unwrap();
    }
}

fn register_fast(ctx: &SessionContext, dir: &str) {
    for t in TABLES {
        let path = format!("{dir}/{t}.parquet");
        let prov = FastParquetTableProvider::try_new(path).unwrap();
        ctx.register_table(*t, Arc::new(prov)).unwrap();
    }
}

async fn time_scan(ctx: &SessionContext, sql: &str, iters: usize) -> f64 {
    // Warmup
    for _ in 0..3 {
        let df = ctx.sql(sql).await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let stream = datafusion::physical_plan::execute_stream(plan, ctx.task_ctx()).unwrap();
        let _ = stream.try_collect::<Vec<_>>().await.unwrap();
    }
    let t0 = Instant::now();
    for _ in 0..iters {
        let df = ctx.sql(sql).await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let stream = datafusion::physical_plan::execute_stream(plan, ctx.task_ctx()).unwrap();
        let _ = stream.try_collect::<Vec<_>>().await.unwrap();
    }
    t0.elapsed().as_secs_f64() * 1000.0 / iters as f64
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let dir = data_dir();
    let iters: usize = std::env::var("SCAN_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(40);

    println!("scan-only compare: dir={dir} iters={iters}");
    println!("{:5}  {:10}  {:8}  {:8}  {:8}  {:>7}", "query", "table", "cols", "fast", "emat", "delta");
    println!("{}", "-".repeat(70));

    for (query, scans) in QUERY_SCANS {
        let mut total_fast = 0.0;
        let mut total_emat = 0.0;
        for (table, columns) in *scans {
            // Build an expression that forces each column to be
            // materialized. `count(*)` would be pushed down to row-
            // counting and never touch the columns. We sum a hash of
            // each column instead — DataFusion can't elide the read.
            let agg: Vec<String> = columns
                .iter()
                .map(|c| format!("max(cast({c} as varchar))"))
                .collect();
            let sql = format!("SELECT {} FROM {}", agg.join(", "), table);

            let ctx_fast = make_ctx();
            register_fast(&ctx_fast, &dir);
            let fast_ms = time_scan(&ctx_fast, &sql, iters).await;

            let ctx_emat = make_ctx();
            register_emat(&ctx_emat, &dir);
            let emat_ms = time_scan(&ctx_emat, &sql, iters).await;

            let delta = (emat_ms / fast_ms - 1.0) * 100.0;
            println!(
                "{:5}  {:10}  {:8}  {:7.2}  {:7.2}  {:+6.1}%",
                query,
                table,
                columns.len(),
                fast_ms,
                emat_ms,
                delta
            );
            total_fast += fast_ms;
            total_emat += emat_ms;
        }
        let delta_total = (total_emat / total_fast - 1.0) * 100.0;
        println!(
            "{:5}  {:10}  {:8}  {:7.2}  {:7.2}  {:+6.1}%  (total)",
            query, "", "", total_fast, total_emat, delta_total
        );
        println!();
    }
}
