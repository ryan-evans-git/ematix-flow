//! Q10 SF=100 wide-string PRIZE-BOUND spike (de-risk for the LateGather lever).
//!
//! Fresh SF=100 EXPLAIN ANALYZE + a DuckDB profile (2026-06-23) reframed Q10's
//! loss: the lineitem DECODE is at parity with DuckDB (ematix 9.60s vs DuckDB
//! scan+filter 9.93s). The real gap is (a) a separable FilterExec over 600M rows
//! and (b) carrying the FIVE wide customer/nation strings (c_name, c_address,
//! c_phone, c_comment, n_name) through the 11.46M-row join intermediate and then
//! hashing them in the 7-col group-by key:
//!   c⋈o join 2.98s  +  nation join 1.29s  +  agg group-id 4.19s
//! vs DuckDB's 0.90 / 0.09 / 1.65. The wide-string handling is the dominant
//! engine-fixable excess, NOT the agg kernel in isolation (FD-agg was NO-GO).
//!
//! This spike BOUNDS the prize WITHOUT building the LateGather wiring: it runs the
//! stock Q10 against "narrow" variants that drop the wide strings, so the
//! optimizer prunes them from the scan/join and groups on a narrow key. The gap
//! (stock − narrow) is the upper bound on what a wide-string late-materialization
//! lever (LateGatherExec + BuildRowId, already banked) could recover. If narrow
//! approaches DuckDB's ~2.0s wall, the lever is validated and worth the build.
//!
//!   TPCH_DATA_DIR (default examples/tpch/data/sf100), TRIALS (5), WARMUPS (2)
//!   EMAT_EXPLAIN_ANALYZE=<variant>  (stock|narrow|narrow_nation) — per-op dump

use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::Array;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::preset;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TABLES: &[&str] = &["lineitem", "orders", "customer", "nation"];

fn cpu_secs() -> f64 {
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut u) } != 0 {
        return f64::NAN;
    }
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    tv(u.ru_utime) + tv(u.ru_stime)
}
fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn build_ctx(data_dir: &str) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let state = preset::with_optimizer_rules(
        SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_default_features(),
    )
    .build();
    let ctx = SessionContext::new_with_state(state);
    for t in TABLES {
        let p = format!("{data_dir}/{t}.parquet");
        ctx.register_table(*t, Arc::new(EmatixFastParquetTableProvider::try_new(p)?))?;
    }
    Ok(ctx)
}

const FROM_WHERE: &str = "from customer, orders, lineitem, nation \
    where c_custkey=o_custkey and l_orderkey=o_orderkey \
    and o_orderdate>=date '1993-10-01' and o_orderdate<date '1994-01-01' \
    and l_returnflag='R' and c_nationkey=n_nationkey";

fn sql_for(variant: &str) -> String {
    match variant {
        // Full Q10 — 7-col wide-string group key, all 5 wide strings carried.
        "stock" => format!(
            "select c_custkey, c_name, sum(l_extendedprice*(1-l_discount)) as revenue, \
             c_acctbal, n_name, c_address, c_phone, c_comment {FROM_WHERE} \
             group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment \
             order by revenue desc"
        ),
        // LOWER BOUND: group by c_custkey (i64) only; no wide strings anywhere.
        // The optimizer prunes c_name/address/phone/comment/acctbal from the
        // customer scan and (likely) the nation join entirely.
        "narrow" => format!(
            "select c_custkey, sum(l_extendedprice*(1-l_discount)) as revenue {FROM_WHERE} \
             group by c_custkey order by revenue desc"
        ),
        // The FD-minimal key {c_custkey, n_name}: keeps the nation join + n_name in
        // the group key (a short 25-NDV string), but drops the 5 WIDE customer
        // strings. This is ≈ what a LateGather agg would hash (modulo rowid).
        "narrow_nation" => format!(
            "select c_custkey, n_name, sum(l_extendedprice*(1-l_discount)) as revenue {FROM_WHERE} \
             group by c_custkey, n_name order by revenue desc"
        ),
        // P2 (DataFusion-native late-mat): narrow agg on {c_custkey, n_name}, then
        // RE-JOIN customer to re-attach the 5 wide strings onto the 3.88M groups.
        // All native DataFusion (parallel Partitioned join + SinglePartitioned agg →
        // eff ~12), NO EmatixHashJoinExec / custom gather. Customer is scanned twice:
        // narrow in the agg subquery, wide in the re-attach. Tests whether the
        // eff-12 path carries a cheap re-attach (vs the documented arm-B NO-GO).
        "reattach" => format!(
            "with agg as (select c_custkey, n_name, \
               sum(l_extendedprice*(1-l_discount)) as revenue {FROM_WHERE} \
               group by c_custkey, n_name) \
             select a.c_custkey, c.c_name, a.revenue, c.c_acctbal, a.n_name, \
               c.c_address, c.c_phone, c.c_comment \
             from agg a, customer c where a.c_custkey = c.c_custkey \
             order by a.revenue desc"
        ),
        other => panic!("unknown variant {other}"),
    }
}

async fn explain_analyze(
    ctx: &SessionContext,
    sql: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Warm once so decode/build are hot, then dump the annotated plan.
    let _ = collect(
        ctx.sql(sql).await?.create_physical_plan().await?,
        ctx.task_ctx(),
    )
    .await?;
    let ea = ctx
        .sql(&format!("EXPLAIN ANALYZE {sql}"))
        .await?
        .collect()
        .await?;
    for b in &ea {
        let c = b.column(b.num_columns() - 1);
        if let Some(s) = c
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
        {
            for i in 0..s.len() {
                println!("{}", s.value(i));
            }
        }
    }
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir =
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf100".to_string());
    let trials: usize = std::env::var("TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let warmups: usize = std::env::var("WARMUPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);
    unsafe {
        std::env::set_var("EMAT_PLAN_CACHE", "0");
    }
    let ctx = build_ctx(&data_dir)?;

    if let Ok(v) = std::env::var("EMAT_EXPLAIN_ANALYZE") {
        let variant = if v == "1" { "stock".to_string() } else { v };
        println!("== EXPLAIN ANALYZE variant={variant} ==");
        return explain_analyze(&ctx, &sql_for(&variant))
            .await
            .map_err(Into::into);
    }

    println!("Q10 wide-string prize bound  data={data_dir}  warmups={warmups} trials={trials}\n");
    println!(
        "{:<16} {:>9} {:>8} {:>6} {:>10}",
        "variant", "wall_ms", "cpu_s", "eff", "rows"
    );
    let variants = ["stock", "narrow_nation", "narrow", "reattach"];
    let mut stock_wall = 0.0;
    for variant in variants {
        let sql = sql_for(variant);
        let fresh = |ctx: &SessionContext, sql: String| {
            let ctx = ctx.clone();
            async move { ctx.sql(&sql).await?.create_physical_plan().await }
        };
        for _ in 0..warmups {
            let _ = collect(fresh(&ctx, sql.clone()).await?, ctx.task_ctx()).await?;
        }
        let (mut ws, mut cs) = (vec![], vec![]);
        let mut rows = 0usize;
        for _ in 0..trials {
            let p = fresh(&ctx, sql.clone()).await?;
            let c0 = cpu_secs();
            let t = Instant::now();
            let out = collect(p, ctx.task_ctx()).await?;
            ws.push(t.elapsed().as_secs_f64() * 1000.0);
            cs.push(cpu_secs() - c0);
            rows = out.iter().map(|b| b.num_rows()).sum();
        }
        let (wm, cm) = (median(&mut ws), median(&mut cs));
        println!(
            "{variant:<16} {wm:>9.1} {cm:>8.2} {:>6.1} {rows:>10}",
            cm / (wm / 1000.0)
        );
        if variant == "stock" {
            stock_wall = wm;
        } else {
            println!(
                "{:>16}   -> {:.0}ms saved vs stock ({:.1}% of stock wall)",
                "",
                stock_wall - wm,
                (stock_wall - wm) / stock_wall * 100.0
            );
        }
    }
    println!(
        "\nDuckDB SF=100 floor (profiled 2026-06-23): ~1950ms wall / ~20.5 CPU-s.\n\
         If `narrow` approaches that, wide-string late-materialization is the lever."
    );
    Ok(())
}
