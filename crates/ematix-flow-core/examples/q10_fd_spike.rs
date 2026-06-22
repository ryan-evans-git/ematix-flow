//! Q10 FD-group-key-minimization Phase-0 de-risk spike (SF=100). KILL-GATE.
//!
//! Q10's SF=100 loss is COMPUTE-EXCESS: the customer aggregate groups by 7 cols
//! (c_custkey + c_name/c_acctbal/c_phone/n_name/c_address/c_comment), spending
//! ~4.86 CPU-s in `time_calculating_group_ids` (98% of the agg) hashing 5 WIDE
//! STRING cols, on a 43.5GB materialized output — while the SUM itself is 55ms.
//! DuckDB groups by c_custkey alone (the other 6 are functionally dependent).
//!
//! This hand-writes the FD-minimized query two ways and measures whether the
//! target plan shape beats the current 7-key plan, all through the PRODUCTION
//! preset ctx (preset::with_optimizer_rules == run_shard.rs). KILL-GATE: if
//! neither variant beats `base` isolated-warm at SF=100, stop (honours the prior
//! `Q10.WS.0` NO-GO); if one wins, build the optimizer rule.
//!
//!   TPCH_DATA_DIR (default examples/tpch/data/sf100), TRIALS (5), WARMUPS (2)

use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Array, Float64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::preset;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TABLES: &[&str] = &[
    "lineitem", "orders", "customer", "supplier", "nation", "region", "part", "partsupp",
];

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

/// Cumulative process CPU seconds (user+sys, all threads) via getrusage. Diff
/// around a collect → that query's total CPU; /wall = mean cores busy.
fn cpu_secs() -> f64 {
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut u) } != 0 {
        return f64::NAN;
    }
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    tv(u.ru_utime) + tv(u.ru_stime)
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

/// (rows, sum_revenue) — order-independent equivalence check across variants.
fn checksum(batches: &[RecordBatch]) -> (usize, f64) {
    let mut rows = 0usize;
    let mut sum = 0.0f64;
    for b in batches {
        rows += b.num_rows();
        if let Ok(idx) = b.schema().index_of("revenue") {
            if let Some(a) = b.column(idx).as_any().downcast_ref::<Float64Array>() {
                for i in 0..a.len() {
                    if a.is_valid(i) {
                        sum += a.value(i);
                    }
                }
            }
        }
    }
    (rows, sum)
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

    // base: the current production plan — GROUP BY 7 cols (incl. 5 wide strings).
    let base = "select c_custkey, c_name, sum(l_extendedprice*(1-l_discount)) as revenue, \
        c_acctbal, n_name, c_address, c_phone, c_comment \
        from customer, orders, lineitem, nation \
        where c_custkey=o_custkey and l_orderkey=o_orderkey \
        and o_orderdate>=date '1993-10-01' and o_orderdate<date '1994-01-01' \
        and l_returnflag='R' and c_nationkey=n_nationkey \
        group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment \
        order by revenue desc";

    // A: group by c_custkey only; carry the FD-dependent cols via first_value
    // (no wide-string hashing in the group key). May fail to parse on some
    // DataFusion versions — caught per-variant; B is the robust arm.
    let var_a = "select c_custkey, first_value(c_name) as c_name, \
        sum(l_extendedprice*(1-l_discount)) as revenue, first_value(c_acctbal) as c_acctbal, \
        first_value(n_name) as n_name, first_value(c_address) as c_address, \
        first_value(c_phone) as c_phone, first_value(c_comment) as c_comment \
        from customer, orders, lineitem, nation \
        where c_custkey=o_custkey and l_orderkey=o_orderkey \
        and o_orderdate>=date '1993-10-01' and o_orderdate<date '1994-01-01' \
        and l_returnflag='R' and c_nationkey=n_nationkey \
        group by c_custkey order by revenue desc";

    // B: group orders⋈lineitem by o_custkey (i64 key), join customer+nation back
    // for the wide cols. Kills BOTH the wide-key group-ids AND the 43.5GB agg
    // output. Uses only sum + joins → guaranteed to parse.
    let var_b = "with agg as (select o_custkey as ck, \
        sum(l_extendedprice*(1-l_discount)) as revenue \
        from orders, lineitem \
        where l_orderkey=o_orderkey and o_orderdate>=date '1993-10-01' \
        and o_orderdate<date '1994-01-01' and l_returnflag='R' group by o_custkey) \
        select c_custkey, c_name, agg.revenue, c_acctbal, n_name, c_address, c_phone, c_comment \
        from agg, customer, nation \
        where agg.ck=c_custkey and c_nationkey=n_nationkey order by agg.revenue desc";

    let variants = [
        ("base(7-key)", base),
        ("A(first_value)", var_a),
        ("B(groupPK+join)", var_b),
    ];

    println!("Q10 FD-minimization spike  data={data_dir}  warmups={warmups} trials={trials}");
    println!(
        "{:<18} {:>9} {:>8} {:>5} {:>11} {:>16}  check",
        "variant", "wall_ms", "cpu_s", "eff", "rows", "sum_revenue"
    );

    let mut base_ck: Option<(usize, f64)> = None;
    let mut base_wall = f64::NAN;
    for (name, sql) in &variants {
        let mut errored = false;
        for _ in 0..warmups {
            let ctx = build_ctx(&data_dir)?;
            match ctx.sql(sql).await {
                Ok(df) => match df.collect().await {
                    Ok(_) => {}
                    Err(e) => {
                        println!("{name:<18} COLLECT ERR: {e}");
                        errored = true;
                        break;
                    }
                },
                Err(e) => {
                    println!("{name:<18} SQL ERR: {e}");
                    errored = true;
                    break;
                }
            }
        }
        if errored {
            continue;
        }
        let mut walls = Vec::new();
        let mut cpus = Vec::new();
        let mut ck = (0usize, 0.0f64);
        for _ in 0..trials {
            let ctx = build_ctx(&data_dir)?;
            let c0 = cpu_secs();
            let t = Instant::now();
            let df = ctx.sql(sql).await?;
            let batches = df.collect().await?;
            walls.push(t.elapsed().as_secs_f64() * 1000.0);
            cpus.push(cpu_secs() - c0);
            ck = checksum(&batches);
        }
        let wm = median(&mut walls);
        let cm = median(&mut cpus);
        let eff = cm / (wm / 1000.0);
        let check = match base_ck {
            None => {
                base_ck = Some(ck);
                base_wall = wm;
                "(baseline)".to_string()
            }
            Some((r, s)) => {
                let ok = ck.0 == r && (ck.1 - s).abs() <= (s.abs() * 1e-6).max(1.0);
                let speedup = base_wall / wm;
                if ok {
                    format!("MATCH  {speedup:.2}x vs base")
                } else {
                    format!("MISMATCH! base=({r} rows, {s:.1})")
                }
            }
        };
        println!(
            "{name:<18} {wm:>9.1} {cm:>8.2} {eff:>5.1} {:>11} {:>16.1}  {check}",
            ck.0, ck.1
        );
    }
    println!(
        "\nKILL-GATE: a variant must MATCH base rows/revenue AND beat base wall to justify the rule."
    );
    Ok(())
}
