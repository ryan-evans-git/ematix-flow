//! Measure the PLANNED Q08 (SQL → bind → sequential interpreted executor)
//! against the clock, at a chosen scale factor — the honest "interpreter
//! gap" baseline before the planner grows into the parallel morsel driver.
//!
//! Usage: `cargo run --release -p ematix-flow-engine --example
//! planned_q08_bench [sf1|sf10] [trials]`

use std::path::PathBuf;
use std::time::Instant;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::plan::execute;
use ematix_flow_engine::vector::LogicalType;

const Q08_SQL: &str = "\
select extract(year from o_orderdate) as o_year, \
       sum(case when n2.n_name = 'BRAZIL' \
                then l_extendedprice * (1 - l_discount) else 0 end) \
         / sum(l_extendedprice * (1 - l_discount)) as mkt_share \
from part, supplier, lineitem, orders, customer, nation n1, nation n2, region \
where p_partkey = l_partkey \
  and s_suppkey = l_suppkey \
  and l_orderkey = o_orderkey \
  and o_custkey = c_custkey \
  and c_nationkey = n1.n_nationkey \
  and n1.n_regionkey = r_regionkey \
  and s_nationkey = n2.n_nationkey \
  and r_name = 'AMERICA' \
  and o_orderdate between date '1995-01-01' and date '1996-12-31' \
  and p_type = 'ECONOMY ANODIZED STEEL' \
group by extract(year from o_orderdate)";

fn main() {
    let sf = std::env::args().nth(1).unwrap_or_else(|| "sf10".into());
    let trials: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let data =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("../../examples/tpch/data/{sf}"));
    let t = |name: &str| data.join(format!("{name}.parquet"));

    use LogicalType::*;
    let mut c = Catalog::new();
    c.register_table(
        "part",
        t("part"),
        &[("p_partkey", 0, Int64), ("p_type", 4, Utf8)],
    );
    c.register_table(
        "supplier",
        t("supplier"),
        &[("s_suppkey", 0, Int64), ("s_nationkey", 3, Int64)],
    );
    c.register_table(
        "lineitem",
        t("lineitem"),
        &[
            ("l_orderkey", 0, Int64),
            ("l_partkey", 1, Int64),
            ("l_suppkey", 2, Int64),
            ("l_extendedprice", 5, Float64),
            ("l_discount", 6, Float64),
        ],
    );
    c.register_table(
        "orders",
        t("orders"),
        &[
            ("o_orderkey", 0, Int64),
            ("o_custkey", 1, Int64),
            ("o_orderdate", 4, Date32),
        ],
    );
    c.register_table(
        "customer",
        t("customer"),
        &[("c_custkey", 0, Int64), ("c_nationkey", 3, Int64)],
    );
    c.register_table(
        "nation",
        t("nation"),
        &[
            ("n_nationkey", 0, Int64),
            ("n_name", 1, Utf8),
            ("n_regionkey", 2, Int64),
        ],
    );
    c.register_table(
        "region",
        t("region"),
        &[("r_regionkey", 0, Int64), ("r_name", 1, Utf8)],
    );

    let q = bind_sql(Q08_SQL, &c).expect("bind");
    // Warm-up + correctness echo.
    let r = execute(&q).expect("execute");
    println!("{sf} Q08 planned rows:");
    for row in &r.rows {
        println!("  {row:?}");
    }
    let mut times = Vec::new();
    for i in 0..trials {
        let t0 = Instant::now();
        let r = execute(&q).expect("execute");
        let ms = t0.elapsed().as_secs_f64() * 1e3;
        times.push(ms);
        println!("trial {i}: {ms:.1} ms ({} rows)", r.rows.len());
    }
    times.sort_by(|a, b| a.total_cmp(b));
    println!(
        "planned Q08 {sf}: median {:.1} ms over {trials} trials",
        times[trials / 2]
    );
}
