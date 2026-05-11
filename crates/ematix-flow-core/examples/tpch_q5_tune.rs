//! Σ.D4 survey: TPC-H Q5 (6-way join + GROUP BY) hand-written fused
//! post-join aggregate.
//!
//! Issue [#51]. Σ.D4's Q3 day-1 result (`tpch_q3_tune.rs`) showed the
//! fused agg path is 5× faster than DataFusion's aggregate-only step,
//! but Q3 is JOIN-bound so the end-to-end win is only ~7%. This file
//! measures the next post-join target — Q5, a 6-way join with single
//! SUM and small-cardinality GROUP BY (5 Asian nations).
//!
//! The point of the survey is to confirm the **agg-step 5× speedup is
//! consistent across post-join shapes**. If it is, Σ.D4 is worth
//! wrapping as a `FusedPostJoinAggregateExec` even when individual
//! query end-to-end wins are modest, because the speedup compounds
//! with Σ.D5 (CASE-WHEN) and Σ.D6 (top-K) and applies to all post-
//! join multi-agg shapes uniformly.
//!
//! Reference (M3 Pro / SF=1):
//!   * DataFusion Q5 parquet (May 5 baseline)        no Σ.A1 number recorded
//!   * PySpark Q5                                  356.6 ms (2026-05-11)
//!
//! Usage:
//!     cargo run --release -p ematix-flow-core --example tpch_q5_tune
//!
//! [#51]: https://github.com/ryan-evans-git/ematix-flow/issues/51

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Array, Float64Array, RecordBatch, StringViewArray};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use futures_util::TryStreamExt;

const Q5: &str = include_str!("../../../examples/tpch/queries/q05.sql");

const Q5_JOIN_SQL: &str = "
    select n_name, l_extendedprice, l_discount
    from customer, orders, lineitem, supplier, nation, region
    where c_custkey = o_custkey
      and l_orderkey = o_orderkey
      and l_suppkey = s_suppkey
      and c_nationkey = s_nationkey
      and s_nationkey = n_nationkey
      and n_regionkey = r_regionkey
      and r_name = 'ASIA'
      and o_orderdate >= date '1994-01-01'
      and o_orderdate < date '1995-01-01'
";

const Q5_AGG_ONLY_SQL: &str = "
    select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue
    from joined
    group by n_name
    order by revenue desc
";

fn run_fused_q5(batches: &[RecordBatch]) -> HashMap<String, f64> {
    let mut groups: HashMap<String, f64> = HashMap::with_capacity(8);
    for batch in batches {
        let n_name = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("n_name Utf8View");
        let price = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("l_extendedprice Float64");
        let disc = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("l_discount Float64");
        let price_v = price.values();
        let disc_v = disc.values();
        for i in 0..batch.num_rows() {
            let n = n_name.value(i);
            let rev = price_v[i] * (1.0 - disc_v[i]);
            // Avoid allocating a new String per row: probe with &str via
            // `raw_entry` if the key already exists, only allocate on
            // first sight. On stable Rust the `entry_ref` API doesn't
            // exist yet; emulate via `get_mut` + `insert`.
            if let Some(slot) = groups.get_mut(n) {
                *slot += rev;
            } else {
                groups.insert(n.to_string(), rev);
            }
        }
    }
    groups
}

async fn make_parquet_ctx(parquet_dir: &str) -> SessionContext {
    let ctx = SessionContext::new();
    for table in [
        "region", "nation", "supplier", "customer", "part", "partsupp",
        "orders", "lineitem",
    ] {
        let path = format!("{parquet_dir}/{table}.parquet");
        ctx.register_parquet(table, &path, Default::default())
            .await
            .unwrap();
    }
    ctx
}

async fn bench_sql(label: &str, ctx: &SessionContext, sql: &str) {
    let _ = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut times = Vec::with_capacity(5);
    for _ in 0..5 {
        let start = Instant::now();
        let _: Vec<RecordBatch> = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "  {label:<60}  median {:>6.2} ms  (min {:>5.2}  max {:>5.2})",
        times[2], times[0], times[4],
    );
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let data_dir = manifest
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("examples/tpch/data/sf1");
    let data_dir = data_dir.to_str().unwrap();
    println!("==> Σ.D4 survey: Q5 — 6-way join, single SUM, small GROUP BY");
    println!("==> data: {data_dir}");
    println!("==> reference: PySpark 356.6 ms");
    println!();

    println!("--- Section 1: DataFusion Q5 end-to-end (parquet) ---");
    let ctx = make_parquet_ctx(data_dir).await;
    bench_sql("default SessionConfig, parquet, full Q5", &ctx, Q5).await;
    println!();

    println!("--- Section 2: pre-join, then DataFusion aggregate-only ---");
    let staging = make_parquet_ctx(data_dir).await;
    let df = staging.sql(Q5_JOIN_SQL).await.unwrap();
    let schema = Arc::new(df.schema().as_arrow().clone());
    let joined: Vec<RecordBatch> = df
        .execute_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let total: usize = joined.iter().map(|b| b.num_rows()).sum();
    println!(
        "  (joined: {} batches, {total} rows post-join)",
        joined.len()
    );
    let mem = MemTable::try_new(schema, vec![joined.clone()]).unwrap();
    let mem_ctx = SessionContext::new();
    mem_ctx.register_table("joined", Arc::new(mem)).unwrap();
    bench_sql(
        "DataFusion aggregate-only over pre-joined MemTable",
        &mem_ctx,
        Q5_AGG_ONLY_SQL,
    )
    .await;
    println!();

    println!("--- Section 3: hand-written fused, single-thread ---");
    let warm = run_fused_q5(&joined);
    let mut times = Vec::with_capacity(5);
    for _ in 0..5 {
        let start = Instant::now();
        let _ = run_fused_q5(&joined);
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "  hand-written fused, single-thread                             median {:>6.2} ms  (min {:>5.2}  max {:>5.2})",
        times[2], times[0], times[4],
    );

    println!();
    println!("--- Section 4: sanity — Q5 fused output ---");
    let mut all_groups: Vec<(String, f64)> = warm.into_iter().collect();
    all_groups.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    println!("  {} groups (expected: 5 Asian nations):", all_groups.len());
    for (n, rev) in &all_groups {
        println!("    {n}: revenue={rev:.4}");
    }
}
