//! Σ.D6 day-1: TPC-H Q10 hand-written fused agg + bounded-heap top-K.
//!
//! Issue [#53]. Q10 is the canonical "top customers by revenue" workload —
//! 4-way join (customer ⋈ orders ⋈ lineitem ⋈ nation) with date and
//! return-flag filters, group by 7 customer columns, ORDER BY revenue
//! DESC (and typically LIMIT 20 in real use, though our q10.sql doesn't
//! cap it).
//!
//! Σ.D6's insight: instead of producing every group and sorting all of
//! them, maintain a **bounded min-heap of size N** during the aggregate
//! pass. End state: the heap holds exactly the top N. Cost is O(G log N)
//! per shard instead of O(G log G); for typical N=20 and G in the
//! millions, that's a meaningful difference.
//!
//! For a phase-1 day-1 prototype we use the customer-key as the sole
//! group key (it transitively determines all the other group columns
//! via the join) to keep the hand-written loop tight.
//!
//! Reference (M3 Pro / SF=1):
//!   * PySpark Q10                                  460.6 ms
//!
//! Usage:
//!     cargo run --release -p ematix-flow-core --example tpch_q10_tune
//!
//! [#53]: https://github.com/ryan-evans-git/ematix-flow/issues/53

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Array, Float64Array, Int64Array, RecordBatch};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use futures_util::TryStreamExt;

const Q10: &str = include_str!("../../../examples/tpch/queries/q10.sql");

const Q10_JOIN_SQL: &str = "
    select c_custkey, l_extendedprice, l_discount
    from customer, orders, lineitem, nation
    where c_custkey = o_custkey
      and l_orderkey = o_orderkey
      and o_orderdate >= date '1993-10-01'
      and o_orderdate < date '1994-01-01'
      and l_returnflag = 'R'
      and c_nationkey = n_nationkey
";

/// Hand-written fused aggregate + top-K: build per-shard hashmap, then
/// fold each shard's groups into a bounded min-heap of size `top_n`.
/// Top of the heap is the *smallest* revenue we'd evict; if a new
/// group's revenue exceeds it we pop the smallest, push the new one.
fn run_fused_q10_topk(batches: &[RecordBatch], top_n: usize) -> Vec<(i64, f64)> {
    // Phase A: aggregate.
    let mut groups: HashMap<i64, f64> = HashMap::with_capacity(16_384);
    for batch in batches {
        let custkey = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("c_custkey Int64");
        let price = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("l_extendedprice f64");
        let disc = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("l_discount f64");
        let ck_v = custkey.values();
        let price_v = price.values();
        let disc_v = disc.values();
        for i in 0..batch.num_rows() {
            let rev = price_v[i] * (1.0 - disc_v[i]);
            *groups.entry(ck_v[i]).or_insert(0.0) += rev;
        }
    }
    // Phase B: bounded min-heap. We want the top N by revenue, so the
    // heap stores tuples ordered by `Reverse(revenue)` — the heap's
    // top (= max of Reverse(x)) is the *smallest* revenue, the one
    // we'd evict next.
    let mut heap: BinaryHeap<(Reverse<u64>, i64)> = BinaryHeap::with_capacity(top_n + 1);
    for (ck, rev) in &groups {
        // f64 doesn't impl Ord; pack into u64 via to_bits but flip the
        // sign so that bitwise-Reverse-min == numeric-min. For positive
        // revenues (all our values), to_bits is monotonic, so we can
        // compare directly.
        let key = Reverse(rev.to_bits());
        if heap.len() < top_n {
            heap.push((key, *ck));
        } else if let Some(&(top, _)) = heap.peek() {
            // top is the *smallest* revenue currently in the heap
            // (because of Reverse). If our new rev is bigger
            // (rev.to_bits() > top.0 for positives), it deserves
            // to be in the top N.
            if rev.to_bits() > top.0 {
                heap.pop();
                heap.push((key, *ck));
            }
        }
    }
    // Drain heap, sort descending by revenue.
    let mut out: Vec<(i64, f64)> = heap
        .into_iter()
        .map(|(k, ck)| (ck, f64::from_bits(k.0)))
        .collect();
    out.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    out
}

async fn make_parquet_ctx(parquet_dir: &str) -> SessionContext {
    let ctx = SessionContext::new();
    for table in [
        "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
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
    println!("==> Σ.D6 day-1: Q10 — 4-way join + GROUP BY + ORDER LIMIT");
    println!("==> data: {data_dir}");
    println!("==> reference: PySpark 460.6 ms");
    println!();

    println!("--- Section 1: DataFusion Q10 end-to-end (parquet, full sort, no LIMIT) ---");
    let ctx = make_parquet_ctx(data_dir).await;
    bench_sql("default SessionConfig", &ctx, Q10).await;
    println!();

    println!("--- Section 2: DataFusion Q10 + LIMIT 20 (parquet) ---");
    let limit_sql = format!("{} limit 20", Q10.trim().trim_end_matches(';'));
    bench_sql(
        "default SessionConfig, with explicit LIMIT 20",
        &ctx,
        &limit_sql,
    )
    .await;
    println!();

    println!("--- Section 3: pre-join + DataFusion aggregate + top 20 ---");
    let staging = make_parquet_ctx(data_dir).await;
    let df = staging.sql(Q10_JOIN_SQL).await.unwrap();
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
        "DataFusion agg-only + order/limit 20 over MemTable",
        &mem_ctx,
        "select c_custkey, sum(l_extendedprice * (1 - l_discount)) as revenue \
         from joined group by c_custkey order by revenue desc limit 20",
    )
    .await;
    println!();

    println!("--- Section 4: hand-written fused agg + heap top-20 ---");
    let warm = run_fused_q10_topk(&joined, 20);
    let mut times = Vec::with_capacity(5);
    for _ in 0..5 {
        let start = Instant::now();
        let _ = run_fused_q10_topk(&joined, 20);
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "  hand-written fused + top-20 heap, single-thread               median {:>6.2} ms  (min {:>5.2}  max {:>5.2})",
        times[2], times[0], times[4],
    );
    println!();

    println!("--- Section 5: sanity — top 5 customers by revenue ---");
    for (ck, rev) in warm.iter().take(5) {
        println!("    c_custkey={ck} revenue={rev:.4}");
    }
}
