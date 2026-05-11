//! Σ.D5 day-1: TPC-H Q14 — `SUM(CASE WHEN p THEN x ELSE 0)` fusion.
//!
//! Issue [#52]. Q14 is the simplest test of the Σ.D5 hypothesis —
//! a 2-way join (lineitem ⋈ part), no GROUP BY, two SUMs of which one
//! has a CASE-WHEN guard:
//!
//! ```sql
//! 100.00 * sum(case when p_type like 'PROMO%'
//!                   then l_extendedprice * (1 - l_discount)
//!                   else 0 end)
//!         / sum(l_extendedprice * (1 - l_discount))
//! ```
//!
//! Today's DataFusion path:
//!   1. FilterExec (WHERE shipdate range) — materializes BooleanArray.
//!   2. ProjectionExec evaluates the CASE-WHEN per row, producing a
//!      new column. Internally, this is another BooleanArray
//!      (p_type LIKE 'PROMO%') multiplied by the revenue expression.
//!   3. AggregateExec runs two SUM kernels, one per output.
//!
//! The fused-pass kernel collapses all of this:
//!
//! ```rust
//! for row in joined_batches {
//!     let revenue = price[i] * (1.0 - disc[i]);
//!     total += revenue;
//!     if p_type.value(i).starts_with("PROMO") {
//!         promo += revenue;
//!     }
//! }
//! ```
//!
//! One pass, no BooleanArray materialization, the prefix-check is a
//! per-row branch the autovectorizer can fold into the hot loop.
//!
//! Reference (M3 Pro / SF=1):
//!   * PySpark Q14                            119.2 ms (2026-05-11)
//!
//! Usage:
//!     cargo run --release -p ematix-flow-core --example tpch_q14_tune
//!
//! [#52]: https://github.com/ryan-evans-git/ematix-flow/issues/52

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{
    Array, Float64Array, RecordBatch, StringViewArray,
};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use futures_util::TryStreamExt;

const Q14: &str = include_str!("../../../examples/tpch/queries/q14.sql");

const Q14_JOIN_SQL: &str = "
    select p_type, l_extendedprice, l_discount
    from lineitem, part
    where l_partkey = p_partkey
      and l_shipdate >= date '1995-09-01'
      and l_shipdate < date '1995-10-01'
";

const Q14_AGG_ONLY_SQL: &str = "
    select 100.00 * sum(case when p_type like 'PROMO%'
                              then l_extendedprice * (1 - l_discount)
                              else 0 end)
                  / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
    from joined
";

/// Single-pass fused dual-SUM with inline CASE-WHEN prefix check.
fn run_fused_q14(batches: &[RecordBatch]) -> (f64, f64) {
    let mut promo: f64 = 0.0;
    let mut total: f64 = 0.0;
    let promo_prefix = b"PROMO";
    for batch in batches {
        let p_type = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("p_type Utf8View");
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
        let price_v = price.values();
        let disc_v = disc.values();
        for i in 0..batch.num_rows() {
            let revenue = price_v[i] * (1.0 - disc_v[i]);
            total += revenue;
            let bytes = p_type.value(i).as_bytes();
            if bytes.len() >= 5 && &bytes[..5] == promo_prefix {
                promo += revenue;
            }
        }
    }
    (promo, total)
}

/// Parallel variant — shard the batches across threads.
fn run_fused_q14_parallel(batches: &[RecordBatch], workers: usize) -> (f64, f64) {
    let n = batches.len();
    let chunk = n.div_ceil(workers.max(1));
    std::thread::scope(|s| {
        let handles: Vec<_> = (0..workers)
            .map(|w| {
                let lo = (w * chunk).min(n);
                let hi = ((w + 1) * chunk).min(n);
                let slice = &batches[lo..hi];
                s.spawn(move || run_fused_q14(slice))
            })
            .collect();
        let mut promo = 0.0;
        let mut total = 0.0;
        for h in handles {
            let (p, t) = h.join().unwrap();
            promo += p;
            total += t;
        }
        (promo, total)
    })
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
    println!("==> Σ.D5 day-1: Q14 — 2-way join + dual SUM with inline CASE-WHEN");
    println!("==> data: {data_dir}");
    println!("==> reference: PySpark 119.2 ms");
    println!();

    println!("--- Section 1: DataFusion Q14 end-to-end (parquet) ---");
    let ctx = make_parquet_ctx(data_dir).await;
    bench_sql("default SessionConfig, parquet, full Q14", &ctx, Q14).await;
    println!();

    println!("--- Section 2: pre-join + DataFusion agg-only ---");
    let staging = make_parquet_ctx(data_dir).await;
    let df = staging.sql(Q14_JOIN_SQL).await.unwrap();
    let schema = Arc::new(df.schema().as_arrow().clone());
    let joined: Vec<RecordBatch> = df
        .execute_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let total_rows: usize = joined.iter().map(|b| b.num_rows()).sum();
    println!(
        "  (joined: {} batches, {total_rows} rows post-join)",
        joined.len()
    );
    let mem = MemTable::try_new(schema, vec![joined.clone()]).unwrap();
    let mem_ctx = SessionContext::new();
    mem_ctx.register_table("joined", Arc::new(mem)).unwrap();
    bench_sql(
        "DataFusion agg-only over pre-joined MemTable",
        &mem_ctx,
        Q14_AGG_ONLY_SQL,
    )
    .await;
    println!();

    println!("--- Section 3: hand-written fused dual-SUM + inline CASE, single-thread ---");
    let (promo_warm, total_warm) = run_fused_q14(&joined);
    let mut times = Vec::with_capacity(5);
    for _ in 0..5 {
        let start = Instant::now();
        let _ = run_fused_q14(&joined);
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "  hand-written fused, single-thread                             median {:>6.2} ms  (min {:>5.2}  max {:>5.2})",
        times[2], times[0], times[4],
    );

    let ncpu = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    println!();
    println!("--- Section 4: hand-written fused dual-SUM + inline CASE, {ncpu}-thread ---");
    let _ = run_fused_q14_parallel(&joined, ncpu);
    let mut times = Vec::with_capacity(5);
    for _ in 0..5 {
        let start = Instant::now();
        let _ = run_fused_q14_parallel(&joined, ncpu);
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "  hand-written fused, parallel                                  median {:>6.2} ms  (min {:>5.2}  max {:>5.2})",
        times[2], times[0], times[4],
    );

    println!();
    println!("--- Section 5: sanity — Q14 final ratio ---");
    let promo_revenue = 100.0 * promo_warm / total_warm;
    println!("  promo {promo_warm:.4} / total {total_warm:.4} = {promo_revenue:.4}%");
}
