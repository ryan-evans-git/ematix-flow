//! Σ.E2 day-1: isolate DataFusion's parquet read cost.
//!
//! Σ.E1's Q14 EXPLAIN ANALYZE pinned `time_elapsed_scanning_total = 86 ms`
//! across 14 partitions (~6 ms wall) as the dominant single cost on a
//! query Polars completes end-to-end in 12.5 ms. This example isolates
//! the read step from any SQL execution to confirm the gap is in the
//! decoder, not in something else.
//!
//! Measurement matrix:
//!   * 4 columns (Q6 shape): l_quantity, l_extendedprice, l_discount, l_shipdate
//!   * 3 columns (Q14 shape): l_partkey, l_extendedprice, l_discount, l_shipdate
//!   * Two flavors: cold (one query per fresh ctx) + warm (re-query same ctx)
//!
//! Reference points to expect:
//!   * Σ.D6 Q10 deep-dive: DataFusion 5.96 ms MemTable vs Polars 1.9 ms — the gap
//!     was the decoder.
//!   * Σ.E1 Q14: lineitem scan ~6 ms wall in EXPLAIN; total query 22.95 ms.

use std::path::PathBuf;
use std::time::Instant;

use datafusion::arrow::array::RecordBatch;
use datafusion::prelude::SessionContext;
use futures_util::TryStreamExt;

async fn make_ctx(parquet_path: &str) -> SessionContext {
    let ctx = SessionContext::new();
    ctx.register_parquet("lineitem", parquet_path, Default::default())
        .await
        .unwrap();
    ctx
}

async fn bench_sql(label: &str, sql: &str, parquet: &str) {
    let mut times = Vec::with_capacity(5);
    // warm-up (1 untimed) — but we want cold-ctx numbers, so we build
    // a fresh ctx each iter to simulate "first read of the day".
    for _ in 0..5 {
        let ctx = make_ctx(parquet).await;
        let start = Instant::now();
        let _: Vec<RecordBatch> = ctx
            .sql(sql)
            .await
            .unwrap()
            .execute_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "  {label:<60}  median {:>6.2} ms  (min {:>5.2}  max {:>5.2})",
        times[2], times[0], times[4],
    );
}

async fn bench_sql_warm(label: &str, sql: &str, parquet: &str) {
    let ctx = make_ctx(parquet).await;
    // Warm-up
    let _: Vec<RecordBatch> = ctx
        .sql(sql)
        .await
        .unwrap()
        .execute_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut times = Vec::with_capacity(5);
    for _ in 0..5 {
        let start = Instant::now();
        let _: Vec<RecordBatch> = ctx
            .sql(sql)
            .await
            .unwrap()
            .execute_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
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
    let parquet = manifest
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("examples/tpch/data/sf1/lineitem.parquet");
    let parquet = parquet.to_str().unwrap();
    println!("==> Σ.E2: DataFusion parquet read isolation");
    println!("==> data: {parquet}");
    println!("==> SF=1 lineitem: 6,001,215 rows, 16 columns, ~620 MB");
    println!();

    println!("--- Cold-context read (parquet open each iteration) ---");
    bench_sql(
        "select * from lineitem  (16 columns, full file)",
        "select * from lineitem",
        parquet,
    )
    .await;
    bench_sql(
        "select 4-col Q6 projection",
        "select l_quantity, l_extendedprice, l_discount, l_shipdate from lineitem",
        parquet,
    )
    .await;
    bench_sql(
        "select 4-col Q6 projection + Q6 filter",
        "select l_quantity, l_extendedprice, l_discount, l_shipdate \
         from lineitem \
         where l_shipdate >= date '1994-01-01' and l_shipdate < date '1995-01-01' \
           and l_discount between 0.05 and 0.07 and l_quantity < 24",
        parquet,
    )
    .await;
    bench_sql(
        "select 4-col Q14 projection + Q14 filter",
        "select l_partkey, l_extendedprice, l_discount, l_shipdate \
         from lineitem \
         where l_shipdate >= date '1995-09-01' and l_shipdate < date '1995-10-01'",
        parquet,
    )
    .await;
    println!();

    println!("--- Warm-context read (parquet metadata cached, hot row groups) ---");
    bench_sql_warm(
        "select * from lineitem  (16 columns)",
        "select * from lineitem",
        parquet,
    )
    .await;
    bench_sql_warm(
        "select 4-col Q6 projection",
        "select l_quantity, l_extendedprice, l_discount, l_shipdate from lineitem",
        parquet,
    )
    .await;
    bench_sql_warm(
        "select 4-col Q6 projection + Q6 filter",
        "select l_quantity, l_extendedprice, l_discount, l_shipdate \
         from lineitem \
         where l_shipdate >= date '1994-01-01' and l_shipdate < date '1995-01-01' \
           and l_discount between 0.05 and 0.07 and l_quantity < 24",
        parquet,
    )
    .await;
    bench_sql_warm(
        "select 4-col Q14 projection + Q14 filter",
        "select l_partkey, l_extendedprice, l_discount, l_shipdate \
         from lineitem \
         where l_shipdate >= date '1995-09-01' and l_shipdate < date '1995-10-01'",
        parquet,
    )
    .await;
    println!();

    println!("==> reference targets:");
    println!("    Q6 end-to-end via DataFusion           ~19 ms  (from `tpch_q6_tune.rs`)");
    println!("    Q6 end-to-end via Polars                ~12 ms  (parquet path)");
    println!("    Q14 end-to-end via DataFusion          ~23 ms  (from EXPLAIN run)");
    println!("    Q14 end-to-end via Polars               ~13 ms");
    println!();
    println!("    If the projection+filter reads above are >50% of the");
    println!("    end-to-end query time, the gap is firmly in the");
    println!("    parquet decoder. If <30%, the gap is in compute.");
}
