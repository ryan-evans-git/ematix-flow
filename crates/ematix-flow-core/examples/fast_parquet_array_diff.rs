//! Σ.E2 diagnostic: dump the per-column Arrow array representation of
//! the first batch of Q01's lineitem projection from both paths.
//!
//! Hypothesis under test: the 2.2× FilterExec slowdown for Q01 SF=10
//! comes from different in-memory array encodings (e.g. dictionary vs
//! materialized Utf8). The EXPLAIN ANALYZE output showed FilterExec
//! emitting 3.5 GB from DF vs 2.3 GB from FastParquet for identical
//! rows — strong signal of representation mismatch.
//!
//! Q01 projects: [l_quantity, l_extendedprice, l_discount, l_tax,
//!                l_returnflag, l_linestatus, l_shipdate]
//!
//! Usage:
//!   TPCH_DATA_DIR=$(pwd)/examples/tpch/data/sf10 \
//!     cargo run --release -p ematix-flow-core --example fast_parquet_array_diff

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::execution::TaskContext;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use futures_util::StreamExt;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const Q01_PROJECTION: &[&str] = &[
    "l_quantity",
    "l_extendedprice",
    "l_discount",
    "l_tax",
    "l_returnflag",
    "l_linestatus",
    "l_shipdate",
];

fn dump_batch(label: &str, batch: &RecordBatch) {
    println!("=== {label} ===");
    println!("  rows: {}", batch.num_rows());
    println!("  cols: {}", batch.num_columns());
    println!(
        "  total memory: {} bytes ({:.2} MB)",
        batch.get_array_memory_size(),
        batch.get_array_memory_size() as f64 / (1024.0 * 1024.0)
    );
    for (i, field) in batch.schema().fields().iter().enumerate() {
        let arr = batch.column(i);
        let mem = arr.get_array_memory_size();
        let buf_layout = arr.to_data();
        let buffer_count = buf_layout.buffers().len();
        let buffer_bytes: usize = buf_layout.buffers().iter().map(|b| b.len()).sum();
        let null_count = buf_layout.null_count();
        let has_nulls_buf = buf_layout.nulls().is_some();
        println!(
            "    [{i}] {:<20} {:?}  mem={} buffers={} buf_bytes={} nulls={}/{}",
            field.name(),
            arr.data_type(),
            mem,
            buffer_count,
            buffer_bytes,
            null_count,
            if has_nulls_buf { "Y" } else { "N" },
        );
    }
}

async fn first_batch_via_datafusion(parquet_path: &str) -> RecordBatch {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.register_parquet("lineitem", parquet_path, Default::default())
        .await
        .unwrap();
    let cols = Q01_PROJECTION.join(", ");
    // No LIMIT — we want a real batch, not a 1-row truncation. We'll
    // only inspect the first batch the stream emits.
    let sql = format!("SELECT {cols} FROM lineitem");
    let df = ctx.sql(&sql).await.unwrap();
    let mut stream = df.execute_stream().await.unwrap();
    stream
        .next()
        .await
        .expect("at least one batch")
        .expect("batch decodes ok")
}

async fn first_batch_via_fast_parquet(parquet_path: &str) -> RecordBatch {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let prov = FastParquetTableProvider::try_new(parquet_path.to_string()).unwrap();
    // Find the field indices of the Q01 projection.
    let projection: Vec<usize> = Q01_PROJECTION
        .iter()
        .map(|name| {
            prov.schema()
                .fields()
                .iter()
                .position(|f| f.name() == name)
                .unwrap_or_else(|| panic!("no field named {name}"))
        })
        .collect();
    let state = ctx.state();
    let exec = prov
        .scan(&state, Some(&projection), &[], None)
        .await
        .unwrap();
    let task_ctx = Arc::new(TaskContext::default());
    let mut stream = exec.execute(0, task_ctx).unwrap();
    stream
        .next()
        .await
        .expect("at least one batch")
        .expect("batch decodes ok")
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let data_dir = match std::env::var("TPCH_DATA_DIR") {
        Ok(s) => PathBuf::from(s),
        Err(_) => manifest
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("examples/tpch/data/sf1"),
    };
    let parquet = data_dir
        .join("lineitem.parquet")
        .to_string_lossy()
        .into_owned();
    println!("==> Q01 projection first-batch array dump");
    println!("==> data: {parquet}");
    println!();

    let df_batch = first_batch_via_datafusion(&parquet).await;
    dump_batch("DataFusion default (register_parquet)", &df_batch);
    println!();

    let fp_batch = first_batch_via_fast_parquet(&parquet).await;
    dump_batch("FastParquetTableProvider", &fp_batch);
    println!();

    // Per-column delta — quick visual scan.
    println!("=== Per-column type comparison ===");
    let df_schema = df_batch.schema();
    let fp_schema = fp_batch.schema();
    for (i, name) in Q01_PROJECTION.iter().enumerate() {
        let dt_df = df_schema.field(i).data_type();
        let dt_fp = fp_schema.field(i).data_type();
        let tag = if dt_df == dt_fp { "OK " } else { "DIFF" };
        println!("  {tag}  {name:<20} df={dt_df:?}  fp={dt_fp:?}");
    }
}
