//! Σ.E5.2 diagnostic: per-operator timing for Q1 (rule OFF) on both
//! providers via DataFusion `EXPLAIN ANALYZE`. The output reports each
//! operator's `elapsed_compute` and `output_rows`, which gives a
//! straight bisect between scan-side cost and aggregate-side cost.
//!
//! Modes:
//!   * FastParquet (Utf8View)
//!   * EmatixFast (Utf8)
//!   * EmatixFast (Dict)

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const Q1_SQL: &str = "
    SELECT
        l_returnflag, l_linestatus,
        sum(l_quantity) AS sum_qty,
        sum(l_extendedprice) AS sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
        avg(l_quantity) AS avg_qty,
        avg(l_extendedprice) AS avg_price,
        avg(l_discount) AS avg_disc,
        count(*) AS count_order
    FROM lineitem
    WHERE l_shipdate <= DATE '1998-09-02'
    GROUP BY l_returnflag, l_linestatus
    ORDER BY l_returnflag, l_linestatus
";

#[derive(Clone, Copy)]
enum Provider { Fast, EmatUtf8, EmatDict }

fn data_path() -> String {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    match std::env::var("TPCH_DATA_DIR") {
        Ok(s) => format!("{s}/lineitem.parquet"),
        Err(_) => manifest
            .parent().unwrap()
            .parent().unwrap()
            .join("examples/tpch/data/sf1/lineitem.parquet")
            .to_string_lossy().into_owned(),
    }
}

async fn build_ctx(path: &str, prov: Provider) -> SessionContext {
    let cfg = SessionConfig::new().with_target_partitions(14);
    let state = SessionStateBuilder::new().with_config(cfg).with_default_features().build();
    let ctx = SessionContext::new_with_state(state);
    match prov {
        Provider::Fast => {
            let p = FastParquetTableProvider::try_new(path).unwrap();
            ctx.register_table("lineitem", Arc::new(p)).unwrap();
        }
        Provider::EmatUtf8 => {
            let p = EmatixFastParquetTableProvider::try_new(path).unwrap();
            ctx.register_table("lineitem", Arc::new(p)).unwrap();
        }
        Provider::EmatDict => {
            let p = EmatixFastParquetTableProvider::try_new(path).unwrap().with_dict_preservation(true);
            ctx.register_table("lineitem", Arc::new(p)).unwrap();
        }
    }
    ctx
}

async fn explain_one(label: &str, ctx: &SessionContext) {
    // Warmup
    for _ in 0..3 {
        let _ = ctx.sql(Q1_SQL).await.unwrap().collect().await.unwrap();
    }
    let q = format!("EXPLAIN ANALYZE {Q1_SQL}");
    let df = ctx.sql(&q).await.unwrap();
    let batches = df.collect().await.unwrap();
    println!("\n================ {label} ================");
    for b in &batches {
        let pretty = datafusion::arrow::util::pretty::pretty_format_batches(&[b.clone()]).unwrap();
        println!("{pretty}");
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let path = data_path();
    println!("==> Σ.E5.2 EXPLAIN ANALYZE: Q1 rule-OFF, 14 partitions");
    println!("==> data: {path}");

    let ctx_fast = build_ctx(&path, Provider::Fast).await;
    let ctx_eutf8 = build_ctx(&path, Provider::EmatUtf8).await;
    let ctx_edict = build_ctx(&path, Provider::EmatDict).await;

    explain_one("FastParquet (Utf8View)", &ctx_fast).await;
    explain_one("EmatixFast (Utf8)", &ctx_eutf8).await;
    explain_one("EmatixFast (Dict)", &ctx_edict).await;
}
