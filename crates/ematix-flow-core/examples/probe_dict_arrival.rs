//! Probe: do TPC-H lineitem/orders columns arrive at the Arrow surface
//! as `Dictionary(UInt32, Utf8)` (the only shape EnableDictGroupCountRule
//! recognises) — or as plain Utf8/Utf8View?

use datafusion::prelude::SessionContext;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use futures_util::TryStreamExt;
use std::sync::Arc;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let dir = "examples/tpch/data/sf1";
    let candidates: &[(&str, &str, &str)] = &[
        ("lineitem", "l_returnflag", "Emat+FastParquet"),
        ("lineitem", "l_linestatus", "Emat+FastParquet"),
        ("lineitem", "l_shipmode",   "Emat+FastParquet"),
        ("orders",   "o_orderpriority", "FastParquet"),
        ("part",     "p_brand", "FastParquet"),
        ("part",     "p_type",  "FastParquet"),
    ];
    println!("=== probe: dict-arrival shape on TPC-H SF=1 columns ===\n");
    for (table, col, providers) in candidates {
        println!("--- {table}.{col}  ({providers}) ---");
        // Emat path (lineitem only — others lack column types Emat supports)
        if *table == "lineitem" {
            let ctx = SessionContext::new();
            let path = format!("{dir}/{table}.parquet");
            let prov = EmatixFastParquetTableProvider::try_new(&path).unwrap();
            ctx.register_table(*table, Arc::new(prov)).unwrap();
            let df = ctx.sql(&format!("SELECT {col} FROM {table} LIMIT 1")).await.unwrap();
            let plan = df.create_physical_plan().await.unwrap();
            let mut s = plan.execute(0, ctx.task_ctx()).unwrap();
            let batch = s.try_next().await.unwrap().unwrap();
            println!("  Emat:        {:?}", batch.schema().field(0).data_type());
        }
        // FastParquet path
        let ctx = SessionContext::new();
        let path = format!("{dir}/{table}.parquet");
        let prov = FastParquetTableProvider::try_new(&path).unwrap();
        ctx.register_table(*table, Arc::new(prov)).unwrap();
        let df = ctx.sql(&format!("SELECT {col} FROM {table} LIMIT 1")).await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let mut s = plan.execute(0, ctx.task_ctx()).unwrap();
        let batch = s.try_next().await.unwrap().unwrap();
        println!("  FastParquet: {:?}", batch.schema().field(0).data_type());
        // DataFusion's default parquet reader
        let ctx = SessionContext::new();
        ctx.register_parquet(*table, &path, Default::default()).await.unwrap();
        let df = ctx.sql(&format!("SELECT {col} FROM {table} LIMIT 1")).await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let mut s = plan.execute(0, ctx.task_ctx()).unwrap();
        let batch = s.try_next().await.unwrap().unwrap();
        println!("  default:     {:?}", batch.schema().field(0).data_type());
        println!();
    }
}
