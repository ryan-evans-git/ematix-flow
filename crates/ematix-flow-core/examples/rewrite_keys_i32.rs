//! REV.8.u32 spike — rewrite TPC-H key columns from Int64 → Int32.
//!
//! De-risks the decoder-level "downcast on read" lever (the only viable
//! form of DuckDB's `__internal_compress_integral_uinteger`, per the
//! Σ.Q.L11 rejection). Instead of modifying the parquet decoder, we
//! *pre-store* the Q18 join/group keys as Int32 (they fit: orderkeys ≤
//! ~600M at SF=100 < i32::MAX). Running Q18 on this data exercises the
//! native i32 agg/join path with NO per-row CAST — isolating the
//! key-width benefit that Σ.Q.L11's SQL `ARROW_CAST` could not (its
//! +1.7% was the per-row cast cost, not the key width).
//!
//! Usage:
//!   SRC_DIR=examples/tpch/data/sf10 OUT_DIR=/tmp/sf10_i32 \
//!     cargo run --release -p ematix-flow-core --example rewrite_keys_i32

use std::env;

use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let src = env::var("SRC_DIR").expect("SRC_DIR");
    let out = env::var("OUT_DIR").expect("OUT_DIR");
    std::fs::create_dir_all(&out).expect("create OUT_DIR");

    // Q18's keys: c_custkey, o_orderkey, o_custkey, l_orderkey. (DuckDB
    // compresses exactly these.)
    let tables: Vec<(&str, Vec<&str>)> = vec![
        ("customer", vec!["c_custkey"]),
        ("orders", vec!["o_orderkey", "o_custkey"]),
        ("lineitem", vec!["l_orderkey"]),
    ];

    let ctx = SessionContext::new();
    for (t, keys) in tables {
        let path = format!("{src}/{t}.parquet");
        ctx.register_parquet(t, &path, ParquetReadOptions::default())
            .await?;
        let schema = ctx.table(t).await?.schema().clone();
        let proj: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| {
                let name = f.name();
                if keys.contains(&name.as_str()) {
                    format!("CAST(\"{name}\" AS INT) AS \"{name}\"")
                } else {
                    format!("\"{name}\"")
                }
            })
            .collect();
        let outpath = format!("{out}/{t}.parquet");
        let _ = std::fs::remove_file(&outpath);
        let sql = format!(
            "COPY (SELECT {} FROM \"{t}\") TO '{outpath}' STORED AS PARQUET",
            proj.join(", ")
        );
        println!("rewriting {t} ({} cols, keys→Int32: {keys:?}) -> {outpath}", schema.fields().len());
        ctx.sql(&sql).await?.collect().await?;
    }
    println!("done");
    Ok(())
}
