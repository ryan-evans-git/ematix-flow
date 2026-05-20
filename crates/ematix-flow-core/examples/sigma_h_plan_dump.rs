//! Σ.H scaffold: dump the physical plan tree for a target TPC-H
//! query so the new `filter_join_multi_agg` shape can be designed
//! against real DataFusion output.
//!
//! Run:
//!   cargo run --release -p ematix-flow-core \
//!       --example sigma_h_plan_dump -- 3 5 10 19
//!
//! Each numeric arg is a TPC-H query ID. Output goes to stdout as
//! `displayable(...).indent(false)` — DataFusion's standard
//! plan-display.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;

use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("usage: sigma_h_plan_dump <q_id> [<q_id> ...]");
        std::process::exit(1);
    }

    let repo_root = env::current_dir()?;
    let data_dir = repo_root.join("examples/tpch/data/sf1");
    let ctx = SessionContext::new();
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        let path_str = path.to_str().ok_or("path not utf-8")?.to_string();
        let prov = EmatixFastParquetTableProvider::try_new(path_str)?.with_dict_preservation(true);
        ctx.register_table(*t, Arc::new(prov))?;
    }

    for id in args {
        let n: u32 = id.parse()?;
        let path = repo_root.join(format!("examples/tpch/queries/q{:02}.sql", n));
        let sql = fs::read_to_string(&path)?;
        println!("================= Q{:02} =================", n);
        println!("--- SQL ---");
        println!("{}", sql.trim());
        println!("--- physical plan ---");
        match ctx.sql(&sql).await {
            Ok(df) => match df.create_physical_plan().await {
                Ok(p) => println!("{}", displayable(p.as_ref()).indent(false)),
                Err(e) => println!("(plan error) {e}"),
            },
            Err(e) => println!("(parse error) {e}"),
        }
        println!();
    }

    Ok(())
}
