//! Run Q07 at SF=10 with and without Σ.AD; compare row count + values.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::array::Array;
use datafusion::arrow::array::AsArray;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::preset;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf10"));
    let sql = std::fs::read_to_string("examples/tpch/queries/q07.sql")?;

    // Baseline.
    let baseline = run(&dir, &sql, false).await?;
    // With Σ.AD.
    let rewritten = run(&dir, &sql, true).await?;

    if baseline.len() != rewritten.len() {
        return Err(format!(
            "row count differs: baseline={}, rewritten={}",
            baseline.len(),
            rewritten.len()
        )
        .into());
    }
    let mut max_rel = 0.0_f64;
    for (i, (b, r)) in baseline.iter().zip(rewritten.iter()).enumerate() {
        let rel = if b.abs() > 1e-12 {
            ((r - b) / b).abs()
        } else {
            (r - b).abs()
        };
        if rel > max_rel {
            max_rel = rel;
        }
        if rel > 1e-6 {
            return Err(format!("row {i}: baseline={b}, rewritten={r}, rel_err={rel:e}").into());
        }
    }
    println!(
        "Q07 SF=10 Σ.AD: PASS ({} rows, max rel err = {:e})",
        baseline.len(),
        max_rel
    );
    Ok(())
}

async fn run(
    dir: &std::path::Path,
    sql: &str,
    with_dim_push: bool,
) -> Result<Vec<f64>, Box<dyn std::error::Error>> {
    let state = preset::with_optimizer_rules(
        SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(14))
            .with_default_features(),
    )
    .build();
    let ctx = SessionContext::new_with_state(state);
    for t in TPCH_TABLES {
        let path = dir
            .join(format!("{t}.parquet"))
            .to_string_lossy()
            .to_string();
        let prov = EmatixFastParquetTableProvider::try_new(path)?;
        ctx.register_table(*t, Arc::new(prov))?;
    }
    let df = ctx.sql(sql).await?;
    let plan = df.into_optimized_plan()?;
    // Always apply Σ.U (matches bench config when EMAT_AGG_SEMI=1).
    let plan = ematix_flow_core::agg_filter_pushdown::push_filter_into_agg(plan)?;
    let plan = if with_dim_push {
        ematix_flow_core::dim_join_pushdown::push_dim_join_into_chain(plan)?
    } else {
        plan
    };
    let df = ctx.execute_logical_plan(plan).await?;
    let batches = df.collect().await?;
    // Extract revenue column (last column) as f64.
    let mut out = Vec::new();
    for b in &batches {
        let last = b.num_columns() - 1;
        let arr = b.column(last);
        let f = arr.as_primitive_opt::<datafusion::arrow::datatypes::Float64Type>();
        if let Some(f64arr) = f {
            for i in 0..f64arr.len() {
                if f64arr.is_null(i) {
                    out.push(f64::NAN);
                } else {
                    out.push(f64arr.value(i));
                }
            }
        }
    }
    Ok(out)
}
