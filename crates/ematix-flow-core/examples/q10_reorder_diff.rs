//! Q10.RG plan-diff (2026-06-09): why does the SHAPE-GATED reorder leave
//! Q10's permissive-reorder win (~236ms SF=100) on the table?
//!
//! Reproduces the production `FlowQueryPlanner::rewrite` prefix
//! (`push_filter_into_agg` -> `push_dim_join_into_chain`) on Q10's optimized
//! logical plan, then runs BOTH reorder entry points
//! (`reorder_inner_joins_shape_gated` = production, `reorder_inner_joins` =
//! permissive) with `EMAT_REORDER_DEBUG=1` so the per-chain gate verdict
//! prints. Tells us EXACTLY which gate constraint
//! (blind / composite / ambiguous / like / aggkey / too_many) rejects Q10
//! under shape-gating while the permissive path accepts it.
//!
//! Plan-time ONLY — no query execution; near-zero CPU (footer reads for stats).
//!
//!   EMAT_REORDER_DEBUG=1 TPCH_DATA_DIR=examples/tpch/data/sf100 TPCH_QUERY=10 \
//!     cargo run --release -p ematix-flow-core --example q10_reorder_diff --features triangulation

use std::path::Path;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("examples/tpch/data/sf100"));
    let q: u8 = std::env::var("TPCH_QUERY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let sql_path = format!("examples/tpch/queries/q{q:02}.sql");
    let raw = std::fs::read_to_string(&sql_path)?;
    let sql = raw.trim().trim_end_matches(';');
    eprintln!("Q{q:02} plan-diff, data={}", data_dir.display());

    let ctx = SessionContext::new();
    register_tables(&ctx, &data_dir)?;

    let optimized = ctx.sql(sql).await?.into_optimized_plan()?;
    println!("================ OPTIMIZED LOGICAL PLAN (pre-rewrite) ================");
    println!("{}", optimized.display_indent());

    // Reproduce FlowQueryPlanner::rewrite prefix (agg_semi -> dim_push) so the
    // reorder sees exactly the plan production hands it. Functions are
    // best-effort: an unchanged plan means the rule didn't fire on Q10.
    let mut pre = optimized.clone();
    match ematix_flow_core::agg_filter_pushdown::push_filter_into_agg(pre.clone()) {
        Ok(p) => {
            let changed = format!("{}", p.display_indent()) != format!("{}", pre.display_indent());
            println!("\n[agg_semi]  fired = {changed}");
            pre = p;
        }
        Err(e) => println!("\n[agg_semi]  err: {e}"),
    }
    match ematix_flow_core::dim_join_pushdown::push_dim_join_into_chain(pre.clone()) {
        Ok(p) => {
            let changed = format!("{}", p.display_indent()) != format!("{}", pre.display_indent());
            println!("[dim_push]  fired = {changed}");
            pre = p;
        }
        Err(e) => println!("[dim_push]  err: {e}"),
    }
    let pre_str = format!("{}", pre.display_indent());
    println!("\n================ PRE-REORDER PLAN (what reorder sees) ================");
    println!("{pre_str}");

    // The gate verdict prints to stderr (EMAT_REORDER_DEBUG=1). Label each call.
    println!("\n================ SHAPE-GATED reorder (production path) ================");
    eprintln!("\n>>>>> SHAPE-GATED gate verdicts >>>>>");
    let gated = ematix_flow_core::join_reorder::reorder_inner_joins_shape_gated(pre.clone())?;
    let gated_str = format!("{}", gated.display_indent());
    println!("{gated_str}");

    println!("\n================ PERMISSIVE reorder ================");
    eprintln!("\n>>>>> PERMISSIVE gate verdicts >>>>>");
    let perm = ematix_flow_core::join_reorder::reorder_inner_joins(pre.clone())?;
    let perm_str = format!("{}", perm.display_indent());
    println!("{perm_str}");

    println!("\n================ SUMMARY ================");
    println!("shape-gated reordered Q10 : {}", gated_str != pre_str);
    println!("permissive  reordered Q10 : {}", perm_str != pre_str);
    println!("shape-gated == permissive : {}", gated_str == perm_str);
    if gated_str == pre_str && perm_str != pre_str {
        println!(
            "VERDICT: shape-gate REJECTS Q10 (left = pre-reorder order); permissive ACCEPTS. \
             See the SHAPE-GATED gate verdict line above for the blocking constraint."
        );
    } else if gated_str == perm_str && gated_str != pre_str {
        println!(
            "VERDICT: both reorder identically — the 236ms is NOT a gate difference; \
             look elsewhere (re-optimize pass / partition / noise)."
        );
    }
    Ok(())
}

fn register_tables(
    ctx: &SessionContext,
    data_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        let use_emat = *t == "lineitem" || *t == "orders";
        if use_emat {
            ctx.register_table(
                *t,
                Arc::new(EmatixFastParquetTableProvider::try_new(
                    path.to_string_lossy(),
                )?),
            )?;
        } else {
            ctx.register_table(
                *t,
                Arc::new(FastParquetTableProvider::try_new(path.to_string_lossy())?),
            )?;
        }
    }
    Ok(())
}
