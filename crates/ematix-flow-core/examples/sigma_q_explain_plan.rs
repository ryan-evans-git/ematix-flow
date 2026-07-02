//! Σ.Q diagnostic — dump structural physical plan (NOT ANALYZE) so we
//! can see which side of each HashJoinExec is build vs probe and whether
//! statistics propagated.
//!
//! Usage:
//!   Q=18 TPCH_DATA_DIR=examples/tpch/data/sf10 \
//!     ./target/release/examples/sigma_q_explain_plan

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::preset;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let q: u8 = std::env::var("Q")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(18);
    let dir =
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf10".to_string());

    // Match the bench's milestone rule set 1:1 via preset.rs so plan
    // dumps reflect what the bench actually runs. Σ.X audit caught
    // two divergences here (missing PushDownLeftSemi rule + missing
    // all-tables-Emat registration) — preset.rs is the single source
    // of truth.
    let state = preset::with_optimizer_rules(
        SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(14))
            .with_default_features(),
    )
    .build();
    let ctx = SessionContext::new_with_state(state);

    // Match the bench's `EMAT_ALL_TABLES_EMAT=1` default: every table
    // registered as `EmatixFastParquetTableProvider` so the L9 bloom
    // sideband rule can target any scan. Set `EMAT_ALL_TABLES_EMAT=0`
    // to revert to lineitem-only Emat for A/B diagnosis.
    let all_emat = std::env::var("EMAT_ALL_TABLES_EMAT")
        .ok()
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true);
    for t in TPCH_TABLES {
        let path = PathBuf::from(&dir).join(format!("{t}.parquet"));
        if all_emat || *t == "lineitem" {
            let prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy())?;
            ctx.register_table(*t, Arc::new(prov))?;
        } else {
            let prov = FastParquetTableProvider::try_new(path.to_string_lossy())?;
            ctx.register_table(*t, Arc::new(prov))?;
        }
    }

    let sql = std::fs::read_to_string(format!("examples/tpch/queries/q{q:02}.sql"))?;
    let df = ctx.sql(&sql).await?;
    if std::env::var("EMAT_DUMP_LOGICAL").is_ok() {
        println!("=== Q{q:02} unoptimized LogicalPlan ===");
        println!("{}", df.logical_plan().display_indent());
        let optimized = df.clone().into_optimized_plan()?;
        println!("\n=== Q{q:02} optimized LogicalPlan ===");
        println!("{}", optimized.display_indent());
    }
    let df = if std::env::var("EMAT_REORDER").is_ok()
        || std::env::var("EMAT_AGG_SEMI").is_ok()
        || std::env::var("EMAT_DIM_PUSH").is_ok()
    {
        let optimized = df.into_optimized_plan()?;
        println!("=== Q{q:02} optimized LogicalPlan (pre-rewrite) ===");
        println!("{}", optimized.display_indent());
        let mut rewritten = optimized;
        if std::env::var("EMAT_AGG_SEMI").is_ok() {
            rewritten = ematix_flow_core::agg_filter_pushdown::push_filter_into_agg(rewritten)?;
        }
        if std::env::var("EMAT_DIM_PUSH").is_ok() {
            rewritten = ematix_flow_core::dim_join_pushdown::push_dim_join_into_chain(rewritten)?;
        }
        if std::env::var("EMAT_REORDER").is_ok() {
            // Σ.AH.X Lever G: default is the permissive path (preserves
            // Σ.T behavior); opt into the gated path via `EMAT_REORDER_SHAPE_GATED=1`.
            let gated = std::env::var("EMAT_REORDER_SHAPE_GATED")
                .ok()
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);
            rewritten = if gated {
                ematix_flow_core::join_reorder::reorder_inner_joins_shape_gated(rewritten)?
            } else {
                ematix_flow_core::join_reorder::reorder_inner_joins(rewritten)?
            };
        }
        println!("\n=== Q{q:02} optimized LogicalPlan (POST-rewrite) ===");
        println!("{}", rewritten.display_indent());
        ctx.execute_logical_plan(rewritten).await?
    } else {
        df
    };
    let plan = df.create_physical_plan().await?;
    println!("\n=== Q{q:02} physical plan ===");
    println!("{}", displayable(plan.as_ref()).indent(true));

    // Σ.AH.3 premise probe: for every HashJoinExec, print join type,
    // partition mode, and each side's REPORTED `num_rows` estimate — the
    // numbers `JoinSelection` (and any stats-driven swap rule) actually
    // sees. A "mis-ordered build" only exists *for a stats-driven rule*
    // when reported build rows exceed reported probe rows; if the runtime
    // rows are inverted but the estimates are not, the gap is estimation
    // (NDV/LIKE selectivity), not side-selection.
    println!("\n=== Q{q:02} HashJoinExec build/probe reported cardinality ===");
    dump_join_stats(&plan, 0);
    Ok(())
}

fn dump_join_stats(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>, depth: usize) {
    use datafusion::physical_plan::joins::HashJoinExec;
    if let Some(hj) = plan.as_any().downcast_ref::<HashJoinExec>() {
        let rows = |side: &Arc<dyn datafusion::physical_plan::ExecutionPlan>| {
            side.partition_statistics(None)
                .ok()
                .map(|s| format!("{:?}", s.num_rows))
                .unwrap_or_else(|| "err".to_string())
        };
        println!(
            "depth={depth} {:?} {:?} on={:?} build(L)={} probe(R)={}",
            hj.join_type(),
            hj.partition_mode(),
            hj.on(),
            rows(hj.left()),
            rows(hj.right()),
        );
    }
    if let Some(f) = plan
        .as_any()
        .downcast_ref::<datafusion::physical_plan::filter::FilterExec>()
    {
        let out_rows = plan
            .partition_statistics(None)
            .ok()
            .map(|s| format!("{:?}", s.num_rows))
            .unwrap_or_else(|| "err".to_string());
        let in_stats = f.input().partition_statistics(None).ok();
        let in_rows = in_stats
            .as_ref()
            .map(|s| format!("{:?}", s.num_rows))
            .unwrap_or_else(|| "err".to_string());
        println!(
            "depth={depth} FilterExec pred={} default_sel={} in_rows={in_rows} out_rows={out_rows}",
            f.predicate(),
            f.default_selectivity(),
        );
        if let Some(s) = in_stats {
            for (i, cs) in s.column_statistics.iter().enumerate() {
                println!(
                    "  input col{i}: min={:?} max={:?} distinct={:?}",
                    cs.min_value, cs.max_value, cs.distinct_count
                );
            }
        }
    }
    for child in plan.children() {
        dump_join_stats(child, depth + 1);
    }
}
