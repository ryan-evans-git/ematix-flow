//! TPC-H Q21 correctness probe — ematix-flow returns 1 row, DuckDB
//! returns 411. Show the actual row(s) and the logical+physical plans
//! so we can see where decorrelation collapses results.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::dedupe_aggregate_rule::DedupeAggregateForFloatDeterminism;
use ematix_flow_core::dict_aggregate_rule::EnableDictGroupCountRule;
use ematix_flow_core::fused_aggregate_filter_multi_agg_rule::InjectFilterMultiAggRule;
use ematix_flow_core::fused_aggregate_filter_sum_rule::InjectFilterSumRule;
use futures_util::TryStreamExt;

const TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = PathBuf::from(
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf1".to_string()),
    );

    let mode = std::env::var("Q21_MODE").unwrap_or_else(|_| "all".to_string());
    println!("Q21_MODE = {mode}  (none / dict / multi / sum / dedupe / v040 / all)");
    let partitions: usize = std::env::var("PARTITIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(14);
    println!("PARTITIONS = {partitions}");
    let mut builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(partitions))
        .with_default_features();
    // Dedupe FIRST — converts duplicated Final+Partial pairs to Single
    // before InjectFilterMultiAggRule / InjectFilterSumRule can consume
    // one side of the pair and leave the other as the parallel-SUM
    // source of f64 non-determinism (Q15).
    if matches!(mode.as_str(), "dedupe" | "all") {
        builder =
            builder.with_physical_optimizer_rule(Arc::new(
                DedupeAggregateForFloatDeterminism::default(),
            ));
    }
    if matches!(mode.as_str(), "dict" | "all" | "v040") {
        builder = builder.with_physical_optimizer_rule(Arc::new(EnableDictGroupCountRule));
    }
    if matches!(mode.as_str(), "multi" | "all" | "v040") {
        builder = builder.with_physical_optimizer_rule(Arc::new(InjectFilterMultiAggRule));
    }
    if matches!(mode.as_str(), "sum" | "all" | "v040") {
        builder = builder.with_physical_optimizer_rule(Arc::new(InjectFilterSumRule));
    }
    let state = builder.build();
    let ctx = SessionContext::new_with_state(state);
    for t in TABLES {
        let path = data_dir.join(format!("{t}.parquet")).to_string_lossy().into_owned();
        if *t == "lineitem" {
            let prov = EmatixFastParquetTableProvider::try_new(path)?;
            ctx.register_table(*t, Arc::new(prov))?;
        } else {
            let prov = FastParquetTableProvider::try_new(path)?;
            ctx.register_table(*t, Arc::new(prov))?;
        }
    }

    let qfile = std::env::var("Q").unwrap_or_else(|_| "21".to_string());
    let sql = std::fs::read_to_string(format!("examples/tpch/queries/q{qfile}.sql"))?;

    let show_plan = std::env::var("SHOW_PLAN").is_ok();
    let show_logical = std::env::var("SHOW_LOGICAL").is_ok();
    println!("=== Q output rows (bench-style: execute_stream only) ===");
    let df = ctx.sql(&sql).await?;
    if show_logical {
        println!("--- logical plan ---");
        println!("{}", df.clone().logical_plan().display_indent_schema());
        println!();
        let optimized = df.clone().into_optimized_plan()?;
        println!("--- optimized logical plan ---");
        println!("{}", optimized.display_indent_schema());
    }
    if show_plan {
        let df_clone = df.clone();
        let plan = df_clone.create_physical_plan().await?;
        println!("--- physical plan ---");
        println!(
            "{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
        );
    }
    let stream = df.execute_stream().await?;
    let batches: Vec<_> = stream.try_collect().await?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("Total rows: {total}");

    // For Q15 specifically: probe the CTE's output to see if SUM values
    // differ when the MultiAgg rule rewrites the CTE.
    if qfile == "15" {
        println!("\n=== Q15 — top revenues by supplier (probe of CTE) ===");
        let probe_sql = "SELECT l_suppkey, sum(l_extendedprice * (1 - l_discount)) AS total_revenue \
                         FROM lineitem \
                         WHERE l_shipdate >= date '1996-01-01' AND l_shipdate < date '1996-04-01' \
                         GROUP BY l_suppkey \
                         ORDER BY total_revenue DESC \
                         LIMIT 3";
        let probe_df = ctx.sql(probe_sql).await?;
        let probe_batches: Vec<_> = probe_df.execute_stream().await?.try_collect().await?;
        println!("{}", pretty_format_batches(&probe_batches)?);
        // And the MAX from the same shape.
        let max_sql = "SELECT MAX(total_revenue) FROM ( \
                       SELECT l_suppkey, sum(l_extendedprice * (1 - l_discount)) AS total_revenue \
                       FROM lineitem \
                       WHERE l_shipdate >= date '1996-01-01' AND l_shipdate < date '1996-04-01' \
                       GROUP BY l_suppkey)";
        let max_df = ctx.sql(max_sql).await?;
        let max_batches: Vec<_> = max_df.execute_stream().await?.try_collect().await?;
        println!("MAX(total_revenue):");
        println!("{}", pretty_format_batches(&max_batches)?);
    }

    // Sanity probes: count what ematix sees independently of the join shape.
    println!("\n=== Sanity probes ===");
    for (label, q) in [
        ("SAUDI suppliers", "SELECT COUNT(*) FROM supplier s JOIN nation n ON s.s_nationkey = n.n_nationkey WHERE n.n_name = 'SAUDI ARABIA'"),
        ("SAUDI suppliers with late F-status lineitems",
         "SELECT COUNT(DISTINCT s.s_suppkey) FROM supplier s JOIN nation n ON s.s_nationkey = n.n_nationkey \
          JOIN lineitem l ON l.l_suppkey = s.s_suppkey \
          JOIN orders o ON o.o_orderkey = l.l_orderkey \
          WHERE n.n_name = 'SAUDI ARABIA' AND o.o_orderstatus = 'F' AND l.l_receiptdate > l.l_commitdate"),
        ("EXISTS subquery alone — orderkeys with ≥ 2 distinct suppliers",
         "SELECT COUNT(DISTINCT l1.l_orderkey) FROM lineitem l1 \
          WHERE EXISTS (SELECT 1 FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey)"),
    ] {
        match ctx.sql(q).await {
            Ok(df) => {
                let batches: Vec<_> = df.collect().await?;
                println!("  {label}:");
                println!("{}", pretty_format_batches(&batches)?);
            }
            Err(e) => println!("  {label}: ERROR {e}"),
        }
    }

    Ok(())
}
