//! REV.17 Phase 0 — empirical build-side cardinality probe.
//!
//! The Q18 CollectLeft win (and the Q16 win, and the Q17 *loss*) all
//! hinge on one DataFusion decision: `JoinSelection` picks
//! `PartitionMode::Partitioned` (hash-repartition the whole fact probe)
//! whenever the build subtree's `partition_statistics()` report
//! `num_rows` AND `total_byte_size = Precision::Absent` — which is the
//! case for any semi/anti/aggregate/HAVING output. Our
//! `ForceCollectLeftForSemiBoundedBuildRule` works around this with a
//! purely STRUCTURAL gate (does the build contain a semi/anti join),
//! which over-fires on Q17 (the agg-semi build over lineitem is large,
//! so broadcasting it costs ~+274 ms / +15%).
//!
//! Before designing the principled fix we must MEASURE what the engine
//! actually knows at the decision point. For each query, this walks the
//! production physical plan and, for every `HashJoinExec`, prints:
//!
//!   * join_type + final partition_mode (CollectLeft = our rule fired)
//!   * for BOTH children: `partition_statistics()` num_rows &
//!     total_byte_size with their Precision (Absent / Inexact / Exact)
//!     = exactly what `supports_collect_by_thresholds` evaluated;
//!   * which side is semi/anti-bounded (the structural gate signal);
//!   * leaf-scan Exact row footprint (sum of base-table rows feeding
//!     the side — always known);
//!   * runtime GROUND TRUTH: stream-count the side's actual output rows
//!     (capped) when it's the semi-bounded build or its leaf footprint
//!     is small. Large probe sides are reported by footprint only (not
//!     executed).
//!
//! The output answers: is the build's `num_rows` Absent for all three?
//! Does a static OR runtime ratio (build_rows vs probe_rows) cleanly
//! separate the CollectLeft WINS (Q16/Q18: build << probe) from the
//! LOSS (Q17: build ~ probe)? That decides between fix philosophies:
//!   (A) propagate a static cardinality estimate so DF self-selects,
//!   (B) replace DF's absolute threshold with a relative ratio gate,
//!   (C) runtime-adaptive (decide after seeing the real build size).
//!
//! Usage:
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 TPCH_QUERIES=16,17,18 \
//!     cargo run --release -p ematix-flow-core --example collect_left_card_probe
//!
//! ematix-only; does not touch BENCHMARKS.md.

use std::path::Path;
use std::sync::Arc;

use datafusion::common::JoinType;
use datafusion::common::stats::Precision;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use futures_util::StreamExt;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

/// Cap on rows we'll stream-count before bailing (keeps a large semi
/// build from decoding the whole fact table).
const COUNT_CAP: usize = 80_000_000;
/// A side whose leaf footprint is below this gets stream-counted even
/// if it's not semi-bounded; above it we trust the footprint (it's a
/// big probe we don't want to decode).
const EXEC_FOOTPRINT_LIMIT: usize = 5_000_000;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| {
            std::path::PathBuf::from("crates/ematix-flow-core/examples/tpch/data/sf10")
        });
    let queries: Vec<u8> = std::env::var("TPCH_QUERIES")
        .unwrap_or_else(|_| "16,17,18".to_string())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    // Production preset: milestone rules + FlowQueryPlanner +
    // ForceCollectLeftForSemiBoundedBuildRule (default-on). So the
    // dumped plan is exactly what production runs.
    let mut session_config = SessionConfig::new().with_target_partitions(14);
    // REV.17.2: mirror the bench knob so the dumped plan reflects a raised
    // collect-left threshold (see which joins newly flip to CollectLeft).
    if let Some(rows) = std::env::var("EMAT_COLLECT_LEFT_THRESHOLD_ROWS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
    {
        let opts = session_config.options_mut();
        opts.optimizer.hash_join_single_partition_threshold_rows = rows;
        opts.optimizer.hash_join_single_partition_threshold = rows.saturating_mul(64);
    }
    let builder = ematix_flow_core::preset::with_optimizer_rules(
        SessionStateBuilder::new()
            .with_config(session_config)
            .with_default_features(),
    );
    let ctx = SessionContext::new_with_state(builder.build());
    register_tables(&ctx, &data_dir)?;

    eprintln!("REV.17 Phase 0 — build-side cardinality probe");
    eprintln!("data = {}\n", data_dir.display());

    for q in queries {
        let sql_path = format!("examples/tpch/queries/q{q:02}.sql");
        let sql = match std::fs::read_to_string(&sql_path) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Q{q:02}: cannot read {sql_path}: {e}");
                continue;
            }
        };
        println!("\n========================= Q{q:02} =========================");
        let plan = ctx.sql(&sql).await?.create_physical_plan().await?;

        let mut joins: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
        collect_joins(&plan, &mut joins);
        if joins.is_empty() {
            println!("  (no HashJoinExec — rule is a no-op here)");
            continue;
        }
        for (i, j) in joins.iter().enumerate() {
            let hj = j.as_any().downcast_ref::<HashJoinExec>().unwrap();
            let mode = hj.partition_mode();
            let fired = matches!(mode, PartitionMode::CollectLeft);
            println!(
                "\n  [join {i}] {:?}  mode={:?}{}",
                hj.join_type(),
                mode,
                if fired { "   <-- CollectLeft (our rule fired)" } else { "" },
            );
            describe_side(&ctx, "LEFT (build) ", hj.left()).await;
            describe_side(&ctx, "RIGHT (probe)", hj.right()).await;
            verdict(&ctx, hj).await;
        }
    }
    Ok(())
}

/// Print stats + footprint + (conditionally) ground-truth row count for
/// one side of a join.
async fn describe_side(ctx: &SessionContext, label: &str, side: &Arc<dyn ExecutionPlan>) {
    let stats = side.partition_statistics(None).ok();
    let (nrows, nbytes) = match &stats {
        Some(s) => (prec_usize(&s.num_rows), prec_usize(&s.total_byte_size)),
        None => ("<err>".into(), "<err>".into()),
    };
    let semi = subtree_has_semi(side);
    let leaf = leaf_rows(side);
    let parts = side.output_partitioning().partition_count();

    print!(
        "    {label}: stat.num_rows={nrows:<14} stat.bytes={nbytes:<16} \
         semi={semi:<5} leaf_footprint={} parts={parts}",
        leaf.map(|n| n.to_string()).unwrap_or_else(|| "?".into()),
    );

    // Runtime ground-truth counting is opt-in (EMAT_PROBE_COUNT=1): it
    // executes the subtree, and a plan is a DAG of Arc-shared single-use
    // RepartitionExec nodes, so counting multiple overlapping subtrees of
    // the SAME plan panics ("partition not used yet"). The static stats
    // above are usually enough; enable counting only to spot-check one
    // query (it re-plans per count to avoid the shared-node hazard).
    let do_count = std::env::var("EMAT_PROBE_COUNT").is_ok();
    let want_count = do_count
        && (semi || leaf.map(|n| n <= EXEC_FOOTPRINT_LIMIT).unwrap_or(false));
    if want_count {
        let (n, capped) = count_rows(ctx, side.clone()).await;
        println!("  ACTUAL_rows={}{}", n, if capped { "+ (capped)" } else { "" });
    } else {
        println!();
    }
}

/// Print a one-line read on whether this join's broadcast is a win
/// (build << probe) or a risk (build ~ probe) using leaf footprints.
async fn verdict(_ctx: &SessionContext, hj: &HashJoinExec) {
    // In CollectLeft the LEFT side is the broadcast build, RIGHT streams.
    // Compare what the engine ESTIMATES for each (the number JoinSelection
    // actually weighs) — broadcast is a win when build_est << probe_est.
    let lstat = hj.left().partition_statistics(None).ok();
    let rstat = hj.right().partition_statistics(None).ok();
    let lrows = lstat.as_ref().and_then(|s| s.num_rows.get_value().copied());
    let rrows = rstat.as_ref().and_then(|s| s.num_rows.get_value().copied());
    if let (Some(b), Some(p)) = (lrows, rrows) {
        let ratio = if b == 0 { f64::INFINITY } else { p as f64 / b as f64 };
        let read = if ratio >= 8.0 {
            "probe >> build  => broadcast WINS"
        } else if ratio <= 0.125 {
            "build >> probe  => broadcast LOSES (build is the big side!)"
        } else {
            "build ~ probe   => marginal"
        };
        println!("    => est build(L)~{b} probe(R)~{p}  probe/build = {ratio:.2}x  [{read}]");
    } else {
        println!("    => est build(L)={lrows:?} probe(R)={rrows:?}  (ratio unknowable)");
    }
}

/// Stream-count all output rows of `plan` (across all partitions via a
/// coalesce), discarding batches, capped at COUNT_CAP.
async fn count_rows(ctx: &SessionContext, plan: Arc<dyn ExecutionPlan>) -> (usize, bool) {
    let coalesced: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(plan));
    let mut stream = match coalesced.execute(0, ctx.task_ctx()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("    (count failed: {e})");
            return (0, false);
        }
    };
    let mut n = 0usize;
    while let Some(b) = stream.next().await {
        match b {
            Ok(batch) => {
                n += batch.num_rows();
                if n >= COUNT_CAP {
                    return (n, true);
                }
            }
            Err(e) => {
                eprintln!("    (count stream error after {n}: {e})");
                break;
            }
        }
    }
    (n, false)
}

fn prec_usize(p: &Precision<usize>) -> String {
    match p {
        Precision::Exact(v) => format!("Exact({v})"),
        Precision::Inexact(v) => format!("Inexact({v})"),
        Precision::Absent => "Absent".into(),
    }
}

/// Recursively collect every HashJoinExec node.
fn collect_joins(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<Arc<dyn ExecutionPlan>>) {
    if plan.as_any().is::<HashJoinExec>() {
        out.push(plan.clone());
    }
    for c in plan.children() {
        collect_joins(c, out);
    }
}

/// Does this subtree contain a semi/anti join (the structural "build is
/// membership-bounded" signal the force rule keys on)?
fn subtree_has_semi(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(hj) = plan.as_any().downcast_ref::<HashJoinExec>() {
        if matches!(
            hj.join_type(),
            JoinType::LeftSemi | JoinType::RightSemi | JoinType::LeftAnti | JoinType::RightAnti
        ) {
            return true;
        }
    }
    plan.children().iter().any(|c| subtree_has_semi(c))
}

/// Sum Exact/Inexact num_rows at the leaves (base scans) of a subtree.
fn leaf_rows(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    let children = plan.children();
    if children.is_empty() {
        return plan
            .partition_statistics(None)
            .ok()
            .and_then(|s| s.num_rows.get_value().copied());
    }
    let mut total = 0usize;
    let mut any = false;
    for c in children {
        if let Some(r) = leaf_rows(c) {
            total += r;
            any = true;
        }
    }
    any.then_some(total)
}

fn register_tables(
    ctx: &SessionContext,
    data_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let all_emat = std::env::var("EMAT_ALL_TABLES_EMAT")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        let use_emat = all_emat || *t == "lineitem" || *t == "orders";
        if use_emat {
            let prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy())?;
            ctx.register_table(*t, Arc::new(prov))?;
        } else {
            let prov = FastParquetTableProvider::try_new(path.to_string_lossy())?;
            ctx.register_table(*t, Arc::new(prov))?;
        }
    }
    Ok(())
}
