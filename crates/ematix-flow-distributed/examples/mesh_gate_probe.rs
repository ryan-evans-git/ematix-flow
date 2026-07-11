//! Σ.MG probe — per-query mesh-gate cost signals, printed as TSV.
//!
//! For each TPC-H query, build the plan EXACTLY as the distributed
//! session does (arrow-rs `register_parquet` listings + the preset
//! rule chain, no gate / no peers) and walk the pre-split plan for
//! the candidate AUTO signals:
//!
//! - `scan_bytes`: summed known scan-leaf bytes (the current gate's
//!   only signal)
//! - `dom_rows`: row count of the dominant (largest) scan leaf
//! - `min_ratio`: over every hash join whose PROBE subtree contains
//!   the dominant scan, min of est(build side) / dom_rows — the
//!   "bloom-prunable selective join" signal. Small ratio ⇒
//!   single-node would bloom the big scan down to ~nothing; the mesh
//!   (whose arrow scans carry no runtime blooms) shuffles it unpruned.
//! - `survival`: est(topmost join containing the dominant scan) /
//!   dom_rows
//! - `blooms`: bloom-emitter candidates found on the optimized
//!   logical plan
//! - `instances`: max mesh-scale scan instances of one base table
//!   (the Σ.MG self-join guard signal)
//!
//! Usage: `TPCH_DATA_DIR=... cargo run --release -p
//! ematix-flow-distributed --example mesh_gate_probe`
//!
//! Output: one TSV line per query (empty cell = signal unavailable).

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::preset::{self, HarnessOverrides};

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

fn queries_dir() -> PathBuf {
    let mut d = std::env::current_dir().expect("cwd");
    loop {
        let cand = d.join("examples/tpch/queries");
        if cand.join("q01.sql").exists() {
            return cand;
        }
        if !d.pop() {
            panic!("examples/tpch/queries not found above cwd");
        }
    }
}

fn table_source(data_dir: &std::path::Path, table: &str) -> PathBuf {
    let dir = data_dir.join(table);
    let dir_has_files = std::fs::read_dir(&dir)
        .map(|mut d| d.next().is_some())
        .unwrap_or(false);
    if dir.is_dir() && dir_has_files {
        dir
    } else {
        data_dir.join(format!("{table}.parquet"))
    }
}

/// Largest scan leaf: (bytes, rows), plus the summed known bytes.
fn scan_leaf_stats(plan: &Arc<dyn ExecutionPlan>) -> (u64, Option<(u64, u64, usize)>) {
    fn walk(
        plan: &Arc<dyn ExecutionPlan>,
        idx: &mut usize,
        sum: &mut u64,
        dom: &mut Option<(u64, u64, usize)>,
    ) {
        let children = plan.children();
        if children.is_empty() {
            let leaf_idx = *idx;
            *idx += 1;
            if let Ok(s) = plan.partition_statistics(None) {
                use datafusion::common::stats::Precision;
                let bytes = match s.total_byte_size {
                    Precision::Exact(n) | Precision::Inexact(n) => Some(n as u64),
                    Precision::Absent => None,
                };
                let rows = match s.num_rows {
                    Precision::Exact(n) | Precision::Inexact(n) => Some(n as u64),
                    Precision::Absent => None,
                };
                if let Some(b) = bytes {
                    *sum = sum.saturating_add(b);
                    if dom.map(|(db, _, _)| b > db).unwrap_or(true) {
                        *dom = Some((b, rows.unwrap_or(0), leaf_idx));
                    }
                }
            }
            return;
        }
        for c in children {
            walk(c, idx, sum, dom);
        }
    }
    let mut idx = 0;
    let mut sum = 0;
    let mut dom = None;
    walk(plan, &mut idx, &mut sum, &mut dom);
    (sum, dom)
}

/// Does `plan`'s subtree contain the scan leaf with index `target`?
/// (Leaf indexing must match `scan_leaf_stats`'s pre-order walk.)
fn subtree_contains_leaf(plan: &Arc<dyn ExecutionPlan>, target: usize, next: &mut usize) -> bool {
    let children = plan.children();
    if children.is_empty() {
        let hit = *next == target;
        *next += 1;
        return hit;
    }
    let mut found = false;
    for c in children {
        if subtree_contains_leaf(c, target, next) {
            found = true;
        }
    }
    found
}

/// min over hash joins whose probe side contains the dominant leaf of
/// est(build)/dom_rows. Pre-order leaf indexing shared with
/// `scan_leaf_stats`.
fn min_build_ratio(plan: &Arc<dyn ExecutionPlan>, dom_leaf: usize, dom_rows: u64) -> Option<f64> {
    use datafusion::physical_plan::joins::HashJoinExec;
    fn walk(
        plan: &Arc<dyn ExecutionPlan>,
        leaf_cursor: &mut usize,
        dom_leaf: usize,
        dom_rows: u64,
        best: &mut Option<f64>,
    ) {
        if let Some(hj) = plan.as_any().downcast_ref::<HashJoinExec>() {
            // Determine which side holds the dominant leaf without
            // disturbing the global cursor: count leaves in left.
            let mut probe_cursor = *leaf_cursor;
            let left_has = subtree_contains_leaf(hj.left(), dom_leaf, &mut probe_cursor);
            let mut right_cursor = probe_cursor;
            let right_has = subtree_contains_leaf(hj.right(), dom_leaf, &mut right_cursor);
            // Build side is LEFT in DF hash joins; probe is RIGHT.
            if right_has && !left_has && dom_rows > 0 {
                if let Some(build) = ematix_flow_core::join_side_rule::estimate_rows(hj.left()) {
                    let ratio = build / dom_rows as f64;
                    if best.map(|b| ratio < b).unwrap_or(true) {
                        *best = Some(ratio);
                    }
                }
            }
        }
        let children = plan.children();
        if children.is_empty() {
            *leaf_cursor += 1;
            return;
        }
        for c in children {
            walk(c, leaf_cursor, dom_leaf, dom_rows, best);
        }
    }
    let mut cursor = 0;
    let mut best = None;
    walk(plan, &mut cursor, dom_leaf, dom_rows, &mut best);
    best
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> datafusion::common::Result<()> {
    let data_dir = PathBuf::from(std::env::var("TPCH_DATA_DIR").expect("TPCH_DATA_DIR"));
    let qdir = queries_dir();

    let cfg = SessionConfig::new().with_collect_statistics(true);
    let overrides = HarnessOverrides {
        auto_target_partitions: true,
        ..HarnessOverrides::default()
    };
    let base = SessionStateBuilder::new()
        .with_config(cfg)
        .with_default_features();
    let (builder, _handles) = preset::with_optimizer_rules_overridden(base, &overrides);
    let ctx = Arc::new(SessionContext::from(builder.build()));

    for t in TPCH_TABLES {
        let p = table_source(&data_dir, t);
        ctx.register_parquet(*t, p.to_str().unwrap(), Default::default())
            .await?;
    }

    println!("query\tscan_bytes\tdom_rows\tmin_ratio\tsurvival\tblooms\tinstances");
    for i in 1..=22 {
        let sql = std::fs::read_to_string(qdir.join(format!("q{i:02}.sql")))?;
        // Multi-statement files: take the first statement.
        let stmt = sql.split(';').next().unwrap_or(&sql);
        let df = ctx.sql(stmt).await?;
        let logical = df.clone().into_optimized_plan()?;
        let blooms = ematix_flow_distributed::bloom_emitter::emit_build_side_blooms(
            &ctx,
            &logical,
            &ematix_flow_distributed::bloom_emitter::BloomEmitterOptions::default(),
        )
        .await
        .map(|m| m.len())
        .unwrap_or(0);
        let plan = df.create_physical_plan().await?;
        let instances = ematix_flow_distributed::mesh_gate::max_same_table_scan_instances(
            &plan,
            ematix_flow_distributed::mesh_gate::DEFAULT_MESH_MIN_BYTES,
        );
        let (sum_bytes, dom) = scan_leaf_stats(&plan);
        match dom {
            Some((_, dom_rows, dom_leaf)) => {
                let ratio = min_build_ratio(&plan, dom_leaf, dom_rows);
                let survival = topmost_join_survival(&plan, dom_leaf, dom_rows);
                println!(
                    "Q{i:02}\t{sum_bytes}\t{dom_rows}\t{}\t{}\t{blooms}\t{instances}",
                    ratio.map(|r| format!("{r:.2e}")).unwrap_or_default(),
                    survival.map(|s| format!("{s:.3}")).unwrap_or_default()
                );
            }
            None => println!("Q{i:02}\t{sum_bytes}\t\t\t\t{blooms}\t{instances}"),
        }
    }
    Ok(())
}

/// est(TOPMOST hash join whose subtree contains the dominant leaf) /
/// dom_rows — the fraction of the dominant scan the join chain lets
/// SURVIVE. The single-node plan realizes this shrink at scan time
/// via runtime blooms; the distributed plan (arrow scans, no blooms)
/// shuffles the unpruned scan. Small survival ⇒ single-node wins.
fn topmost_join_survival(
    plan: &Arc<dyn ExecutionPlan>,
    dom_leaf: usize,
    dom_rows: u64,
) -> Option<f64> {
    use datafusion::physical_plan::joins::HashJoinExec;
    if dom_rows == 0 {
        return None;
    }
    // Pre-order DFS mirroring scan_leaf_stats' leaf indexing; the
    // FIRST HashJoin (shallowest, found before descending) whose
    // subtree contains the dominant leaf is the topmost.
    fn walk(
        plan: &Arc<dyn ExecutionPlan>,
        cursor: &mut usize,
        dom_leaf: usize,
        dom_rows: u64,
        found: &mut Option<f64>,
    ) {
        if found.is_some() {
            // Still must advance the cursor through remaining leaves.
        }
        if plan.as_any().is::<HashJoinExec>() && found.is_none() {
            let mut probe = *cursor;
            if subtree_contains_leaf(plan, dom_leaf, &mut probe) {
                if let Some(est) = ematix_flow_core::join_side_rule::estimate_rows(plan) {
                    *found = Some(est / dom_rows as f64);
                }
                // Whether or not it priced, this was the topmost; stop
                // recording (found stays None if unpriceable — callers
                // treat that as "no signal").
                if found.is_none() {
                    *found = Some(f64::NAN);
                }
            }
        }
        let children = plan.children();
        if children.is_empty() {
            *cursor += 1;
            return;
        }
        for c in children {
            walk(c, cursor, dom_leaf, dom_rows, found);
        }
    }
    let mut cursor = 0;
    let mut found = None;
    walk(plan, &mut cursor, dom_leaf, dom_rows, &mut found);
    found.filter(|f| !f.is_nan())
}
