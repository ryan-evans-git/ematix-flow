//! Σ.AJ.1 Lever B (POC) — broadcast existing bloom emits to sibling
//! same-parquet-path scans elsewhere in the plan.
//!
//! ## Why this exists
//!
//! The existing `EnableCascadingBloomRule` walks each HashJoinExec's
//! **probe subtree** looking for FK-chain-stem-matching scans. That
//! handles dimension-cascading-through-fact patterns (Q07, Q08) where
//! the sibling scan is in the same probe subtree.
//!
//! But Q17 has a different shape: the part⋈lineitem inner join emits
//! a bloom on `l_partkey` for the OUTER lineitem scan. The SUBQUERY's
//! lineitem scan (computing `AVG(l_quantity) GROUP BY l_partkey`) is
//! in the BUILD subtree of the OUTER join — **not reachable from any
//! single HashJoinExec's probe walk**.
//!
//! Stage profile shows the subquery's AggregateExec is 1994 ms (29% of
//! Q17's total compute). The AVG computes for all 200k distinct
//! partkeys, but only ~200 survive the downstream outer-join filter —
//! **99.9% of the AVG work is thrown away**. Pushing the bloom into
//! the subquery's scan would drop AggregateExec input from 60M to
//! ~2000 rows. Expected wall savings: 80-120 ms on Q17.
//!
//! ## POC scope (2026-05-27)
//!
//! Opt-in via `EMAT_L9_BROADCAST_SIBLINGS=1`. **Default OFF.**
//!
//! For each `BuildSideBloomEmitterExec` already present in the plan
//! (installed by `EnableRuntimeBloomSidebandRule` upstream), find
//! sibling `EmatixFastParquetExec` scans that:
//!   1. Have the same parquet path as the emitter's primary target
//!   2. Have the bloom's key column (by FK-stem match) in their projection
//!   3. Don't already have a `runtime_sideband` attached
//!
//! Attach the emitter's bloom to those siblings via:
//!   - A new sideband per sibling
//!   - Sideband added to the emitter's `extra_targets`
//!   - Sibling scan wrapped with `with_runtime_sideband`
//!
//! ## POC correctness caveat
//!
//! This POC does NOT verify that the sibling's column transitively
//! joins back to the bloom's source table on the bloom key. It's
//! semantically valid for Q17's pattern (subquery's lineitem ⋈ part
//! on the same l_partkey) but unsafe for Q21's pattern (l1/l2/l3
//! correlated subqueries reference DIFFERENT join contexts).
//!
//! The existing cascade rule's `self-sibling-same-path-skip` guard
//! exists precisely because Q21 attached a bloom to the wrong-context
//! sibling. This POC bypasses that guard — it's expected to be unsafe
//! for Q21-shape queries.
//!
//! **The POC is for measuring the Q17 wall-time impact.** If positive,
//! the production rule needs a transitive-join-safety gate before it
//! can be default-on or applied broadly.

use std::sync::Arc;

use datafusion::common::Result as DfResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;

use crate::bridge_filter_sideband::BridgeFilterSideband;
use crate::build_side_bloom_emitter_exec::BuildSideBloomEmitterExec;
use crate::ematix_fast_parquet::EmatixFastParquetExec;
use crate::fk_chain::{fk_chain_stem, share_fk_chain};

/// Install the broadcast-siblings POC rule. Opt-in via
/// `EMAT_L9_BROADCAST_SIBLINGS=1`. **Default off** — this is a POC
/// that bypasses the cascading rule's self-sibling-same-path guard
/// and is unsafe for queries where multiple same-path scans serve
/// semantically different join contexts (Q21-shape).
pub fn install_broadcast_sibling_blooms_rule(builder: SessionStateBuilder) -> SessionStateBuilder {
    builder.with_physical_optimizer_rule(Arc::new(BroadcastSiblingBloomsRule))
}

#[derive(Debug, Default)]
pub struct BroadcastSiblingBloomsRule;

impl PhysicalOptimizerRule for BroadcastSiblingBloomsRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if !crate::flags::present("EMAT_L9_BROADCAST_SIBLINGS") {
            return Ok(plan);
        }
        let trace = crate::flags::present("EMAT_L9_TRACE");

        // Pass 1: identify emitters + primary target scans.
        //
        // For each BuildSideBloomEmitterExec in the plan, find the
        // EmatixFastParquetExec that holds its `sideband` (the
        // primary target). We use reference-equality on the sideband
        // Arc to identify the primary scan.
        let mut emitter_infos: Vec<EmitterInfo> = Vec::new();
        collect_emitter_infos(&plan, &mut emitter_infos);
        if emitter_infos.is_empty() {
            return Ok(plan);
        }

        // Pass 2: for each emitter, find the primary scan and its
        // path + target column. We carry the emitter_idx so Pass 3
        // can reach back into `emitter_infos[idx].emitter_arc` for
        // the rewrite step.
        let mut resolved: Vec<ResolvedEmitter> = Vec::new();
        for (idx, info) in emitter_infos.iter().enumerate() {
            if let Some(mut r) = resolve_primary(&plan, info) {
                r.emitter_idx = idx;
                resolved.push(r);
            }
        }
        if resolved.is_empty() {
            return Ok(plan);
        }

        // Pass 3: for each resolved emitter, find sibling scans
        // elsewhere in the plan and build (sibling Arc, new sideband,
        // sibling col_idx in file schema) attachments.
        let mut all_attachments: Vec<EmitterAttachments> = Vec::new();
        for r in &resolved {
            let attachments = find_sibling_attachments(&plan, r, trace);
            if !attachments.is_empty() {
                if trace {
                    eprintln!(
                        "[broadcast] emitter on {} col={} → {} sibling(s)",
                        r.primary_path,
                        r.primary_col_name,
                        attachments.len()
                    );
                }
                all_attachments.push(EmitterAttachments {
                    emitter_arc: emitter_infos[r.emitter_idx].emitter_arc.clone(),
                    attachments,
                });
            }
        }
        if all_attachments.is_empty() {
            return Ok(plan);
        }

        // Pass 4: rewrite the plan.
        // For each emitter that has attachments, replace it with a
        // new BuildSideBloomEmitterExec whose `extra_targets` includes
        // the new sidebands. For each sibling scan, replace with one
        // that has the corresponding sideband attached.
        rewrite_plan_with_broadcasts(plan, &all_attachments)
    }

    fn name(&self) -> &str {
        "ematix_flow_broadcast_sibling_blooms"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

// -------- internals --------

/// Per-emitter info captured during the first walk.
struct EmitterInfo {
    /// Arc of the emitter node itself (for Arc::ptr_eq in rewrite).
    emitter_arc: Arc<dyn ExecutionPlan>,
    /// Reference to the primary sideband. We compare with the
    /// `runtime_sideband` on candidate scans via the BridgeFilterSideband's
    /// embedded Arc pointer.
    primary_sideband: BridgeFilterSideband,
    /// File-schema col_idx of the bloom's target column on the primary scan.
    primary_target_col_idx: usize,
}

fn collect_emitter_infos(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<EmitterInfo>) {
    if let Some(em) = plan.as_any().downcast_ref::<BuildSideBloomEmitterExec>() {
        out.push(EmitterInfo {
            emitter_arc: plan.clone(),
            primary_sideband: em.sideband().clone(),
            primary_target_col_idx: em.target_col_idx(),
        });
    }
    for c in plan.children() {
        collect_emitter_infos(c, out);
    }
}

/// Fully-resolved info about an emitter: where its primary target
/// scan lives, the column name (looked up from the scan's file schema),
/// and the path.
struct ResolvedEmitter {
    emitter_idx: usize, // index into emitter_infos
    primary_path: String,
    primary_col_name: String,
    primary_scan_arc: Arc<dyn ExecutionPlan>,
}

fn resolve_primary(plan: &Arc<dyn ExecutionPlan>, info: &EmitterInfo) -> Option<ResolvedEmitter> {
    let primary = find_scan_with_sideband(plan, &info.primary_sideband)?;
    let scan = primary.as_any().downcast_ref::<EmatixFastParquetExec>()?;
    // target_col_idx is a FILE-schema index (Σ.Q.L14). Look up the
    // column name from the file schema.
    let col_name = scan
        .file_schema()
        .fields()
        .get(info.primary_target_col_idx)?
        .name()
        .clone();
    Some(ResolvedEmitter {
        emitter_idx: 0, // filled by caller
        primary_path: scan.path().to_string(),
        primary_col_name: col_name,
        primary_scan_arc: primary,
    })
}

/// Locate the scan whose `runtime_sideband` matches the given sideband
/// by reference (sidebands carry an `Arc<Mutex<...>>`-style identity).
fn find_scan_with_sideband(
    plan: &Arc<dyn ExecutionPlan>,
    target_sideband: &BridgeFilterSideband,
) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(scan) = plan.as_any().downcast_ref::<EmatixFastParquetExec>() {
        if let Some(sb) = scan.runtime_sideband() {
            if sb.ptr_eq(target_sideband) {
                return Some(plan.clone());
            }
        }
    }
    for c in plan.children() {
        if let Some(hit) = find_scan_with_sideband(c, target_sideband) {
            return Some(hit);
        }
    }
    None
}

struct SiblingAttachment {
    /// The scan node Arc to be wrapped with a new sideband.
    scan_arc: Arc<dyn ExecutionPlan>,
    /// File-schema col_idx for the target column on the sibling scan.
    target_col_idx: usize,
    /// New sideband to be attached + added to emitter's extras.
    new_sideband: BridgeFilterSideband,
}

struct EmitterAttachments {
    emitter_arc: Arc<dyn ExecutionPlan>,
    attachments: Vec<SiblingAttachment>,
}

fn find_sibling_attachments(
    plan: &Arc<dyn ExecutionPlan>,
    r: &ResolvedEmitter,
    trace: bool,
) -> Vec<SiblingAttachment> {
    let mut out = Vec::new();
    collect_sibling_candidates(plan, r, &mut out, trace);
    out
}

fn collect_sibling_candidates(
    plan: &Arc<dyn ExecutionPlan>,
    r: &ResolvedEmitter,
    out: &mut Vec<SiblingAttachment>,
    trace: bool,
) {
    if let Some(scan) = plan.as_any().downcast_ref::<EmatixFastParquetExec>() {
        // Skip the primary itself.
        if Arc::ptr_eq(plan, &r.primary_scan_arc) {
            return;
        }
        // Must be same parquet path.
        if scan.path() != r.primary_path {
            return;
        }
        // Must NOT already have a sideband (avoid double-attach).
        if scan.runtime_sideband().is_some() {
            if trace {
                eprintln!("[broadcast] skip {} — already has sideband", scan.path());
            }
            return;
        }
        // Find a column in the projection whose FK-stem matches the
        // primary's target column.
        let file_schema = scan.file_schema();
        let mut match_idx: Option<usize> = None;
        for (i, field) in file_schema.fields().iter().enumerate() {
            if share_fk_chain(field.name(), &r.primary_col_name) && scan.projection().contains(&i) {
                match_idx = Some(i);
                break;
            }
        }
        let Some(target_col_idx) = match_idx else {
            if trace {
                eprintln!(
                    "[broadcast] skip {} — no projected column with stem={:?}",
                    scan.path(),
                    fk_chain_stem(&r.primary_col_name)
                );
            }
            return;
        };
        out.push(SiblingAttachment {
            scan_arc: plan.clone(),
            target_col_idx,
            new_sideband: BridgeFilterSideband::new(),
        });
        return;
    }
    for c in plan.children() {
        collect_sibling_candidates(c, r, out, trace);
    }
}

fn rewrite_plan_with_broadcasts(
    plan: Arc<dyn ExecutionPlan>,
    all: &[EmitterAttachments],
) -> DfResult<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|node| {
        // Replace each sibling scan with one carrying its new sideband.
        for em in all {
            for att in &em.attachments {
                if Arc::ptr_eq(&node, &att.scan_arc) {
                    if let Some(scan) = node.as_any().downcast_ref::<EmatixFastParquetExec>() {
                        let new = scan.with_runtime_sideband(att.new_sideband.clone());
                        return Ok(Transformed::yes(new as Arc<dyn ExecutionPlan>));
                    }
                }
            }
        }
        // Replace each emitter with one having extended extras.
        for em in all {
            if Arc::ptr_eq(&node, &em.emitter_arc) {
                if let Some(emitter) = node.as_any().downcast_ref::<BuildSideBloomEmitterExec>() {
                    let mut new_extras: Vec<(usize, BridgeFilterSideband)> =
                        emitter.extra_targets().to_vec();
                    for att in &em.attachments {
                        new_extras.push((att.target_col_idx, att.new_sideband.clone()));
                    }
                    let new_emitter = BuildSideBloomEmitterExec::try_new_with_extras(
                        emitter.input().clone(),
                        emitter.key_col_idx(),
                        emitter.target_col_idx(),
                        emitter.sideband().clone(),
                        new_extras,
                        emitter.expected_total_keys(),
                    )?;
                    return Ok(Transformed::yes(
                        Arc::new(new_emitter) as Arc<dyn ExecutionPlan>
                    ));
                }
            }
        }
        Ok(Transformed::no(node))
    })
    .data()
}
