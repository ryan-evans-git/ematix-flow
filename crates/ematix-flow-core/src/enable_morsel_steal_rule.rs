//! `EnableMorselStealRule` — turn on work-stealing decode for
//! `EmatixFastParquetExec` scans that feed a join.
//!
//! Morsel-engine P2 (docs/plans/MORSEL_ENGINE.md) found work-stealing
//! decode (the shared `SharedRgCursor`) is **shape-bimodal** at SF=10:
//!
//! - It HELPS scans feeding a join — Q14 −6%, Q17 −9%, Q18 −18%, Q12 −5%.
//!   Balancing the scan across all cores lets the join pipeline overlap and
//!   avoids waiting on a straggler partition stuck with the heavy row groups.
//! - It HURTS pure scan→fused-agg — Q06 +22% (confirmed isolated). The
//!   decode is the whole critical path, so the concurrent-decode contention
//!   tax just piles on with nothing to overlap.
//!
//! So we enable the per-scan `morsel_steal` flag only when the scan has a
//! join ancestor. `EMAT_MORSEL_STEAL=1` force-enables everywhere, `=0`
//! force-disables (the A/B baseline); unset honors this rule's flag.

use std::sync::Arc;

use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;

use crate::ematix_fast_parquet::EmatixFastParquetExec;

#[derive(Debug, Default)]
pub struct EnableMorselStealRule;

/// Any join operator: HashJoinExec, NestedLoopJoinExec, SortMergeJoinExec,
/// SymmetricHashJoinExec, and our EmatixHashJoinExec all carry "Join" in
/// their operator name.
fn node_is_join(name: &str) -> bool {
    name.contains("Join")
}

fn rewrite(node: Arc<dyn ExecutionPlan>, under_join: bool) -> Result<Arc<dyn ExecutionPlan>> {
    // Recurse children first. A child is "under a join" if this node — or
    // any ancestor — is a join.
    let child_under = under_join || node_is_join(node.name());
    let new_children = node
        .children()
        .into_iter()
        .map(|c| rewrite(c.clone(), child_under))
        .collect::<Result<Vec<_>>>()?;
    let node = if new_children.is_empty() {
        node
    } else {
        node.with_new_children(new_children)?
    };
    // A scan with a join ancestor → enable work-stealing decode.
    if under_join {
        if let Some(scan) = node.as_any().downcast_ref::<EmatixFastParquetExec>() {
            return Ok(scan.with_morsel_steal());
        }
    }
    Ok(node)
}

impl PhysicalOptimizerRule for EnableMorselStealRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        rewrite(plan, false)
    }

    fn name(&self) -> &str {
        "EnableMorselStealRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
