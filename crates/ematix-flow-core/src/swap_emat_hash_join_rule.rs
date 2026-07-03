//! HJ.3 — pre-plan swap rule: replace stock `HashJoinExec` with
//! `EmatixHashJoinExec` ONLY on the validated shape, so the L13 kernel
//! probe (2.36× faster on Q08's part⋈lineitem) runs while every other
//! join stays on DataFusion.
//!
//! Shape gate (all required):
//!   - `JoinType::Inner`
//!   - `PartitionMode::CollectLeft` — DataFusion already proved the build
//!     (LEFT) side is small enough to broadcast/collect, so this doubles
//!     as the build-cardinality gate (no separate stats check needed).
//!   - no extra non-equi `filter`
//!   - exactly one equi-key, both sides a bare `Column`, both i64-widenable
//!     (Int64/Int32 — what the bridge's `key_as_i64` supports).
//!
//! Output column mapping is derived from `join_schema()` (build fields ++
//! probe fields) by matching each projected output field by NAME, with a
//! uniqueness guard: if any output name is absent or ambiguous in the
//! join schema, we BAIL (leave the stock join) — correctness over reach.
//!
//! Opt-in via `EMAT_HASH_JOIN=1` (dormant by default, like every risky
//! lever's first ship). The rule runs after the built-in physical rules,
//! so `partition_mode()` is already assigned.

#![allow(deprecated)] // CoalesceBatchesExec deprecated upstream; BatchCoalescer migration is a follow-up

use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::common::{JoinType, Result as DfResult};
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::repartition::RepartitionExec;

use crate::emat_hash_join::JoinColumn;
use crate::emat_hash_join_exec::EmatixHashJoinExec;

#[derive(Debug, Default)]
pub struct SwapEmatixHashJoinRule;

fn widens_to_i64(dt: &DataType) -> bool {
    matches!(dt, DataType::Int64 | DataType::Int32)
}

/// SF100.6 — opt-in (with `EMAT_HASH_JOIN=1`) extension to ALSO swap
/// `PartitionMode::Partitioned` joins, not just CollectLeft. EmatixHashJoinExec
/// requires only `UnspecifiedDistribution` and builds a single shared table
/// probed in-place, so stripping the Partitioned join's RepartitionExec inputs
/// yields the DuckDB-style no-shuffle parallel-probe join that DataFusion lacks
/// as a stock operator. Tightly gated by the probe-cardinality floor (the v1
/// build is single-threaded, so this must be a big-fact probe to pay off).
fn partitioned_enabled() -> bool {
    crate::flags::opt_in("EMAT_HJ_PARTITIONED")
}

/// Strip Hash `RepartitionExec` / `CoalesceBatchesExec` wrappers to recover the
/// pre-shuffle input. Both preserve schema (so the join's key indices + output
/// name-mapping stay valid) and, for a same-count hash repartition (the SF=100
/// fact-join case), partition count too. EmatixHashJoinExec then sees no
/// required distribution → EnforceDistribution inserts no shuffle.
fn strip_shuffle(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let mut cur = plan.clone();
    loop {
        if let Some(rp) = cur.as_any().downcast_ref::<RepartitionExec>() {
            cur = rp.input().clone();
        } else if let Some(cb) = cur.as_any().downcast_ref::<CoalesceBatchesExec>() {
            cur = cb.input().clone();
        } else {
            return cur;
        }
    }
}

/// HJ.5c — the *reliable* probe-cardinality signal: the largest base-SCAN row
/// count in a subtree, from parquet file stats rather than the unreliable
/// mid-chain join-output estimates that made the simple gate over/under-fire
/// (Q02/Q11 false-fired on inflated estimates; Q18/Q07 blocked on bad ones).
/// Walks to the leaves and takes the max `num_rows`: a fact-probing join's
/// probe subtree anchors on the fact scan (lineitem ≈ 60M), a dim⋈dim join on
/// a small dim scan. `None` if no leaf reports stats → caller blocks (safe).
fn max_leaf_scan_rows(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    let children = plan.children();
    if children.is_empty() {
        return plan
            .partition_statistics(None)
            .ok()
            .and_then(|s| s.num_rows.get_value().copied());
    }
    children.into_iter().filter_map(max_leaf_scan_rows).max()
}

/// Returns the replacement operator if `hj` matches the validated shape,
/// else `None` (caller keeps the stock join).
fn try_swap(hj: &HashJoinExec) -> Option<EmatixHashJoinExec> {
    if *hj.join_type() != JoinType::Inner {
        return None;
    }
    // CollectLeft: DataFusion already collected the build → inputs used as-is.
    // Partitioned (opt-in EMAT_HJ_PARTITIONED=1): strip the RepartitionExec
    // shuffles below, giving a no-shuffle parallel-probe join.
    let partitioned = match *hj.partition_mode() {
        PartitionMode::CollectLeft => false,
        PartitionMode::Partitioned if partitioned_enabled() => true,
        _ => return None,
    };
    if hj.filter().is_some() {
        return None;
    }
    if hj.on().len() != 1 {
        return None;
    }
    let (lexpr, rexpr) = &hj.on()[0];
    let lcol = lexpr.as_any().downcast_ref::<Column>()?;
    let rcol = rexpr.as_any().downcast_ref::<Column>()?;

    let left_schema = hj.left().schema();
    let right_schema = hj.right().schema();
    let lt = left_schema.field(lcol.index()).data_type();
    let rt = right_schema.field(rcol.index()).data_type();
    if !widens_to_i64(lt) || !widens_to_i64(rt) {
        return None;
    }

    // HJ.5c — absolute probe-cardinality gate. The kernel's per-row miss-rejection
    // saving only exceeds its (≈fixed) tag-table build cost when the PROBE (RIGHT,
    // since build = LEFT/CollectLeft) is large in ABSOLUTE rows. `EMAT_HJ_MIN_PROBE`
    // (default 12M) is that floor, scan-anchored to parquet file stats (the
    // reliable signal; mid-chain join-output estimates over/under-fire). At SF≤1
    // even the fact table is < 12M so the lever stays dormant — fine, those queries
    // are sub-millisecond. NOTE — an absolute floor is the ONLY workable gate here:
    // a *relative* gate cannot separate "lineitem@SF10 = 60M" (fire) from
    // "partsupp@SF100 = 80M" (the same rows, in a query with no fact table) because
    // both are simply "the biggest table in their query". Two relative gates were
    // measured and refuted — build-RATIO false-fires Q02/Q11 (their build anchors
    // on a tiny dim → huge ratio, +40/+30%); fraction-of-plan-max ALSO false-fires
    // Q02/Q11 (partsupp IS their plan_max, so probe ≥ 50%·max trivially holds).
    // The probe must clear the floor; Absent probe stats → block (a real fact probe
    // has known stats). `EMAT_HJ_RATIO` (default 0/off) is an optional extra
    // build-ratio constraint kept for A/B reproducibility.
    let min_probe: usize = crate::flags::usize_or("EMAT_HJ_MIN_PROBE", 12_000_000);
    let ratio: usize = crate::flags::usize_or("EMAT_HJ_RATIO", 0);
    if partitioned && min_probe == 0 && ratio == 0 {
        return None;
    }
    let probe_scan = max_leaf_scan_rows(hj.right())?;
    if probe_scan < min_probe {
        return None;
    }
    if ratio > 0 {
        match max_leaf_scan_rows(hj.left()) {
            Some(br) if probe_scan >= ratio.saturating_mul(br.max(1)) => {}
            _ => return None,
        }
    }

    // Derive the output column mapping. `join_schema()` is build ++ probe
    // (unprojected); `schema()` is the actual projected output.
    let left_len = left_schema.fields().len();
    let join_schema = hj.join_schema();
    let out_schema = hj.schema();
    let mut output = Vec::with_capacity(out_schema.fields().len());
    for out_f in out_schema.fields() {
        let mut hit: Option<usize> = None;
        let mut count = 0usize;
        for (j, jf) in join_schema.fields().iter().enumerate() {
            if jf.name() == out_f.name() {
                hit = Some(j);
                count += 1;
            }
        }
        if count != 1 {
            // ambiguous (duplicate name) or missing → bail, keep stock join
            return None;
        }
        let j = hit.unwrap();
        output.push(if j < left_len {
            JoinColumn::Build(j)
        } else {
            JoinColumn::Probe(j - left_len)
        });
    }

    // Partitioned: strip the RepartitionExec inputs so the swapped operator's
    // UnspecifiedDistribution leaves no shuffle. CollectLeft: inputs as-is.
    let (build_input, probe_input) = if partitioned {
        (strip_shuffle(hj.left()), strip_shuffle(hj.right()))
    } else {
        (hj.left().clone(), hj.right().clone())
    };
    Some(EmatixHashJoinExec::new(
        build_input,
        probe_input,
        lcol.index(),
        rcol.index(),
        output,
        out_schema,
    ))
}

/// A `HashJoinExec` carrying a non-equi residual `filter` is the physical
/// signature of a correlated-subquery / range join — Q17's
/// `l_quantity < 0.2*avg(l_quantity)`, Q20's qty-vs-`0.5*sum`. These come from
/// decorrelating a scalar/aggregate subquery.
fn is_filtered_hash_join(node: &Arc<dyn ExecutionPlan>) -> bool {
    node.as_any()
        .downcast_ref::<HashJoinExec>()
        .map(|hj| hj.filter().is_some())
        .unwrap_or(false)
}

/// Top-down rewrite that swaps eligible joins to `EmatixHashJoinExec`, EXCEPT
/// inside the subtree of a filtered (correlated-subquery / range) join.
///
/// Why the exclusion (HJ.5c, Q17 dig): the SIMD-tag kernel wins when a big-fact
/// Inner probe is a meaningful fraction of the query and its reduction
/// propagates. Under a filtered join the opposite holds — the decorrelated
/// subquery aggregate dominates the cost (Q17: a 2 M-group `avg` over a second
/// 60 M lineitem scan), so making the fact probe faster barely moves the total
/// while the operator's overhead shows up net-negative (measured Q17 +51%,
/// fresh-ctx canonical harness; it does NOT yield to any probe-cardinality gate
/// because the probe genuinely IS the 60 M fact). `blocked` is set once we
/// descend below such a join and propagates to the whole subtree.
fn rewrite_node(
    node: Arc<dyn ExecutionPlan>,
    blocked: bool,
    trace: bool,
    batch_size: usize,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let child_blocked = blocked || is_filtered_hash_join(&node);
    let children = node.children();
    let mut new_children = Vec::with_capacity(children.len());
    let mut changed = false;
    for c in children {
        let nc = rewrite_node(c.clone(), child_blocked, trace, batch_size)?;
        if !Arc::ptr_eq(&nc, c) {
            changed = true;
        }
        new_children.push(nc);
    }
    let node = if changed {
        node.with_new_children(new_children)?
    } else {
        node
    };
    if !blocked {
        if let Some(hj) = node.as_any().downcast_ref::<HashJoinExec>() {
            if let Some(exec) = try_swap(hj) {
                if trace {
                    eprintln!(
                        "[HJ.swap] Inner join on=[(build@{}, probe@{})] → EmatixHashJoinExec (+coalesce {batch_size})",
                        exec.build_key_idx(),
                        exec.probe_key_idx()
                    );
                }
                let coalesced: Arc<dyn ExecutionPlan> = Arc::new(CoalesceBatchesExec::new(
                    Arc::new(exec) as Arc<dyn ExecutionPlan>,
                    batch_size,
                ));
                return Ok(coalesced);
            } else if trace {
                eprintln!("[HJ.swap] skip HashJoinExec (shape gate not met)");
            }
        }
    } else if trace && node.as_any().downcast_ref::<HashJoinExec>().is_some() {
        eprintln!("[HJ.swap] skip HashJoinExec (under filtered/correlated-subquery join)");
    }
    Ok(node)
}

impl PhysicalOptimizerRule for SwapEmatixHashJoinRule {
    fn name(&self) -> &str {
        "SwapEmatixHashJoinRule"
    }

    fn schema_check(&self) -> bool {
        // We reproduce the join's exact output schema, so the check holds.
        true
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Opt-in. Dormant unless explicitly enabled.
        let enabled = crate::flags::opt_in("EMAT_HASH_JOIN");
        if !enabled {
            return Ok(plan);
        }
        let trace = crate::flags::present("EMAT_HASH_JOIN_TRACE");
        // `batch_size` feeds the post-swap CoalesceBatchesExec (HJ.5c): the kernel
        // emits ONE output batch per probe input batch, so a low-hit-rate fact probe
        // produces tiny survivor batches (Q17: 61.4 K rows over 916 batches ≈ 67
        // rows/batch). DataFusion's native HashJoinExec emits ~`batch_size`-row
        // batches; re-coalescing matches that so the downstream Repartition/join
        // don't choke on micro-batches (Q17 dig: downstream build 2.4→27.5 ms).
        let batch_size = config.execution.batch_size;
        rewrite_node(plan, false, trace, batch_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::NullEquality;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_plan::expressions::Column;

    fn mem(name: &str, n: i64) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from((0..n).collect::<Vec<i64>>()))],
        )
        .unwrap();
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    // Distinct column names (build "b", probe "a") so the output name-mapping is
    // unambiguous (two "a" columns would trip the ambiguity bail, not the gate).
    fn inner_collect_left(
        build: Arc<dyn ExecutionPlan>,
        probe: Arc<dyn ExecutionPlan>,
    ) -> HashJoinExec {
        let on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> =
            vec![(Arc::new(Column::new("b", 0)), Arc::new(Column::new("a", 0)))];
        HashJoinExec::try_new(
            build,
            probe,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap()
    }

    /// The absolute probe floor blocks a tiny probe; with the floor disabled, the
    /// shape gate (Inner / CollectLeft / single i64 key / no filter) passes.
    #[test]
    fn try_swap_floor_then_shape_gate() {
        // Crate-wide env lock: EMAT_HJ_MIN_PROBE is mutated here and in
        // try_swap_rejects_non_inner; the parallel runner interleaves them.
        let _env = crate::flags::EMAT_ENV_TEST_LOCK.blocking_lock();
        let hj = inner_collect_left(mem("b", 3), mem("a", 3));
        // Default 12M floor → a 3-row probe is blocked.
        unsafe { std::env::remove_var("EMAT_HJ_MIN_PROBE") };
        assert!(
            try_swap(&hj).is_none(),
            "tiny probe must be blocked by the 12M absolute floor"
        );
        // Floor disabled → the shape gate alone admits this Inner i64 CollectLeft.
        unsafe { std::env::set_var("EMAT_HJ_MIN_PROBE", "0") };
        let admitted = try_swap(&hj).is_some();
        unsafe { std::env::remove_var("EMAT_HJ_MIN_PROBE") };
        assert!(
            admitted,
            "Inner/CollectLeft/single-i64-key/no-filter must swap once the floor is disabled"
        );
    }

    /// A non-Inner join is never swapped (the kernel is Inner-only).
    #[test]
    fn try_swap_rejects_non_inner() {
        // See try_swap_floor_then_shape_gate — same EMAT_HJ_MIN_PROBE window.
        let _env = crate::flags::EMAT_ENV_TEST_LOCK.blocking_lock();
        let on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> =
            vec![(Arc::new(Column::new("b", 0)), Arc::new(Column::new("a", 0)))];
        let hj = HashJoinExec::try_new(
            mem("b", 3),
            mem("a", 3),
            on,
            None,
            &JoinType::LeftSemi,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap();
        unsafe { std::env::set_var("EMAT_HJ_MIN_PROBE", "0") };
        let got = try_swap(&hj);
        unsafe { std::env::remove_var("EMAT_HJ_MIN_PROBE") };
        assert!(got.is_none(), "LeftSemi must not swap (Inner-only kernel)");
    }

    /// The correlated-subquery/range-join discriminator: a plain (unfiltered)
    /// HashJoinExec is not flagged; the rewrite only blocks subtrees under a
    /// join carrying a non-equi residual `filter`.
    #[test]
    fn is_filtered_hash_join_false_for_plain_join() {
        let hj: Arc<dyn ExecutionPlan> = Arc::new(inner_collect_left(mem("b", 3), mem("a", 3)));
        assert!(!is_filtered_hash_join(&hj));
        // A non-join node is likewise not flagged.
        assert!(!is_filtered_hash_join(&mem("a", 3)));
    }
}
