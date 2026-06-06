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

use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode};
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
    std::env::var("EMAT_HJ_PARTITIONED")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
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

    // HJ.5b — probe-vs-build cardinality gate. The custom operator pays off only
    // on a FACT-probing join: the PROBE (RIGHT, since build=LEFT/CollectLeft)
    // must be (a) absolutely large (≥ `EMAT_HJ_MIN_PROBE`, default 1M) AND (b)
    // large RELATIVE to the build (≥ `EMAT_HJ_RATIO` × build rows, default 50).
    // The RATIO is the fact-vs-dim signal: Q08 part⋈lineitem ≈ 4600× passes;
    // dim⋈dim joins (Q02/Q11) ≈ 1× block — an absolute floor alone over-fires on
    // them because DataFusion OVER-estimates their join-output cardinality, while
    // dropping the absolute floor would lose the orders/mid-probe wins (Q07/Q21).
    // `num_rows` Absent on either side → block (the wins probe a fact scan with
    // KNOWN large stats). Set both envs 0 to disable.
    // Default 12M = best-measured at SF=10 (HJ.5c): scan-anchored, sits between
    // partsupp (8M) and orders (15M) so dim⋈dim joins (Q02/Q11) block while
    // orders/lineitem-probing star joins fire → −1..−3.3% suite, no large stable
    // regressions (only noisy Q17). CAVEAT — this absolute threshold is
    // SF-specific (wrong at SF=100, where partsupp is 80M); a scan-anchored RATIO
    // (probe_scan ≥ K × build_scan, scale-invariant) is the proper default-on
    // gate, now reliable since both sides use scan stats. `EMAT_HJ_RATIO` enables
    // it. The earlier ESTIMATE-based ratio failed; scan-anchored should not.
    let min_probe: usize = std::env::var("EMAT_HJ_MIN_PROBE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(12_000_000);
    let ratio: usize = std::env::var("EMAT_HJ_RATIO")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    // Partitioned-mode swap with NO cardinality gate would risk single-threading
    // a huge build → refuse. The gate is exactly what restricts us to the
    // big-fact probe join (e.g. orders⋈lineitem) and away from dim⋈dim joins.
    if partitioned && min_probe == 0 && ratio == 0 {
        return None;
    }
    if min_probe > 0 || ratio > 0 {
        // HJ.5c: scan-anchored, not join-output-estimate. Probe = RIGHT subtree's
        // largest base scan; build = LEFT subtree's largest base scan (only used
        // when the ratio knob is enabled).
        match (
            max_leaf_scan_rows(hj.right()),
            max_leaf_scan_rows(hj.left()),
        ) {
            (Some(pr), Some(br)) if pr >= min_probe && pr >= ratio.saturating_mul(br.max(1)) => {}
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
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Opt-in. Dormant unless explicitly enabled.
        let enabled = std::env::var("EMAT_HASH_JOIN")
            .ok()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        if !enabled {
            return Ok(plan);
        }
        let trace = std::env::var_os("EMAT_HASH_JOIN_TRACE").is_some();
        let out = plan.transform_up(|node| {
            if let Some(hj) = node.as_any().downcast_ref::<HashJoinExec>() {
                if let Some(exec) = try_swap(hj) {
                    if trace {
                        eprintln!(
                            "[HJ.swap] Inner/CollectLeft join on=[({}@{}, {}@{})] → EmatixHashJoinExec",
                            "build", exec.build_key_idx(), "probe", exec.probe_key_idx()
                        );
                    }
                    return Ok(Transformed::yes(Arc::new(exec) as Arc<dyn ExecutionPlan>));
                } else if trace {
                    eprintln!("[HJ.swap] skip HashJoinExec (shape gate not met)");
                }
            }
            Ok(Transformed::no(node))
        })?;
        Ok(out.data)
    }
}
