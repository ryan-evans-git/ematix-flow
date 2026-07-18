//! v2 S1.2 — grouping-set recognizer (planner-interception core for
//! Phase GS). See `docs/PHASE_V2_S1_GROUPING_SETS.md`.
//!
//! This slice is the **recognizer**: the pure, testable core of the
//! physical-optimizer rule that will swap DataFusion's generic multi-set
//! `AggregateExec` for the native `FusedGroupingSetAggregateExec`. It
//! reads DF53's grouping-set physical shape *exactly* as pinned in
//! §4.0 (via the `gs_plan_probe` example):
//!
//!   * the multi-set node is the **Partial** `AggregateExec` — the one
//!     whose `PhysicalGroupBy::groups().len() > 1`;
//!   * each set is a null-mask `Vec<bool>` where **`true` = the column is
//!     rolled up** (absent) in that set — so `present(i) == !mask[i]`;
//!   * the grouping id DF expects downstream puts the **leftmost** group
//!     column in the **high** bit:
//!     `id(set) = Σ_{i : mask[i]} 2^(n-1-i)`.
//!
//! The recognizer extracts that structure and applies the decline gates
//! (`EMAT_GROUPING_SETS_FUSED=0` opt-out; `> GS_MAX_SETS` cap — the CUBE
//! blow-up guard). The operator itself and the rule that installs it land
//! in the next slice; until then nothing is rewired, so this is inert on
//! the query path.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, UInt64Array, new_null_array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream, collect,
};
use futures_util::stream;

/// Default cap on the number of grouping sets we take over natively — the
/// CUBE blow-up guard (`CUBE(k)` = 2^k sets). Above this the recognizer
/// declines and DataFusion's generic exec runs unchanged, rather than
/// building an unbounded number of per-set hash tables. Override with
/// `EMAT_GS_MAX_SETS`. Default 16 ⇒ ROLLUP up to 15 cols, CUBE up to 4.
pub const GS_MAX_SETS_DEFAULT: usize = 16;

/// The resolved `GS_MAX_SETS` cap (`EMAT_GS_MAX_SETS`, default
/// [`GS_MAX_SETS_DEFAULT`]).
pub fn gs_max_sets() -> usize {
    crate::flags::usize_or("EMAT_GS_MAX_SETS", GS_MAX_SETS_DEFAULT)
}

/// Whether native fused grouping-set execution is enabled. Tri-state
/// `EMAT_GROUPING_SETS_FUSED` (matches the `EMAT_SCALAR_AGG_BOOST`
/// default-ON convention): `=0` forces the DataFusion path (A/B +
/// debugging), `=1`/unset keep the fused path.
pub fn fused_grouping_sets_enabled() -> bool {
    crate::flags::tri_state("EMAT_GROUPING_SETS_FUSED").unwrap_or(true)
}

/// The grouping-set structure extracted from a multi-set `AggregateExec`,
/// enough for the operator to emit per-set rows with the correct
/// `__grouping_id` and rolled-up-column nulling.
#[derive(Debug, Clone)]
pub struct RecognizedGroupingSet {
    /// The full group-column universe `[c0, …, c_{n-1}]` (expr + output
    /// alias), in DF's declared order — `c0` is the high grouping-id bit.
    pub universe: Vec<(Arc<dyn PhysicalExpr>, String)>,
    /// One null-mask per grouping set. `mask[i] == true` ⇒ column `i` is
    /// rolled up (absent) in this set; `false` ⇒ present. Verbatim from
    /// `PhysicalGroupBy::groups()` (§4.0 polarity).
    pub set_masks: Vec<Vec<bool>>,
    /// The aggregate expressions to compute per set (shared across sets).
    pub aggs: Vec<Arc<AggregateFunctionExpr>>,
}

impl RecognizedGroupingSet {
    /// Number of group columns in the universe (`n`).
    pub fn universe_len(&self) -> usize {
        self.universe.len()
    }

    /// The `__grouping_id` integer for set `set_idx`, in DF53's exact
    /// convention (§4.0): the leftmost universe column is the high bit, and
    /// a bit is set iff that column is rolled up (`mask[i] == true`).
    /// `id = Σ_{i : mask[i]} 2^(n-1-i)`.
    pub fn grouping_id_of(&self, set_idx: usize) -> u64 {
        grouping_id_for_mask(&self.set_masks[set_idx], self.universe_len())
    }

    /// The column indices *present* (not rolled up) in set `set_idx`.
    pub fn present_cols(&self, set_idx: usize) -> Vec<usize> {
        self.set_masks[set_idx]
            .iter()
            .enumerate()
            .filter_map(|(i, rolled_up)| (!*rolled_up).then_some(i))
            .collect()
    }
}

/// Pure §4.0 grouping-id: leftmost group column (index 0) is the HIGH bit,
/// a bit is set iff that column is rolled up. `id = Σ_{i : mask[i]} 2^(n-1-i)`.
pub fn grouping_id_for_mask(mask: &[bool], n: usize) -> u64 {
    mask.iter()
        .enumerate()
        .filter(|(_, rolled_up)| **rolled_up)
        .map(|(i, _)| 1u64 << (n - 1 - i))
        .sum()
}

/// Recognize a multi-set `AggregateExec` (the **Partial** node of DF's
/// grouping-set lowering — §4.0) as a candidate for native fused
/// execution, extracting its set masks + aggregate exprs.
///
/// Returns `None` (⇒ leave DataFusion's plan in place) when:
///   * the fused path is disabled (`EMAT_GROUPING_SETS_FUSED=0`);
///   * this is not a multi-set node (`groups().len() <= 1`) — the disjoint
///     guard that keeps ordinary single-group aggregates on their own
///     fused path untouched;
///   * the set count exceeds [`gs_max_sets`] (the CUBE blow-up guard).
///
/// Correctness-first: this only *recognizes* the grouping-set structure.
/// Aggregate-shape validation (which aggregates the operator can compute)
/// and the actual operator swap are the next slice; a caller must still
/// gate on aggregate support before rewriting.
pub fn recognize_grouping_set(agg: &AggregateExec) -> Option<RecognizedGroupingSet> {
    if !fused_grouping_sets_enabled() {
        return None;
    }
    // The multi-set masks live on the input-reading phase (§4.0): `Partial`
    // when DF two-phases through a repartition, or `Single`/`SinglePartitioned`
    // when the input is already single-partition. The `Final`/`FinalPartitioned`
    // re-aggregation phases carry a single mask over `[cols…, __grouping_id]`,
    // so `masks.len() > 1` already excludes them; the explicit reject below
    // is belt-and-suspenders so we never rewrite a re-aggregation node.
    let group_by = agg.group_expr();
    let masks = group_by.groups();
    if masks.len() <= 1 {
        return None;
    }
    if matches!(
        agg.mode(),
        AggregateMode::Final | AggregateMode::FinalPartitioned
    ) {
        return None;
    }
    if masks.len() > gs_max_sets() {
        // Loud decline — "TPC-DS runs native" must not silently mean
        // "except the wide cubes" (§7 CUBE blow-up).
        if crate::flags::present("EMAT_DEBUG") {
            eprintln!(
                "[grouping_sets] declined: {} sets > GS_MAX_SETS {} — DataFusion generic exec retained",
                masks.len(),
                gs_max_sets()
            );
        }
        return None;
    }

    let universe: Vec<(Arc<dyn PhysicalExpr>, String)> = group_by
        .expr()
        .iter()
        .map(|(e, name)| (Arc::clone(e), name.clone()))
        .collect();
    let n = universe.len();
    // Every mask must cover the full universe (DF invariant); bail rather
    // than mis-index if that ever changes.
    if masks.iter().any(|m| m.len() != n) {
        return None;
    }

    Some(RecognizedGroupingSet {
        universe,
        set_masks: masks.to_vec(),
        aggs: agg.aggr_expr().to_vec(),
    })
}

/// Native single-pass grouping-set aggregate operator (Model A, §4.1).
///
/// Replaces DataFusion's whole `Final → Repartition → Partial` grouping-set
/// stack. Scans the (expensive) child **once**, materialises it, then runs a
/// vectorized aggregate per set over the shared cached batches, and stitches
/// each set's groups into DF's `Final` output schema — rolled-up columns set
/// to typed NULL, the `__grouping_id` literal filled in per §4.0. The top
/// `ProjectionExec` DF already planned (GROUPING()/GROUPING_ID bit-extraction)
/// then consumes the output unchanged.
///
/// **What is fused today:** the single-child-scan (Model A's key win over
/// DF's expand-then-aggregate, which pushes `n_sets ×` the rows through the
/// accumulator). Per-set aggregation currently delegates to DataFusion's
/// vectorized `AggregateExec`; swapping that for the ematix fused
/// accumulators (the `FusedAggregateExec` kernels) is the documented S1
/// follow-on gated by the vectorization-proof micro-bench (§5). Correctness
/// and the single-scan shape land first.
///
/// **Memory:** materialising the child is bounded by the child size; the
/// `GS_MAX_SETS` cap plus S1.3's live-group bound/spill hooks address SF≥10.
#[derive(Debug)]
pub struct FusedGroupingSetAggregateExec {
    /// The child below DF's `Partial` node — scanned once.
    input: Arc<dyn ExecutionPlan>,
    /// Group-column universe (expr + alias), `c0` = high grouping-id bit.
    universe: Vec<(Arc<dyn PhysicalExpr>, String)>,
    /// Per-set null-masks (`true` = rolled up), verbatim from DF (§4.0).
    set_masks: Vec<Vec<bool>>,
    /// Aggregate expressions, computed per set.
    aggs: Vec<Arc<AggregateFunctionExpr>>,
    /// DF's `Final`-node output schema — the drop-in target:
    /// `[universe…, __grouping_id, agg_finals…]`.
    output_schema: SchemaRef,
    /// Index of the `__grouping_id` field in `output_schema` (= universe len).
    gid_idx: usize,
    properties: Arc<PlanProperties>,
}

impl FusedGroupingSetAggregateExec {
    /// Build the operator. `output_schema` must be DF's grouping-set `Final`
    /// node schema so the downstream projection resolves unchanged; the
    /// `__grouping_id` field is located by name.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        recognized: RecognizedGroupingSet,
        output_schema: SchemaRef,
    ) -> DfResult<Self> {
        let gid_idx = output_schema
            .fields()
            .iter()
            .position(|f| f.name() == GROUPING_ID_COL)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "grouping-set Final schema has no `{GROUPING_ID_COL}` column: {:?}",
                    output_schema
                ))
            })?;
        // The universe columns must be exactly the fields before __grouping_id.
        if gid_idx != recognized.universe.len() {
            return Err(DataFusionError::Internal(format!(
                "grouping-set schema mismatch: {} universe cols but __grouping_id at index {gid_idx}",
                recognized.universe.len()
            )));
        }
        let eq = EquivalenceProperties::new(output_schema.clone());
        let properties = Arc::new(PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            universe: recognized.universe,
            set_masks: recognized.set_masks,
            aggs: recognized.aggs,
            output_schema,
            gid_idx,
            properties,
        })
    }

    fn grouping_id_of(&self, set_idx: usize) -> u64 {
        grouping_id_for_mask(&self.set_masks[set_idx], self.universe.len())
    }
}

/// The synthetic grouping-id column DF carries on grouping-set aggregates.
pub const GROUPING_ID_COL: &str = "__grouping_id";

impl DisplayAs for FusedGroupingSetAggregateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "FusedGroupingSetAggregateExec: sets={}, universe={}, aggs={}",
            self.set_masks.len(),
            self.universe.len(),
            self.aggs.len()
        )
    }
}

impl ExecutionPlan for FusedGroupingSetAggregateExec {
    fn name(&self) -> &str {
        "FusedGroupingSetAggregateExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    /// Collect the child once into a single partition so the per-set
    /// aggregates share one materialisation (Model A single-scan).
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let input = children.pop().ok_or_else(|| {
            DataFusionError::Internal("FusedGroupingSetAggregateExec needs exactly 1 child".into())
        })?;
        Ok(Arc::new(Self {
            input,
            universe: self.universe.clone(),
            set_masks: self.set_masks.clone(),
            aggs: self.aggs.clone(),
            output_schema: self.output_schema.clone(),
            gid_idx: self.gid_idx,
            properties: self.properties.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "FusedGroupingSetAggregateExec emits only partition 0, got {partition}"
            )));
        }
        let input = self.input.clone();
        let universe = self.universe.clone();
        let set_masks = self.set_masks.clone();
        let aggs = self.aggs.clone();
        let output_schema = self.output_schema.clone();
        let gid_idx = self.gid_idx;
        let gids: Vec<u64> = (0..set_masks.len())
            .map(|i| self.grouping_id_of(i))
            .collect();

        let schema_for_adapter = output_schema.clone();
        let fut = async move {
            // 1. Materialise the child ONCE (the single scan). Input is
            //    coalesced to one partition by required_input_distribution.
            let child_schema = input.schema();
            let batches = collect(input, context.clone()).await?;
            let source = MemorySourceConfig::try_new_exec(&[batches], child_schema, None)?;

            // 2. One vectorized aggregate per set over the shared source.
            let mut out: Vec<RecordBatch> = Vec::with_capacity(set_masks.len());
            for (set_idx, mask) in set_masks.iter().enumerate() {
                let present: Vec<usize> = mask
                    .iter()
                    .enumerate()
                    .filter_map(|(i, rolled)| (!*rolled).then_some(i))
                    .collect();
                let group_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = present
                    .iter()
                    .map(|&i| (Arc::clone(&universe[i].0), universe[i].1.clone()))
                    .collect();
                let group_by = PhysicalGroupBy::new_single(group_exprs);
                let per_set: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
                    AggregateMode::Single,
                    group_by,
                    aggs.clone(),
                    vec![None; aggs.len()],
                    source.clone(),
                    source.schema(),
                )?);
                let res = collect(per_set, context.clone()).await?;
                for rb in &res {
                    out.push(stitch_set(
                        rb,
                        &output_schema,
                        &present,
                        gid_idx,
                        gids[set_idx],
                    )?);
                }
            }

            Ok::<Vec<RecordBatch>, DataFusionError>(out)
        };

        // Flatten the Vec<RecordBatch> future into a batch stream. Both
        // arms yield the same Vec<DfResult<RecordBatch>> so the stream has a
        // single concrete type.
        use futures_util::StreamExt;
        let batch_stream = stream::once(fut).flat_map(|res| {
            let items: Vec<DfResult<RecordBatch>> = match res {
                Ok(batches) => batches.into_iter().map(Ok).collect(),
                Err(e) => vec![Err(e)],
            };
            stream::iter(items)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_for_adapter,
            batch_stream,
        )))
    }
}

/// Stitch one set's aggregate result (`rb`, schema `[present…, aggs…]`) into
/// the grouping-set `Final` `output_schema` (`[universe…, __grouping_id,
/// aggs…]`): present universe columns pass through in universe order,
/// rolled-up ones become typed NULL, `__grouping_id` is the set's literal id.
fn stitch_set(
    rb: &RecordBatch,
    output_schema: &SchemaRef,
    present: &[usize],
    gid_idx: usize,
    gid: u64,
) -> DfResult<RecordBatch> {
    let rows = rb.num_rows();
    let n_universe = gid_idx; // universe fills [0, gid_idx)
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(output_schema.fields().len());

    // universe columns 0..n_universe, in the full universe order.
    for j in 0..n_universe {
        let field_ty = output_schema.field(j).data_type();
        match present.iter().position(|&p| p == j) {
            // present: the k-th present column is rb.column(k).
            Some(k) => {
                let src = rb.column(k);
                let col = if src.data_type() == field_ty {
                    Arc::clone(src)
                } else {
                    cast(src, field_ty)?
                };
                cols.push(col);
            }
            // rolled up: typed NULL for every row.
            None => cols.push(new_null_array(field_ty, rows)),
        }
    }

    // __grouping_id literal, cast to the Final schema's id type.
    let gid_ty = output_schema.field(gid_idx).data_type();
    let gid_arr: ArrayRef = Arc::new(UInt64Array::from(vec![gid; rows]));
    cols.push(cast(&gid_arr, gid_ty)?);

    // aggregate columns: rb columns after the present keys, cast to target.
    let n_present = present.len();
    for out_idx in (gid_idx + 1)..output_schema.fields().len() {
        let src = rb.column(n_present + (out_idx - gid_idx - 1));
        let field_ty = output_schema.field(out_idx).data_type();
        let col = if src.data_type() == field_ty {
            Arc::clone(src)
        } else {
            cast(src, field_ty)?
        };
        cols.push(col);
    }

    RecordBatch::try_new(output_schema.clone(), cols)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Find the first multi-set `AggregateExec` (`groups().len() > 1`) in the
/// subtree — DF's input-reading Partial/Single grouping-set node.
fn find_multiset_partial(plan: &Arc<dyn ExecutionPlan>) -> Option<&AggregateExec> {
    if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
        if agg.group_expr().groups().len() > 1 {
            return Some(agg);
        }
    }
    for c in plan.children() {
        if let Some(found) = find_multiset_partial(c) {
            return Some(found);
        }
    }
    None
}

/// `PhysicalOptimizerRule` that replaces DataFusion's grouping-set
/// `Final → Repartition → Partial` stack with the native
/// [`FusedGroupingSetAggregateExec`] (Phase GS, §4.3). Matches the `Final`
/// grouping-set node (its group key carries `__grouping_id`), recognizes the
/// multi-set `Partial` beneath it, and — subject to the recognizer's decline
/// gates (opt-out, `GS_MAX_SETS`) — swaps in the operator over the Partial's
/// child, preserving the `Final` output schema so the projection above is
/// untouched. Non-grouping-set aggregates never match (disjoint by the
/// `__grouping_id` + multi-set requirement), so ordinary plans are unchanged.
#[derive(Debug, Default)]
pub struct InjectGroupingSetRule;

impl PhysicalOptimizerRule for InjectGroupingSetRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let result = plan.transform_down(|node| match try_replace_grouping_set(&node)? {
            Some(new) => Ok(Transformed::yes(new)),
            None => Ok(Transformed::no(node)),
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "ematix_flow_inject_grouping_set"
    }

    fn schema_check(&self) -> bool {
        // The operator reproduces DF's Final output schema exactly, so the
        // schema must be preserved end-to-end — keep the check on.
        true
    }
}

/// If `node` is a grouping-set `Final` aggregate with a recognizable
/// multi-set `Partial` beneath it, return the [`FusedGroupingSetAggregateExec`]
/// replacement; else `None`.
fn try_replace_grouping_set(
    node: &Arc<dyn ExecutionPlan>,
) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
    let Some(final_agg) = node.as_any().downcast_ref::<AggregateExec>() else {
        return Ok(None);
    };
    // Only the Final re-aggregation phase carries the assembled
    // `[cols…, __grouping_id]` group key we key the replacement on.
    if !matches!(
        final_agg.mode(),
        AggregateMode::Final | AggregateMode::FinalPartitioned
    ) {
        return Ok(None);
    }
    let is_grouping_set = final_agg
        .group_expr()
        .expr()
        .iter()
        .any(|(_, name)| name == GROUPING_ID_COL);
    if !is_grouping_set {
        return Ok(None);
    }

    // Find + recognize the multi-set Partial beneath this Final.
    let Some(partial) = find_multiset_partial(node) else {
        return Ok(None);
    };
    let Some(recognized) = recognize_grouping_set(partial) else {
        return Ok(None); // declined (opt-out / GS_MAX_SETS / unsupported)
    };
    // The operator scans the Partial's single child once.
    let child = partial.children();
    let [input] = child.as_slice() else {
        return Ok(None);
    };
    let op =
        FusedGroupingSetAggregateExec::try_new(Arc::clone(input), recognized, final_agg.schema())?;
    Ok(Some(Arc::new(op)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;

    // ---- pure grouping-id math (the §4.0 convention) ----

    #[test]
    fn grouping_id_leftmost_col_is_high_bit() {
        // n = 2, universe [a, b]: a is bit 1 (value 2), b is bit 0 (value 1).
        assert_eq!(grouping_id_for_mask(&[false, false], 2), 0); // (a,b) both present
        assert_eq!(grouping_id_for_mask(&[true, false], 2), 2); // a rolled up
        assert_eq!(grouping_id_for_mask(&[false, true], 2), 1); // b rolled up
        assert_eq!(grouping_id_for_mask(&[true, true], 2), 3); // grand total
    }

    #[test]
    fn grouping_id_three_cols() {
        // n = 3, universe [a, b, c]: bits a=4, b=2, c=1.
        assert_eq!(grouping_id_for_mask(&[false, false, false], 3), 0);
        assert_eq!(grouping_id_for_mask(&[true, false, false], 3), 4);
        assert_eq!(grouping_id_for_mask(&[false, true, false], 3), 2);
        assert_eq!(grouping_id_for_mask(&[false, false, true], 3), 1);
        assert_eq!(grouping_id_for_mask(&[true, true, true], 3), 7);
    }

    // ---- recognizer against real DF53 physical plans ----

    /// A **plain** DataFusion session (no ematix rules — importantly not
    /// `preset::session_context`, which now installs `InjectGroupingSetRule`
    /// and would swap the Partial node out before these tests see it). This
    /// gives DF's raw grouping-set stack to recognize, and pure-DF execution
    /// as the parity reference.
    async fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("x"), Some("y")])),
                Arc::new(StringArray::from(vec![Some("p"), None])),
                Arc::new(Int64Array::from(vec![1_i64, 2])),
            ],
        )
        .unwrap();
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        ctx
    }

    /// Find the first multi-set `AggregateExec` (groups().len() > 1) in the
    /// physical tree — the Partial node DF emits for grouping sets.
    fn find_multiset(plan: &dyn ExecutionPlan) -> Option<&AggregateExec> {
        // Not recursive-into-children via return of a borrowed ref across a
        // loop cleanly, so collect by walking with an explicit stack.
        let mut stack: Vec<&dyn ExecutionPlan> = vec![plan];
        while let Some(node) = stack.pop() {
            if let Some(agg) = node.as_any().downcast_ref::<AggregateExec>() {
                if agg.group_expr().groups().len() > 1 {
                    return Some(agg);
                }
            }
            for c in node.children() {
                stack.push(c.as_ref());
            }
        }
        None
    }

    async fn physical(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
        ctx.sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    }

    /// Collect a plan and render every row as a `|`-joined string via arrow's
    /// type-generic formatter (handles all types + nulls), sorted so the
    /// comparison is order-independent.
    async fn sorted_rows(plan: Arc<dyn ExecutionPlan>, ctx: &SessionContext) -> Vec<String> {
        use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
        let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();
        let opts = FormatOptions::default();
        let mut rows = Vec::new();
        for b in &batches {
            let fmts: Vec<ArrayFormatter> = b
                .columns()
                .iter()
                .map(|c| ArrayFormatter::try_new(c, &opts).unwrap())
                .collect();
            for r in 0..b.num_rows() {
                let cells: Vec<String> = fmts.iter().map(|f| f.value(r).to_string()).collect();
                rows.push(cells.join("|"));
            }
        }
        rows.sort();
        rows
    }

    fn plan_str(plan: &Arc<dyn ExecutionPlan>) -> String {
        format!(
            "{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
        )
    }

    /// End-to-end parity: applying `InjectGroupingSetRule` to a real DF
    /// grouping-set plan (a) actually installs the operator, and (b) produces
    /// byte-identical results to DataFusion's own grouping-set execution.
    async fn assert_rule_parity(sql: &str) {
        let _guard = crate::flags::EMAT_ENV_TEST_LOCK.lock().await;
        let ctx = ctx().await;
        let df_plan = physical(&ctx, sql).await;
        let df_rows = sorted_rows(df_plan.clone(), &ctx).await;

        let optimized = InjectGroupingSetRule
            .optimize(df_plan, &ConfigOptions::default())
            .unwrap();
        assert!(
            plan_str(&optimized).contains("FusedGroupingSetAggregateExec"),
            "rule must install the operator; plan was:\n{}",
            plan_str(&optimized)
        );
        let op_rows = sorted_rows(optimized, &ctx).await;

        assert_eq!(
            op_rows, df_rows,
            "operator diverged from DataFusion for `{sql}`"
        );
    }

    #[tokio::test]
    async fn rule_parity_rollup() {
        assert_rule_parity(
            "SELECT a, b, grouping(a) ga, grouping(b) gb, sum(v) s \
             FROM t GROUP BY ROLLUP(a, b)",
        )
        .await;
    }

    #[tokio::test]
    async fn rule_parity_cube() {
        assert_rule_parity("SELECT a, b, sum(v) s FROM t GROUP BY CUBE(a, b)").await;
    }

    #[tokio::test]
    async fn rule_parity_explicit_sets_with_grand_total() {
        assert_rule_parity("SELECT a, b, sum(v) s FROM t GROUP BY GROUPING SETS ((a, b), (a), ())")
            .await;
    }

    #[tokio::test]
    async fn recognizes_rollup_three_sets_with_correct_masks() {
        // recognize_grouping_set reads EMAT_* env; hold the lock so the
        // env-mutating decline tests can't race the read (house rule).
        let _guard = crate::flags::EMAT_ENV_TEST_LOCK.lock().await;
        let ctx = ctx().await;
        let plan = physical(&ctx, "SELECT a, b, sum(v) FROM t GROUP BY ROLLUP(a, b)").await;
        let agg = find_multiset(plan.as_ref()).expect("rollup must have a multi-set Partial agg");
        let rec = recognize_grouping_set(agg).expect("recognizer should accept ROLLUP(a,b)");

        assert_eq!(rec.universe_len(), 2);
        assert_eq!(rec.set_masks.len(), 3, "ROLLUP(a,b) = 3 sets");
        // Every set's grouping id + present cols must decode per §4.0. The
        // 3 sets are (a,b), (a), () in some order — assert the SET of ids.
        let mut ids: Vec<u64> = (0..3).map(|i| rec.grouping_id_of(i)).collect();
        ids.sort();
        assert_eq!(
            ids,
            vec![0, 1, 3],
            "ROLLUP(a,b) ids = {{(a,b)=0,(a)=1,()=3}}"
        );
    }

    #[tokio::test]
    async fn recognizes_cube_four_sets() {
        let _guard = crate::flags::EMAT_ENV_TEST_LOCK.lock().await;
        let ctx = ctx().await;
        let plan = physical(&ctx, "SELECT a, b, sum(v) FROM t GROUP BY CUBE(a, b)").await;
        let agg = find_multiset(plan.as_ref()).expect("cube must have a multi-set Partial agg");
        let rec = recognize_grouping_set(agg).expect("recognizer should accept CUBE(a,b)");

        assert_eq!(rec.set_masks.len(), 4, "CUBE(a,b) = 4 sets");
        let mut ids: Vec<u64> = (0..4).map(|i| rec.grouping_id_of(i)).collect();
        ids.sort();
        assert_eq!(ids, vec![0, 1, 2, 3], "CUBE(a,b) ids = all of {{0,1,2,3}}");
    }

    #[tokio::test]
    async fn present_cols_track_mask_polarity() {
        let _guard = crate::flags::EMAT_ENV_TEST_LOCK.lock().await;
        let ctx = ctx().await;
        let plan = physical(&ctx, "SELECT a, b, sum(v) FROM t GROUP BY CUBE(a, b)").await;
        let agg = find_multiset(plan.as_ref()).unwrap();
        let rec = recognize_grouping_set(agg).unwrap();

        // For every set: present_cols == the non-rolled-up indices, and the
        // grouping id has exactly the rolled-up bits set.
        for i in 0..rec.set_masks.len() {
            let present = rec.present_cols(i);
            let rolled: Vec<usize> = (0..rec.universe_len())
                .filter(|c| !present.contains(c))
                .collect();
            let expect_id: u64 = rolled
                .iter()
                .map(|c| 1u64 << (rec.universe_len() - 1 - c))
                .sum();
            assert_eq!(rec.grouping_id_of(i), expect_id);
        }
    }

    #[tokio::test]
    async fn declines_ordinary_single_group_by() {
        let ctx = ctx().await;
        // Plain GROUP BY a — single set; recognizer must decline so the
        // existing single-group fused path is untouched (non-regression).
        let plan = physical(&ctx, "SELECT a, sum(v) FROM t GROUP BY a").await;
        assert!(
            find_multiset(plan.as_ref()).is_none(),
            "plain GROUP BY must not present a multi-set AggregateExec"
        );
    }

    #[tokio::test]
    async fn declines_when_over_gs_max_sets() {
        let ctx = ctx().await;
        let plan = physical(&ctx, "SELECT a, b, sum(v) FROM t GROUP BY CUBE(a, b)").await;
        let agg = find_multiset(plan.as_ref()).unwrap();
        // Temporarily cap at 2 sets: CUBE(a,b)=4 > 2 → decline.
        let _guard = crate::flags::EMAT_ENV_TEST_LOCK.lock().await;
        let key = "EMAT_GS_MAX_SETS";
        let prev = std::env::var(key).ok();
        unsafe { std::env::set_var(key, "2") };
        let got = recognize_grouping_set(agg);
        match prev {
            Some(v) => unsafe { std::env::set_var(key, v) },
            None => unsafe { std::env::remove_var(key) },
        }
        assert!(got.is_none(), "4 sets > GS_MAX_SETS=2 must decline");
    }

    #[tokio::test]
    async fn declines_when_opted_out() {
        let ctx = ctx().await;
        let plan = physical(&ctx, "SELECT a, b, sum(v) FROM t GROUP BY ROLLUP(a, b)").await;
        let agg = find_multiset(plan.as_ref()).unwrap();
        let _guard = crate::flags::EMAT_ENV_TEST_LOCK.lock().await;
        let key = "EMAT_GROUPING_SETS_FUSED";
        let prev = std::env::var(key).ok();
        unsafe { std::env::set_var(key, "0") };
        let got = recognize_grouping_set(agg);
        match prev {
            Some(v) => unsafe { std::env::set_var(key, v) },
            None => unsafe { std::env::remove_var(key) },
        }
        assert!(got.is_none(), "EMAT_GROUPING_SETS_FUSED=0 must decline");
    }
}
