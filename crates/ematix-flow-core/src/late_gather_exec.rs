//! Q10-flip increment 3, step 3 — `LateGatherExec`: the pull-side of wide-string
//! late materialization.
//!
//! The aggregate below this operator groups on a compact `__cust_rowid` (a
//! `UInt32` GLOBAL build row index emitted by the customer-build
//! `EmatixHashJoinExec` via [`JoinColumn::BuildRowId`]) instead of carrying the 3
//! wide customer strings (c_name / c_address / c_comment) through the 11.46M-row
//! join intermediate (Q10 SF=100: a 6.5 GB `CoalesceBatches`). This node sits
//! ABOVE the aggregate and re-attaches the wide columns at the ~3.88M aggregate
//! outputs by gathering them from the SAME resident customer build batches —
//! SHARED via the join's `Arc<OnceCell<Arc<EmatHashJoiner>>>`, so there is NO
//! customer re-scan (the re-scan path measured +20.6% in the Q10.WS dig).
//!
//! This mirrors DuckDB's late materialization (it gathers the wide strings only
//! at the final groups, not through the join). Shape-general, not Q10-keyed: the
//! group key (a PK) functionally determines the wide cols, so they need not flow
//! through the join.
//!
//! INERT until the FlowQueryPlanner walker (increment 3 step 4) installs it; this
//! file is the operator + its unit test only — no production plan references it.

use std::any::Any;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, UInt32Array};
use arrow_schema::SchemaRef;
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use futures_util::StreamExt;
use tokio::sync::OnceCell;

use crate::emat_hash_join::EmatHashJoiner;

/// Source of an output column of [`LateGatherExec`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LateGatherColumn {
    /// Pass through column `idx` of the input (aggregate) batch unchanged.
    Input(usize),
    /// Gather build-schema column `idx` from the shared join build, resolving
    /// each output row through the `__cust_rowid` (`UInt32`) column.
    Build(usize),
}

/// Assemble one late-gather output batch: gather the referenced wide build
/// columns by the input batch's row-id column, then interleave them with the
/// pass-through (aggregate) columns per `output`.
///
/// Factored out of [`LateGatherExec::execute`] so the gather+assembly logic is
/// unit-testable without standing up an upstream `ExecutionPlan`. Returns a
/// kernel-style `String` error (the operator maps it to `DataFusionError`).
pub(crate) fn assemble_late_gather(
    input: &RecordBatch,
    joiner: &EmatHashJoiner,
    rowid_idx: usize,
    output: &[LateGatherColumn],
    out_schema: &SchemaRef,
) -> Result<RecordBatch, String> {
    let ids = input
        .column(rowid_idx)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or("LateGatherExec: rowid column is not UInt32")?;
    // Distinct build columns to gather, in first-seen order — one gather call.
    let mut build_idxs: Vec<usize> = Vec::new();
    for c in output {
        if let LateGatherColumn::Build(i) = c {
            if !build_idxs.contains(i) {
                build_idxs.push(*i);
            }
        }
    }
    let gathered = joiner.gather_build_cols(ids, &build_idxs)?;
    let cols: Vec<ArrayRef> = output
        .iter()
        .map(|c| match c {
            LateGatherColumn::Input(i) => input.column(*i).clone(),
            LateGatherColumn::Build(i) => {
                // position() is over ≤ a handful of wide cols — negligible.
                let pos = build_idxs.iter().position(|x| x == i).unwrap();
                gathered[pos].clone()
            }
        })
        .collect();
    RecordBatch::try_new(out_schema.clone(), cols)
        .map_err(|e| format!("LateGatherExec assemble: {e}"))
}

pub struct LateGatherExec {
    /// Input = the (rewritten) aggregate, whose output carries `__cust_rowid`
    /// instead of the wide build columns.
    input: Arc<dyn ExecutionPlan>,
    /// SHARED with the customer-build join below — already initialized by the
    /// time the aggregate emits (the agg pulls through the join). No re-scan.
    build_once: Arc<OnceCell<Arc<EmatHashJoiner>>>,
    /// Index of the `__cust_rowid` `UInt32` column in the input schema.
    rowid_idx: usize,
    output: Vec<LateGatherColumn>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl LateGatherExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        build_once: Arc<OnceCell<Arc<EmatHashJoiner>>>,
        rowid_idx: usize,
        output: Vec<LateGatherColumn>,
        output_schema: SchemaRef,
    ) -> Self {
        let properties = Arc::new(Self::compute_properties(&input, &output_schema));
        Self {
            input,
            build_once,
            rowid_idx,
            output,
            schema: output_schema,
            properties,
        }
    }

    /// 1:1 per-batch operator → preserve the input's partitioning.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        output_schema: &SchemaRef,
    ) -> PlanProperties {
        let eq = EquivalenceProperties::new(output_schema.clone());
        PlanProperties::new(
            eq,
            input.output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl std::fmt::Debug for LateGatherExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LateGatherExec(rowid@{}, out_cols={})",
            self.rowid_idx,
            self.output.len()
        )
    }
}

impl DisplayAs for LateGatherExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let n_gathered = self
            .output
            .iter()
            .filter(|c| matches!(c, LateGatherColumn::Build(_)))
            .count();
        write!(
            f,
            "LateGatherExec: rowid@{}, gathered_cols={}",
            self.rowid_idx, n_gathered
        )
    }
}

impl ExecutionPlan for LateGatherExec {
    fn name(&self) -> &str {
        "LateGatherExec"
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

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "LateGatherExec expects 1 child, got {}",
                children.len()
            )));
        }
        // PRESERVE the shared build handle (do NOT mint a fresh OnceCell).
        let input = children[0].clone();
        let properties = Arc::new(Self::compute_properties(&input, &self.schema));
        Ok(Arc::new(Self {
            input,
            build_once: self.build_once.clone(),
            rowid_idx: self.rowid_idx,
            output: self.output.clone(),
            schema: self.schema.clone(),
            properties,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, ctx)?;
        let build_once = self.build_once.clone();
        let rowid_idx = self.rowid_idx;
        let output = self.output.clone();
        let out_schema = self.schema.clone();
        let stream_schema = self.schema.clone();

        let mapped = input.map(move |rb| {
            let rb = rb?;
            // The shared build is initialized by the join below before the agg
            // (this op's input) can emit; `get()` is therefore Some here.
            let joiner = build_once.get().cloned().ok_or_else(|| {
                DataFusionError::Internal(
                    "LateGatherExec: shared join build not initialized".into(),
                )
            })?;
            assemble_late_gather(&rb, &joiner, rowid_idx, &output, &out_schema)
                .map_err(DataFusionError::Internal)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            mapped.boxed(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::emat_hash_join::JoinColumn;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    /// (i64 key, Utf8 wide payload) build batch — customer-shaped.
    fn ck_name_batch(keys: Vec<i64>, names: Vec<&str>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("c_custkey", DataType::Int64, false),
                Field::new("c_name", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn late_gather_reattaches_wide_col_by_rowid_cross_batch() {
        // Customer-like build across 2 batches (so gather uses interleave, the
        // SF=100 path). global ids: 0=(10,alice) 1=(20,bob) 2=(30,carol)
        // 3=(40,dave). build key idx 0; the join `output` is irrelevant to
        // gather (BuildRowId placeholder).
        let b0 = ck_name_batch(vec![10, 20], vec!["alice", "bob"]); // 0,1
        let b1 = ck_name_batch(vec![30, 40], vec!["carol", "dave"]); // 2,3
        let dummy = Arc::new(Schema::new(vec![Field::new(
            "rid",
            DataType::UInt32,
            false,
        )]));
        let joiner =
            EmatHashJoiner::try_build(&[b0, b1], 0, 0, vec![JoinColumn::BuildRowId], dummy)
                .unwrap();

        // Rewritten aggregate output: [c_custkey, __cust_rowid]. Say the agg
        // produced groups for custkeys 30,10,40 → rowids 2,0,3 (out of order,
        // spanning both build batches).
        let agg_schema = Arc::new(Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, false),
            Field::new("__cust_rowid", DataType::UInt32, false),
        ]));
        let agg = RecordBatch::try_new(
            agg_schema,
            vec![
                Arc::new(Int64Array::from(vec![30i64, 10, 40])),
                Arc::new(UInt32Array::from(vec![2u32, 0, 3])),
            ],
        )
        .unwrap();

        // Final schema: [c_custkey passthrough, c_name gathered from build col 1].
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, false),
            Field::new("c_name", DataType::Utf8, false),
        ]));
        let out = assemble_late_gather(
            &agg,
            &joiner,
            1, // __cust_rowid is input col 1
            &[LateGatherColumn::Input(0), LateGatherColumn::Build(1)],
            &out_schema,
        )
        .unwrap();

        assert_eq!(out.num_rows(), 3);
        let ck = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let nm = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            ck.values(),
            &[30i64, 10, 40],
            "custkey passthrough preserved"
        );
        assert_eq!(
            (0..3).map(|i| nm.value(i)).collect::<Vec<_>>(),
            vec!["carol", "alice", "dave"],
            "rowid 2/0/3 → carol/alice/dave gathered cross-batch from shared build"
        );
    }
}
