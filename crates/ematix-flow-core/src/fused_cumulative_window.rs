//! v2 S2.5 — fused cumulative-window operator (q51 A/B prototype).
//!
//! The window gate (S2.0) left **q51** OPEN: its `BoundedWindowAggExec`s
//! (cumulative `sum`/`max` over `Decimal128`, `PARTITION BY item_sk ORDER
//! BY d_date ROWS UNBOUNDED PRECEDING AND CURRENT ROW`) are ~59% of SF10
//! compute. DataFusion already runs these single-pass in `mode=[Sorted]`,
//! so this is a **constant-factor** bet: a tight streaming i128
//! running-accumulator vs DF's generic bounded-frame evaluator.
//!
//! This operator is scoped to exactly that shape and gated on
//! `EMAT_WINDOW_FUSED=1` (read fresh at plan time) so it can be A/B'd
//! against DF-native in one process. It **ships only if it beats
//! DF-native** — otherwise it is reverted (the S1 discipline).
//!
//! Correctness: streaming, carries running state across batches; resets
//! at each partition-key change (input is sorted, so partitions are
//! contiguous); SQL null semantics (sum/max ignore null inputs, output is
//! null until the first non-null in a partition). Scale is uniform (2)
//! across q51's decimals, so running sum = i128 add and max = i128 max —
//! no rescaling.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Decimal128Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DataFusionError, Result as DfResult, ScalarValue};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{WindowFrameBound, WindowFrameUnits};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, InputOrderMode, PlanProperties,
    SendableRecordBatchStream,
};
use futures_util::StreamExt;

/// Fused cumulative-window rewrite. **Default ON** (proven 1.65× on q51
/// SF10, row-identical); kill-switch `EMAT_WINDOW_FUSED=0`. Read fresh at
/// plan time so a single-process A/B toggles cleanly.
fn fused_enabled() -> bool {
    crate::flags::enabled("EMAT_WINDOW_FUSED")
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum AggKind {
    Sum,
    Max,
}

#[derive(Clone)]
struct AggSpec {
    kind: AggKind,
    in_idx: usize,
    precision: u8,
    scale: i8,
    field: Field,
}

/// A recognized all-cumulative `BoundedWindowAggExec`.
#[derive(Clone)]
struct CumSpec {
    part_idx: usize,
    aggs: Vec<AggSpec>,
    output_schema: SchemaRef,
}

/// Recognize a `BoundedWindowAggExec` whose every window expr is a
/// cumulative `sum`/`max` over `Decimal128` (scale 2) sharing a single
/// `Int64` partition-by column. Returns None on any mismatch.
fn recognize(bw: &BoundedWindowAggExec) -> Option<CumSpec> {
    // Only when the input is FULLY sorted by [partition, order] — the
    // operator relies on partitions being contiguous. (q51 is Sorted; a
    // Linear/PartiallySorted window would break the running-state reset.)
    if !matches!(bw.input_order_mode, InputOrderMode::Sorted) {
        return None;
    }
    let wexprs = bw.window_expr();
    if wexprs.is_empty() {
        return None;
    }
    let input_schema = bw.input().schema();
    let pb = wexprs[0].partition_by();
    if pb.len() != 1 {
        return None;
    }
    let part_idx = pb[0].as_any().downcast_ref::<Column>()?.index();
    if input_schema.field(part_idx).data_type() != &DataType::Int64 {
        return None;
    }

    let mut aggs = Vec::with_capacity(wexprs.len());
    for w in wexprs {
        // Same single partition column.
        let wpb = w.partition_by();
        if wpb.len() != 1 || wpb[0].as_any().downcast_ref::<Column>()?.index() != part_idx {
            return None;
        }
        // Frame: ROWS UNBOUNDED PRECEDING AND CURRENT ROW.
        let frame = w.get_window_frame();
        if frame.units != WindowFrameUnits::Rows
            || !matches!(
                frame.start_bound,
                WindowFrameBound::Preceding(ScalarValue::UInt64(None))
            )
            || !matches!(frame.end_bound, WindowFrameBound::CurrentRow)
        {
            return None;
        }
        // Kind from the window function name.
        let name = w.name();
        let kind = if name.starts_with("sum(") {
            AggKind::Sum
        } else if name.starts_with("max(") {
            AggKind::Max
        } else {
            return None;
        };
        // Single Column arg.
        let args = w.expressions();
        if args.len() != 1 {
            return None;
        }
        let in_idx = args[0].as_any().downcast_ref::<Column>()?.index();
        // Output + input must be Decimal128 with scale 2.
        let field = w.field().ok()?;
        let (precision, scale) = match field.data_type() {
            DataType::Decimal128(p, 2) => (*p, 2i8),
            _ => return None,
        };
        if !matches!(
            input_schema.field(in_idx).data_type(),
            DataType::Decimal128(_, 2)
        ) {
            return None;
        }
        aggs.push(AggSpec {
            kind,
            in_idx,
            precision,
            scale,
            field: field.as_ref().clone(),
        });
    }

    // Output schema = input fields ++ each window field (matches DF's
    // BoundedWindowAggExec::create_schema).
    let mut fields: Vec<Field> = input_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    for a in &aggs {
        fields.push(a.field.clone());
    }
    let output_schema = Arc::new(Schema::new(fields));
    Some(CumSpec {
        part_idx,
        aggs,
        output_schema,
    })
}

/// Streaming running-accumulator state, carried across batches.
struct RunState {
    /// `Some(key)` once a partition has been seen; the key may be a null
    /// partition value (represented as `None` inside).
    cur_key: Option<Option<i64>>,
    /// Per-agg running value (`None` = no non-null seen yet this partition).
    accs: Vec<Option<i128>>,
}

/// Compute the cumulative results for one already-sorted input batch,
/// carrying/updating `state`. Emits the input columns unchanged plus one
/// running-result `Decimal128` column per agg.
fn process_batch(
    state: &mut RunState,
    spec: &CumSpec,
    schema: &SchemaRef,
    batch: RecordBatch,
) -> DfResult<RecordBatch> {
    let n = batch.num_rows();
    let pk = batch.column(spec.part_idx).as_primitive::<Int64Type>();
    let vals: Vec<&Decimal128Array> = spec
        .aggs
        .iter()
        .map(|a| {
            batch
                .column(a.in_idx)
                .as_primitive::<datafusion::arrow::datatypes::Decimal128Type>()
        })
        .collect();

    let na = spec.aggs.len();
    let mut out_vals: Vec<Vec<i128>> = vec![vec![0i128; n]; na];
    let mut out_valid: Vec<Vec<bool>> = vec![vec![false; n]; na];

    for row in 0..n {
        let key = if pk.is_null(row) {
            None
        } else {
            Some(pk.value(row))
        };
        if state.cur_key != Some(key) {
            state.cur_key = Some(key);
            for a in &mut state.accs {
                *a = None;
            }
        }
        for (ai, agg) in spec.aggs.iter().enumerate() {
            let vcol = vals[ai];
            if !vcol.is_null(row) {
                let v = vcol.value(row);
                state.accs[ai] = Some(match (agg.kind, state.accs[ai]) {
                    (AggKind::Sum, Some(acc)) => acc + v,
                    (AggKind::Sum, None) => v,
                    (AggKind::Max, Some(acc)) => acc.max(v),
                    (AggKind::Max, None) => v,
                });
            }
            if let Some(r) = state.accs[ai] {
                out_vals[ai][row] = r;
                out_valid[ai][row] = true;
            }
        }
    }

    let mut cols: Vec<ArrayRef> = batch.columns().to_vec();
    for (ai, agg) in spec.aggs.iter().enumerate() {
        let arr =
            Decimal128Array::from_iter((0..n).map(|r| out_valid[ai][r].then_some(out_vals[ai][r])))
                .with_precision_and_scale(agg.precision, agg.scale)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        cols.push(Arc::new(arr));
    }
    RecordBatch::try_new(schema.clone(), cols)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Fused cumulative-window operator. Preserves the child's partitioning +
/// emission; appends one running-result column per recognized window agg.
pub struct FusedCumulativeWindowExec {
    input: Arc<dyn ExecutionPlan>,
    spec: CumSpec,
    properties: Arc<PlanProperties>,
}

impl FusedCumulativeWindowExec {
    fn new(input: Arc<dyn ExecutionPlan>, spec: CumSpec) -> Self {
        let child_props = input.properties();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(spec.output_schema.clone()),
            child_props.output_partitioning().clone(),
            child_props.emission_type,
            child_props.boundedness,
        ));
        Self {
            input,
            spec,
            properties,
        }
    }
}

impl fmt::Debug for FusedCumulativeWindowExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "FusedCumulativeWindowExec(aggs={})",
            self.spec.aggs.len()
        )
    }
}

impl DisplayAs for FusedCumulativeWindowExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let kinds: Vec<&str> = self
            .spec
            .aggs
            .iter()
            .map(|a| match a.kind {
                AggKind::Sum => "sum",
                AggKind::Max => "max",
            })
            .collect();
        write!(
            f,
            "FusedCumulativeWindowExec: part_col={}, cumulative=[{}]",
            self.spec.part_idx,
            kinds.join(", ")
        )
    }
}

impl ExecutionPlan for FusedCumulativeWindowExec {
    fn name(&self) -> &str {
        "FusedCumulativeWindowExec"
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

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let input = children.pop().ok_or_else(|| {
            DataFusionError::Internal("FusedCumulativeWindowExec needs exactly 1 child".into())
        })?;
        Ok(Arc::new(Self::new(input, self.spec.clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let spec = self.spec.clone();
        let schema = self.spec.output_schema.clone();
        let state = RunState {
            cur_key: None,
            accs: vec![None; spec.aggs.len()],
        };
        let out = input.scan(state, move |state, batch_res| {
            let mapped = batch_res.and_then(|batch| process_batch(state, &spec, &schema, batch));
            futures_util::future::ready(Some(mapped))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.spec.output_schema.clone(),
            out,
        )))
    }
}

/// Physical rule: swap a recognized cumulative `BoundedWindowAggExec` for
/// [`FusedCumulativeWindowExec`], gated on `EMAT_WINDOW_FUSED=1`.
#[derive(Debug, Default)]
pub struct InjectCumulativeWindowRule;

impl PhysicalOptimizerRule for InjectCumulativeWindowRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if !fused_enabled() {
            return Ok(plan);
        }
        let out = plan.transform_up(|node| {
            if let Some(bw) = node.as_any().downcast_ref::<BoundedWindowAggExec>() {
                if let Some(spec) = recognize(bw) {
                    let op = FusedCumulativeWindowExec::new(bw.input().clone(), spec);
                    return Ok(Transformed::yes(Arc::new(op) as Arc<dyn ExecutionPlan>));
                }
            }
            Ok(Transformed::no(node))
        })?;
        Ok(out.data)
    }

    fn name(&self) -> &str {
        "ematix_cumulative_window"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Decimal128Array, Int64Array};
    use datafusion::arrow::datatypes::Field;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::{collect, displayable};

    fn set_fused(on: bool) {
        // SAFETY: guarded by EMAT_ENV_TEST_LOCK in the caller.
        unsafe { std::env::set_var("EMAT_WINDOW_FUSED", if on { "1" } else { "0" }) };
    }

    /// The fused cumulative-window operator must produce **row-identical**
    /// output to DF-native on the shared v2 session — and it must actually
    /// fire (the plan uses `FusedCumulativeWindowExec`, no
    /// `BoundedWindowAggExec` left). Hermetic: a tiny sorted Decimal128
    /// table, no data files. Guards the S2.5 operator's correctness.
    #[tokio::test]
    async fn fused_cumulative_window_matches_df_native() {
        let _env = crate::flags::EMAT_ENV_TEST_LOCK.lock().await;

        let schema = Arc::new(Schema::new(vec![
            Field::new("p", DataType::Int64, false),
            Field::new("o", DataType::Int64, false),
            Field::new("v", DataType::Decimal128(10, 2), true),
        ]));
        // p=1: v=10,20,30 → cume 10,30,60 ; p=2: v=5,NULL,15 → cume 5,5,20.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 1, 1, 2, 2, 2])),
                Arc::new(Int64Array::from(vec![1_i64, 2, 3, 1, 2, 3])),
                Arc::new(
                    Decimal128Array::from(vec![
                        Some(1000_i128),
                        Some(2000),
                        Some(3000),
                        Some(500),
                        None,
                        Some(1500),
                    ])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
                ),
            ],
        )
        .unwrap();

        let sql = "SELECT p, o, sum(v) OVER (PARTITION BY p ORDER BY o \
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cs \
                   FROM t ORDER BY p, o";

        let render = |batches: &[RecordBatch]| -> Vec<String> {
            use datafusion::arrow::util::display::array_value_to_string;
            let mut out = Vec::new();
            for b in batches {
                for r in 0..b.num_rows() {
                    let cells: Vec<String> = (0..b.num_columns())
                        .map(|c| array_value_to_string(b.column(c), r).unwrap_or_default())
                        .collect();
                    out.push(cells.join("|"));
                }
            }
            out
        };

        // Fused (default-on) — assert it fires, capture rows.
        set_fused(true);
        let ctx = crate::preset::session_context();
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch.clone()]]).unwrap()),
        )
        .unwrap();
        let fused_plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let tree = format!("{}", displayable(fused_plan.as_ref()).indent(false));
        assert!(
            tree.contains("FusedCumulativeWindowExec"),
            "operator must fire; plan:\n{tree}"
        );
        assert!(
            !tree.contains("BoundedWindowAggExec"),
            "no DF window op should remain; plan:\n{tree}"
        );
        let fused_rows = render(&collect(fused_plan, ctx.task_ctx()).await.unwrap());

        // DF-native (=0).
        set_fused(false);
        let ctx2 = crate::preset::session_context();
        ctx2.register_table(
            "t",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        let df_rows = render(
            &collect(
                ctx2.sql(sql)
                    .await
                    .unwrap()
                    .create_physical_plan()
                    .await
                    .unwrap(),
                ctx2.task_ctx(),
            )
            .await
            .unwrap(),
        );

        assert_eq!(fused_rows, df_rows, "fused output must match DF-native");
        // Pin the actual cumulative values (incl. the NULL-skipping row).
        assert_eq!(
            fused_rows,
            vec![
                "1|1|10.00",
                "1|2|30.00",
                "1|3|60.00",
                "2|1|5.00",
                "2|2|5.00", // v NULL → running sum unchanged
                "2|3|20.00",
            ]
        );

        // SAFETY: guarded by the env lock; restore default.
        unsafe { std::env::remove_var("EMAT_WINDOW_FUSED") };
    }
}
