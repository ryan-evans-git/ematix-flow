//! `BatchSizeOverrideExec` — run a subtree with a query-scoped execution batch
//! size.
//!
//! Batch size is a RUNTIME `TaskContext` parameter: the operators that re-batch
//! (HashJoinExec / RepartitionExec / AggregateExec output sizing, the parquet
//! readers' windows) read `task_ctx.session_config().batch_size()` at
//! `execute()` time. There is NO plan-time hook for it — a plan-time re-plan
//! with a boosted session is inert (profiled). This node bridges that gap: at
//! `execute()` it threads a `TaskContext` carrying an overridden
//! `SessionConfig::batch_size` to its child, so the ENTIRE subtree below runs at
//! that batch size — WITHOUT mutating the session (which would change every other
//! query the session runs).
//!
//! Used by the wide-string late-materialization path
//! ([`crate::late_mat_agg_planner`]): the `LateGatherExec` reattach is cheap only
//! with few large build batches, and the win also needs the 600M-row probe fact
//! decode at a large batch — but a GLOBAL large batch regresses the other 21
//! TPC-H queries (prod-D). Wrapping ONLY the late-mat query's root in this node
//! makes the large batch query-scoped: Q10 SF=100 → 1941ms (beats DuckDB) while
//! every other query keeps the session default. General infra (not Q10-keyed).

use std::any::Any;
use std::sync::Arc;

use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

/// Pass-through node that executes its child under an overridden batch size.
#[derive(Debug)]
pub struct BatchSizeOverrideExec {
    input: Arc<dyn ExecutionPlan>,
    batch_size: usize,
    /// Mirrors the child's properties (this node is a 1:1 identity transform).
    properties: Arc<PlanProperties>,
}

impl BatchSizeOverrideExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, batch_size: usize) -> Self {
        let properties = input.properties().clone();
        Self {
            input,
            batch_size,
            properties,
        }
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
}

impl DisplayAs for BatchSizeOverrideExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "BatchSizeOverrideExec: batch_size={}", self.batch_size)
    }
}

impl ExecutionPlan for BatchSizeOverrideExec {
    fn name(&self) -> &str {
        "BatchSizeOverrideExec"
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

    /// Identity transform → preserves the child's ordering.
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "BatchSizeOverrideExec expects 1 child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self::new(children[0].clone(), self.batch_size)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        // Thread a context with the overridden batch size to the whole subtree.
        let cfg = context
            .session_config()
            .clone()
            .with_batch_size(self.batch_size);
        let boosted = Arc::new(TaskContext::new(
            context.task_id(),
            context.session_id(),
            cfg,
            context.scalar_functions().clone(),
            context.aggregate_functions().clone(),
            context.window_functions().clone(),
            context.runtime_env(),
        ));
        self.input.execute(partition, boosted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::{collect, ExecutionPlanProperties};
    use datafusion::prelude::SessionContext;

    /// A leaf that RECORDS the `batch_size` of the context it is executed with,
    /// so we can prove the override threads through. Emits an empty stream.
    #[derive(Debug)]
    struct ProbeExec {
        seen: Arc<Mutex<Option<usize>>>,
        schema: SchemaRef,
        props: Arc<PlanProperties>,
    }
    impl ProbeExec {
        fn new(seen: Arc<Mutex<Option<usize>>>) -> Self {
            let schema: SchemaRef =
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
            let props = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ));
            Self { seen, schema, props }
        }
    }
    impl DisplayAs for ProbeExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "ProbeExec")
        }
    }
    impl ExecutionPlan for ProbeExec {
        fn name(&self) -> &str {
            "ProbeExec"
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            &self.props
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            _c: Vec<Arc<dyn ExecutionPlan>>,
        ) -> DfResult<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(
            &self,
            _partition: usize,
            context: Arc<TaskContext>,
        ) -> DfResult<SendableRecordBatchStream> {
            *self.seen.lock().unwrap() = Some(context.session_config().batch_size());
            let s = futures_util::stream::empty::<DfResult<RecordBatch>>();
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema.clone(),
                s,
            )))
        }
    }

    /// The node threads its overridden `batch_size` into the child's execute
    /// context (so the WHOLE subtree below runs at that size), while the bare
    /// child sees the session default — proving the override is query-scoped.
    #[tokio::test]
    async fn threads_overridden_batch_size_to_child() {
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let default_bs = task_ctx.session_config().batch_size();

        // Bare child: sees the session default.
        let seen0 = Arc::new(Mutex::new(None));
        let bare: Arc<dyn ExecutionPlan> = Arc::new(ProbeExec::new(seen0.clone()));
        let _ = collect(bare, task_ctx.clone()).await.unwrap();
        assert_eq!(*seen0.lock().unwrap(), Some(default_bs));

        // Wrapped child: sees the override (1_048_576), NOT the session default.
        let seen1 = Arc::new(Mutex::new(None));
        let probe: Arc<dyn ExecutionPlan> = Arc::new(ProbeExec::new(seen1.clone()));
        let wrapped: Arc<dyn ExecutionPlan> =
            Arc::new(BatchSizeOverrideExec::new(probe, 1_048_576));
        assert_eq!(wrapped.output_partitioning().partition_count(), 1);
        let _ = collect(wrapped, task_ctx.clone()).await.unwrap();
        assert_eq!(
            *seen1.lock().unwrap(),
            Some(1_048_576),
            "child must execute under the overridden batch size"
        );
        // And the session itself is untouched (query-scoped, not global).
        assert_eq!(
            task_ctx.session_config().batch_size(),
            default_bs,
            "the session config must be unchanged"
        );
    }
}
