//! `PartitionClaimResetExec` — RANGE.AGG Stage 2's partitioning cap.
//!
//! A 1:1 pass-through node that forwards its child's streams, schema
//! and statistics unchanged, but ADVERTISES
//! `Partitioning::UnknownPartitioning(n)` (n = child partition count)
//! and a fresh, empty `EquivalenceProperties`.
//!
//! ## Why
//!
//! [`crate::clustered_agg_rule::ClusteredSinglePhaseAggRule`] (RANGE.AGG)
//! rewrites a cluster-key group-by into
//! `AggregateExec(SinglePartitioned)` over a key-disjoint re-chunked
//! [`crate::ematix_fast_parquet::EmatixFastParquetExec`]. Stage 2 has
//! the chunked scan claim `Partitioning::Hash([group_key], n)` so
//! `EnforceDistribution` accepts it as the agg's input without
//! re-inserting the full-input hash shuffle (Q18 SF=100: 600M rows /
//! ~9.6 GB). That claim is row-correct FOR THE AGGREGATION — every
//! group's rows land in exactly one partition — but it is a lie about
//! hash-bucket placement: chunk boundaries are key ranges, not hash
//! buckets. If the claim propagated past the agg (AggregateExec maps
//! its input partitioning onto its output), a partitioned HashJoin
//! above could elide its build-side repartition and pair genuinely
//! hash-partitioned build partitions with range-chunked probe
//! partitions → silently missing join matches.
//!
//! This node is the cap: the rule wraps the rewritten aggregate in a
//! `PartitionClaimResetExec`, so everything downstream sees
//! `UnknownPartitioning` and `EnforceDistribution` re-satisfies any
//! downstream hash requirement by repartitioning the AGG'S OUTPUT —
//! which for Q18 sits above the `HAVING sum > 300` filter: ~10k rows
//! instead of 600M.
//!
//! Equivalence properties are deliberately NOT forwarded: they are
//! rebuilt empty from the schema, so no equivalence/ordering fact
//! derived from the false hash claim can leak either.
//!
//! ## Plan-cache interaction
//!
//! The plan cache ([`crate::plan_cache`]) keys on canonicalised SQL —
//! plan nodes never hash into the key. Per-node participation is (a)
//! the `is_cacheable` name denylist (stateful nodes opt out) and (b)
//! re-execute safety of `with_new_children`. This node is stateless
//! and `with_new_children` builds a fresh instance around the new
//! child, so cached plans containing it rebuild and re-execute
//! safely.

use std::any::Any;
use std::sync::Arc;

use datafusion::common::{DataFusionError, Result as DfResult, Statistics};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};

/// Pass-through that re-advertises its child's partitioning as
/// `UnknownPartitioning` (see module docs).
#[derive(Debug)]
pub struct PartitionClaimResetExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl PartitionClaimResetExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let child_props = input.properties();
        let n = child_props.output_partitioning().partition_count();
        let properties = Arc::new(PlanProperties::new(
            // Fresh (empty) equivalence properties — nothing derived
            // from the child's (possibly claimed) partitioning.
            EquivalenceProperties::new(input.schema()),
            Partitioning::UnknownPartitioning(n),
            child_props.emission_type,
            child_props.boundedness,
        ));
        Self { input, properties }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for PartitionClaimResetExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "PartitionClaimResetExec: partitions={}",
            self.properties.output_partitioning().partition_count()
        )
    }
}

impl ExecutionPlan for PartitionClaimResetExec {
    fn name(&self) -> &str {
        "PartitionClaimResetExec"
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

    /// Identity transform → per-partition order is preserved (we just
    /// don't ADVERTISE any ordering, which is the safe direction).
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    /// The child is a completed single-phase aggregate whose partition
    /// count the RANGE.AGG rule chose deliberately — never ask
    /// `EnforceDistribution` to round-robin more parallelism into it.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "PartitionClaimResetExec expects 1 child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self::new(Arc::clone(&children[0]))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DfResult<Statistics> {
        self.input.partition_statistics(partition)
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::ExecutionPlanProperties;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    fn two_partition_source() -> Arc<dyn ExecutionPlan> {
        use datafusion::datasource::memory::MemorySourceConfig;
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let b2 = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![4, 5]))])
            .unwrap();
        MemorySourceConfig::try_new_exec(&[vec![b1], vec![b2]], schema, None).unwrap()
    }

    /// The node forwards partition count and rows unchanged while
    /// resetting the ADVERTISED partitioning to Unknown — even when
    /// the child claims Hash.
    #[tokio::test]
    async fn resets_partitioning_and_forwards_rows() {
        let source = two_partition_source();
        // Wrap the source's properties in a fake Hash claim by
        // asserting on a reset over the plain source first (Unknown in
        // → Unknown out, same count)…
        let reset: Arc<dyn ExecutionPlan> =
            Arc::new(PartitionClaimResetExec::new(Arc::clone(&source)));
        assert_eq!(reset.output_partitioning().partition_count(), 2);
        assert!(matches!(
            reset.output_partitioning(),
            Partitioning::UnknownPartitioning(2)
        ));
        // …and the row pass-through is exact.
        let ctx = SessionContext::new();
        let batches = collect(reset, ctx.task_ctx()).await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 5);
    }

    /// A child advertising `Partitioning::Hash` must come out as
    /// `UnknownPartitioning` with the same partition count, and the
    /// reset's equivalence properties must be fresh (no constants /
    /// orderings inherited).
    #[tokio::test]
    async fn hides_a_hash_claim_from_parents() {
        // Cheap Hash-claiming wrapper for the test.
        #[derive(Debug)]
        struct HashClaim {
            input: Arc<dyn ExecutionPlan>,
            props: Arc<PlanProperties>,
        }
        impl DisplayAs for HashClaim {
            fn fmt_as(
                &self,
                _t: DisplayFormatType,
                f: &mut std::fmt::Formatter,
            ) -> std::fmt::Result {
                write!(f, "HashClaim")
            }
        }
        impl ExecutionPlan for HashClaim {
            fn name(&self) -> &str {
                "HashClaim"
            }
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn properties(&self) -> &Arc<PlanProperties> {
                &self.props
            }
            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
                vec![&self.input]
            }
            fn with_new_children(
                self: Arc<Self>,
                mut c: Vec<Arc<dyn ExecutionPlan>>,
            ) -> DfResult<Arc<dyn ExecutionPlan>> {
                let input = c.remove(0);
                let props = self.props.clone();
                Ok(Arc::new(Self { input, props }))
            }
            fn execute(
                &self,
                partition: usize,
                context: Arc<TaskContext>,
            ) -> DfResult<SendableRecordBatchStream> {
                self.input.execute(partition, context)
            }
        }

        let source = two_partition_source();
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(source.schema()),
            Partitioning::Hash(vec![Arc::new(Column::new("k", 0))], 2),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));
        let claiming: Arc<dyn ExecutionPlan> = Arc::new(HashClaim {
            input: source,
            props,
        });
        assert!(matches!(
            claiming.output_partitioning(),
            Partitioning::Hash(_, 2)
        ));

        let reset: Arc<dyn ExecutionPlan> = Arc::new(PartitionClaimResetExec::new(claiming));
        assert!(
            matches!(
                reset.output_partitioning(),
                Partitioning::UnknownPartitioning(2)
            ),
            "the hash claim must not survive the reset node"
        );

        // with_new_children rebuilds a fresh node (plan-cache rebuild
        // path) that still resets.
        let rebuilt = reset
            .with_new_children(vec![two_partition_source()])
            .unwrap();
        assert!(matches!(
            rebuilt.output_partitioning(),
            Partitioning::UnknownPartitioning(2)
        ));
    }
}
