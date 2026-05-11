//! Σ.D1: `FusedFilterSumExec` — single-pass fused filter-+-SUM physical operator.
//!
//! Issue [#44]. Day-1 spike proved a hand-written single-pass loop hits 1.0 ms
//! on TPC-H Q6 SF=1 / 14 threads (vs DataFusion's `FilterExec → AggregateExec`
//! path at 5.96 ms and Polars at 1.9 ms). This module wraps that loop as a
//! DataFusion `ExecutionPlan` so the optimizer can swap it in for matching
//! plan shapes.
//!
//! Day-2 scope (this file): minimum-viable Q6-shaped operator constructed by
//! hand. Holds a hard-coded `Q6Predicate` and the four canonical column names
//! (`l_quantity`, `l_extendedprice`, `l_discount`, `l_shipdate`). The
//! `try_new_q6` constructor validates the child schema; `execute` collects
//! batches from every child partition, fans the fused loop across logical
//! cores via `std::thread::scope` inside a `tokio::spawn_blocking`, and
//! emits a single-row batch on output partition 0.
//!
//! Day-3 scope (issue #44, follow-on commit): a `PhysicalOptimizerRule` that
//! recognizes `Aggregate(SUM(price*disc)) over Filter(predicate)` plan
//! shapes and rewrites them to this exec. Generalizes the predicate/agg-expr
//! from "hard-coded Q6" to "arbitrary `Aggregate(SUM) over Filter` whose
//! predicate is pure column comparisons + arithmetic". Out of scope today.
//!
//! [#44]: https://github.com/ryan-evans-git/ematix-flow/issues/44

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Date32Array, Float64Array, RecordBatch,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};
use futures_util::stream::{self, TryStreamExt};

/// Closed-range parameters for the canonical TPC-H Q6 predicate.
///
/// ```text
/// l_shipdate ∈ [date_lo, date_hi)        — Date32, days since 1970-01-01
/// l_discount ∈ [disc_lo, disc_hi]        — Float64
/// l_quantity <  qty_hi                   — Float64
/// ```
///
/// Kept as plain values (not `PhysicalExpr`) for the day-2 spike. The
/// day-3 planner rule will extract these from a generic `PhysicalExpr`
/// AST after pattern-matching on the predicate shape.
#[derive(Debug, Clone, Copy)]
pub struct Q6Predicate {
    pub date_lo: i32,
    pub date_hi: i32,
    pub disc_lo: f64,
    pub disc_hi: f64,
    pub qty_hi: f64,
}

/// Single-pass fused filter-+-SUM operator for the Q6 plan shape.
///
/// **Hot path.** Drains every batch from every child partition into a single
/// in-memory `Vec<RecordBatch>`, then runs the fused loop sharded across
/// logical cores. Emits one row on output partition 0 containing the running
/// f64 sum.
///
/// **Not streaming.** The operator holds all input in memory before computing
/// the result — `EmissionType::Final`, `Boundedness::Bounded`. For streaming
/// SUM aggregation a different operator shape would apply.
#[derive(Debug)]
pub struct FusedFilterSumExec {
    input: Arc<dyn ExecutionPlan>,
    predicate: Q6Predicate,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl FusedFilterSumExec {
    /// Build a Q6-shaped fused exec over `input`. Validates that the child
    /// schema contains the four required columns by name with the expected
    /// types. Output schema is one column, `revenue: Float64`.
    pub fn try_new_q6(
        input: Arc<dyn ExecutionPlan>,
        predicate: Q6Predicate,
    ) -> DfResult<Self> {
        Self::validate_input_schema(&input.schema())?;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "revenue",
            DataType::Float64,
            false,
        )]));
        let eq_props = EquivalenceProperties::new(schema.clone());
        let properties = Arc::new(PlanProperties::new(
            eq_props,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            predicate,
            schema,
            properties,
        })
    }

    fn validate_input_schema(schema: &SchemaRef) -> DfResult<()> {
        let required = [
            ("l_quantity", DataType::Float64),
            ("l_extendedprice", DataType::Float64),
            ("l_discount", DataType::Float64),
            ("l_shipdate", DataType::Date32),
        ];
        for (name, expected) in required {
            let field = schema.field_with_name(name).map_err(|_| {
                DataFusionError::Plan(format!(
                    "FusedFilterSumExec: child schema missing column `{name}`",
                ))
            })?;
            if field.data_type() != &expected {
                return Err(DataFusionError::Plan(format!(
                    "FusedFilterSumExec: column `{name}` has type {:?}, expected {expected:?}",
                    field.data_type(),
                )));
            }
        }
        Ok(())
    }
}

impl DisplayAs for FusedFilterSumExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        let p = &self.predicate;
        write!(
            f,
            "FusedFilterSumExec(q6: shipdate∈[{},{}), discount∈[{},{}], quantity<{})",
            p.date_lo, p.date_hi, p.disc_lo, p.disc_hi, p.qty_hi,
        )
    }
}

impl ExecutionPlan for FusedFilterSumExec {
    fn name(&self) -> &str {
        "FusedFilterSumExec"
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
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let new_input = children.pop().ok_or_else(|| {
            DataFusionError::Internal(
                "FusedFilterSumExec requires exactly 1 child".into(),
            )
        })?;
        Ok(Arc::new(Self::try_new_q6(new_input, self.predicate)?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "FusedFilterSumExec emits only partition 0, got {partition}"
            )));
        }
        let input = self.input.clone();
        let predicate = self.predicate;
        let schema = self.schema.clone();

        let in_schema = input.schema();
        let idx_qty = in_schema.index_of("l_quantity")?;
        let idx_price = in_schema.index_of("l_extendedprice")?;
        let idx_disc = in_schema.index_of("l_discount")?;
        let idx_ship = in_schema.index_of("l_shipdate")?;
        let indices = ColumnIndices {
            qty: idx_qty,
            price: idx_price,
            disc: idx_disc,
            ship: idx_ship,
        };

        let input_partitions = input.properties().partitioning.partition_count();

        let schema_for_batch = schema.clone();
        let fut = async move {
            // Drain every input partition.
            let mut batches: Vec<RecordBatch> = Vec::new();
            for p in 0..input_partitions {
                let mut s = input.execute(p, context.clone())?;
                while let Some(b) = s.try_next().await? {
                    batches.push(b);
                }
            }

            // Run the fused loop on a blocking worker so we don't
            // hijack the tokio runtime for ~ms of pure CPU work.
            let result = tokio::task::spawn_blocking(move || {
                let workers = std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(8);
                run_fused_parallel(&batches, workers, predicate, indices)
            })
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "FusedFilterSumExec: blocking-task join failed: {e}"
                ))
            })?;

            let revenue: ArrayRef = Arc::new(Float64Array::from(vec![result]));
            let batch = RecordBatch::try_new(schema_for_batch, vec![revenue])?;
            Ok::<RecordBatch, DataFusionError>(batch)
        };

        let s = stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, s)))
    }
}

#[derive(Debug, Clone, Copy)]
struct ColumnIndices {
    qty: usize,
    price: usize,
    disc: usize,
    ship: usize,
}

/// Parallel fused loop. Same algorithm as the spike's
/// `tpch_q6_tune::run_fused_parallel`: shard the batches across
/// `workers` `std::thread::scope` threads, run the inline-predicate
/// f64-sum loop on each shard, sum the partials on the main thread.
fn run_fused_parallel(
    batches: &[RecordBatch],
    workers: usize,
    p: Q6Predicate,
    idx: ColumnIndices,
) -> f64 {
    let n = batches.len();
    let chunk = n.div_ceil(workers.max(1));
    std::thread::scope(|s| {
        let handles: Vec<_> = (0..workers)
            .map(|w| {
                let lo = (w * chunk).min(n);
                let hi = ((w + 1) * chunk).min(n);
                let slice = &batches[lo..hi];
                s.spawn(move || run_fused_shard(slice, p, idx))
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).sum()
    })
}

fn run_fused_shard(batches: &[RecordBatch], p: Q6Predicate, idx: ColumnIndices) -> f64 {
    let mut sum: f64 = 0.0;
    for batch in batches {
        let qty = batch
            .column(idx.qty)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("validated as Float64");
        let price = batch
            .column(idx.price)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("validated as Float64");
        let disc = batch
            .column(idx.disc)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("validated as Float64");
        let ship = batch
            .column(idx.ship)
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("validated as Date32");
        let qty_v = qty.values();
        let price_v = price.values();
        let disc_v = disc.values();
        let ship_v = ship.values();
        for i in 0..batch.num_rows() {
            let s = ship_v[i];
            let d = disc_v[i];
            let q = qty_v[i];
            if s >= p.date_lo
                && s < p.date_hi
                && d >= p.disc_lo
                && d <= p.disc_hi
                && q < p.qty_hi
            {
                sum += price_v[i] * d;
            }
        }
    }
    sum
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Date32Builder, Float64Builder};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    /// Build a plain `MemTable`-backed input plan with the given batch
    /// as the sole partition. Cuts past DataFusion's renamed physical-
    /// plan APIs (`MemoryExec` is gone in DF 53 in favour of
    /// `MemorySourceConfig`, which isn't re-exported through the
    /// umbrella crate).
    async fn input_plan_from_batch(batch: RecordBatch) -> Arc<dyn ExecutionPlan> {
        let schema = batch.schema();
        let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(mem)).unwrap();
        let df = ctx.sql("SELECT * FROM t").await.unwrap();
        df.create_physical_plan().await.unwrap()
    }

    fn make_test_batch() -> RecordBatch {
        // 6 rows. Predicate (Q6 canonical):
        //   shipdate ∈ [1994-01-01=8766, 1995-01-01=9131)
        //   discount ∈ [0.05, 0.07]
        //   quantity < 24
        // Row 0:  match — contributes 100*0.06 = 6.0
        // Row 1:  fails  date (too early)
        // Row 2:  fails  date (too late)
        // Row 3:  fails  discount (below 0.05)
        // Row 4:  fails  discount (above 0.07)
        // Row 5:  fails  quantity (= 24, not <)
        let mut qty = Float64Builder::new();
        let mut price = Float64Builder::new();
        let mut disc = Float64Builder::new();
        let mut ship = Date32Builder::new();
        for (q, pr, dc, sd) in [
            (10.0, 100.0, 0.06, 8800), //  match → 6.0
            (10.0, 100.0, 0.06, 8000), //  pre-1994
            (10.0, 100.0, 0.06, 9500), //  post-1995
            (10.0, 100.0, 0.04, 8800), //  disc low
            (10.0, 100.0, 0.08, 8800), //  disc high
            (24.0, 100.0, 0.06, 8800), //  qty == 24
        ] {
            qty.append_value(q);
            price.append_value(pr);
            disc.append_value(dc);
            ship.append_value(sd);
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_shipdate", DataType::Date32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(qty.finish()),
                Arc::new(price.finish()),
                Arc::new(disc.finish()),
                Arc::new(ship.finish()),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn fused_exec_returns_expected_sum() {
        let input = input_plan_from_batch(make_test_batch()).await;

        let predicate = Q6Predicate {
            date_lo: 8766,
            date_hi: 9131,
            disc_lo: 0.05,
            disc_hi: 0.07,
            qty_hi: 24.0,
        };
        let exec = Arc::new(FusedFilterSumExec::try_new_q6(input, predicate).unwrap());

        let session = SessionContext::new();
        let task_ctx = session.task_ctx();
        let mut stream = exec.execute(0, task_ctx).unwrap();
        let batch = stream
            .try_next()
            .await
            .expect("stream yields ok")
            .expect("stream yields a batch");
        assert_eq!(batch.num_rows(), 1);
        let revenue = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        // Only row 0 matches the predicate: 100.0 * 0.06 = 6.0.
        assert!(
            (revenue - 6.0).abs() < 1e-9,
            "revenue {revenue} != expected 6.0"
        );
    }

    /// Build an empty-batch input plan with an arbitrary schema, used
    /// by the schema-validation tests below.
    async fn input_plan_with_schema(schema: SchemaRef) -> Arc<dyn ExecutionPlan> {
        let mem = MemTable::try_new(schema, vec![vec![]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(mem)).unwrap();
        let df = ctx.sql("SELECT * FROM t").await.unwrap();
        df.create_physical_plan().await.unwrap()
    }

    #[tokio::test]
    async fn try_new_rejects_missing_column() {
        // Drops `l_shipdate` deliberately.
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
        ]));
        let input = input_plan_with_schema(schema).await;
        let res = FusedFilterSumExec::try_new_q6(
            input,
            Q6Predicate {
                date_lo: 0,
                date_hi: 1,
                disc_lo: 0.0,
                disc_hi: 1.0,
                qty_hi: 100.0,
            },
        );
        let err = res.expect_err("missing l_shipdate should fail validation");
        let msg = format!("{err}");
        assert!(
            msg.contains("l_shipdate"),
            "error should mention missing column: {msg}",
        );
    }

    #[tokio::test]
    async fn try_new_rejects_wrong_column_type() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_quantity", DataType::Int64, false), // wrong
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_shipdate", DataType::Date32, false),
        ]));
        let input = input_plan_with_schema(schema).await;
        let res = FusedFilterSumExec::try_new_q6(
            input,
            Q6Predicate {
                date_lo: 0,
                date_hi: 1,
                disc_lo: 0.0,
                disc_hi: 1.0,
                qty_hi: 100.0,
            },
        );
        let err = res.expect_err("Int64 quantity should fail validation");
        let msg = format!("{err}");
        assert!(
            msg.contains("l_quantity") && msg.contains("Int64"),
            "error should mention column + actual type: {msg}",
        );
    }
}
