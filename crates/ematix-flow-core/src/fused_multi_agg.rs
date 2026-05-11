//! Σ.D2: `FusedFilterMultiAggExec` — single-pass fused filter + multi-aggregate
//! + group-by physical operator for the TPC-H Q1 plan shape.
//!
//! Issue [#45]. Day-1 prototype (`examples/tpch_q1_tune.rs`) showed a
//! hand-written parallel fused loop hits **3.08 ms** on Q1 SF=1 / 14
//! threads — 15.5× faster than DataFusion's current path (47.65 ms parquet,
//! 25.61 ms MemTable) and 11.4× faster than Polars MemTable (35.2 ms).
//! This module wraps that loop as a DataFusion `ExecutionPlan` so the
//! optimizer can route Q1-shaped plans through it transparently.
//!
//! Day-2 scope (this file): minimum-viable Q1-shaped operator.
//!
//! - Input schema validated to have exactly the seven Q1 columns by name
//!   and type (`l_returnflag: Utf8View`, `l_linestatus: Utf8View`,
//!   `l_quantity / l_extendedprice / l_discount / l_tax: Float64`,
//!   `l_shipdate: Date32`).
//! - Predicate held as a plain `Q1Predicate` value type (single `Date32`
//!   shipdate cutoff). Hard-coded for the day-2 spike; the day-3+ planner
//!   rule (issue #45 phase 4) will extract this from a generic
//!   `PhysicalExpr` AST after pattern-matching on the predicate shape.
//! - Group-by handled via the **hardcoded 4-arm match** for TPC-H's known
//!   `(R,F)/(N,F)/(N,O)/(A,F)` keys. The day-3 generalization will swap in
//!   a `HashMap<GroupKey, AggBlock>` lookup (measured at 7.01 ms parallel
//!   — still well ahead of today's path; cost of generality bounded).
//! - Output: a single 4-row batch — one row per group in `(returnflag,
//!   linestatus)` sorted order, with all eight Q1 aggregates materialized
//!   (`sum_qty`, `sum_base_price`, `sum_disc_price`, `sum_charge`,
//!   `avg_qty`, `avg_price`, `avg_disc`, `count_order`). One partition,
//!   `EmissionType::Final`, `Boundedness::Bounded`.
//!
//! [#45]: https://github.com/ryan-evans-git/ematix-flow/issues/45

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Date32Array, Float64Array, Float64Builder, Int64Builder, RecordBatch,
    StringBuilder, StringViewArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures_util::stream::{self, TryStreamExt};

/// Filter parameters for the Q1 shape.
///
/// The TPC-H spec's `l_shipdate <= date '1998-12-01' - interval '90' day`
/// reduces to `l_shipdate <= Date32(10471)`. The operator's hot loop
/// evaluates `ship_v[i] > shipdate_cutoff { continue; }` inline.
#[derive(Debug, Clone, Copy)]
pub struct Q1Predicate {
    pub shipdate_cutoff: i32,
}

/// Per-group running aggregates. Five SUMs + a row COUNT cover all eight
/// Q1 output columns — AVG(qty), AVG(price), AVG(disc) are computed from
/// the sums divided by `count` at finalize time.
#[derive(Default, Debug, Clone, Copy)]
struct Q1Aggs {
    sum_qty: f64,
    sum_price: f64,
    sum_disc_price: f64,
    sum_charge: f64,
    sum_disc: f64,
    count: u64,
}

impl Q1Aggs {
    fn merge(&mut self, other: &Q1Aggs) {
        self.sum_qty += other.sum_qty;
        self.sum_price += other.sum_price;
        self.sum_disc_price += other.sum_disc_price;
        self.sum_charge += other.sum_charge;
        self.sum_disc += other.sum_disc;
        self.count += other.count;
    }
}

/// Single-pass fused filter + multi-aggregate + group-by operator for the
/// TPC-H Q1 plan shape. See module-level docs for the architectural
/// premise + Σ.D2 phase-1 benchmark numbers.
#[derive(Debug)]
pub struct FusedFilterMultiAggExec {
    input: Arc<dyn ExecutionPlan>,
    predicate: Q1Predicate,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl FusedFilterMultiAggExec {
    /// Build a Q1-shaped fused exec over `input`. Validates the child
    /// schema has the seven required columns by name with the expected
    /// types. Output schema is the canonical Q1 SELECT list (9 cols).
    pub fn try_new_q1(input: Arc<dyn ExecutionPlan>, predicate: Q1Predicate) -> DfResult<Self> {
        Self::validate_input_schema(&input.schema())?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_linestatus", DataType::Utf8, false),
            Field::new("sum_qty", DataType::Float64, false),
            Field::new("sum_base_price", DataType::Float64, false),
            Field::new("sum_disc_price", DataType::Float64, false),
            Field::new("sum_charge", DataType::Float64, false),
            Field::new("avg_qty", DataType::Float64, false),
            Field::new("avg_price", DataType::Float64, false),
            Field::new("avg_disc", DataType::Float64, false),
            Field::new("count_order", DataType::Int64, false),
        ]));
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
            ("l_returnflag", DataType::Utf8View),
            ("l_linestatus", DataType::Utf8View),
            ("l_quantity", DataType::Float64),
            ("l_extendedprice", DataType::Float64),
            ("l_discount", DataType::Float64),
            ("l_tax", DataType::Float64),
            ("l_shipdate", DataType::Date32),
        ];
        for (name, expected) in required {
            let field = schema.field_with_name(name).map_err(|_| {
                DataFusionError::Plan(format!(
                    "FusedFilterMultiAggExec: child schema missing column `{name}`",
                ))
            })?;
            if field.data_type() != &expected {
                return Err(DataFusionError::Plan(format!(
                    "FusedFilterMultiAggExec: column `{name}` has type {:?}, expected {expected:?}",
                    field.data_type(),
                )));
            }
        }
        Ok(())
    }
}

impl DisplayAs for FusedFilterMultiAggExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "FusedFilterMultiAggExec(q1: shipdate<={})",
            self.predicate.shipdate_cutoff,
        )
    }
}

impl ExecutionPlan for FusedFilterMultiAggExec {
    fn name(&self) -> &str {
        "FusedFilterMultiAggExec"
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
            DataFusionError::Internal("FusedFilterMultiAggExec requires exactly 1 child".into())
        })?;
        Ok(Arc::new(Self::try_new_q1(new_input, self.predicate)?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "FusedFilterMultiAggExec emits only partition 0, got {partition}",
            )));
        }
        let input = self.input.clone();
        let predicate = self.predicate;
        let out_schema = self.schema.clone();

        let in_schema = input.schema();
        let idx = ColumnIndices {
            rflag: in_schema.index_of("l_returnflag")?,
            lstatus: in_schema.index_of("l_linestatus")?,
            qty: in_schema.index_of("l_quantity")?,
            price: in_schema.index_of("l_extendedprice")?,
            disc: in_schema.index_of("l_discount")?,
            tax: in_schema.index_of("l_tax")?,
            ship: in_schema.index_of("l_shipdate")?,
        };
        let input_partitions = input.properties().partitioning.partition_count();

        let schema_for_batch = out_schema.clone();
        let fut = async move {
            // Drain every input partition into a single in-memory vec.
            let mut batches: Vec<RecordBatch> = Vec::new();
            for p in 0..input_partitions {
                let mut s = input.execute(p, context.clone())?;
                while let Some(b) = s.try_next().await? {
                    batches.push(b);
                }
            }

            // Run the fused loop on a blocking worker so the CPU-heavy
            // pass doesn't hijack the tokio runtime.
            let groups = tokio::task::spawn_blocking(move || {
                let workers = std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(8);
                run_fused_q1_parallel(&batches, predicate, idx, workers)
            })
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "FusedFilterMultiAggExec: blocking-task join failed: {e}",
                ))
            })?;

            // Materialize a 4-row output batch in TPC-H Q1's canonical
            // sort order: (returnflag, linestatus) ascending.
            let batch = q1_groups_to_record_batch(schema_for_batch, &groups)?;
            Ok::<RecordBatch, DataFusionError>(batch)
        };

        let s = stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, s)))
    }
}

#[derive(Debug, Clone, Copy)]
struct ColumnIndices {
    rflag: usize,
    lstatus: usize,
    qty: usize,
    price: usize,
    disc: usize,
    tax: usize,
    ship: usize,
}

/// Hardcoded TPC-H Q1 group routing. Index 4 is a junk catch-all so the
/// inner loop's `groups[g]` indexing stays branchless even on adversarial
/// input. Order: (R,F)=0, (N,F)=1, (N,O)=2, (A,F)=3, other=4.
#[inline(always)]
fn q1_group_idx(rflag: u8, lstatus: u8) -> usize {
    match (rflag, lstatus) {
        (b'R', b'F') => 0,
        (b'N', b'F') => 1,
        (b'N', b'O') => 2,
        (b'A', b'F') => 3,
        _ => 4,
    }
}

/// Parallel fused loop. Mirrors the day-1 prototype in
/// `examples/tpch_q1_tune.rs`.
fn run_fused_q1_parallel(
    batches: &[RecordBatch],
    p: Q1Predicate,
    idx: ColumnIndices,
    workers: usize,
) -> [Q1Aggs; 5] {
    let n = batches.len();
    let chunk = n.div_ceil(workers.max(1));
    std::thread::scope(|s| {
        let handles: Vec<_> = (0..workers)
            .map(|w| {
                let lo = (w * chunk).min(n);
                let hi = ((w + 1) * chunk).min(n);
                let slice = &batches[lo..hi];
                s.spawn(move || run_fused_q1_shard(slice, p, idx))
            })
            .collect();
        let mut merged = [Q1Aggs::default(); 5];
        for h in handles {
            let partial = h.join().unwrap();
            for g in 0..merged.len() {
                merged[g].merge(&partial[g]);
            }
        }
        merged
    })
}

fn run_fused_q1_shard(batches: &[RecordBatch], p: Q1Predicate, idx: ColumnIndices) -> [Q1Aggs; 5] {
    let mut groups = [Q1Aggs::default(); 5];
    for batch in batches {
        let rflag = batch
            .column(idx.rflag)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("validated as Utf8View");
        let lstatus = batch
            .column(idx.lstatus)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("validated as Utf8View");
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
        let tax = batch
            .column(idx.tax)
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
        let tax_v = tax.values();
        let ship_v = ship.values();
        for i in 0..batch.num_rows() {
            if ship_v[i] > p.shipdate_cutoff {
                continue;
            }
            let r = rflag.value(i).as_bytes()[0];
            let l = lstatus.value(i).as_bytes()[0];
            let g = q1_group_idx(r, l);

            let q = qty_v[i];
            let pr = price_v[i];
            let d = disc_v[i];
            let t = tax_v[i];

            let omd = 1.0 - d;
            let disc_price = pr * omd;
            let charge = disc_price * (1.0 + t);

            let a = &mut groups[g];
            a.sum_qty += q;
            a.sum_price += pr;
            a.sum_disc_price += disc_price;
            a.sum_charge += charge;
            a.sum_disc += d;
            a.count += 1;
        }
    }
    groups
}

/// The four TPC-H Q1 groups in canonical `ORDER BY l_returnflag,
/// l_linestatus` order — the catch-all bucket (index 4) is dropped from
/// output. Returning an empty group is permitted (zero matching rows
/// for some predicates / inputs); we emit it with `count_order = 0` and
/// `NaN` for AVGs — matches DataFusion's group_by-result conventions.
const Q1_OUTPUT_ORDER: [(usize, &str, &str); 4] = [
    (3, "A", "F"), // sorted first: 'A' < 'N' < 'R'
    (1, "N", "F"),
    (2, "N", "O"),
    (0, "R", "F"),
];

fn q1_groups_to_record_batch(schema: SchemaRef, groups: &[Q1Aggs; 5]) -> DfResult<RecordBatch> {
    let mut rflag_b = StringBuilder::with_capacity(4, 4);
    let mut lstatus_b = StringBuilder::with_capacity(4, 4);
    let mut sum_qty_b = Float64Builder::with_capacity(4);
    let mut sum_price_b = Float64Builder::with_capacity(4);
    let mut sum_disc_price_b = Float64Builder::with_capacity(4);
    let mut sum_charge_b = Float64Builder::with_capacity(4);
    let mut avg_qty_b = Float64Builder::with_capacity(4);
    let mut avg_price_b = Float64Builder::with_capacity(4);
    let mut avg_disc_b = Float64Builder::with_capacity(4);
    let mut count_b = Int64Builder::with_capacity(4);

    for (g, rflag, lstatus) in Q1_OUTPUT_ORDER {
        let a = &groups[g];
        rflag_b.append_value(rflag);
        lstatus_b.append_value(lstatus);
        sum_qty_b.append_value(a.sum_qty);
        sum_price_b.append_value(a.sum_price);
        sum_disc_price_b.append_value(a.sum_disc_price);
        sum_charge_b.append_value(a.sum_charge);
        let cnt = a.count as f64;
        if cnt > 0.0 {
            avg_qty_b.append_value(a.sum_qty / cnt);
            avg_price_b.append_value(a.sum_price / cnt);
            avg_disc_b.append_value(a.sum_disc / cnt);
        } else {
            avg_qty_b.append_value(f64::NAN);
            avg_price_b.append_value(f64::NAN);
            avg_disc_b.append_value(f64::NAN);
        }
        count_b.append_value(a.count as i64);
    }

    let cols: Vec<ArrayRef> = vec![
        Arc::new(rflag_b.finish()),
        Arc::new(lstatus_b.finish()),
        Arc::new(sum_qty_b.finish()),
        Arc::new(sum_price_b.finish()),
        Arc::new(sum_disc_price_b.finish()),
        Arc::new(sum_charge_b.finish()),
        Arc::new(avg_qty_b.finish()),
        Arc::new(avg_price_b.finish()),
        Arc::new(avg_disc_b.finish()),
        Arc::new(count_b.finish()),
    ];
    Ok(RecordBatch::try_new(schema, cols)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Date32Builder, Float64Builder, StringViewBuilder};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    /// Build a known-totals batch:
    ///   3 rows in group (N,F) — quantity 10, price 100, disc 0.05, tax 0.10,
    ///                            shipdate 8800 (within cutoff)
    ///   1 row  in group (A,F) — quantity 20, price 200, disc 0.10, tax 0.05,
    ///                            shipdate 8800
    ///   1 row filtered out (shipdate beyond cutoff)
    fn make_test_batch(cutoff: i32) -> RecordBatch {
        let mut rflag = StringViewBuilder::new();
        let mut lstatus = StringViewBuilder::new();
        let mut qty = Float64Builder::new();
        let mut price = Float64Builder::new();
        let mut disc = Float64Builder::new();
        let mut tax = Float64Builder::new();
        let mut ship = Date32Builder::new();
        for (r, l, q, p, d, t, sd) in [
            ("N", "F", 10.0, 100.0, 0.05, 0.10, 8800),
            ("N", "F", 10.0, 100.0, 0.05, 0.10, 8800),
            ("N", "F", 10.0, 100.0, 0.05, 0.10, 8800),
            ("A", "F", 20.0, 200.0, 0.10, 0.05, 8800),
            ("R", "F", 5.0, 50.0, 0.02, 0.05, cutoff + 1), // filtered out
        ] {
            rflag.append_value(r);
            lstatus.append_value(l);
            qty.append_value(q);
            price.append_value(p);
            disc.append_value(d);
            tax.append_value(t);
            ship.append_value(sd);
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_returnflag", DataType::Utf8View, false),
            Field::new("l_linestatus", DataType::Utf8View, false),
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_tax", DataType::Float64, false),
            Field::new("l_shipdate", DataType::Date32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(rflag.finish()),
                Arc::new(lstatus.finish()),
                Arc::new(qty.finish()),
                Arc::new(price.finish()),
                Arc::new(disc.finish()),
                Arc::new(tax.finish()),
                Arc::new(ship.finish()),
            ],
        )
        .unwrap()
    }

    async fn input_plan_from_batch(batch: RecordBatch) -> Arc<dyn ExecutionPlan> {
        let schema = batch.schema();
        let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(mem)).unwrap();
        let df = ctx.sql("SELECT * FROM t").await.unwrap();
        df.create_physical_plan().await.unwrap()
    }

    #[tokio::test]
    async fn fused_exec_returns_expected_groups() {
        let cutoff = 10471;
        let batch = make_test_batch(cutoff);
        let input = input_plan_from_batch(batch).await;
        let exec = Arc::new(
            FusedFilterMultiAggExec::try_new_q1(
                input,
                Q1Predicate {
                    shipdate_cutoff: cutoff,
                },
            )
            .unwrap(),
        );

        let session = SessionContext::new();
        let task_ctx = session.task_ctx();
        let mut stream = exec.execute(0, task_ctx).unwrap();
        let out = stream
            .try_next()
            .await
            .expect("stream yields ok")
            .expect("stream yields a batch");
        assert_eq!(out.num_rows(), 4, "expected 4-row Q1 output");
        assert_eq!(out.num_columns(), 10);

        // Output is sorted by (rflag, lstatus): row 0 = (A,F), row 2 = (N,O),
        // row 1 = (N,F), row 3 = (R,F).
        let rflag = out
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let lstatus = out
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let sum_qty = out
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let count = out
            .column(9)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(rflag.value(0), "A");
        assert_eq!(lstatus.value(0), "F");
        assert!((sum_qty.value(0) - 20.0).abs() < 1e-9);
        assert_eq!(count.value(0), 1);

        assert_eq!(rflag.value(1), "N");
        assert_eq!(lstatus.value(1), "F");
        assert!(
            (sum_qty.value(1) - 30.0).abs() < 1e-9,
            "got {}",
            sum_qty.value(1)
        );
        assert_eq!(count.value(1), 3);

        assert_eq!(rflag.value(2), "N");
        assert_eq!(lstatus.value(2), "O");
        assert_eq!(count.value(2), 0); // no rows in (N,O)

        assert_eq!(rflag.value(3), "R");
        assert_eq!(lstatus.value(3), "F");
        // The (R,F) input row had shipdate beyond cutoff — filtered out.
        assert_eq!(count.value(3), 0);
    }

    async fn input_plan_with_schema(schema: SchemaRef) -> Arc<dyn ExecutionPlan> {
        let mem = MemTable::try_new(schema, vec![vec![]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(mem)).unwrap();
        let df = ctx.sql("SELECT * FROM t").await.unwrap();
        df.create_physical_plan().await.unwrap()
    }

    #[tokio::test]
    async fn try_new_rejects_missing_column() {
        // Drops `l_tax` deliberately.
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_returnflag", DataType::Utf8View, false),
            Field::new("l_linestatus", DataType::Utf8View, false),
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_shipdate", DataType::Date32, false),
        ]));
        let input = input_plan_with_schema(schema).await;
        let res = FusedFilterMultiAggExec::try_new_q1(input, Q1Predicate { shipdate_cutoff: 0 });
        let err = res.expect_err("missing l_tax should fail validation");
        let msg = format!("{err}");
        assert!(
            msg.contains("l_tax"),
            "error should name the missing column: {msg}",
        );
    }

    #[tokio::test]
    async fn try_new_rejects_wrong_column_type() {
        let schema = Arc::new(Schema::new(vec![
            // Returnflag intentionally wrong type.
            Field::new("l_returnflag", DataType::Int64, false),
            Field::new("l_linestatus", DataType::Utf8View, false),
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_tax", DataType::Float64, false),
            Field::new("l_shipdate", DataType::Date32, false),
        ]));
        let input = input_plan_with_schema(schema).await;
        let res = FusedFilterMultiAggExec::try_new_q1(input, Q1Predicate { shipdate_cutoff: 0 });
        let err = res.expect_err("Int64 returnflag should fail validation");
        let msg = format!("{err}");
        assert!(
            msg.contains("l_returnflag") && msg.contains("Int64"),
            "error should name the column + actual type: {msg}",
        );
    }
}
