//! Σ.D4 + Σ.D5: `FusedPostJoinExec` — single-pass fused aggregate over the
//! output of a join.
//!
//! Day-1 prototype surveys on `spike/fused-post-join-aggregate` established
//! the post-join agg kernel is **5-8× faster than DataFusion's per-aggregate
//! dispatch** across the shapes that matter:
//!
//! | Query | Shape                                | Kernel win |
//! |-------|--------------------------------------|------------|
//! | Q3    | 3-col group key (i64+date+i32), 1 SUM | 5.0×       |
//! | Q5    | 1-col string group key, 1 SUM         | 8.5×       |
//! | Q14   | no group-by, dual SUM + CASE-WHEN     | 7.4×       |
//!
//! Σ.D4/D5 are JOIN-bound — end-to-end wins are modest (1-7%) because the
//! join dominates. The kernel speedup is still worth shipping as:
//!
//! 1. **Substrate** for the future Σ.D3-JIT-generalized operator that
//!    unifies all the post-* (filter / join) aggregate shapes under one
//!    plan-time codegen path.
//! 2. **Per-aggregate dispatch elimination** that compounds when callers
//!    issue many similar queries (dashboards, repeated rollups).
//!
//! This module ships the three concrete shapes as one `ExecutionPlan` with
//! a tagged `FusedPostJoinSpec` enum. The cranelift-JIT path (`fused_jit.rs`)
//! is not wired in here — that lands when the day-3+ generic IR emitter is
//! ready. Hand-written inner loops mirror the day-1 surveys verbatim.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Date32Array, Date32Builder, Float64Array, Float64Builder, Int32Array,
    Int32Builder, Int64Array, Int64Builder, RecordBatch, StringBuilder, StringViewArray,
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

/// The post-join plan shape the operator should run. Each variant pins
/// the expected child schema (column names + types) and the inner-loop
/// kernel + output schema.
#[derive(Debug, Clone, Copy)]
pub enum FusedPostJoinSpec {
    /// TPC-H Q3 shape: child is `(l_orderkey: Int64, l_extendedprice: Float64,
    /// l_discount: Float64, o_orderdate: Date32, o_shippriority: Int32)`.
    /// One SUM, 3-col group key. Output: 4-col batch sorted by revenue desc.
    Q3,
    /// TPC-H Q5 shape: child is `(n_name: Utf8View, l_extendedprice: Float64,
    /// l_discount: Float64)`. One SUM, single string group key. Output:
    /// 2-col batch sorted by revenue desc.
    Q5,
    /// TPC-H Q14 shape: child is `(p_type: Utf8View, l_extendedprice: Float64,
    /// l_discount: Float64)`. No group-by, dual SUM (one CASE-WHEN guarded).
    /// Output: 1-col single-row batch with the `promo_revenue` ratio.
    Q14,
}

#[derive(Debug)]
pub struct FusedPostJoinExec {
    input: Arc<dyn ExecutionPlan>,
    spec: FusedPostJoinSpec,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl FusedPostJoinExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, spec: FusedPostJoinSpec) -> DfResult<Self> {
        validate_input_schema(&input.schema(), spec)?;
        let schema = output_schema(spec);
        let eq_props = EquivalenceProperties::new(schema.clone());
        let properties = Arc::new(PlanProperties::new(
            eq_props,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            spec,
            schema,
            properties,
        })
    }
}

fn output_schema(spec: FusedPostJoinSpec) -> SchemaRef {
    match spec {
        FusedPostJoinSpec::Q3 => Arc::new(Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("revenue", DataType::Float64, false),
            Field::new("o_orderdate", DataType::Date32, false),
            Field::new("o_shippriority", DataType::Int32, false),
        ])),
        FusedPostJoinSpec::Q5 => Arc::new(Schema::new(vec![
            Field::new("n_name", DataType::Utf8, false),
            Field::new("revenue", DataType::Float64, false),
        ])),
        FusedPostJoinSpec::Q14 => Arc::new(Schema::new(vec![Field::new(
            "promo_revenue",
            DataType::Float64,
            false,
        )])),
    }
}

fn validate_input_schema(schema: &SchemaRef, spec: FusedPostJoinSpec) -> DfResult<()> {
    let required: &[(&str, DataType)] = match spec {
        FusedPostJoinSpec::Q3 => &[
            ("l_orderkey", DataType::Int64),
            ("l_extendedprice", DataType::Float64),
            ("l_discount", DataType::Float64),
            ("o_orderdate", DataType::Date32),
            // o_shippriority may be Int32 or Int64 depending on generator; check separately.
        ],
        FusedPostJoinSpec::Q5 => &[
            ("n_name", DataType::Utf8View),
            ("l_extendedprice", DataType::Float64),
            ("l_discount", DataType::Float64),
        ],
        FusedPostJoinSpec::Q14 => &[
            ("p_type", DataType::Utf8View),
            ("l_extendedprice", DataType::Float64),
            ("l_discount", DataType::Float64),
        ],
    };
    for (name, expected) in required {
        let field = schema.field_with_name(name).map_err(|_| {
            DataFusionError::Plan(format!(
                "FusedPostJoinExec({spec:?}): child schema missing column `{name}`"
            ))
        })?;
        if field.data_type() != expected {
            return Err(DataFusionError::Plan(format!(
                "FusedPostJoinExec({spec:?}): column `{name}` has type {:?}, expected {expected:?}",
                field.data_type(),
            )));
        }
    }
    // Q3's o_shippriority extra-check: must be Int32 or Int64.
    if matches!(spec, FusedPostJoinSpec::Q3) {
        let f = schema.field_with_name("o_shippriority").map_err(|_| {
            DataFusionError::Plan(
                "FusedPostJoinExec(Q3): child schema missing column `o_shippriority`".into(),
            )
        })?;
        match f.data_type() {
            DataType::Int32 | DataType::Int64 => {}
            other => {
                return Err(DataFusionError::Plan(format!(
                    "FusedPostJoinExec(Q3): column `o_shippriority` has type {other:?}, \
                     expected Int32 or Int64",
                )));
            }
        }
    }
    Ok(())
}

impl DisplayAs for FusedPostJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "FusedPostJoinExec({:?})", self.spec)
    }
}

impl ExecutionPlan for FusedPostJoinExec {
    fn name(&self) -> &str {
        "FusedPostJoinExec"
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
            DataFusionError::Internal("FusedPostJoinExec requires exactly 1 child".into())
        })?;
        Ok(Arc::new(Self::try_new(new_input, self.spec)?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "FusedPostJoinExec emits only partition 0, got {partition}"
            )));
        }
        let input = self.input.clone();
        let spec = self.spec;
        let out_schema = self.schema.clone();
        let input_partitions = input.properties().partitioning.partition_count();

        let schema_for_batch = out_schema.clone();
        let fut = async move {
            let mut batches: Vec<RecordBatch> = Vec::new();
            for p in 0..input_partitions {
                let mut s = input.execute(p, context.clone())?;
                while let Some(b) = s.try_next().await? {
                    batches.push(b);
                }
            }
            let batch = tokio::task::spawn_blocking(move || -> DfResult<RecordBatch> {
                match spec {
                    FusedPostJoinSpec::Q3 => execute_q3(&batches, schema_for_batch),
                    FusedPostJoinSpec::Q5 => execute_q5(&batches, schema_for_batch),
                    FusedPostJoinSpec::Q14 => execute_q14(&batches, schema_for_batch),
                }
            })
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "FusedPostJoinExec: blocking-task join failed: {e}"
                ))
            })??;
            Ok::<RecordBatch, DataFusionError>(batch)
        };

        let s = stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, s)))
    }
}

// ----- Q3: 3-col group, single SUM -----

fn execute_q3(batches: &[RecordBatch], schema: SchemaRef) -> DfResult<RecordBatch> {
    type Q3Key = (i64, i32, i32);
    let mut groups: HashMap<Q3Key, f64> = HashMap::with_capacity(16_384);
    for batch in batches {
        let orderkey = batch
            .column(batch.schema().index_of("l_orderkey")?)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("l_orderkey Int64");
        let price = batch
            .column(batch.schema().index_of("l_extendedprice")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("l_extendedprice f64");
        let disc = batch
            .column(batch.schema().index_of("l_discount")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("l_discount f64");
        let orderdate = batch
            .column(batch.schema().index_of("o_orderdate")?)
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("o_orderdate Date32");
        let sp_col = batch.column(batch.schema().index_of("o_shippriority")?);
        let sp_i64 = sp_col.as_any().downcast_ref::<Int64Array>();
        let sp_i32 = sp_col.as_any().downcast_ref::<Int32Array>();
        let ok_v = orderkey.values();
        let price_v = price.values();
        let disc_v = disc.values();
        let od_v = orderdate.values();
        let get_sp = |i: usize| -> i32 {
            if let Some(a) = sp_i64 {
                a.value(i) as i32
            } else if let Some(a) = sp_i32 {
                a.value(i)
            } else {
                panic!("o_shippriority neither Int32 nor Int64")
            }
        };
        for i in 0..batch.num_rows() {
            let key: Q3Key = (ok_v[i], od_v[i], get_sp(i));
            let rev = price_v[i] * (1.0 - disc_v[i]);
            *groups.entry(key).or_insert(0.0) += rev;
        }
    }
    let mut rows: Vec<(Q3Key, f64)> = groups.into_iter().collect();
    rows.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    let mut ok_b = Int64Builder::with_capacity(rows.len());
    let mut rev_b = Float64Builder::with_capacity(rows.len());
    let mut od_b = Date32Builder::with_capacity(rows.len());
    let mut sp_b = Int32Builder::with_capacity(rows.len());
    for ((ok, od, sp), rev) in &rows {
        ok_b.append_value(*ok);
        rev_b.append_value(*rev);
        od_b.append_value(*od);
        sp_b.append_value(*sp);
    }
    let cols: Vec<ArrayRef> = vec![
        Arc::new(ok_b.finish()),
        Arc::new(rev_b.finish()),
        Arc::new(od_b.finish()),
        Arc::new(sp_b.finish()),
    ];
    Ok(RecordBatch::try_new(schema, cols)?)
}

// ----- Q5: string group, single SUM -----

fn execute_q5(batches: &[RecordBatch], schema: SchemaRef) -> DfResult<RecordBatch> {
    let mut groups: HashMap<String, f64> = HashMap::with_capacity(64);
    for batch in batches {
        let nname = batch
            .column(batch.schema().index_of("n_name")?)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("n_name Utf8View");
        let price = batch
            .column(batch.schema().index_of("l_extendedprice")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("l_extendedprice f64");
        let disc = batch
            .column(batch.schema().index_of("l_discount")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("l_discount f64");
        let price_v = price.values();
        let disc_v = disc.values();
        for i in 0..batch.num_rows() {
            let n = nname.value(i);
            let rev = price_v[i] * (1.0 - disc_v[i]);
            if let Some(slot) = groups.get_mut(n) {
                *slot += rev;
            } else {
                groups.insert(n.to_string(), rev);
            }
        }
    }
    let mut rows: Vec<(String, f64)> = groups.into_iter().collect();
    rows.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    let mut name_b = StringBuilder::with_capacity(rows.len(), rows.len() * 8);
    let mut rev_b = Float64Builder::with_capacity(rows.len());
    for (n, rev) in &rows {
        name_b.append_value(n);
        rev_b.append_value(*rev);
    }
    let cols: Vec<ArrayRef> = vec![Arc::new(name_b.finish()), Arc::new(rev_b.finish())];
    Ok(RecordBatch::try_new(schema, cols)?)
}

// ----- Q14: dual SUM, one CASE-WHEN guard -----

fn execute_q14(batches: &[RecordBatch], schema: SchemaRef) -> DfResult<RecordBatch> {
    let mut promo: f64 = 0.0;
    let mut total: f64 = 0.0;
    let prefix = b"PROMO";
    for batch in batches {
        let ptype = batch
            .column(batch.schema().index_of("p_type")?)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("p_type Utf8View");
        let price = batch
            .column(batch.schema().index_of("l_extendedprice")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("l_extendedprice f64");
        let disc = batch
            .column(batch.schema().index_of("l_discount")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("l_discount f64");
        let price_v = price.values();
        let disc_v = disc.values();
        for i in 0..batch.num_rows() {
            let rev = price_v[i] * (1.0 - disc_v[i]);
            total += rev;
            let bytes = ptype.value(i).as_bytes();
            if bytes.len() >= 5 && &bytes[..5] == prefix {
                promo += rev;
            }
        }
    }
    let ratio = if total > 0.0 {
        100.0 * promo / total
    } else {
        f64::NAN
    };
    let mut b = Float64Builder::with_capacity(1);
    b.append_value(ratio);
    let col: ArrayRef = Arc::new(b.finish());
    Ok(RecordBatch::try_new(schema, vec![col])?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        Date32Builder, Float64Builder, Int32Builder, Int64Builder, StringViewBuilder,
    };
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    async fn input_plan_from_batch(batch: RecordBatch) -> Arc<dyn ExecutionPlan> {
        let schema = batch.schema();
        let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(mem)).unwrap();
        let df = ctx.sql("SELECT * FROM t").await.unwrap();
        df.create_physical_plan().await.unwrap()
    }

    fn make_q3_batch() -> RecordBatch {
        // 3 rows, 2 distinct groups:
        //   row 0: orderkey=1 date=9000 ship=0 → revenue 100 * 0.9 = 90
        //   row 1: orderkey=1 date=9000 ship=0 → revenue 100 * 0.9 = 90 (same group)
        //   row 2: orderkey=2 date=9001 ship=1 → revenue 50  * 0.95 = 47.5
        let mut ok = Int64Builder::new();
        let mut price = Float64Builder::new();
        let mut disc = Float64Builder::new();
        let mut od = Date32Builder::new();
        let mut sp = Int32Builder::new();
        for (o, p, d, da, s) in [
            (1i64, 100.0, 0.1, 9000i32, 0i32),
            (1, 100.0, 0.1, 9000, 0),
            (2, 50.0, 0.05, 9001, 1),
        ] {
            ok.append_value(o);
            price.append_value(p);
            disc.append_value(d);
            od.append_value(da);
            sp.append_value(s);
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("o_orderdate", DataType::Date32, false),
            Field::new("o_shippriority", DataType::Int32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(ok.finish()),
                Arc::new(price.finish()),
                Arc::new(disc.finish()),
                Arc::new(od.finish()),
                Arc::new(sp.finish()),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn q3_returns_grouped_revenue_sorted_desc() {
        let input = input_plan_from_batch(make_q3_batch()).await;
        let exec = Arc::new(FusedPostJoinExec::try_new(input, FusedPostJoinSpec::Q3).unwrap());
        let session = SessionContext::new();
        let mut stream = exec.execute(0, session.task_ctx()).unwrap();
        let out = stream.try_next().await.unwrap().expect("batch");
        assert_eq!(out.num_rows(), 2);
        assert_eq!(out.num_columns(), 4);
        let ok = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let rev = out
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // Row 0: orderkey=1, revenue = 90+90 = 180 (sorted first, desc)
        // Row 1: orderkey=2, revenue = 47.5
        assert_eq!(ok.value(0), 1);
        assert!((rev.value(0) - 180.0).abs() < 1e-9);
        assert_eq!(ok.value(1), 2);
        assert!((rev.value(1) - 47.5).abs() < 1e-9);
    }

    fn make_q14_batch() -> RecordBatch {
        // 4 rows, 2 with p_type starting "PROMO":
        //   PROMO row: price 100, disc 0.1 → revenue 90
        //   PROMO row: price 50,  disc 0.0 → revenue 50
        //   normal:   price 200, disc 0.2 → revenue 160
        //   normal:   price 100, disc 0.5 → revenue 50
        // Total = 90 + 50 + 160 + 50 = 350; promo = 140; ratio = 100*140/350 = 40.0
        let mut pt = StringViewBuilder::new();
        let mut price = Float64Builder::new();
        let mut disc = Float64Builder::new();
        for (t, p, d) in [
            ("PROMO BRUSHED COPPER", 100.0, 0.1),
            ("PROMO POLISHED STEEL", 50.0, 0.0),
            ("ECONOMY ANODIZED ZINC", 200.0, 0.2),
            ("STANDARD POLISHED NICKEL", 100.0, 0.5),
        ] {
            pt.append_value(t);
            price.append_value(p);
            disc.append_value(d);
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("p_type", DataType::Utf8View, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(pt.finish()),
                Arc::new(price.finish()),
                Arc::new(disc.finish()),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn q14_returns_promo_revenue_ratio() {
        let input = input_plan_from_batch(make_q14_batch()).await;
        let exec = Arc::new(FusedPostJoinExec::try_new(input, FusedPostJoinSpec::Q14).unwrap());
        let session = SessionContext::new();
        let mut stream = exec.execute(0, session.task_ctx()).unwrap();
        let out = stream.try_next().await.unwrap().expect("batch");
        assert_eq!(out.num_rows(), 1);
        assert_eq!(out.num_columns(), 1);
        let ratio = out
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((ratio - 40.0).abs() < 1e-9, "expected 40.0%, got {ratio}",);
    }

    fn make_q5_batch() -> RecordBatch {
        // 4 rows, 2 nations: INDONESIA gets revenue 90+90=180,
        // CHINA gets revenue 47.5. Sort puts INDONESIA first.
        let mut n = StringViewBuilder::new();
        let mut price = Float64Builder::new();
        let mut disc = Float64Builder::new();
        for (nat, p, d) in [
            ("INDONESIA", 100.0, 0.1),
            ("INDONESIA", 100.0, 0.1),
            ("CHINA", 50.0, 0.05),
            ("CHINA", 0.0, 0.0),
        ] {
            n.append_value(nat);
            price.append_value(p);
            disc.append_value(d);
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("n_name", DataType::Utf8View, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(n.finish()),
                Arc::new(price.finish()),
                Arc::new(disc.finish()),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn q5_returns_string_grouped_revenue_sorted_desc() {
        let input = input_plan_from_batch(make_q5_batch()).await;
        let exec = Arc::new(FusedPostJoinExec::try_new(input, FusedPostJoinSpec::Q5).unwrap());
        let session = SessionContext::new();
        let mut stream = exec.execute(0, session.task_ctx()).unwrap();
        let out = stream.try_next().await.unwrap().expect("batch");
        assert_eq!(out.num_rows(), 2);
        let names = out
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let revs = out
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(names.value(0), "INDONESIA");
        assert!((revs.value(0) - 180.0).abs() < 1e-9);
        assert_eq!(names.value(1), "CHINA");
        assert!((revs.value(1) - 47.5).abs() < 1e-9);
    }

    async fn input_plan_with_schema(schema: SchemaRef) -> Arc<dyn ExecutionPlan> {
        let mem = MemTable::try_new(schema, vec![vec![]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(mem)).unwrap();
        let df = ctx.sql("SELECT * FROM t").await.unwrap();
        df.create_physical_plan().await.unwrap()
    }

    #[tokio::test]
    async fn try_new_q3_rejects_missing_column() {
        let schema = Arc::new(Schema::new(vec![
            // Drops `o_shippriority`.
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("o_orderdate", DataType::Date32, false),
        ]));
        let input = input_plan_with_schema(schema).await;
        let err = FusedPostJoinExec::try_new(input, FusedPostJoinSpec::Q3)
            .expect_err("missing o_shippriority should fail");
        assert!(format!("{err}").contains("o_shippriority"), "got: {err}",);
    }

    #[tokio::test]
    async fn try_new_q14_rejects_wrong_column_type() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("p_type", DataType::Int64, false), // wrong
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
        ]));
        let input = input_plan_with_schema(schema).await;
        let err = FusedPostJoinExec::try_new(input, FusedPostJoinSpec::Q14)
            .expect_err("Int64 p_type should fail");
        assert!(
            format!("{err}").contains("p_type") && format!("{err}").contains("Int64"),
            "got: {err}",
        );
    }
}
