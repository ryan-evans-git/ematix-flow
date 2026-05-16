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
    /// TPC-H Q12 shape: child is `(l_shipmode: Utf8View, o_orderpriority:
    /// Utf8View)`. Small-cardinality group-by (2 values: MAIL/SHIP) +
    /// two CASE-WHEN-counted aggregates over o_orderpriority. Output:
    /// 3-col 2-row batch (l_shipmode, high_line_count, low_line_count)
    /// sorted by l_shipmode.
    Q12,
}

#[derive(Debug)]
pub struct FusedPostJoinExec {
    input: Arc<dyn ExecutionPlan>,
    spec: FusedPostJoinSpec,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    /// Optional spec-driven Cranelift JIT (Σ.D3 phase C). Currently only
    /// the Q14 variant supports the JIT path — Q3 and Q5 need hash-
    /// group-by-in-IR (deferred to a future phase) because their group
    /// cardinality is unknown at plan-time.
    jit: Option<Arc<crate::fused_jit::FusedFilterAggJit>>,
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
            jit: None,
        })
    }

    /// Same shape as [`try_new`] but routes the inner loop through the
    /// Cranelift-JIT'd `FusedFilterAggJit`. Currently only supported
    /// for `FusedPostJoinSpec::Q14`; Q3/Q5 fall back to the hand-coded
    /// path. Returns an error for unsupported specs so the caller
    /// can't silently get the wrong execution mode.
    /// Σ.D3 phase D introspection accessors.
    pub fn spec(&self) -> FusedPostJoinSpec {
        self.spec
    }
    pub fn has_jit(&self) -> bool {
        self.jit.is_some()
    }
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn try_new_jit(input: Arc<dyn ExecutionPlan>, spec: FusedPostJoinSpec) -> DfResult<Self> {
        match spec {
            FusedPostJoinSpec::Q14 => {
                let mut exec = Self::try_new(input, spec)?;
                let s = crate::fused_jit::FusedFilterAggSpec::q14_post_join();
                let jit = crate::fused_jit::FusedFilterAggJit::try_build(&s).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "FusedPostJoinExec: Q14 JIT build failed: {e}"
                    ))
                })?;
                exec.jit = Some(Arc::new(jit));
                Ok(exec)
            }
            other => Err(DataFusionError::Plan(format!(
                "FusedPostJoinExec::try_new_jit: spec {other:?} not yet JIT-supported \
                 (Q3/Q5 require hash-group-by-in-IR; deferred to a future Σ.D3 phase)"
            ))),
        }
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
        FusedPostJoinSpec::Q12 => Arc::new(Schema::new(vec![
            Field::new("l_shipmode", DataType::Utf8, false),
            Field::new("high_line_count", DataType::Int64, false),
            Field::new("low_line_count", DataType::Int64, false),
        ])),
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
        FusedPostJoinSpec::Q12 => &[
            ("l_shipmode", DataType::Utf8View),
            ("o_orderpriority", DataType::Utf8View),
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
        let next = if self.jit.is_some() {
            Self::try_new_jit(new_input, self.spec)?
        } else {
            Self::try_new(new_input, self.spec)?
        };
        Ok(Arc::new(next))
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
        let jit = self.jit.clone();
        let input_partitions = input.properties().partitioning.partition_count();

        let schema_for_batch = out_schema.clone();
        let fut = async move {
            // Σ.D3 phase D follow-up: per-partition streaming, no
            // materialisation. One async task per input partition, each
            // maintains its own per-spec accumulator (HashMap for
            // Q3/Q5, [f64; 2] for Q14). When every stream ends, merge
            // partitions' accumulators and emit the final batch.
            //
            // The earlier shape drained every partition into a
            // Vec<RecordBatch> before computing, which serialised the
            // upstream pipeline (FastParquet scan + join had to fully
            // finish before any of our aggregate work started). The
            // InjectFusedQ3/Q5Rule benchmarks landed at -61% / -103%
            // pre-refactor because of exactly that drain-then-compute
            // penalty.
            match spec {
                FusedPostJoinSpec::Q3 => {
                    let mut handles = Vec::with_capacity(input_partitions);
                    for p in 0..input_partitions {
                        let mut s = input.execute(p, context.clone())?;
                        handles.push(tokio::spawn(async move {
                            let mut groups: std::collections::HashMap<(i64, i32, i32), f64> =
                                std::collections::HashMap::with_capacity(4096);
                            while let Some(batch) = s.try_next().await? {
                                accumulate_q3_batch(&batch, &mut groups)?;
                            }
                            Ok::<_, DataFusionError>(groups)
                        }));
                    }
                    let mut merged: std::collections::HashMap<(i64, i32, i32), f64> =
                        std::collections::HashMap::with_capacity(4096);
                    for h in handles {
                        let partial = h.await.map_err(|e| {
                            DataFusionError::Execution(format!(
                                "FusedPostJoinExec(Q3): worker join failed: {e}"
                            ))
                        })??;
                        for (k, v) in partial {
                            *merged.entry(k).or_insert(0.0) += v;
                        }
                    }
                    emit_q3(schema_for_batch, merged)
                }
                FusedPostJoinSpec::Q5 => {
                    let mut handles = Vec::with_capacity(input_partitions);
                    for p in 0..input_partitions {
                        let mut s = input.execute(p, context.clone())?;
                        handles.push(tokio::spawn(async move {
                            let mut groups: std::collections::HashMap<String, f64> =
                                std::collections::HashMap::with_capacity(64);
                            while let Some(batch) = s.try_next().await? {
                                accumulate_q5_batch(&batch, &mut groups)?;
                            }
                            Ok::<_, DataFusionError>(groups)
                        }));
                    }
                    let mut merged: std::collections::HashMap<String, f64> =
                        std::collections::HashMap::with_capacity(64);
                    for h in handles {
                        let partial = h.await.map_err(|e| {
                            DataFusionError::Execution(format!(
                                "FusedPostJoinExec(Q5): worker join failed: {e}"
                            ))
                        })??;
                        for (k, v) in partial {
                            *merged.entry(k).or_insert(0.0) += v;
                        }
                    }
                    emit_q5(schema_for_batch, merged)
                }
                FusedPostJoinSpec::Q12 => {
                    let mut handles = Vec::with_capacity(input_partitions);
                    for p in 0..input_partitions {
                        let mut s = input.execute(p, context.clone())?;
                        handles.push(tokio::spawn(async move {
                            // Q12 buckets: 0 = MAIL, 1 = SHIP, 2 = catch-all
                            // (should always stay empty per the SQL filter
                            // `l_shipmode IN ('MAIL','SHIP')`, but kept so
                            // adversarial data doesn't panic).
                            let mut bins: [Q12Bin; 3] = [Q12Bin::default(); 3];
                            while let Some(batch) = s.try_next().await? {
                                accumulate_q12_batch(&batch, &mut bins)?;
                            }
                            Ok::<_, DataFusionError>(bins)
                        }));
                    }
                    let mut merged: [Q12Bin; 3] = [Q12Bin::default(); 3];
                    for h in handles {
                        let partial = h.await.map_err(|e| {
                            DataFusionError::Execution(format!(
                                "FusedPostJoinExec(Q12): worker join failed: {e}"
                            ))
                        })??;
                        for (m, p) in merged.iter_mut().zip(partial.iter()) {
                            m.high += p.high;
                            m.low += p.low;
                        }
                    }
                    emit_q12(schema_for_batch, merged)
                }
                FusedPostJoinSpec::Q14 => {
                    // Q14 post-join-only variant. We keep the
                    // materialise-then-compute shape here for the
                    // JIT path's pre-seed-from-outputs contract; the
                    // hand-coded path is small
                    // enough that drain cost doesn't matter. If a
                    // future user complains, mirror the Q3/Q5 streaming
                    // shape above using a `[f64; 2]` accumulator.
                    let mut batches: Vec<RecordBatch> = Vec::new();
                    for p in 0..input_partitions {
                        let mut s = input.execute(p, context.clone())?;
                        while let Some(b) = s.try_next().await? {
                            batches.push(b);
                        }
                    }
                    tokio::task::spawn_blocking(move || -> DfResult<RecordBatch> {
                        match jit {
                            Some(j) => execute_q14_jit(&batches, schema_for_batch, &j),
                            None => execute_q14(&batches, schema_for_batch),
                        }
                    })
                    .await
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "FusedPostJoinExec(Q14): blocking-task join failed: {e}"
                        ))
                    })?
                }
            }
        };

        let s = stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, s)))
    }
}

// ----- Q3: 3-col group, single SUM -----

/// Per-batch Q3 fold: add each row's contribution to `groups` keyed by
/// `(l_orderkey, o_orderdate, o_shippriority)`. Streaming workers call
/// this in a loop and pass `groups` through batch-to-batch.
fn accumulate_q3_batch(
    batch: &RecordBatch,
    groups: &mut HashMap<(i64, i32, i32), f64>,
) -> DfResult<()> {
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
        let key = (ok_v[i], od_v[i], get_sp(i));
        let rev = price_v[i] * (1.0 - disc_v[i]);
        *groups.entry(key).or_insert(0.0) += rev;
    }
    Ok(())
}

fn emit_q3(schema: SchemaRef, groups: HashMap<(i64, i32, i32), f64>) -> DfResult<RecordBatch> {
    let mut rows: Vec<((i64, i32, i32), f64)> = groups.into_iter().collect();
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

fn accumulate_q5_batch(batch: &RecordBatch, groups: &mut HashMap<String, f64>) -> DfResult<()> {
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
    Ok(())
}

/// Per-bucket running counts for Q12. `high` and `low` are
/// independent because each row contributes 1 to exactly one (the
/// CASE-WHENs are complementary on the priority enum) — but we keep
/// both fields rather than computing one from the other to stay
/// branchless inside `accumulate_q12_batch`.
#[derive(Default, Clone, Copy)]
struct Q12Bin {
    high: i64,
    low: i64,
}

/// Per-batch Q12 fold over `(l_shipmode, o_orderpriority)` columns.
/// MAIL goes to bin 0, SHIP to bin 1, anything else to bin 2 (which
/// is dropped from the output). High-priority orders (1-URGENT or
/// 2-HIGH) increment `high`; everything else increments `low`. The
/// CASE-WHEN guards reduce to a 1-byte prefix check on the priority
/// string since TPC-H's `o_orderpriority` always starts with one of
/// `1`, `2`, `3`, `4`, `5` followed by `-<NAME>`.
fn accumulate_q12_batch(batch: &RecordBatch, bins: &mut [Q12Bin; 3]) -> DfResult<()> {
    let shipmode = batch
        .column(batch.schema().index_of("l_shipmode")?)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("l_shipmode Utf8View");
    let priority = batch
        .column(batch.schema().index_of("o_orderpriority")?)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("o_orderpriority Utf8View");
    for i in 0..batch.num_rows() {
        let sm = shipmode.value(i).as_bytes();
        let bin = match sm.first().copied() {
            Some(b'M') => 0, // MAIL
            Some(b'S') => 1, // SHIP
            _ => 2,
        };
        let pr = priority.value(i).as_bytes();
        // High priority iff first byte is '1' (URGENT) or '2' (HIGH).
        let is_high = matches!(pr.first().copied(), Some(b'1') | Some(b'2'));
        if is_high {
            bins[bin].high += 1;
        } else {
            bins[bin].low += 1;
        }
    }
    Ok(())
}

fn emit_q12(schema: SchemaRef, bins: [Q12Bin; 3]) -> DfResult<RecordBatch> {
    // Output the two real buckets in alphabetical order (MAIL < SHIP),
    // dropping the catch-all bin.
    let mut name_b = StringBuilder::with_capacity(2, 16);
    let mut high_b = Int64Builder::with_capacity(2);
    let mut low_b = Int64Builder::with_capacity(2);
    name_b.append_value("MAIL");
    high_b.append_value(bins[0].high);
    low_b.append_value(bins[0].low);
    name_b.append_value("SHIP");
    high_b.append_value(bins[1].high);
    low_b.append_value(bins[1].low);
    let cols: Vec<ArrayRef> = vec![
        Arc::new(name_b.finish()),
        Arc::new(high_b.finish()),
        Arc::new(low_b.finish()),
    ];
    Ok(RecordBatch::try_new(schema, cols)?)
}

fn emit_q5(schema: SchemaRef, groups: HashMap<String, f64>) -> DfResult<RecordBatch> {
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

// ----- Q14 JIT path -----
//
// Same kernel surface as `execute_q14` but the inner CASE-WHEN +
// dual-SUM loop is the Cranelift-JIT'd `FusedFilterAggJit` built from
// `FusedFilterAggSpec::q14_post_join()`. The JIT's outputs[0] is the
// PROMO-guarded sum, outputs[1] is the unguarded sum; the ratio
// `100 * promo / total` is computed here on the host (the JIT only
// does the per-row accumulation, not the final division).

fn execute_q14_jit(
    batches: &[RecordBatch],
    schema: SchemaRef,
    jit: &crate::fused_jit::FusedFilterAggJit,
) -> DfResult<RecordBatch> {
    let mut cells: [f64; 2] = [0.0, 0.0];
    for batch in batches {
        let ptype = batch
            .column(batch.schema().index_of("p_type")?)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("p_type validated as Utf8View");
        let price = batch
            .column(batch.schema().index_of("l_extendedprice")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("l_extendedprice Float64");
        let disc = batch
            .column(batch.schema().index_of("l_discount")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("l_discount Float64");
        let inputs: [*const u8; 3] = [
            ptype.views().as_ptr().cast::<u8>(),
            price.values().as_ptr().cast::<u8>(),
            disc.values().as_ptr().cast::<u8>(),
        ];
        // SAFETY: Arrow guarantees views/values length >= num_rows; cells
        // has exactly `jit.n_outputs() == 2` elements.
        debug_assert_eq!(jit.n_outputs(), 2);
        unsafe {
            jit.run(batch.num_rows() as i64, inputs.as_ptr(), cells.as_mut_ptr());
        }
    }
    let promo = cells[0];
    let total = cells[1];
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

    /// Σ.D3 phase C retrofit: Q14 JIT'd path must return the same
    /// promo_revenue ratio as the hand-coded Q14 path on the same input.
    /// The intermediate (promo, total) cells are bit-identical between
    /// hand-coded and JIT'd because both walk rows in the same order
    /// with the same fadd sequence; the final ratio is computed by the
    /// host (same `100*p/t` formula on both sides).
    #[tokio::test]
    async fn q14_jit_matches_hand_coded_bit_identical() {
        let hand_input = input_plan_from_batch(make_q14_batch()).await;
        let hand_exec =
            Arc::new(FusedPostJoinExec::try_new(hand_input, FusedPostJoinSpec::Q14).unwrap());
        let jit_input = input_plan_from_batch(make_q14_batch()).await;
        let jit_exec =
            Arc::new(FusedPostJoinExec::try_new_jit(jit_input, FusedPostJoinSpec::Q14).unwrap());
        let session = SessionContext::new();

        let mut hand_s = hand_exec.execute(0, session.task_ctx()).unwrap();
        let hand_out = hand_s.try_next().await.unwrap().unwrap();
        let mut jit_s = jit_exec.execute(0, session.task_ctx()).unwrap();
        let jit_out = jit_s.try_next().await.unwrap().unwrap();

        let h = hand_out
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        let j = jit_out
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(
            h.to_bits(),
            j.to_bits(),
            "Q14 ratio: hand={h}, jit={j} (must be bit-identical)"
        );
    }

    /// try_new_jit should reject Q3 and Q5 explicitly (they need hash-
    /// group-by-in-IR, which isn't yet supported). The caller gets a
    /// clear error rather than silently falling through to the hand-
    /// coded path.
    #[tokio::test]
    async fn try_new_jit_rejects_q3_and_q5() {
        // Reuse Q3's input schema for the Q3 check — we just need a
        // syntactically-valid input.
        let q3_input = input_plan_from_batch(make_q3_batch()).await;
        let err = FusedPostJoinExec::try_new_jit(q3_input, FusedPostJoinSpec::Q3).unwrap_err();
        assert!(format!("{err}").contains("Q3"), "got: {err}");

        let q5_input = input_plan_from_batch(make_q5_batch()).await;
        let err = FusedPostJoinExec::try_new_jit(q5_input, FusedPostJoinSpec::Q5).unwrap_err();
        assert!(format!("{err}").contains("Q5"), "got: {err}");
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
