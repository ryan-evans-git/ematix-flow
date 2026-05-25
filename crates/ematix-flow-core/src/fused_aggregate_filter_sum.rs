//! Σ.G.2e-1: `FilterSumSpec` — runtime-configured single-bucket SUM
//! over an AND-chain of `(column ⊕ literal)` filter clauses.
//!
//! First `AggregateSpec` impl whose shape is **not** hardcoded at
//! compile time. Unlike [`crate::fused_aggregate::Q6Spec`] which
//! carries fixed `Q6Predicate { date_lo, date_hi, disc_lo, disc_hi,
//! qty_hi }` fields, `FilterSumSpec` accepts an arbitrary
//! [`crate::fused_jit::FusedFilterAggSpec`] at construction and lets
//! the Cranelift JIT build the appropriate kernel.
//!
//! ## Why JIT-only
//!
//! Q6Spec's hand path is fast because each clause is a fixed-shape
//! Rust expression that LLVM auto-vectorises. A runtime-iterated
//! `Vec<Clause>` hand path would defeat that — see
//! `docs/PHASE_SIGMA_G2E_PREDICATE_EXTRACTION.md` § "Why this is gated
//! on Σ.G.3" for the reasoning. The Cranelift IR emitter
//! ([`crate::fused_jit::FusedFilterAggJit`]) already supports
//! arbitrary clause counts via baked-in immediate constants in the
//! generated assembly, so JIT-only is the natural shape for the
//! generic spec.
//!
//! `try_new` therefore builds the JIT eagerly and stores it on the
//! spec. There is no `try_new_hand` constructor; construction can
//! only fail at JIT build time.
//!
//! ## Scope
//!
//! Σ.G.2e-1 is single-bucket SUM (no group-by, one aggregate). The
//! JIT IR already supports group-by + multi-aggregate; this slice
//! just doesn't expose those — that's [`crate::fused_aggregate::Q1Spec`]'s
//! shape, which Σ.G.2f generalises in the same way (a
//! `FilterMultiAggSpec` companion to this module).
//!
//! No bench gate at this slice — the spec is unreachable from real
//! SQL until Σ.G.2e-2 ships `InjectFilterSumRule`. The Σ.G.2e-2 bench
//! gate then validates the spec end-to-end through DataFusion against
//! the existing `Q6Spec` JIT path.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringViewArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result as DfResult};

use crate::fused_aggregate::AggregateSpec;
use crate::fused_jit::{ColumnTy, FusedFilterAggJit, FusedFilterAggSpec};

/// Runtime-configured single-bucket SUM. Holds the JIT IR description,
/// the built Cranelift kernel, the resolved per-input column indices
/// into the child plan's schema, and the spec's single-column output
/// schema. See module-level docs for the design rationale.
///
/// `Clone` is satisfied via the contained `Arc<FusedFilterAggJit>`,
/// which is a refcount bump — workers across partitions share the
/// same compiled module.
#[derive(Debug, Clone)]
pub struct FilterSumSpec {
    /// Description of the JIT'd function: input column types, AND-chain
    /// predicate clauses, and the aggregate expression. `aggregates.len()`
    /// is always 1 in this slice; `group` is always `None`.
    pub jit_spec: FusedFilterAggSpec,
    /// Built Cranelift kernel. Owned via Arc so cloning the spec
    /// across worker partitions is cheap. Fallback when the spec
    /// shape doesn't match any registered specialisation.
    pub jit: Arc<FusedFilterAggJit>,
    /// Column indices into the input batch's schema, in the order
    /// `jit_spec.inputs` describes. Resolved once at construction
    /// against the child plan's schema; the hot loop reads the
    /// underlying buffers directly.
    pub col_indices: Vec<usize>,
    /// The spec's emitted schema: one Float64 column whose name the
    /// caller chose (typically the SQL alias on the SUM expression).
    pub output_schema: SchemaRef,
    /// Σ.U.A: shape-specialised kernel, if the spec matches a
    /// registered shape (Q06-family, …). When present, `process_batch`
    /// dispatches to this kernel instead of the Cranelift JIT —
    /// 1.30× faster on the Q06 shape via LLVM autovec.
    pub lane_kernel: Option<crate::lane_filter_sum_kernel::LaneFilterSumKernel>,
}

impl FilterSumSpec {
    /// Build a `FilterSumSpec` from a `FusedFilterAggSpec` IR
    /// description plus the child plan's input schema. Resolves each
    /// `jit_spec.inputs[i]`'s position to a column index in
    /// `child_schema`, validates the actual column types match the
    /// IR's declared `ColumnTy`, builds the JIT kernel, and stores
    /// everything on the spec.
    ///
    /// The `input_columns` argument names each input by position in
    /// the same order as `jit_spec.inputs`. The caller is responsible
    /// for matching them up — typically the SQL injection rule
    /// (Σ.G.2e-2) does this by inspecting the `FilterExec`'s referenced
    /// column names.
    ///
    /// `output_column_name` is the alias the SUM result gets in the
    /// emitted batch — for canonical Q6-shaped SQL this is `"revenue"`.
    ///
    /// Returns a Plan error if:
    /// - any `input_columns[i]` is missing from `child_schema`
    /// - any input column's actual type doesn't match `jit_spec.inputs[i]`
    /// - `jit_spec.aggregates.len() != 1` (this slice is single-bucket SUM)
    /// - `jit_spec.group.is_some()` (this slice is no-group)
    /// - the Cranelift JIT build fails (rare; indicates an internal IR error)
    pub fn try_new(
        jit_spec: FusedFilterAggSpec,
        input_columns: &[&str],
        child_schema: &SchemaRef,
        output_column_name: &str,
    ) -> DfResult<Self> {
        if jit_spec.aggregates.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "FilterSumSpec: single-bucket SUM only; got {} aggregates",
                jit_spec.aggregates.len()
            )));
        }
        if jit_spec.group.is_some() {
            return Err(DataFusionError::Plan(
                "FilterSumSpec: no-group only; use FilterMultiAggSpec (Σ.G.2f) for group-by".into(),
            ));
        }
        if input_columns.len() != jit_spec.inputs.len() {
            return Err(DataFusionError::Plan(format!(
                "FilterSumSpec: {} input_columns provided but jit_spec describes {}",
                input_columns.len(),
                jit_spec.inputs.len()
            )));
        }

        let mut col_indices = Vec::with_capacity(input_columns.len());
        for (i, &name) in input_columns.iter().enumerate() {
            let idx = child_schema.index_of(name).map_err(|_| {
                DataFusionError::Plan(format!(
                    "FilterSumSpec: child schema missing column `{name}`"
                ))
            })?;
            let actual = child_schema.field(idx).data_type();
            let expected = jit_spec.inputs[i];
            if !matches_column_ty(actual, expected) {
                return Err(DataFusionError::Plan(format!(
                    "FilterSumSpec: column `{name}` has type {actual:?}, JIT spec expects {expected:?}"
                )));
            }
            col_indices.push(idx);
        }

        let jit = FusedFilterAggJit::try_build(&jit_spec)
            .map_err(|e| DataFusionError::Plan(format!("FilterSumSpec: JIT build failed: {e}")))?;

        // Σ.U.A: try to bind a shape-specialised kernel. `None` here
        // is fine — process_batch will fall back to the JIT path.
        let lane_kernel = crate::lane_filter_sum_kernel::LaneFilterSumKernel::from_spec(&jit_spec);

        let output_schema = Arc::new(Schema::new(vec![Field::new(
            output_column_name,
            DataType::Float64,
            false,
        )]));

        Ok(Self {
            jit_spec,
            jit: Arc::new(jit),
            col_indices,
            output_schema,
            lane_kernel,
        })
    }
}

fn matches_column_ty(actual: &DataType, expected: ColumnTy) -> bool {
    matches!(
        (actual, expected),
        (DataType::Float64, ColumnTy::Float64)
            | (DataType::Date32, ColumnTy::Date32)
            | (DataType::Int32, ColumnTy::Int32)
            | (DataType::Int64, ColumnTy::Int64)
            | (DataType::Utf8View, ColumnTy::Utf8View)
    )
}

impl AggregateSpec for FilterSumSpec {
    type Accumulator = f64;

    #[inline]
    fn process_batch(&self, batch: &RecordBatch, acc: &mut Self::Accumulator) -> DfResult<()> {
        // Extract one raw `*const u8` per JIT input, in the order
        // `jit_spec.inputs` declared. The `Vec` allocation here is
        // ~32 bytes per batch and amortises across the per-batch
        // kernel work below.
        let n = self.col_indices.len();
        let mut input_ptrs: Vec<*const u8> = Vec::with_capacity(n);
        for (i, &col_idx) in self.col_indices.iter().enumerate() {
            let column = batch.column(col_idx);
            let ptr = column_buffer_ptr(column.as_ref(), self.jit_spec.inputs[i]);
            input_ptrs.push(ptr);
        }

        let n_rows = batch.num_rows();
        // Σ.U.A: dispatch to the shape-specialised kernel when one
        // was bound at construction. Otherwise fall back to the
        // Cranelift JIT — same numerical result, ~30% slower on
        // shapes that the lane kernel handles.
        if let Some(ref kernel) = self.lane_kernel {
            let contribution = unsafe { kernel.process(&input_ptrs, n_rows) };
            *acc += contribution;
        } else {
            // JIT path: seed the output cell from the accumulator so
            // the JIT's read-modify-store keeps accumulating across
            // batches.
            let mut out: [f64; 1] = [*acc];
            // SAFETY: every column's `.values()` slice has at least
            // `batch.num_rows()` elements (Arrow invariant); `out`
            // has one element matching the single-aggregate spec.
            unsafe {
                self.jit
                    .run(n_rows as i64, input_ptrs.as_ptr(), out.as_mut_ptr());
            }
            *acc = out[0];
        }
        Ok(())
    }

    fn finalize(&self, acc: Self::Accumulator) -> DfResult<RecordBatch> {
        let arr: ArrayRef = Arc::new(Float64Array::from(vec![acc]));
        Ok(RecordBatch::try_new(self.output_schema.clone(), vec![arr])?)
    }

    fn merge(&self, left: Self::Accumulator, right: Self::Accumulator) -> Self::Accumulator {
        left + right
    }

    fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn validate_input_schema(&self, schema: &SchemaRef) -> DfResult<()> {
        // Re-validate that the indices still point at the right types.
        // If an upstream optimiser rule rewrote the child plan and shifted
        // column positions, we'd want to either fail loudly (here) or
        // rebuild the indices. Failing loudly is fine for the Σ.G.2e-1
        // slice — Σ.G.2e-2 wires the rule with `with_new_children`
        // properly.
        for (i, &col_idx) in self.col_indices.iter().enumerate() {
            if col_idx >= schema.fields().len() {
                return Err(DataFusionError::Plan(format!(
                    "FilterSumSpec: cached col_idx {col_idx} out of bounds for schema with {} fields",
                    schema.fields().len()
                )));
            }
            let actual = schema.field(col_idx).data_type();
            let expected = self.jit_spec.inputs[i];
            if !matches_column_ty(actual, expected) {
                return Err(DataFusionError::Plan(format!(
                    "FilterSumSpec: child schema column {col_idx} has type {actual:?}, JIT spec expects {expected:?}"
                )));
            }
        }
        Ok(())
    }
}

/// Read a raw `*const u8` from a column's underlying values buffer,
/// picking the typed downcast based on the JIT spec's declared
/// `ColumnTy`. The validation in `try_new` + `validate_input_schema`
/// guarantees the downcast cannot fail.
#[inline]
fn column_buffer_ptr(column: &dyn Array, ty: ColumnTy) -> *const u8 {
    match ty {
        ColumnTy::Float64 => column
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("validated as Float64")
            .values()
            .as_ptr()
            .cast::<u8>(),
        ColumnTy::Date32 => column
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("validated as Date32")
            .values()
            .as_ptr()
            .cast::<u8>(),
        ColumnTy::Int32 => column
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("validated as Int32")
            .values()
            .as_ptr()
            .cast::<u8>(),
        ColumnTy::Int64 => column
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("validated as Int64")
            .values()
            .as_ptr()
            .cast::<u8>(),
        ColumnTy::Utf8View => column
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("validated as Utf8View")
            .views()
            .as_ptr()
            .cast::<u8>(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Date32Array, Float64Array};
    use arrow_schema::{DataType, Field, Schema};

    /// Build a Q6-equivalent fixture (10 rows, 4 matching) and verify
    /// `FilterSumSpec` configured with `FusedFilterAggSpec::q6` computes
    /// the same revenue as the canonical Q6Spec test.
    fn q6_fixture() -> (RecordBatch, SchemaRef) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_shipdate", DataType::Date32, false),
        ]));
        // Same fixture as Q6Spec's `small_lineitem_batch` test: matching
        // rows 0/2/4/6 contribute 6 + 7 + 6 + 5 = 24.
        let qty = Float64Array::from(vec![
            23.0, 30.0, 10.0, 25.0, 15.0, 24.0, 20.0, 35.0, 22.0, 5.0,
        ]);
        let price = Float64Array::from(vec![100.0; 10]);
        let disc = Float64Array::from(vec![
            0.06, 0.05, 0.07, 0.05, 0.06, 0.05, 0.05, 0.05, 0.04, 0.10,
        ]);
        let ship = Date32Array::from(vec![
            9000, 8000, 9100, 9200, 8900, 9000, 8800, 9050, 9020, 9080,
        ]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(qty),
                Arc::new(price),
                Arc::new(disc),
                Arc::new(ship),
            ],
        )
        .unwrap();
        (batch, schema)
    }

    #[test]
    fn filter_sum_spec_matches_q6_canonical_revenue() {
        // Σ.G.2e-1 substrate-equivalence check: building FilterSumSpec
        // from FusedFilterAggSpec::q6(...) and feeding the canonical Q6
        // fixture should reproduce the exact 24.0 the existing Q6Spec
        // tests assert. This validates the new spec's process_batch +
        // finalize round-trip against a known answer.
        let jit_spec = FusedFilterAggSpec::q6(8766, 9131, 0.05, 0.07, 24.0);
        let (batch, schema) = q6_fixture();
        // jit_spec input order from FusedFilterAggSpec::q6: shipdate,
        // discount, quantity, extprice.
        let spec = FilterSumSpec::try_new(
            jit_spec,
            &["l_shipdate", "l_discount", "l_quantity", "l_extendedprice"],
            &schema,
            "revenue",
        )
        .unwrap();

        let mut acc: f64 = 0.0;
        spec.process_batch(&batch, &mut acc).unwrap();
        assert!(
            (acc - 24.0).abs() < 1e-9,
            "expected revenue 24.0, got {acc}"
        );

        let out = spec.finalize(acc).unwrap();
        assert_eq!(out.num_rows(), 1);
        assert_eq!(out.num_columns(), 1);
        assert_eq!(out.schema().field(0).name(), "revenue");
        let v = out
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((v - 24.0).abs() < 1e-9);
    }

    #[test]
    fn filter_sum_spec_rejects_unsupported_shapes() {
        let (_, schema) = q6_fixture();

        // > 1 aggregate
        let mut multi = FusedFilterAggSpec::q6(8766, 9131, 0.05, 0.07, 24.0);
        multi
            .aggregates
            .push(crate::fused_jit::AggExpr::SumColumn(2));
        let err = FilterSumSpec::try_new(
            multi,
            &["l_shipdate", "l_discount", "l_quantity", "l_extendedprice"],
            &schema,
            "revenue",
        )
        .unwrap_err();
        assert!(
            format!("{err}").contains("single-bucket SUM only"),
            "unexpected error: {err}"
        );

        // group-by present
        let mut grouped = FusedFilterAggSpec::q6(8766, 9131, 0.05, 0.07, 24.0);
        grouped.group = Some(crate::fused_jit::GroupSpec {
            key_columns: vec![0],
            known_keys: vec![vec![b'X']],
        });
        let err = FilterSumSpec::try_new(
            grouped,
            &["l_shipdate", "l_discount", "l_quantity", "l_extendedprice"],
            &schema,
            "revenue",
        )
        .unwrap_err();
        assert!(
            format!("{err}").contains("no-group only"),
            "unexpected error: {err}"
        );

        // input_columns / jit_spec.inputs length mismatch
        let q6 = FusedFilterAggSpec::q6(8766, 9131, 0.05, 0.07, 24.0);
        let err = FilterSumSpec::try_new(q6, &["l_shipdate"], &schema, "revenue").unwrap_err();
        assert!(
            format!("{err}").contains("input_columns provided but"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn filter_sum_spec_rejects_wrong_column_type() {
        let (_, schema) = q6_fixture();
        // Pass the Float64 column where the IR expects a Date32 column.
        let q6 = FusedFilterAggSpec::q6(8766, 9131, 0.05, 0.07, 24.0);
        let err = FilterSumSpec::try_new(
            q6,
            &[
                "l_quantity", // Float64 — JIT wants Date32 for shipdate
                "l_discount",
                "l_quantity",
                "l_extendedprice",
            ],
            &schema,
            "revenue",
        )
        .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("l_quantity") && msg.contains("Float64") && msg.contains("Date32"),
            "error should name the column + types: {msg}"
        );
    }

    #[test]
    fn filter_sum_spec_accumulates_across_batches() {
        // The seed-from-acc + store-to-acc pattern in process_batch
        // means processing the same batch twice = double the first call's
        // contribution. If the seeding ever drifts (e.g. the JIT's
        // outputs[0] isn't read at function entry), this trips.
        let jit_spec = FusedFilterAggSpec::q6(8766, 9131, 0.05, 0.07, 24.0);
        let (batch, schema) = q6_fixture();
        let spec = FilterSumSpec::try_new(
            jit_spec,
            &["l_shipdate", "l_discount", "l_quantity", "l_extendedprice"],
            &schema,
            "revenue",
        )
        .unwrap();

        let mut once: f64 = 0.0;
        spec.process_batch(&batch, &mut once).unwrap();

        let mut twice: f64 = 0.0;
        spec.process_batch(&batch, &mut twice).unwrap();
        spec.process_batch(&batch, &mut twice).unwrap();

        assert!(
            (twice - 2.0 * once).abs() < 1e-9,
            "expected {} (= 2 × {}), got {}",
            2.0 * once,
            once,
            twice
        );
    }
}
