//! Σ.G.2 first slice: `AggregateSpec` trait + `Q6Spec` impl.
//!
//! Goal: unify the per-query `FusedQN` operators behind a single
//! generic `FusedAggregateExec<S: AggregateSpec>` parameterised by
//! a shape descriptor. Per the [Σ.G.1 audit](FUSED_AGGREGATE_SHAPES.md),
//! Q1 + Q6 + Q12 share enough structure (single-table scan +
//! pushed-down filter + fixed-cardinality grouping) to fold cleanly;
//! Q3/Q5 stay on `FusedPostJoinExec`.
//!
//! This module ships:
//!   - The `AggregateSpec` trait (per-batch dispatch, accumulator
//!     type as an associated type so monomorphisation keeps the
//!     hot loop inlined)
//!   - `Q6Spec` — the first concrete shape; functionally equivalent
//!     to [`FusedFilterSumExec`]'s hand path
//!
//! **NOT shipped here:** the generic `FusedAggregateExec` operator
//! and the planner-rule rewire. Those land *only* if the
//! perf-equivalence bench in `examples/sigma_g2_q6_unified_vs_hand.rs`
//! shows ≤ 3 % delta vs the hand loop on real lineitem data — see
//! the gate criterion in [PHASE_SIGMA_G_GENERIC_FUSED_AGGREGATE.md].
//!
//! **Bench finding (2026-05-17):** delegating `Q6Spec::process_batch`
//! to the existing free function `process_q6_batch_hand` gates at
//! **+0.18 %** (45.79 → 45.88 µs / 65k-row batch). Earlier attempts
//! that *duplicated* the inner loop inside the trait impl regressed
//! by 12–39 % despite identical source — LLVM's per-shape codegen
//! is sensitive to where the loop body lives. The pattern that works:
//! the trait is the dispatch surface, the hot loops stay in free
//! functions, and each `impl AggregateSpec` just forwards.

use std::sync::Arc;

use arrow_array::{Float64Array, RecordBatch};
use arrow_schema::SchemaRef;
use datafusion::common::Result as DfResult;

use crate::fused::Q6Predicate;
use crate::fused_multi_agg::{Q1Aggs, Q1ColumnIndices, Q1Predicate};

/// Per-shape descriptor for the unified fused aggregate path.
///
/// `process_batch` is the hot loop — invoked once per `RecordBatch`
/// with a `&mut Acc` so the implementation can keep arbitrary
/// per-shape accumulator state without box-allocations or trait
/// dispatch *inside* the loop. The trait dispatch is at the batch
/// boundary, not the row boundary. This is the key property that
/// lets the unified path stay competitive with hand-written
/// operators: each impl block monomorphises, and the inner loop is
/// fully visible to LLVM's auto-vectoriser at compile time.
///
/// `finalize` runs once per partition shard at end-of-input,
/// turning the accumulator state into an `ArrayRef` matching
/// `output_schema()`.
///
/// `validate_input_schema` is called once at operator construction
/// to catch type / column-name mismatches before any data flows.
pub trait AggregateSpec: Send + Sync + std::fmt::Debug + 'static {
    /// Per-shard accumulator state. Must be `Default` so the
    /// operator can spin one up per parallel shard before draining
    /// batches into it.
    type Accumulator: Default + Send;

    /// Process one input batch into the accumulator.
    ///
    /// Implementations should keep this method monomorphic over the
    /// batch column types (cache the `.values()` slices once, then
    /// loop on raw indices) so LLVM auto-vectorises the loop body.
    fn process_batch(&self, batch: &RecordBatch, acc: &mut Self::Accumulator) -> DfResult<()>;

    /// Convert the merged accumulator state to the operator's
    /// output `RecordBatch`. Called once per execute() at end-of-
    /// input after merging per-shard accumulators.
    ///
    /// Returns a full `RecordBatch` (not a single `ArrayRef`) so
    /// multi-column shapes like Q1 (10 output columns) can be
    /// expressed cleanly; single-column shapes like Q6 just wrap
    /// their one array into a one-column batch.
    fn finalize(&self, acc: Self::Accumulator) -> DfResult<RecordBatch>;

    /// Merge two per-shard accumulators. Called when the operator
    /// shards batches across rayon workers; each shard produces an
    /// `Accumulator`, then we left-fold-merge them before
    /// `finalize`.
    fn merge(&self, left: Self::Accumulator, right: Self::Accumulator) -> Self::Accumulator;

    /// The schema this operator emits. Captured at construction so
    /// the property table doesn't have to ask the spec per call.
    fn output_schema(&self) -> SchemaRef;

    /// Verify the child plan's schema has the columns + types this
    /// spec expects. Failure here surfaces as a Plan error at
    /// operator construction.
    fn validate_input_schema(&self, schema: &SchemaRef) -> DfResult<()>;
}

// ---------------------------------------------------------------
// Q6Spec — first concrete impl. Equivalent to the hand path in
// `FusedFilterSumExec` / `process_q6_batch_hand`.
// ---------------------------------------------------------------

/// Q6 column indices.
///
/// Σ.G.2c finding: aliased to `crate::fused::ColumnIndices` so the
/// trait-method body can pass them straight to `process_q6_batch_hand`
/// with no per-batch struct conversion. The two were field-identical
/// before the alias; defining `Q6ColumnIndices` as a distinct
/// nominal type was costing ~5 % at the operator level even though
/// LLVM saw equivalent fields, because the conversion-emitting code
/// was inside the per-batch trait method.
pub type Q6ColumnIndices = crate::fused::ColumnIndices;

/// Single-table SUM with a 5-bound range filter — the TPC-H Q6
/// shape. Wraps the existing [`Q6Predicate`] plus the column
/// indices.
#[derive(Debug, Clone)]
pub struct Q6Spec {
    pub predicate: Q6Predicate,
    pub indices: Q6ColumnIndices,
    pub output_schema: SchemaRef,
}

impl Q6Spec {
    /// Construct a Q6Spec from the predicate, resolving column
    /// indices against the child schema. Returns a Plan error if
    /// any of the four required columns is missing or has the
    /// wrong type. Mirrors [`FusedFilterSumExec::validate_input_schema`].
    pub fn try_new(predicate: Q6Predicate, child_schema: &SchemaRef) -> DfResult<Self> {
        let required: [(&str, arrow_schema::DataType); 4] = [
            ("l_quantity", arrow_schema::DataType::Float64),
            ("l_extendedprice", arrow_schema::DataType::Float64),
            ("l_discount", arrow_schema::DataType::Float64),
            ("l_shipdate", arrow_schema::DataType::Date32),
        ];
        for (name, expected) in &required {
            let field = child_schema.field_with_name(name).map_err(|_| {
                datafusion::common::DataFusionError::Plan(format!(
                    "Q6Spec: child schema missing column `{name}`"
                ))
            })?;
            if field.data_type() != expected {
                return Err(datafusion::common::DataFusionError::Plan(format!(
                    "Q6Spec: column `{name}` has type {:?}, expected {expected:?}",
                    field.data_type()
                )));
            }
        }
        let indices = crate::fused::ColumnIndices {
            qty: child_schema.index_of("l_quantity").unwrap(),
            price: child_schema.index_of("l_extendedprice").unwrap(),
            disc: child_schema.index_of("l_discount").unwrap(),
            ship: child_schema.index_of("l_shipdate").unwrap(),
        };
        let output_schema =
            std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "revenue",
                arrow_schema::DataType::Float64,
                false,
            )]));
        Ok(Self {
            predicate,
            indices,
            output_schema,
        })
    }
}

impl AggregateSpec for Q6Spec {
    type Accumulator = f64;

    #[inline(always)]
    fn process_batch(&self, batch: &RecordBatch, acc: &mut Self::Accumulator) -> DfResult<()> {
        // The trait method is the dispatch surface; the actual hot
        // loop lives in `fused::process_q6_batch_hand`. Duplicating
        // the body here regressed the bench by 12-39 % even with
        // identical source (Σ.G.2 perf gate, 2026-05-17). LLVM keys
        // its per-shape codegen off the free function — keep it
        // there, forward from the impl.
        //
        // Σ.G.2c follow-up: `Q6ColumnIndices = fused::ColumnIndices`
        // is a type alias, so passing `self.indices` directly avoids
        // a per-batch struct-conversion that was costing ~5 % at the
        // operator level.
        *acc += crate::fused::process_q6_batch_hand(batch, self.predicate, self.indices);
        Ok(())
    }

    fn finalize(&self, acc: Self::Accumulator) -> DfResult<RecordBatch> {
        let arr: arrow_array::ArrayRef = Arc::new(Float64Array::from(vec![acc]));
        Ok(RecordBatch::try_new(self.output_schema.clone(), vec![arr])?)
    }

    fn merge(&self, left: Self::Accumulator, right: Self::Accumulator) -> Self::Accumulator {
        left + right
    }

    fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn validate_input_schema(&self, schema: &SchemaRef) -> DfResult<()> {
        // Re-validate at execute time — schema may have been
        // rewritten by an upstream optimizer rule between
        // `try_new` and the first `process_batch`. Cheap (4 column
        // lookups) so paying it again is fine.
        Q6Spec::try_new(self.predicate, schema).map(|_| ())
    }
}

// ---------------------------------------------------------------
// Q1Spec — second concrete impl. Equivalent to the hand path in
// `FusedFilterMultiAggExec` / `process_q1_batch_hand`. Demonstrates
// the multi-aggregate + fixed-cardinality GROUP BY shape on top of
// the same `AggregateSpec` trait that Q6 used.
// ---------------------------------------------------------------

/// Single-table multi-aggregate with a 4-arm fixed group-by — the
/// TPC-H Q1 shape. Accumulator is `[Q1Aggs; 5]`: four real groups
/// plus an out-of-band catch-all bucket so the inner loop indexing
/// stays branchless. The catch-all is dropped at finalize time.
#[derive(Debug, Clone)]
pub struct Q1Spec {
    pub predicate: Q1Predicate,
    pub indices: Q1ColumnIndices,
    pub output_schema: SchemaRef,
}

impl Q1Spec {
    /// Construct a Q1Spec from the predicate, resolving column
    /// indices against the child schema. Returns a Plan error if
    /// any of the seven required columns is missing or has the
    /// wrong type. Mirrors `FusedFilterMultiAggExec::validate_input_schema`.
    pub fn try_new(predicate: Q1Predicate, child_schema: &SchemaRef) -> DfResult<Self> {
        let required: [(&str, arrow_schema::DataType); 7] = [
            ("l_returnflag", arrow_schema::DataType::Utf8View),
            ("l_linestatus", arrow_schema::DataType::Utf8View),
            ("l_quantity", arrow_schema::DataType::Float64),
            ("l_extendedprice", arrow_schema::DataType::Float64),
            ("l_discount", arrow_schema::DataType::Float64),
            ("l_tax", arrow_schema::DataType::Float64),
            ("l_shipdate", arrow_schema::DataType::Date32),
        ];
        for (name, expected) in &required {
            let field = child_schema.field_with_name(name).map_err(|_| {
                datafusion::common::DataFusionError::Plan(format!(
                    "Q1Spec: child schema missing column `{name}`"
                ))
            })?;
            if field.data_type() != expected {
                return Err(datafusion::common::DataFusionError::Plan(format!(
                    "Q1Spec: column `{name}` has type {:?}, expected {expected:?}",
                    field.data_type()
                )));
            }
        }
        let indices = Q1ColumnIndices {
            rflag: child_schema.index_of("l_returnflag").unwrap(),
            lstatus: child_schema.index_of("l_linestatus").unwrap(),
            qty: child_schema.index_of("l_quantity").unwrap(),
            price: child_schema.index_of("l_extendedprice").unwrap(),
            disc: child_schema.index_of("l_discount").unwrap(),
            tax: child_schema.index_of("l_tax").unwrap(),
            ship: child_schema.index_of("l_shipdate").unwrap(),
        };
        // Canonical Q1 SELECT list — must match `FusedFilterMultiAggExec`.
        let output_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("l_returnflag", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("l_linestatus", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("sum_qty", arrow_schema::DataType::Float64, false),
            arrow_schema::Field::new("sum_base_price", arrow_schema::DataType::Float64, false),
            arrow_schema::Field::new("sum_disc_price", arrow_schema::DataType::Float64, false),
            arrow_schema::Field::new("sum_charge", arrow_schema::DataType::Float64, false),
            arrow_schema::Field::new("avg_qty", arrow_schema::DataType::Float64, false),
            arrow_schema::Field::new("avg_price", arrow_schema::DataType::Float64, false),
            arrow_schema::Field::new("avg_disc", arrow_schema::DataType::Float64, false),
            arrow_schema::Field::new("count_order", arrow_schema::DataType::Int64, false),
        ]));
        Ok(Self {
            predicate,
            indices,
            output_schema,
        })
    }
}

impl AggregateSpec for Q1Spec {
    type Accumulator = [Q1Aggs; 5];

    #[inline(always)]
    fn process_batch(&self, batch: &RecordBatch, acc: &mut Self::Accumulator) -> DfResult<()> {
        // Same pattern as Q6Spec: the trait is the dispatch surface,
        // the hot loop lives in `fused_multi_agg::process_q1_batch_hand`.
        // See the Q6 bench finding in the module docstring for why.
        crate::fused_multi_agg::process_q1_batch_hand(batch, self.predicate, self.indices, acc);
        Ok(())
    }

    fn finalize(&self, acc: Self::Accumulator) -> DfResult<RecordBatch> {
        crate::fused_multi_agg::q1_groups_to_record_batch(self.output_schema.clone(), &acc)
    }

    fn merge(&self, mut left: Self::Accumulator, right: Self::Accumulator) -> Self::Accumulator {
        for i in 0..5 {
            left[i].merge(&right[i]);
        }
        left
    }

    fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn validate_input_schema(&self, schema: &SchemaRef) -> DfResult<()> {
        Q1Spec::try_new(self.predicate, schema).map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Date32Array, Float64Array};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn small_lineitem_batch() -> (RecordBatch, SchemaRef) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_shipdate", DataType::Date32, false),
        ]));
        // Predicate (Q6 SF=1 canonical): shipdate ∈ [1994-01-01, 1995-01-01),
        // discount ∈ [0.05, 0.07], quantity < 24.
        // Build 10 rows; expected match rows: 0, 2, 4, 6.
        let qty = Float64Array::from(vec![
            23.0, 30.0, 10.0, 25.0, 15.0, 24.0, 20.0, 35.0, 22.0, 5.0,
        ]);
        let price = Float64Array::from(vec![100.0; 10]);
        let disc = Float64Array::from(vec![
            0.06, 0.05, 0.07, 0.05, 0.06, 0.05, 0.05, 0.05, 0.04, 0.10,
        ]);
        // 1994-01-01 == Date32 8766; 1995-01-01 == 9131.
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
    fn q6spec_validates_schema_correctly() {
        let (_, schema) = small_lineitem_batch();
        let pred = Q6Predicate {
            date_lo: 8766,
            date_hi: 9131,
            disc_lo: 0.05,
            disc_hi: 0.07,
            qty_hi: 24.0,
        };
        let spec = Q6Spec::try_new(pred, &schema).unwrap();
        assert_eq!(spec.indices.qty, 0);
        assert_eq!(spec.indices.price, 1);
        assert_eq!(spec.indices.disc, 2);
        assert_eq!(spec.indices.ship, 3);

        // Bad schema: wrong type
        let bad = Arc::new(Schema::new(vec![
            Field::new("l_quantity", DataType::Int64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_shipdate", DataType::Date32, false),
        ]));
        assert!(Q6Spec::try_new(pred, &bad).is_err());

        // Bad schema: missing column
        let missing = Arc::new(Schema::new(vec![
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
        ]));
        assert!(Q6Spec::try_new(pred, &missing).is_err());
    }

    #[test]
    fn q6spec_matches_hand_calculation() {
        let (batch, schema) = small_lineitem_batch();
        let pred = Q6Predicate {
            date_lo: 8766,
            date_hi: 9131,
            disc_lo: 0.05,
            disc_hi: 0.07,
            qty_hi: 24.0,
        };
        let spec = Q6Spec::try_new(pred, &schema).unwrap();
        let mut acc = 0.0;
        spec.process_batch(&batch, &mut acc).unwrap();
        // Hand-compute expected: rows where shipdate ∈ [8766, 9131),
        // disc ∈ [0.05, 0.07], qty < 24:
        //   row 0: qty=23 ✓ price=100 disc=0.06 ship=9000 ✓ → 100*0.06 = 6
        //   row 1: qty=30 ✗
        //   row 2: qty=10 ✓ disc=0.07 ✓ ship=9100 ✓ → 100*0.07 = 7
        //   row 3: qty=25 ✗
        //   row 4: qty=15 ✓ disc=0.06 ✓ ship=8900 ✓ → 100*0.06 = 6
        //   row 5: qty=24 ✗ (not < 24)
        //   row 6: qty=20 ✓ disc=0.05 ✓ ship=8800 ✓ → 100*0.05 = 5
        //   row 7: qty=35 ✗
        //   row 8: qty=22 ✓ disc=0.04 ✗
        //   row 9: qty=5 ✓  disc=0.10 ✗
        // Expected sum: 6 + 7 + 6 + 5 = 24
        assert!((acc - 24.0).abs() < 1e-9, "got {acc}");
    }

    #[test]
    fn q6spec_finalize_emits_single_row() {
        let (_, schema) = small_lineitem_batch();
        let pred = Q6Predicate {
            date_lo: 8766,
            date_hi: 9131,
            disc_lo: 0.05,
            disc_hi: 0.07,
            qty_hi: 24.0,
        };
        let spec = Q6Spec::try_new(pred, &schema).unwrap();
        let batch = spec.finalize(42.5).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
        let f = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((f.value(0) - 42.5).abs() < 1e-9);
    }

    #[test]
    fn q6spec_merge_is_associative_addition() {
        let (_, schema) = small_lineitem_batch();
        let pred = Q6Predicate {
            date_lo: 8766,
            date_hi: 9131,
            disc_lo: 0.05,
            disc_hi: 0.07,
            qty_hi: 24.0,
        };
        let spec = Q6Spec::try_new(pred, &schema).unwrap();
        assert_eq!(spec.merge(3.0, 4.0), 7.0);
        assert_eq!(spec.merge(0.0, 0.0), 0.0);
    }

    // ------------- Q1Spec tests -------------

    fn small_q1_batch(cutoff: i32) -> (RecordBatch, SchemaRef) {
        use arrow_array::{Date32Array, StringViewArray};
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_returnflag", DataType::Utf8View, false),
            Field::new("l_linestatus", DataType::Utf8View, false),
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_tax", DataType::Float64, false),
            Field::new("l_shipdate", DataType::Date32, false),
        ]));
        // 3 (N,F) rows + 1 (A,F) row in-window + 1 (R,F) row filtered out
        let rflag = StringViewArray::from(vec!["N", "N", "N", "A", "R"]);
        let lstatus = StringViewArray::from(vec!["F", "F", "F", "F", "F"]);
        let qty = Float64Array::from(vec![10.0, 10.0, 10.0, 20.0, 5.0]);
        let price = Float64Array::from(vec![100.0, 100.0, 100.0, 200.0, 50.0]);
        let disc = Float64Array::from(vec![0.05, 0.05, 0.05, 0.10, 0.02]);
        let tax = Float64Array::from(vec![0.10, 0.10, 0.10, 0.05, 0.05]);
        let ship = Date32Array::from(vec![8800, 8800, 8800, 8800, cutoff + 1]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(rflag),
                Arc::new(lstatus),
                Arc::new(qty),
                Arc::new(price),
                Arc::new(disc),
                Arc::new(tax),
                Arc::new(ship),
            ],
        )
        .unwrap();
        (batch, schema)
    }

    #[test]
    fn q1spec_validates_schema_correctly() {
        let (_, schema) = small_q1_batch(10471);
        let pred = Q1Predicate {
            shipdate_cutoff: 10471,
        };
        let spec = Q1Spec::try_new(pred, &schema).unwrap();
        assert_eq!(spec.indices.rflag, 0);
        assert_eq!(spec.indices.ship, 6);

        // Bad schema: missing column
        let missing = Arc::new(Schema::new(vec![Field::new(
            "l_returnflag",
            DataType::Utf8View,
            false,
        )]));
        assert!(Q1Spec::try_new(pred, &missing).is_err());
    }

    #[test]
    fn q1spec_matches_hand_calculation() {
        let (batch, schema) = small_q1_batch(10471);
        let pred = Q1Predicate {
            shipdate_cutoff: 10471,
        };
        let spec = Q1Spec::try_new(pred, &schema).unwrap();
        let mut acc: [Q1Aggs; 5] = [Q1Aggs::default(); 5];
        spec.process_batch(&batch, &mut acc).unwrap();
        // (N,F) bin idx = 1: 3 rows × {qty=10, price=100, disc=0.05, tax=0.10}
        //   sum_qty = 30; sum_price = 300; sum_disc_price = 3*100*0.95 = 285;
        //   sum_charge = 3*95*1.10 = 313.5; sum_disc = 0.15; count = 3
        assert_eq!(acc[1].count, 3);
        assert!((acc[1].sum_qty - 30.0).abs() < 1e-9);
        assert!((acc[1].sum_price - 300.0).abs() < 1e-9);
        assert!((acc[1].sum_disc_price - 285.0).abs() < 1e-9);
        // (A,F) bin idx = 3: 1 row × {qty=20, price=200, disc=0.10, tax=0.05}
        assert_eq!(acc[3].count, 1);
        assert!((acc[3].sum_qty - 20.0).abs() < 1e-9);
        // Filtered-out row never lands anywhere
        let total: u64 = acc.iter().map(|a| a.count).sum();
        assert_eq!(total, 4);
    }

    #[test]
    fn q1spec_finalize_emits_four_rows_ten_columns() {
        let (batch, schema) = small_q1_batch(10471);
        let pred = Q1Predicate {
            shipdate_cutoff: 10471,
        };
        let spec = Q1Spec::try_new(pred, &schema).unwrap();
        let mut acc: [Q1Aggs; 5] = [Q1Aggs::default(); 5];
        spec.process_batch(&batch, &mut acc).unwrap();
        let out = spec.finalize(acc).unwrap();
        // Q1 output is always 4 rows (one per (rflag,lstatus) group),
        // even for empty groups — matches DataFusion's convention.
        assert_eq!(out.num_rows(), 4);
        assert_eq!(out.num_columns(), 10);
    }

    #[test]
    fn q1spec_merge_is_per_bin_addition() {
        let (_, schema) = small_q1_batch(10471);
        let pred = Q1Predicate {
            shipdate_cutoff: 10471,
        };
        let spec = Q1Spec::try_new(pred, &schema).unwrap();
        let mut a: [Q1Aggs; 5] = [Q1Aggs::default(); 5];
        let mut b: [Q1Aggs; 5] = [Q1Aggs::default(); 5];
        a[0].sum_qty = 10.0;
        a[0].count = 1;
        b[0].sum_qty = 5.0;
        b[0].count = 2;
        b[2].sum_qty = 7.0;
        let merged = spec.merge(a, b);
        assert_eq!(merged[0].sum_qty, 15.0);
        assert_eq!(merged[0].count, 3);
        assert_eq!(merged[2].sum_qty, 7.0);
    }
}
