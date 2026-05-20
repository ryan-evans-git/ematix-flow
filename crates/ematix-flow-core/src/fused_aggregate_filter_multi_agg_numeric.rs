//! Σ.H.1d.1+2 — parallel numeric-keyed `FilterMultiAggSpec`.
//!
//! ## Why this lives in a separate module
//!
//! Σ.H.1b tried to extend `FilterMultiAggSpec`'s existing
//! `GroupKeyKind` enum with `Int64`, `Int32`, `Date32`, `Float64`
//! variants. The 5×20 deep-bench (see
//! `docs/PHASE_SIGMA_H1D_DIAGNOSIS_AND_DESIGN.md`) decomposed the
//! regression into:
//!
//! - **Binary cost (~5%)**: the enum-variant additions changed
//!   LLVM's codegen for paths that don't use the new variants. Q03
//!   was +9.1% slower **even with the rule runtime-disabled**.
//! - **Exec cost (~5%)**: the rule's FilterMultiAggSpec routing was
//!   genuinely slower than DataFusion's default for the new shapes,
//!   especially multi-key (Q10 with 7 keys: +11.7% exec cost).
//!
//! Σ.H.1d's fix isolates the numeric-key handling. The existing
//! `GroupKeyKind` / `GroupKeyAccessor` / `FilterMultiAggSpec` stay
//! byte-for-byte identical to v0.3.0; this new module hosts a
//! parallel `NumericKeyKind` / `NumericKeyAccessor` /
//! `FilterMultiAggSpecNumeric` that the rule dispatches to when a
//! query has all-numeric group keys.
//!
//! The Dict-single, two-key-Utf8View, and perfect-hash templates
//! never see the new types — their codegen is unaffected.
//!
//! ## Status
//!
//! Σ.H.1d.2 ships the 1-key path. All four numeric kinds (Int64,
//! Int32, Date32, Float64) widen to `i64` for the hash key — narrower
//! types are zero-/sign-extended, Float64 is bit-cast via
//! `f64::to_bits()`. This lets a single `HashMap<i64, AggCells>` host
//! every 1-key shape without per-row `Vec<u8>` allocation.
//!
//! Σ.H.1d.3 will add multi-key support (byte-packed composite keys).
//! Σ.H.1d.4 wires the rule's String / Numeric / Mixed dispatch.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result as DfResult};

use crate::fused_aggregate::AggregateSpec;
use crate::fused_jit::{AggExpr, Clause, ClauseOp, ColumnTy};

/// Group-key kind for numeric primitive columns. Mirrors the role
/// of `GroupKeyKind` in `fused_aggregate_filter_multi_agg`, but
/// the two enums never appear in the same `match` — keeping them
/// disjoint preserves codegen of the existing string-keyed path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericKeyKind {
    /// Fixed-width 8-byte primitive group key. Reads `Int64Array`
    /// per row.
    Int64,
    /// Fixed-width 4-byte primitive. Reads `Int32Array`.
    Int32,
    /// Fixed-width 4-byte primitive. Reads `Date32Array`
    /// (days-since-epoch i32).
    Date32,
    /// Fixed-width 8-byte primitive. Reads `Float64Array` and uses
    /// the raw bit pattern as the hash key (NaN/-0.0 collide as
    /// per `f64::to_bits`).
    Float64,
}

impl NumericKeyKind {
    /// Byte width of one packed key cell. Used by Σ.H.1d.3's
    /// composite key buffer when multiple numeric keys are packed.
    pub fn byte_width(self) -> usize {
        match self {
            NumericKeyKind::Int64 | NumericKeyKind::Float64 => 8,
            NumericKeyKind::Int32 | NumericKeyKind::Date32 => 4,
        }
    }

    /// Output Arrow `DataType` this kind emits in `finalize`.
    pub fn output_dtype(self) -> DataType {
        match self {
            NumericKeyKind::Int64 => DataType::Int64,
            NumericKeyKind::Int32 => DataType::Int32,
            NumericKeyKind::Date32 => DataType::Date32,
            NumericKeyKind::Float64 => DataType::Float64,
        }
    }
}

/// One numeric group-key column's spec. Symmetric to
/// `GroupKeyColumn` in `fused_aggregate_filter_multi_agg`.
#[derive(Debug, Clone)]
pub struct NumericKeyColumn {
    pub name: String,
    pub col_idx: usize,
    pub kind: NumericKeyKind,
}

/// Runtime-configured spec for numeric-keyed multi-aggregate
/// group-by. Σ.H.1d.2 ships the 1-key path only; multi-key support
/// lands in Σ.H.1d.3.
#[derive(Debug, Clone)]
pub struct FilterMultiAggSpecNumeric {
    /// AND-chain of predicate clauses. Empty vec = no filter.
    pub predicate: Vec<Clause>,
    /// Input column types (used to interpret `Clause::column` indices
    /// + agg-expr column indices). Position-indexed.
    pub input_tys: Vec<ColumnTy>,
    /// Column indices into the input batch's schema for predicate +
    /// agg inputs.
    pub input_col_indices: Vec<usize>,
    /// Aggregate output specs. One entry per output column.
    pub aggregates: Vec<AggExpr>,
    /// Output column names for the aggregate outputs (length matches
    /// `aggregates.len()`). Group-key column names come first in
    /// `output_schema`.
    pub agg_output_names: Vec<String>,
    /// Numeric group key columns. Σ.H.1d.2 ships len == 1 only.
    pub numeric_keys: Vec<NumericKeyColumn>,
    /// Cached output schema: numeric key columns first, then
    /// aggregate output columns.
    pub output_schema: SchemaRef,
}

impl FilterMultiAggSpecNumeric {
    /// Construct + validate the spec against the child plan's schema.
    /// Returns a `Plan` error on missing columns or type mismatches.
    pub fn try_new(
        predicate: Vec<Clause>,
        input_tys: Vec<ColumnTy>,
        input_column_names: &[&str],
        aggregates: Vec<AggExpr>,
        agg_output_names: Vec<String>,
        numeric_keys: Vec<(String, NumericKeyKind)>,
        child_schema: &SchemaRef,
    ) -> DfResult<Self> {
        if input_tys.len() != input_column_names.len() {
            return Err(DataFusionError::Plan(format!(
                "FilterMultiAggSpecNumeric: input_tys.len()={} but \
                 input_column_names.len()={}",
                input_tys.len(),
                input_column_names.len()
            )));
        }
        if aggregates.len() != agg_output_names.len() {
            return Err(DataFusionError::Plan(format!(
                "FilterMultiAggSpecNumeric: aggregates.len()={} but \
                 agg_output_names.len()={}",
                aggregates.len(),
                agg_output_names.len()
            )));
        }
        // Σ.H.1d.2: 1-key only. Multi-key lands in Σ.H.1d.3.
        if numeric_keys.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "FilterMultiAggSpecNumeric (Σ.H.1d.2): only 1 group key \
                 supported (got {}); multi-key in Σ.H.1d.3",
                numeric_keys.len()
            )));
        }
        // AvgColumn divides at finalize by a CountStar cell.
        let has_avg = aggregates
            .iter()
            .any(|a| matches!(a, AggExpr::AvgColumn(_)));
        let has_count = aggregates.iter().any(|a| matches!(a, AggExpr::CountStar));
        if has_avg && !has_count {
            return Err(DataFusionError::Plan(
                "FilterMultiAggSpecNumeric: AvgColumn requires a CountStar \
                 aggregate to divide by at finalize"
                    .into(),
            ));
        }

        // Resolve predicate/agg input columns.
        let mut input_col_indices = Vec::with_capacity(input_column_names.len());
        for (i, &name) in input_column_names.iter().enumerate() {
            let idx = child_schema.index_of(name).map_err(|_| {
                DataFusionError::Plan(format!(
                    "FilterMultiAggSpecNumeric: child schema missing input column `{name}`"
                ))
            })?;
            let actual = child_schema.field(idx).data_type();
            if !matches_column_ty(actual, input_tys[i]) {
                return Err(DataFusionError::Plan(format!(
                    "FilterMultiAggSpecNumeric: column `{name}` has type {actual:?}, \
                     expected ColumnTy::{:?}",
                    input_tys[i]
                )));
            }
            input_col_indices.push(idx);
        }

        // Resolve numeric group key + validate the actual data type.
        let mut key_specs = Vec::with_capacity(numeric_keys.len());
        for (name, kind) in numeric_keys {
            let col_idx = child_schema.index_of(&name).map_err(|_| {
                DataFusionError::Plan(format!(
                    "FilterMultiAggSpecNumeric: child schema missing group key `{name}`"
                ))
            })?;
            let actual = child_schema.field(col_idx).data_type();
            let ok = matches!(
                (kind, actual),
                (NumericKeyKind::Int64, DataType::Int64)
                    | (NumericKeyKind::Int32, DataType::Int32)
                    | (NumericKeyKind::Date32, DataType::Date32)
                    | (NumericKeyKind::Float64, DataType::Float64)
            );
            if !ok {
                return Err(DataFusionError::Plan(format!(
                    "FilterMultiAggSpecNumeric: group key `{name}` has type {actual:?}, \
                     doesn't match NumericKeyKind::{kind:?}"
                )));
            }
            key_specs.push(NumericKeyColumn {
                name,
                col_idx,
                kind,
            });
        }

        // Build output schema: numeric keys + aggregate outputs.
        let mut fields: Vec<Field> = Vec::with_capacity(key_specs.len() + aggregates.len());
        for k in &key_specs {
            fields.push(Field::new(&k.name, k.kind.output_dtype(), false));
        }
        for (i, agg) in aggregates.iter().enumerate() {
            let dtype = match agg {
                AggExpr::CountStar => DataType::Int64,
                _ => DataType::Float64,
            };
            fields.push(Field::new(&agg_output_names[i], dtype, false));
        }
        let output_schema = Arc::new(Schema::new(fields));

        Ok(Self {
            predicate,
            input_tys,
            input_col_indices,
            aggregates,
            agg_output_names,
            numeric_keys: key_specs,
            output_schema,
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

/// One group's aggregate cell vector. Same convention as
/// `FilterMultiAggSpec`: `f64` for both sums and counts; counts are
/// cast back to `i64` in `finalize`.
type AggCells = Vec<f64>;

#[inline(always)]
fn initial_cell_value(agg: &AggExpr) -> f64 {
    match agg {
        AggExpr::MinColumn(_) => f64::INFINITY,
        AggExpr::MaxColumn(_) => f64::NEG_INFINITY,
        _ => 0.0,
    }
}

fn fresh_cells(aggregates: &[AggExpr]) -> AggCells {
    aggregates.iter().map(initial_cell_value).collect()
}

/// Per-batch accumulator. 1-key only in Σ.H.1d.2; the `i64` key is a
/// widened representation of any of the four numeric kinds:
/// - `Int64` → as-is
/// - `Int32` / `Date32` → sign-extended via `i64::from`
/// - `Float64` → bit-cast: `f64::to_bits() as i64`
///
/// Bit-cast for Float64 means NaN / `-0.0` collide as per their
/// exact bit pattern, matching the behaviour DataFusion's default
/// HashAggregate uses for float keys.
#[derive(Debug, Default)]
pub struct FilterMultiAggSpecNumericAccumulator {
    pub groups: HashMap<i64, AggCells>,
}

/// Per-batch typed-slice cache for predicate + agg input columns.
/// Mirror of `TypedCol` in the string-keyed module. Lives separately
/// here so the existing module's codegen isn't perturbed.
enum NumericTypedCol<'a> {
    F64(&'a [f64]),
    I32(&'a [i32]),
    /// I64 inputs aren't reachable by today's `Clause::Op` (which has
    /// I32 / F64 variants only) — kept here so the spec accepts I64
    /// columns in `input_tys` without surprises. Σ.H.1d.3+ extends
    /// `Clause::Op` if a real query demands Int64 predicates.
    #[allow(dead_code)]
    I64(&'a [i64]),
    Date32(&'a [i32]),
}

impl<'a> NumericTypedCol<'a> {
    fn build(col: &'a dyn Array, ty: ColumnTy) -> DfResult<Self> {
        match ty {
            ColumnTy::Float64 => col
                .as_any()
                .downcast_ref::<Float64Array>()
                .map(|a| NumericTypedCol::F64(a.values()))
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "FilterMultiAggSpecNumeric: expected Float64Array, got {:?}",
                        col.data_type()
                    ))
                }),
            ColumnTy::Int32 => col
                .as_any()
                .downcast_ref::<Int32Array>()
                .map(|a| NumericTypedCol::I32(a.values()))
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "FilterMultiAggSpecNumeric: expected Int32Array, got {:?}",
                        col.data_type()
                    ))
                }),
            ColumnTy::Int64 => col
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| NumericTypedCol::I64(a.values()))
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "FilterMultiAggSpecNumeric: expected Int64Array, got {:?}",
                        col.data_type()
                    ))
                }),
            ColumnTy::Date32 => col
                .as_any()
                .downcast_ref::<Date32Array>()
                .map(|a| NumericTypedCol::Date32(a.values()))
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "FilterMultiAggSpecNumeric: expected Date32Array, got {:?}",
                        col.data_type()
                    ))
                }),
            ColumnTy::Utf8View => Err(DataFusionError::Internal(
                "FilterMultiAggSpecNumeric: Utf8View columns shouldn't appear \
                 in predicate / agg inputs of a numeric-keyed spec"
                    .into(),
            )),
        }
    }

    #[inline(always)]
    fn f64_at(&self, row: usize) -> f64 {
        match self {
            NumericTypedCol::F64(s) => s[row],
            // Predicate/agg cols are validated as Float64 elsewhere;
            // this branch is defensive.
            _ => unreachable!("f64_at called on non-F64 NumericTypedCol"),
        }
    }

    #[inline(always)]
    fn i32_at(&self, row: usize) -> i32 {
        match self {
            NumericTypedCol::I32(s) => s[row],
            NumericTypedCol::Date32(s) => s[row],
            _ => unreachable!("i32_at called on non-i32 NumericTypedCol"),
        }
    }
}

impl FilterMultiAggSpecNumeric {
    fn build_typed_cols<'a>(&self, batch: &'a RecordBatch) -> DfResult<Vec<NumericTypedCol<'a>>> {
        self.input_col_indices
            .iter()
            .enumerate()
            .map(|(slot, &col_idx)| {
                NumericTypedCol::build(batch.column(col_idx).as_ref(), self.input_tys[slot])
            })
            .collect()
    }

    #[inline(always)]
    fn eval_predicate(&self, cols: &[NumericTypedCol], row: usize) -> bool {
        for clause in &self.predicate {
            let col = &cols[clause.column];
            let ok = match clause.op {
                ClauseOp::F64Ge => col.f64_at(row) >= clause.imm_f64,
                ClauseOp::F64Le => col.f64_at(row) <= clause.imm_f64,
                ClauseOp::F64Lt => col.f64_at(row) < clause.imm_f64,
                ClauseOp::F64Gt => col.f64_at(row) > clause.imm_f64,
                ClauseOp::I32Ge => col.i32_at(row) >= clause.imm_i32,
                ClauseOp::I32Le => col.i32_at(row) <= clause.imm_i32,
                ClauseOp::I32Lt => col.i32_at(row) < clause.imm_i32,
                ClauseOp::I32Gt => col.i32_at(row) > clause.imm_i32,
            };
            if !ok {
                return false;
            }
        }
        true
    }

    #[inline(always)]
    fn eval_agg(&self, agg: &AggExpr, cols: &[NumericTypedCol], row: usize) -> f64 {
        match agg {
            AggExpr::SumColumn(i)
            | AggExpr::AvgColumn(i)
            | AggExpr::MinColumn(i)
            | AggExpr::MaxColumn(i) => cols[*i].f64_at(row),
            AggExpr::SumSquaresColumn(i) => {
                let v = cols[*i].f64_at(row);
                v * v
            }
            AggExpr::SumProductColumns(a, b) => cols[*a].f64_at(row) * cols[*b].f64_at(row),
            AggExpr::SumProductOneMinus(a, b) => {
                cols[*a].f64_at(row) * (1.0 - cols[*b].f64_at(row))
            }
            AggExpr::SumProductTwoOneMinusOnePlus(a, b, c) => {
                cols[*a].f64_at(row) * (1.0 - cols[*b].f64_at(row)) * (1.0 + cols[*c].f64_at(row))
            }
            AggExpr::CountStar => 1.0,
            AggExpr::SumProductOneMinusGuardedByPrefix { .. } => 0.0,
        }
    }

    #[inline(always)]
    fn combine_cell(&self, agg: &AggExpr, current: f64, row_value: f64) -> f64 {
        match agg {
            AggExpr::MinColumn(_) => current.min(row_value),
            AggExpr::MaxColumn(_) => current.max(row_value),
            _ => current + row_value,
        }
    }
}

impl AggregateSpec for FilterMultiAggSpecNumeric {
    type Accumulator = FilterMultiAggSpecNumericAccumulator;

    fn process_batch(&self, batch: &RecordBatch, acc: &mut Self::Accumulator) -> DfResult<()> {
        let n_rows = batch.num_rows();
        if n_rows == 0 {
            return Ok(());
        }

        let typed_cols = self.build_typed_cols(batch)?;

        // Σ.H.1d.2 invariant: exactly one numeric key (enforced in
        // try_new). Reach for the only column directly.
        debug_assert_eq!(self.numeric_keys.len(), 1);
        let key_spec = &self.numeric_keys[0];
        let key_col = batch.column(key_spec.col_idx);

        match key_spec.kind {
            NumericKeyKind::Int64 => {
                let arr = key_col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "FilterMultiAggSpecNumeric: expected Int64Array for Int64 key, got {:?}",
                        key_col.data_type()
                    ))
                })?;
                let keys = arr.values();
                self.process_int64_keys(keys, &typed_cols, n_rows, acc);
            }
            NumericKeyKind::Int32 => {
                let arr = key_col
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "FilterMultiAggSpecNumeric: expected Int32Array, got {:?}",
                            key_col.data_type()
                        ))
                    })?;
                let keys = arr.values();
                self.process_int32_keys(keys, &typed_cols, n_rows, acc);
            }
            NumericKeyKind::Date32 => {
                let arr = key_col
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "FilterMultiAggSpecNumeric: expected Date32Array, got {:?}",
                            key_col.data_type()
                        ))
                    })?;
                let keys = arr.values();
                self.process_int32_keys(keys, &typed_cols, n_rows, acc);
            }
            NumericKeyKind::Float64 => {
                let arr = key_col
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "FilterMultiAggSpecNumeric: expected Float64Array, got {:?}",
                            key_col.data_type()
                        ))
                    })?;
                let keys = arr.values();
                self.process_f64_keys(keys, &typed_cols, n_rows, acc);
            }
        }
        Ok(())
    }

    fn finalize(&self, acc: Self::Accumulator) -> DfResult<RecordBatch> {
        // Sort by raw i64 key so output is deterministic across runs.
        let mut entries: Vec<(i64, AggCells)> = acc.groups.into_iter().collect();
        entries.sort_by_key(|(k, _)| *k);

        debug_assert_eq!(self.numeric_keys.len(), 1);
        let key_spec = &self.numeric_keys[0];

        // Decode each group's i64 key back into its native Arrow type.
        let key_column: ArrayRef = match key_spec.kind {
            NumericKeyKind::Int64 => {
                let vals: Vec<i64> = entries.iter().map(|(k, _)| *k).collect();
                Arc::new(Int64Array::from(vals))
            }
            NumericKeyKind::Int32 => {
                let vals: Vec<i32> = entries.iter().map(|(k, _)| *k as i32).collect();
                Arc::new(Int32Array::from(vals))
            }
            NumericKeyKind::Date32 => {
                let vals: Vec<i32> = entries.iter().map(|(k, _)| *k as i32).collect();
                Arc::new(Date32Array::from(vals))
            }
            NumericKeyKind::Float64 => {
                let vals: Vec<f64> = entries
                    .iter()
                    .map(|(k, _)| f64::from_bits(*k as u64))
                    .collect();
                Arc::new(Float64Array::from(vals))
            }
        };

        let count_slot: Option<usize> = self
            .aggregates
            .iter()
            .position(|a| matches!(a, AggExpr::CountStar));

        let mut agg_columns: Vec<ArrayRef> = Vec::with_capacity(self.aggregates.len());
        for (ai, agg) in self.aggregates.iter().enumerate() {
            let column: ArrayRef = match agg {
                AggExpr::CountStar => {
                    let vals: Vec<i64> = entries.iter().map(|(_, c)| c[ai] as i64).collect();
                    Arc::new(Int64Array::from(vals))
                }
                AggExpr::AvgColumn(_) => {
                    let cs = count_slot.expect("AvgColumn requires CountStar — validated");
                    let vals: Vec<f64> = entries
                        .iter()
                        .map(|(_, c)| {
                            let cnt = c[cs];
                            if cnt == 0.0 { f64::NAN } else { c[ai] / cnt }
                        })
                        .collect();
                    Arc::new(Float64Array::from(vals))
                }
                _ => {
                    let vals: Vec<f64> = entries.iter().map(|(_, c)| c[ai]).collect();
                    Arc::new(Float64Array::from(vals))
                }
            };
            agg_columns.push(column);
        }

        let mut all_columns = vec![key_column];
        all_columns.extend(agg_columns);
        RecordBatch::try_new(self.output_schema.clone(), all_columns).map_err(|e| {
            DataFusionError::Internal(format!(
                "FilterMultiAggSpecNumeric finalize: build batch: {e}"
            ))
        })
    }

    fn merge(&self, mut left: Self::Accumulator, right: Self::Accumulator) -> Self::Accumulator {
        for (key, cells) in right.groups {
            let existing = left
                .groups
                .entry(key)
                .or_insert_with(|| fresh_cells(&self.aggregates));
            for (i, v) in cells.into_iter().enumerate() {
                existing[i] = self.combine_cell(&self.aggregates[i], existing[i], v);
            }
        }
        left
    }

    fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn validate_input_schema(&self, schema: &SchemaRef) -> DfResult<()> {
        for (i, &col_idx) in self.input_col_indices.iter().enumerate() {
            let f = schema.field(col_idx);
            if !matches_column_ty(f.data_type(), self.input_tys[i]) {
                return Err(DataFusionError::Plan(format!(
                    "FilterMultiAggSpecNumeric: column `{}` at idx {col_idx} has type {:?}, \
                     expected {:?}",
                    f.name(),
                    f.data_type(),
                    self.input_tys[i]
                )));
            }
        }
        for k in &self.numeric_keys {
            let f = schema.field(k.col_idx);
            let ok = matches!(
                (k.kind, f.data_type()),
                (NumericKeyKind::Int64, DataType::Int64)
                    | (NumericKeyKind::Int32, DataType::Int32)
                    | (NumericKeyKind::Date32, DataType::Date32)
                    | (NumericKeyKind::Float64, DataType::Float64)
            );
            if !ok {
                return Err(DataFusionError::Plan(format!(
                    "FilterMultiAggSpecNumeric: group key `{}` has type {:?}",
                    k.name,
                    f.data_type()
                )));
            }
        }
        Ok(())
    }
}

// === per-kind hot loops ===

impl FilterMultiAggSpecNumeric {
    /// 1-key Int64 hot loop. Each row's `i64` key is the HashMap key
    /// directly — no Vec<u8> allocation per row.
    fn process_int64_keys(
        &self,
        keys: &[i64],
        typed_cols: &[NumericTypedCol],
        n_rows: usize,
        acc: &mut FilterMultiAggSpecNumericAccumulator,
    ) {
        for row in 0..n_rows {
            if !self.eval_predicate(typed_cols, row) {
                continue;
            }
            let cells = acc
                .groups
                .entry(keys[row])
                .or_insert_with(|| fresh_cells(&self.aggregates));
            for (ai, agg) in self.aggregates.iter().enumerate() {
                cells[ai] = self.combine_cell(agg, cells[ai], self.eval_agg(agg, typed_cols, row));
            }
        }
    }

    /// 1-key Int32 / Date32 hot loop. Widens i32 → i64 (sign-extend)
    /// for the HashMap key.
    fn process_int32_keys(
        &self,
        keys: &[i32],
        typed_cols: &[NumericTypedCol],
        n_rows: usize,
        acc: &mut FilterMultiAggSpecNumericAccumulator,
    ) {
        for row in 0..n_rows {
            if !self.eval_predicate(typed_cols, row) {
                continue;
            }
            let key = i64::from(keys[row]);
            let cells = acc
                .groups
                .entry(key)
                .or_insert_with(|| fresh_cells(&self.aggregates));
            for (ai, agg) in self.aggregates.iter().enumerate() {
                cells[ai] = self.combine_cell(agg, cells[ai], self.eval_agg(agg, typed_cols, row));
            }
        }
    }

    /// 1-key Float64 hot loop. Bit-casts f64 → u64 → i64 for the key.
    fn process_f64_keys(
        &self,
        keys: &[f64],
        typed_cols: &[NumericTypedCol],
        n_rows: usize,
        acc: &mut FilterMultiAggSpecNumericAccumulator,
    ) {
        for row in 0..n_rows {
            if !self.eval_predicate(typed_cols, row) {
                continue;
            }
            let key = keys[row].to_bits() as i64;
            let cells = acc
                .groups
                .entry(key)
                .or_insert_with(|| fresh_cells(&self.aggregates));
            for (ai, agg) in self.aggregates.iter().enumerate() {
                cells[ai] = self.combine_cell(agg, cells[ai], self.eval_agg(agg, typed_cols, row));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int32Array, Int64Array};

    fn build_schema(key_name: &str, key_ty: DataType, agg_name: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(key_name, key_ty, false),
            Field::new(agg_name, DataType::Float64, false),
        ]))
    }

    #[test]
    fn byte_width_matches_native_size() {
        assert_eq!(NumericKeyKind::Int64.byte_width(), 8);
        assert_eq!(NumericKeyKind::Float64.byte_width(), 8);
        assert_eq!(NumericKeyKind::Int32.byte_width(), 4);
        assert_eq!(NumericKeyKind::Date32.byte_width(), 4);
    }

    /// 1-key Int64 group-by + SUM. Keys [1, 2, 1, 2, 1] with values
    /// [10, 20, 30, 40, 50] should produce groups {1 → 90, 2 → 60}.
    #[test]
    fn int64_single_key_sum() {
        let schema = build_schema("k", DataType::Int64, "v");
        let keys = Int64Array::from(vec![1i64, 2, 1, 2, 1]);
        let vals = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(keys) as ArrayRef, Arc::new(vals) as ArrayRef],
        )
        .unwrap();

        let spec = FilterMultiAggSpecNumeric::try_new(
            vec![],
            vec![ColumnTy::Float64],
            &["v"],
            vec![AggExpr::SumColumn(0)],
            vec!["sum_v".to_string()],
            vec![("k".to_string(), NumericKeyKind::Int64)],
            &schema,
        )
        .unwrap();

        let mut acc = FilterMultiAggSpecNumericAccumulator::default();
        spec.process_batch(&batch, &mut acc).unwrap();

        // Two distinct keys: 1 and 2.
        assert_eq!(acc.groups.len(), 2);
        assert_eq!(acc.groups.get(&1).unwrap()[0], 90.0);
        assert_eq!(acc.groups.get(&2).unwrap()[0], 60.0);

        // Finalize: sorted by key (1, 2).
        let out = spec.finalize(acc).unwrap();
        assert_eq!(out.num_rows(), 2);
        let out_keys = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let out_sums = out
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(out_keys.values(), &[1, 2]);
        assert_eq!(out_sums.values(), &[90.0, 60.0]);
    }

    /// 1-key Int32 + COUNT(*). Five rows with 3 distinct keys.
    #[test]
    fn int32_single_key_count() {
        let schema = build_schema("k32", DataType::Int32, "v");
        let keys = Int32Array::from(vec![10i32, 20, 10, 30, 20]);
        let vals = Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(keys) as ArrayRef, Arc::new(vals) as ArrayRef],
        )
        .unwrap();

        let spec = FilterMultiAggSpecNumeric::try_new(
            vec![],
            vec![ColumnTy::Float64],
            &["v"],
            vec![AggExpr::CountStar],
            vec!["cnt".to_string()],
            vec![("k32".to_string(), NumericKeyKind::Int32)],
            &schema,
        )
        .unwrap();

        let mut acc = FilterMultiAggSpecNumericAccumulator::default();
        spec.process_batch(&batch, &mut acc).unwrap();

        assert_eq!(acc.groups.len(), 3);
        // Counts as f64 internally; finalize converts to i64.
        assert_eq!(acc.groups.get(&10).unwrap()[0], 2.0);
        assert_eq!(acc.groups.get(&20).unwrap()[0], 2.0);
        assert_eq!(acc.groups.get(&30).unwrap()[0], 1.0);

        let out = spec.finalize(acc).unwrap();
        let out_keys = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let out_cnt = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out_keys.values(), &[10, 20, 30]);
        assert_eq!(out_cnt.values(), &[2, 2, 1]);
    }

    /// 1-key Float64 with SUM. Bit-cast key handling — same float
    /// values should land in the same group.
    #[test]
    fn float64_single_key_sum() {
        let schema = build_schema("k_f", DataType::Float64, "v");
        let keys = Float64Array::from(vec![3.14f64, 2.71, 3.14, 2.71]);
        let vals = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(keys) as ArrayRef, Arc::new(vals) as ArrayRef],
        )
        .unwrap();

        let spec = FilterMultiAggSpecNumeric::try_new(
            vec![],
            vec![ColumnTy::Float64],
            &["v"],
            vec![AggExpr::SumColumn(0)],
            vec!["sum_v".to_string()],
            vec![("k_f".to_string(), NumericKeyKind::Float64)],
            &schema,
        )
        .unwrap();

        let mut acc = FilterMultiAggSpecNumericAccumulator::default();
        spec.process_batch(&batch, &mut acc).unwrap();
        assert_eq!(acc.groups.len(), 2);

        let out = spec.finalize(acc).unwrap();
        let out_keys = out
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let out_sums = out
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // Sorted by raw bit-key, which for positive floats matches
        // ordinal order. 2.71 < 3.14.
        assert_eq!(out_keys.values(), &[2.71, 3.14]);
        assert_eq!(out_sums.values(), &[60.0, 40.0]);
    }

    /// Multi-key constructor must reject (Σ.H.1d.2: single-key only).
    #[test]
    fn multi_key_rejected_in_h1d_2() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k1", DataType::Int64, false),
            Field::new("k2", DataType::Int32, false),
        ]));
        let err = FilterMultiAggSpecNumeric::try_new(
            vec![],
            vec![],
            &[],
            vec![AggExpr::CountStar],
            vec!["cnt".to_string()],
            vec![
                ("k1".to_string(), NumericKeyKind::Int64),
                ("k2".to_string(), NumericKeyKind::Int32),
            ],
            &schema,
        );
        assert!(err.is_err());
        let msg = format!("{}", err.unwrap_err());
        assert!(msg.contains("Σ.H.1d.2"), "got: {msg}");
    }

    /// Predicate filter must apply correctly. Keys [1,2,1,2] with v
    /// = [10,20,30,40] and predicate "v >= 25" leaves rows
    /// (2,40) and (1,30). SUM per group: {1 → 30, 2 → 40}.
    #[test]
    fn predicate_filters_rows() {
        let schema = build_schema("k", DataType::Int64, "v");
        let keys = Int64Array::from(vec![1i64, 2, 1, 2]);
        let vals = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(keys) as ArrayRef, Arc::new(vals) as ArrayRef],
        )
        .unwrap();

        // Predicate: v >= 25 (column 0 of input columns is "v",
        // ColumnTy::Float64).
        let clause = Clause {
            column: 0,
            op: ClauseOp::F64Ge,
            imm_f64: 25.0,
            imm_i32: 0,
        };

        let spec = FilterMultiAggSpecNumeric::try_new(
            vec![clause],
            vec![ColumnTy::Float64],
            &["v"],
            vec![AggExpr::SumColumn(0)],
            vec!["sum_v".to_string()],
            vec![("k".to_string(), NumericKeyKind::Int64)],
            &schema,
        )
        .unwrap();

        let mut acc = FilterMultiAggSpecNumericAccumulator::default();
        spec.process_batch(&batch, &mut acc).unwrap();
        assert_eq!(acc.groups.get(&1).unwrap()[0], 30.0);
        assert_eq!(acc.groups.get(&2).unwrap()[0], 40.0);
    }
}
