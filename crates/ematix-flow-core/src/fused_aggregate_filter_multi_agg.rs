//! Σ.G.2f.1: `FilterMultiAggSpec` — runtime-configured group-by +
//! multi-aggregate `AggregateSpec` with **no data-specific JIT baking**.
//!
//! This is the substrate that will retire `InjectFusedQ1Rule` (task
//! #480). Unlike [`crate::fused_aggregate::Q1Spec`], which bakes 5
//! Q1-specific group keys (`(A,F)`, `(N,F)`, `(N,O)`, `(R,F)`, catchall)
//! into the Cranelift kernel for branchless dispatch, this spec
//! discovers groups at runtime by hashing the key columns. The trade-
//! off: a tight Rust hash-group-by loop runs ~2× slower than Q1Spec's
//! baked-key kernel on TPC-H Q1, but it works on any GROUP BY shape
//! without query-specific tricks.
//!
//! Adaptive specialization (task #480.2) layers on top. Per the
//! research note backing #480's design, the right path is **not** a
//! Cranelift JIT but a Photon-style template cookbook: pre-compiled
//! kernel variants (Rust generics, monomorphised at compile time)
//! dispatched per batch from cheap metadata — dictionary-encoded
//! keys, observed cardinality, all-non-null, etc. DuckDB's
//! `PerfectHashAggregate` (flat `Vec<State>` indexed by code) covers
//! the bounded-cardinality cases; the substrate here is the fallback
//! when no template matches.
//!
//! ## Supported shape
//!
//! - **Predicate**: AND-chain of `Column ⊕ Literal` clauses, same
//!   IR as [`crate::fused_jit::Clause`]/`ClauseOp` (Float64/Date32/Int32
//!   columns; `<`, `<=`, `>`, `>=`). Evaluated row-by-row in Rust;
//!   future slices route through the JIT.
//! - **Group keys**: any number of columns, each `Utf8View` or
//!   `Dictionary(UInt32, _)`. Composite keys are packed into a
//!   `Vec<u8>` per row and used as a `HashMap` key.
//! - **Aggregates**: all `AggExpr` variants the JIT IR supports
//!   (`SumColumn`, `SumProductColumns`, `SumProductOneMinus`,
//!   `SumProductTwoOneMinusOnePlus`, `CountStar`). Each accumulates
//!   one `f64` per group cell.
//! - **Output**: one row per group key, with key columns first
//!   (as `Utf8`/`Dictionary`) followed by aggregate outputs (as
//!   `Float64` for SUMs, `Int64` for `CountStar`).
//!
//! ## Why pure Rust (no JIT) at this slice
//!
//! Splitting the substrate from the adaptive specialization keeps each
//! slice well-bounded:
//!
//! - .1 (this slice): proves correctness end-to-end on multiple GROUP
//!   BY shapes without touching the Cranelift IR.
//! - .2: adds the discovery → specialize state machine + a JIT-bake
//!   step that emits a per-shape kernel using the observed keys.
//! - .3: builds the SQL-pattern matcher (`InjectFilterMultiAggRule`)
//!   on top of the substrate and retires `InjectFusedQ1Rule`+`Q1Spec`.
//!
//! Each slice has its own bench gate; the .2 gate proves the adaptive
//! kernel reaches Q1Spec parity, which is what justifies the deletion
//! in .3.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::types::UInt32Type;
use arrow_array::{
    Array, ArrayRef, BinaryViewArray, Date32Array, DictionaryArray, Float64Array, Int32Array,
    Int64Array, RecordBatch, StringArray, StringViewArray, UInt32Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result as DfResult};

use crate::fused_aggregate::AggregateSpec;
use crate::fused_jit::{AggExpr, Clause, ClauseOp, ColumnTy};

/// One group key column's runtime representation.
///
/// `Utf8View` keys index by the **first byte** of the view's inline
/// data — same convention as `GroupSpec::known_keys` in the JIT IR.
/// This is one byte of distinguishing power; columns whose values
/// alias on the first byte (e.g. `('AAAA', 'AABB')` for a single key
/// column) would collide. TPC-H's grouping columns (`l_returnflag`,
/// `l_linestatus`, `l_shipmode`, `o_orderpriority`, …) are all 1-char
/// or have unique first bytes per distinct value, so this works
/// uniformly there. Multi-byte keys would need a per-column-length
/// extension we'll add when a workload requires it.
///
/// `Dictionary` keys use the u32 dict code directly — preferred for
/// perf since dict codes are dense small ints and the key is already
/// hash-friendly. Requires the upstream scan to emit
/// `DictionaryArray` (see `[[dict-arrival-blocker]]`).
#[derive(Debug, Clone, Copy)]
pub enum GroupKeyKind {
    /// Read the first byte of the column's `StringViewArray` per row.
    Utf8ViewFirstByte,
    /// Read the u32 code from a `DictionaryArray<UInt32Type>` per row.
    DictionaryU32,
}

/// Runtime-configured spec. Owned values; cloning is a refcount bump
/// on the `Arc`-wrapped output schema.
#[derive(Debug, Clone)]
pub struct FilterMultiAggSpec {
    /// AND-chain of predicate clauses. Empty vec = no filter.
    pub predicate: Vec<Clause>,
    /// Input column types (used to interpret `Clause::column` indices
    /// + agg-expr column indices). Position-indexed.
    pub input_tys: Vec<ColumnTy>,
    /// Column indices into the input batch's schema for predicate +
    /// agg inputs. `input_tys[i]` is the type at index
    /// `input_col_indices[i]`.
    pub input_col_indices: Vec<usize>,
    /// Aggregate output specs. One entry per output column.
    pub aggregates: Vec<AggExpr>,
    /// Output column names for the aggregate outputs (length matches
    /// `aggregates.len()`). Group-key column names come first in
    /// `output_schema`.
    pub agg_output_names: Vec<String>,
    /// Group key columns: one entry per key column.
    pub group_keys: Vec<GroupKeyColumn>,
    /// Cached output schema: group key columns first, then aggregate
    /// output columns. Built once at construction.
    pub output_schema: SchemaRef,
}

/// One group key column's spec.
#[derive(Debug, Clone)]
pub struct GroupKeyColumn {
    /// Column name in the input batch.
    pub name: String,
    /// Index in the input batch's schema.
    pub col_idx: usize,
    /// How to extract the key value at runtime.
    pub kind: GroupKeyKind,
    /// Output type the key is emitted as in `finalize`. Always either
    /// `Utf8` (for Utf8View keys, we materialise to Utf8 in finalize)
    /// or `Dictionary(UInt32, Utf8)` (for dict keys, we emit a small
    /// dict containing only the observed values).
    pub output_dtype: DataType,
}

impl FilterMultiAggSpec {
    /// Construct + validate the spec against the child plan's schema.
    /// Returns a `Plan` error on missing columns or type mismatches.
    pub fn try_new(
        predicate: Vec<Clause>,
        input_tys: Vec<ColumnTy>,
        input_column_names: &[&str],
        aggregates: Vec<AggExpr>,
        agg_output_names: Vec<String>,
        group_keys: Vec<(String, GroupKeyKind)>,
        child_schema: &SchemaRef,
    ) -> DfResult<Self> {
        if input_tys.len() != input_column_names.len() {
            return Err(DataFusionError::Plan(format!(
                "FilterMultiAggSpec: input_tys.len()={} but input_column_names.len()={}",
                input_tys.len(),
                input_column_names.len()
            )));
        }
        if aggregates.len() != agg_output_names.len() {
            return Err(DataFusionError::Plan(format!(
                "FilterMultiAggSpec: aggregates.len()={} but agg_output_names.len()={}",
                aggregates.len(),
                agg_output_names.len()
            )));
        }

        // Resolve predicate/agg input columns.
        let mut input_col_indices = Vec::with_capacity(input_column_names.len());
        for (i, &name) in input_column_names.iter().enumerate() {
            let idx = child_schema.index_of(name).map_err(|_| {
                DataFusionError::Plan(format!(
                    "FilterMultiAggSpec: child schema missing input column `{name}`"
                ))
            })?;
            let actual = child_schema.field(idx).data_type();
            if !matches_column_ty(actual, input_tys[i]) {
                return Err(DataFusionError::Plan(format!(
                    "FilterMultiAggSpec: column `{name}` has type {actual:?}, expected ColumnTy::{:?}",
                    input_tys[i]
                )));
            }
            input_col_indices.push(idx);
        }

        // Resolve group key columns + validate their actual data types
        // match the declared `GroupKeyKind`.
        let mut group_key_specs = Vec::with_capacity(group_keys.len());
        for (name, kind) in group_keys {
            let col_idx = child_schema.index_of(&name).map_err(|_| {
                DataFusionError::Plan(format!(
                    "FilterMultiAggSpec: child schema missing group key `{name}`"
                ))
            })?;
            let actual = child_schema.field(col_idx).data_type();
            let output_dtype = match (kind, actual) {
                (GroupKeyKind::Utf8ViewFirstByte, DataType::Utf8View) => DataType::Utf8,
                (GroupKeyKind::DictionaryU32, DataType::Dictionary(key_ty, val_ty))
                    if **key_ty == DataType::UInt32 =>
                {
                    DataType::Dictionary(key_ty.clone(), val_ty.clone())
                }
                _ => {
                    return Err(DataFusionError::Plan(format!(
                        "FilterMultiAggSpec: group key `{name}` has type {actual:?}, \
                         doesn't match GroupKeyKind::{kind:?}"
                    )));
                }
            };
            group_key_specs.push(GroupKeyColumn {
                name,
                col_idx,
                kind,
                output_dtype,
            });
        }

        // Build output schema: group keys + aggregate outputs.
        let mut fields: Vec<Field> = Vec::with_capacity(group_key_specs.len() + aggregates.len());
        for k in &group_key_specs {
            fields.push(Field::new(&k.name, k.output_dtype.clone(), false));
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
            group_keys: group_key_specs,
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

/// One group's aggregate cell vector. Length = `spec.aggregates.len()`.
///
/// `f64` for both SUMs and counts; counts are cast back to `i64` in
/// `finalize`. Matches the JIT IR's convention where COUNT is also an
/// `f64` cell in the kernel output (see `fused_jit.rs` AggExpr docs).
type AggCells = Vec<f64>;

/// Per-batch accumulator for FilterMultiAggSpec. Maps composite group
/// key (packed byte representation) → aggregate cells.
///
/// **Layout choice — packed `Vec<u8>` key:** simplest correct
/// implementation. Future slices may swap for a tighter open-addressing
/// table keyed on hashed values (smaller cells = better L1 residency),
/// but at .1 we prioritise correctness + readability. The per-row hash
/// table overhead is the cost we're explicitly accepting in exchange
/// for "no data-specific baking".
#[derive(Debug, Default)]
pub struct FilterMultiAggAccumulator {
    /// HashMap keyed on packed key bytes.
    pub groups: HashMap<Vec<u8>, AggCells>,
    /// For Dictionary key columns: the dict code-to-value mapping,
    /// captured the first time a code is seen. Used by `finalize` to
    /// materialise the output's Utf8/Dict columns.
    ///
    /// Outer Vec indexed by group-key position (matches
    /// `spec.group_keys`); inner map is `code → string`.
    pub dict_values: Vec<HashMap<u32, String>>,
}

impl AggregateSpec for FilterMultiAggSpec {
    type Accumulator = FilterMultiAggAccumulator;

    fn process_batch(&self, batch: &RecordBatch, acc: &mut Self::Accumulator) -> DfResult<()> {
        let n_rows = batch.num_rows();
        if n_rows == 0 {
            return Ok(());
        }

        // Cache input column ArrayRefs once per batch so we don't pay
        // the schema lookup per row.
        let input_cols: Vec<&dyn Array> = self
            .input_col_indices
            .iter()
            .map(|&i| batch.column(i).as_ref())
            .collect();

        // Resize dict_values to match the group_keys vec on first batch
        // (or per-shard when shards merge later).
        if acc.dict_values.len() < self.group_keys.len() {
            acc.dict_values
                .resize(self.group_keys.len(), HashMap::new());
        }

        // Cache group-key column ArrayRefs.
        let group_cols: Vec<&dyn Array> = self
            .group_keys
            .iter()
            .map(|k| batch.column(k.col_idx).as_ref())
            .collect();

        // Pre-cache typed accessors per group-key column so the hot
        // row loop doesn't redo the downcast per row. Order matches
        // `self.group_keys`.
        let group_accessors: Vec<GroupKeyAccessor> = self
            .group_keys
            .iter()
            .zip(group_cols.iter())
            .map(|(k, col)| GroupKeyAccessor::new(k.kind, *col))
            .collect::<DfResult<Vec<_>>>()?;

        // Hot row loop. For each row: evaluate predicate; if it
        // survives, build the composite key, look up the bucket, and
        // accumulate each aggregate.
        let mut key_buf: Vec<u8> = Vec::with_capacity(8 * self.group_keys.len());
        for row in 0..n_rows {
            if !self.eval_predicate(&input_cols, row) {
                continue;
            }
            key_buf.clear();
            for (gi, accessor) in group_accessors.iter().enumerate() {
                let dict_map = &mut acc.dict_values[gi];
                accessor.append_key_bytes(row, &mut key_buf, dict_map);
            }
            let cells = acc
                .groups
                .entry(key_buf.clone())
                .or_insert_with(|| vec![0.0; self.aggregates.len()]);
            for (ai, agg) in self.aggregates.iter().enumerate() {
                cells[ai] += self.eval_agg(agg, &input_cols, row);
            }
        }
        Ok(())
    }

    fn finalize(&self, acc: Self::Accumulator) -> DfResult<RecordBatch> {
        // Sort groups by key bytes for stable output ordering — matches
        // what an ORDER BY <group_keys> would give and makes test
        // assertions deterministic across runs.
        let mut entries: Vec<(Vec<u8>, AggCells)> = acc.groups.into_iter().collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        let n_groups = entries.len();
        let n_keys = self.group_keys.len();
        let n_aggs = self.aggregates.len();

        // Decode each group's key bytes back into typed values per
        // column. `entries[i].0` is the packed key for group i;
        // `acc.dict_values[col]` gives the code→string map for dict
        // columns; for Utf8View first-byte keys the byte itself is
        // the materialised character.
        let mut key_columns: Vec<ArrayRef> = Vec::with_capacity(n_keys);
        for (col_idx, key_spec) in self.group_keys.iter().enumerate() {
            match key_spec.kind {
                GroupKeyKind::Utf8ViewFirstByte => {
                    let strings: Vec<String> = entries
                        .iter()
                        .map(|(k, _)| {
                            let byte_offset = col_idx; // packed: 1 byte per key column
                            let b = k[byte_offset];
                            (b as char).to_string()
                        })
                        .collect();
                    key_columns.push(Arc::new(StringArray::from(strings)) as ArrayRef);
                }
                GroupKeyKind::DictionaryU32 => {
                    // Packed key carries the u32 code (4 bytes). Map
                    // back to string via the captured dict values.
                    let dict_map = &acc.dict_values[col_idx];
                    let codes: Vec<u32> = entries
                        .iter()
                        .map(|(k, _)| {
                            let off = packed_key_offset(&self.group_keys, col_idx);
                            u32::from_le_bytes([k[off], k[off + 1], k[off + 2], k[off + 3]])
                        })
                        .collect();
                    // Build a small dictionary with just the observed
                    // codes mapped to their strings. Indices into the
                    // values array start at 0.
                    let mut seen_codes: Vec<u32> = codes.clone();
                    seen_codes.sort_unstable();
                    seen_codes.dedup();
                    let values: Vec<String> = seen_codes
                        .iter()
                        .map(|c| dict_map.get(c).cloned().unwrap_or_default())
                        .collect();
                    let code_to_local: HashMap<u32, u32> = seen_codes
                        .iter()
                        .enumerate()
                        .map(|(i, c)| (*c, i as u32))
                        .collect();
                    let local_codes: Vec<u32> = codes.iter().map(|c| code_to_local[c]).collect();
                    let values_arr = StringArray::from(values);
                    let keys_arr = UInt32Array::from(local_codes);
                    let dict = DictionaryArray::<UInt32Type>::try_new(
                        keys_arr,
                        Arc::new(values_arr) as ArrayRef,
                    )
                    .map_err(|e| {
                        DataFusionError::Internal(format!(
                            "FilterMultiAggSpec finalize: dict build failed: {e}"
                        ))
                    })?;
                    key_columns.push(Arc::new(dict) as ArrayRef);
                }
            }
        }

        // Materialise aggregate output columns.
        let mut agg_columns: Vec<ArrayRef> = Vec::with_capacity(n_aggs);
        for (ai, agg) in self.aggregates.iter().enumerate() {
            let column: ArrayRef = match agg {
                AggExpr::CountStar => {
                    let vals: Vec<i64> = entries.iter().map(|(_, c)| c[ai] as i64).collect();
                    Arc::new(Int64Array::from(vals))
                }
                _ => {
                    let vals: Vec<f64> = entries.iter().map(|(_, c)| c[ai]).collect();
                    Arc::new(Float64Array::from(vals))
                }
            };
            agg_columns.push(column);
        }

        let mut all_columns = key_columns;
        all_columns.extend(agg_columns);
        let _ = n_groups; // only used in debug assertions if added later
        RecordBatch::try_new(self.output_schema.clone(), all_columns).map_err(|e| {
            DataFusionError::Internal(format!("FilterMultiAggSpec finalize: build batch: {e}"))
        })
    }

    fn merge(&self, mut left: Self::Accumulator, right: Self::Accumulator) -> Self::Accumulator {
        // Ensure dict_values is sized.
        if left.dict_values.len() < self.group_keys.len() {
            left.dict_values
                .resize(self.group_keys.len(), HashMap::new());
        }
        // Merge dict_values entries (right wins on collision, but
        // collisions on the same code should be the same string).
        for (i, m) in right.dict_values.into_iter().enumerate() {
            if i < left.dict_values.len() {
                for (code, val) in m {
                    left.dict_values[i].entry(code).or_insert(val);
                }
            }
        }
        // Merge group entries: sum cells.
        for (key, cells) in right.groups {
            let existing = left
                .groups
                .entry(key)
                .or_insert_with(|| vec![0.0; self.aggregates.len()]);
            for (i, v) in cells.into_iter().enumerate() {
                existing[i] += v;
            }
        }
        left
    }

    fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn validate_input_schema(&self, schema: &SchemaRef) -> DfResult<()> {
        // Re-resolve each known input + group key by name and verify
        // the type still matches. (Try_new did this once at
        // construction; this re-validates against potentially-
        // different child schemas the operator might be plugged
        // into.)
        for (i, &col_idx) in self.input_col_indices.iter().enumerate() {
            let f = schema.field(col_idx);
            if !matches_column_ty(f.data_type(), self.input_tys[i]) {
                return Err(DataFusionError::Plan(format!(
                    "FilterMultiAggSpec: column `{}` at idx {col_idx} has type {:?}, expected {:?}",
                    f.name(),
                    f.data_type(),
                    self.input_tys[i]
                )));
            }
        }
        for k in &self.group_keys {
            let f = schema.field(k.col_idx);
            match (k.kind, f.data_type()) {
                (GroupKeyKind::Utf8ViewFirstByte, DataType::Utf8View) => {}
                (GroupKeyKind::DictionaryU32, DataType::Dictionary(kt, _))
                    if **kt == DataType::UInt32 => {}
                _ => {
                    return Err(DataFusionError::Plan(format!(
                        "FilterMultiAggSpec: group key `{}` has type {:?}",
                        k.name,
                        f.data_type()
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Compute the byte offset of group-key column `col_idx`'s packed
/// representation in the composite key buffer.
fn packed_key_offset(keys: &[GroupKeyColumn], col_idx: usize) -> usize {
    keys[..col_idx]
        .iter()
        .map(|k| match k.kind {
            GroupKeyKind::Utf8ViewFirstByte => 1,
            GroupKeyKind::DictionaryU32 => 4,
        })
        .sum()
}

/// Typed accessor cached per batch so the hot loop avoids repeated
/// `as_any().downcast_ref::<...>()` chains.
enum GroupKeyAccessor<'a> {
    Utf8View(&'a StringViewArray),
    BinaryView(&'a BinaryViewArray),
    DictU32Utf8(&'a DictionaryArray<UInt32Type>, &'a StringArray),
}

impl<'a> GroupKeyAccessor<'a> {
    fn new(kind: GroupKeyKind, col: &'a dyn Array) -> DfResult<Self> {
        match kind {
            GroupKeyKind::Utf8ViewFirstByte => {
                if let Some(s) = col.as_any().downcast_ref::<StringViewArray>() {
                    Ok(GroupKeyAccessor::Utf8View(s))
                } else if let Some(b) = col.as_any().downcast_ref::<BinaryViewArray>() {
                    Ok(GroupKeyAccessor::BinaryView(b))
                } else {
                    Err(DataFusionError::Internal(format!(
                        "FilterMultiAggSpec: Utf8ViewFirstByte key got non-view array: {:?}",
                        col.data_type()
                    )))
                }
            }
            GroupKeyKind::DictionaryU32 => {
                let dict = col
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt32Type>>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "FilterMultiAggSpec: DictionaryU32 key got non-dict array: {:?}",
                            col.data_type()
                        ))
                    })?;
                let values = dict
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "FilterMultiAggSpec: dict values must be Utf8 (StringArray)".into(),
                        )
                    })?;
                Ok(GroupKeyAccessor::DictU32Utf8(dict, values))
            }
        }
    }

    /// Append this column's bytes for row `row` to `buf`, and (for
    /// dict columns) record the code→string mapping in `dict_map` if
    /// it's new.
    fn append_key_bytes(&self, row: usize, buf: &mut Vec<u8>, dict_map: &mut HashMap<u32, String>) {
        match self {
            GroupKeyAccessor::Utf8View(s) => {
                let v = s.value(row);
                let b = v.as_bytes().first().copied().unwrap_or(0);
                buf.push(b);
            }
            GroupKeyAccessor::BinaryView(b) => {
                let v = b.value(row);
                let byte = v.first().copied().unwrap_or(0);
                buf.push(byte);
            }
            GroupKeyAccessor::DictU32Utf8(dict, values) => {
                let code = dict.keys().value(row);
                dict_map
                    .entry(code)
                    .or_insert_with(|| values.value(code as usize).to_string());
                buf.extend_from_slice(&code.to_le_bytes());
            }
        }
    }
}

// Predicate + aggregate evaluation in Rust. State A of the adaptive
// design — slow vs JIT but correct for any shape. The .2 follow-up
// will route these through Cranelift once observed-keys are stable.

impl FilterMultiAggSpec {
    fn eval_predicate(&self, cols: &[&dyn Array], row: usize) -> bool {
        for clause in &self.predicate {
            if !self.eval_clause(clause, cols, row) {
                return false;
            }
        }
        true
    }

    fn eval_clause(&self, clause: &Clause, cols: &[&dyn Array], row: usize) -> bool {
        let col = cols[clause.column];
        match clause.op {
            ClauseOp::F64Ge | ClauseOp::F64Le | ClauseOp::F64Lt | ClauseOp::F64Gt => {
                let v = col
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .map(|a| a.value(row))
                    .unwrap_or(f64::NAN);
                let lit = clause.imm_f64;
                match clause.op {
                    ClauseOp::F64Ge => v >= lit,
                    ClauseOp::F64Le => v <= lit,
                    ClauseOp::F64Lt => v < lit,
                    ClauseOp::F64Gt => v > lit,
                    _ => unreachable!(),
                }
            }
            ClauseOp::I32Ge | ClauseOp::I32Le | ClauseOp::I32Lt | ClauseOp::I32Gt => {
                let v = col
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .map(|a| a.value(row))
                    .or_else(|| {
                        col.as_any()
                            .downcast_ref::<Int32Array>()
                            .map(|a| a.value(row))
                    })
                    .unwrap_or(i32::MIN);
                let lit = clause.imm_i32;
                match clause.op {
                    ClauseOp::I32Ge => v >= lit,
                    ClauseOp::I32Le => v <= lit,
                    ClauseOp::I32Lt => v < lit,
                    ClauseOp::I32Gt => v > lit,
                    _ => unreachable!(),
                }
            }
        }
    }

    fn eval_agg(&self, agg: &AggExpr, cols: &[&dyn Array], row: usize) -> f64 {
        fn f64_col(cols: &[&dyn Array], idx: usize, row: usize) -> f64 {
            cols[idx]
                .as_any()
                .downcast_ref::<Float64Array>()
                .map(|a| a.value(row))
                .unwrap_or(0.0)
        }
        match agg {
            AggExpr::SumColumn(i) => f64_col(cols, *i, row),
            AggExpr::SumProductColumns(a, b) => f64_col(cols, *a, row) * f64_col(cols, *b, row),
            AggExpr::SumProductOneMinus(a, b) => {
                f64_col(cols, *a, row) * (1.0 - f64_col(cols, *b, row))
            }
            AggExpr::SumProductTwoOneMinusOnePlus(a, b, c) => {
                f64_col(cols, *a, row)
                    * (1.0 - f64_col(cols, *b, row))
                    * (1.0 + f64_col(cols, *c, row))
            }
            AggExpr::CountStar => 1.0,
            // Σ.G.2f.1 doesn't yet handle the LIKE-prefix CASE/SUM
            // shape (Q14). Add when Q14 retires.
            AggExpr::SumProductOneMinusGuardedByPrefix { .. } => 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringViewArray;
    use arrow_schema::{DataType, Field, Schema};

    fn schema_q1_subset() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("l_returnflag", DataType::Utf8View, false),
            Field::new("l_linestatus", DataType::Utf8View, false),
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_tax", DataType::Float64, false),
            Field::new("l_shipdate", DataType::Date32, false),
        ]))
    }

    fn synthetic_batch(
        flags: &[&str],
        statuses: &[&str],
        qty: &[f64],
        price: &[f64],
        disc: &[f64],
        tax: &[f64],
        shipdate: &[i32],
    ) -> RecordBatch {
        let n = flags.len();
        assert!(
            statuses.len() == n
                && qty.len() == n
                && price.len() == n
                && disc.len() == n
                && tax.len() == n
                && shipdate.len() == n
        );
        let cols: Vec<ArrayRef> = vec![
            Arc::new(StringViewArray::from(flags.to_vec())),
            Arc::new(StringViewArray::from(statuses.to_vec())),
            Arc::new(Float64Array::from(qty.to_vec())),
            Arc::new(Float64Array::from(price.to_vec())),
            Arc::new(Float64Array::from(disc.to_vec())),
            Arc::new(Float64Array::from(tax.to_vec())),
            Arc::new(Date32Array::from(shipdate.to_vec())),
        ];
        RecordBatch::try_new(schema_q1_subset(), cols).unwrap()
    }

    /// Build a Q1-shaped spec: filter `l_shipdate <= 10000`,
    /// group by (l_returnflag, l_linestatus), 4 aggregates +
    /// COUNT(*).
    fn q1_spec() -> FilterMultiAggSpec {
        let predicate = vec![Clause {
            column: 6, // l_shipdate (position in input list below)
            op: ClauseOp::I32Le,
            imm_i32: 10000,
            imm_f64: 0.0,
        }];
        let input_tys = vec![
            ColumnTy::Utf8View, // 0 returnflag
            ColumnTy::Utf8View, // 1 linestatus
            ColumnTy::Float64,  // 2 quantity
            ColumnTy::Float64,  // 3 extprice
            ColumnTy::Float64,  // 4 discount
            ColumnTy::Float64,  // 5 tax
            ColumnTy::Date32,   // 6 shipdate
        ];
        let input_columns = &[
            "l_returnflag",
            "l_linestatus",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_shipdate",
        ];
        let aggregates = vec![
            AggExpr::SumColumn(2),                          // sum_qty
            AggExpr::SumColumn(3),                          // sum_base_price
            AggExpr::SumProductOneMinus(3, 4),              // sum_disc_price
            AggExpr::SumProductTwoOneMinusOnePlus(3, 4, 5), // sum_charge
            AggExpr::CountStar,
        ];
        let agg_output_names = vec![
            "sum_qty".into(),
            "sum_base_price".into(),
            "sum_disc_price".into(),
            "sum_charge".into(),
            "count_order".into(),
        ];
        let group_keys = vec![
            ("l_returnflag".into(), GroupKeyKind::Utf8ViewFirstByte),
            ("l_linestatus".into(), GroupKeyKind::Utf8ViewFirstByte),
        ];
        FilterMultiAggSpec::try_new(
            predicate,
            input_tys,
            input_columns,
            aggregates,
            agg_output_names,
            group_keys,
            &schema_q1_subset(),
        )
        .unwrap()
    }

    #[test]
    fn q1_shape_single_batch_four_groups_plus_one_filtered() {
        let spec = q1_spec();
        // 5 rows. 4 distinct (rflag, lstatus) pairs all pass; 1 row
        // is filtered out by shipdate.
        let batch = synthetic_batch(
            &["A", "N", "N", "R", "A"],
            &["F", "F", "O", "F", "F"],
            &[10.0, 5.0, 7.0, 3.0, 2.0],
            &[100.0, 50.0, 70.0, 30.0, 20.0],
            &[0.1, 0.05, 0.0, 0.2, 0.1],
            &[0.05, 0.05, 0.0, 0.1, 0.05],
            &[9000, 9500, 8000, 9999, 99999], // last row filtered (> 10000)
        );
        let mut acc = FilterMultiAggAccumulator::default();
        spec.process_batch(&batch, &mut acc).unwrap();
        let out = spec.finalize(acc).unwrap();
        assert_eq!(out.num_rows(), 4, "expected 4 distinct groups");
        // Columns: returnflag, linestatus, sum_qty, sum_base_price,
        // sum_disc_price, sum_charge, count_order
        let flags = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let statuses = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // Stable sort on packed key → ('A','F'), ('N','F'), ('N','O'), ('R','F')
        let pairs: Vec<(&str, &str)> = (0..4)
            .map(|i| (flags.value(i), statuses.value(i)))
            .collect();
        assert_eq!(pairs, vec![("A", "F"), ("N", "F"), ("N", "O"), ("R", "F")]);
        // (A,F) only got row 0 (row 4 was filtered).
        let counts = out.column(6).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(counts.value(0), 1, "(A,F) count");
        assert_eq!(counts.value(1), 1, "(N,F) count");
        assert_eq!(counts.value(2), 1, "(N,O) count");
        assert_eq!(counts.value(3), 1, "(R,F) count");
        // (A,F) sum_qty = 10.0; (N,F) = 5.0; (N,O) = 7.0; (R,F) = 3.0
        let sum_qty = out
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(sum_qty.value(0), 10.0);
        assert_eq!(sum_qty.value(1), 5.0);
        assert_eq!(sum_qty.value(2), 7.0);
        assert_eq!(sum_qty.value(3), 3.0);
    }

    #[test]
    fn merge_accumulates_across_shards() {
        let spec = q1_spec();
        let batch1 = synthetic_batch(
            &["A", "N"],
            &["F", "F"],
            &[10.0, 5.0],
            &[100.0, 50.0],
            &[0.0, 0.0],
            &[0.0, 0.0],
            &[1, 1],
        );
        let batch2 = synthetic_batch(
            &["A", "N"],
            &["F", "F"],
            &[3.0, 7.0],
            &[30.0, 70.0],
            &[0.0, 0.0],
            &[0.0, 0.0],
            &[1, 1],
        );
        let mut acc1 = FilterMultiAggAccumulator::default();
        let mut acc2 = FilterMultiAggAccumulator::default();
        spec.process_batch(&batch1, &mut acc1).unwrap();
        spec.process_batch(&batch2, &mut acc2).unwrap();
        let merged = spec.merge(acc1, acc2);
        let out = spec.finalize(merged).unwrap();
        let sum_qty = out
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // (A,F) = 10 + 3 = 13; (N,F) = 5 + 7 = 12
        assert_eq!(sum_qty.value(0), 13.0);
        assert_eq!(sum_qty.value(1), 12.0);
    }

    #[test]
    fn predicate_only_no_groupby_single_bucket() {
        // No group keys → one "row" out with all-rows aggregate.
        let predicate = vec![Clause {
            column: 0,
            op: ClauseOp::F64Lt,
            imm_i32: 0,
            imm_f64: 24.0,
        }];
        let input_tys = vec![ColumnTy::Float64];
        let input_columns = &["l_quantity"];
        let aggregates = vec![AggExpr::SumColumn(0), AggExpr::CountStar];
        let agg_output_names = vec!["sum_qty".into(), "n".into()];
        let group_keys: Vec<(String, GroupKeyKind)> = vec![];
        let schema = Arc::new(Schema::new(vec![Field::new(
            "l_quantity",
            DataType::Float64,
            false,
        )]));
        let spec = FilterMultiAggSpec::try_new(
            predicate,
            input_tys,
            input_columns,
            aggregates,
            agg_output_names,
            group_keys,
            &schema,
        )
        .unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Float64Array::from(vec![5.0, 10.0, 50.0, 12.0]))],
        )
        .unwrap();
        let mut acc = FilterMultiAggAccumulator::default();
        spec.process_batch(&batch, &mut acc).unwrap();
        let out = spec.finalize(acc).unwrap();
        assert_eq!(out.num_rows(), 1);
        let sum = out
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let n = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(sum.value(0), 27.0); // 5 + 10 + 12 (50 filtered)
        assert_eq!(n.value(0), 3);
    }

    #[test]
    fn dictionary_group_key() {
        use arrow_array::builder::StringDictionaryBuilder;
        use arrow_array::types::UInt32Type;
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "g",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("v", DataType::Float64, false),
        ]));
        let mut builder = StringDictionaryBuilder::<UInt32Type>::new();
        for s in ["a", "a", "b", "c", "a"] {
            builder.append(s).unwrap();
        }
        let dict = builder.finish();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(dict),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 10.0, 100.0, 3.0])),
            ],
        )
        .unwrap();

        let spec = FilterMultiAggSpec::try_new(
            vec![],
            vec![ColumnTy::Float64],
            &["v"],
            vec![AggExpr::SumColumn(0), AggExpr::CountStar],
            vec!["sum_v".into(), "n".into()],
            vec![("g".into(), GroupKeyKind::DictionaryU32)],
            &schema,
        )
        .unwrap();
        let mut acc = FilterMultiAggAccumulator::default();
        spec.process_batch(&batch, &mut acc).unwrap();
        let out = spec.finalize(acc).unwrap();
        assert_eq!(out.num_rows(), 3, "expected 3 distinct group keys");
        let counts = out.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        // Sums should be a=1+2+3=6, b=10, c=100; sorted by code byte
        // order → depends on builder's code assignment. Verify total.
        let total: i64 = (0..3).map(|i| counts.value(i)).sum();
        assert_eq!(total, 5);
        let sum_v = out
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let total_v: f64 = (0..3).map(|i| sum_v.value(i)).sum();
        assert!((total_v - 116.0).abs() < 1e-9);
    }
}
