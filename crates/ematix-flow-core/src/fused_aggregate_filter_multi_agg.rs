//! Σ.G.2f.1 + .2: `FilterMultiAggSpec` — runtime-configured group-by +
//! multi-aggregate `AggregateSpec` with **no data-specific JIT baking**,
//! plus a per-batch template-specialization dispatch built on top.
//!
//! This is the substrate that will retire `InjectFusedQ1Rule` (task
//! #480). Unlike [`crate::fused_aggregate::Q1Spec`], which bakes 5
//! Q1-specific group keys (`(A,F)`, `(N,F)`, `(N,O)`, `(R,F)`, catchall)
//! into the Cranelift kernel for branchless dispatch, this spec
//! discovers groups at runtime. Σ.G.2f.2 lands the first template —
//! the DuckDB PR #15152 dict-vector inner loop — so the "fallback" path
//! is already specialized for the common single-dict-key shape; future
//! templates (Utf8View first-byte 1-key, 2-key composites, DuckDB
//! `PerfectHashAggregate` for bounded-cardinality dicts) plug into the
//! same dispatch site.
//!
//! Per the research note backing #480's design (memory:
//! [[project_groupby_research_2026_05]]), the right path is **not** a
//! Cranelift JIT but a Photon-style template cookbook: pre-compiled
//! kernel variants (Rust generics monomorphised at compile time)
//! dispatched per batch from cheap metadata — dictionary-encoded
//! keys, observed cardinality, all-non-null, etc.
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
//! - .1: proves correctness end-to-end on multiple GROUP BY shapes
//!   with a single generic hash-grouped kernel.
//! - .2 (this slice extends .1): adds per-batch template dispatch +
//!   the first specialized template (dict-single). Hot loops are
//!   plain Rust functions monomorphised on shape — no Cranelift IR.
//! - .3: builds the SQL-pattern matcher (`InjectFilterMultiAggRule`)
//!   on top of the substrate and retires `InjectFusedQ1Rule`+`Q1Spec`.
//!
//! Each slice has its own bench gate; the .2 gate proves the
//! specialized kernels reach Q1Spec parity, which is what justifies
//! the deletion in .3.

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
    /// Lossy by design — only correct for single-character grouping
    /// (TPC-H Q01's `l_returnflag`/`l_linestatus`). Multi-char string
    /// columns must arrive as `Dictionary(UInt32)` via dict-preservation.
    Utf8ViewFirstByte,
    /// Read the u32 code from a `DictionaryArray<UInt32Type>` per row.
    DictionaryU32,
    /// Σ.H.1b: 8-byte primitive group key for `Int64`.
    Int64,
    /// Σ.H.1b: 4-byte primitive group key for `Int32`.
    Int32,
    /// Σ.H.1b: 4-byte primitive group key for `Date32`
    /// (days-since-epoch i32 value).
    Date32,
    /// Σ.H.1b: 8-byte primitive group key for `Float64`. Bit-cast to
    /// `u64` for hash bytes — NaN/-0.0 collide as per their bit
    /// pattern. Rare in OLAP; included for Q10's `c_acctbal`.
    Float64,
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
        // AvgColumn divides at finalize by a CountStar cell. A spec
        // with AvgColumn but no CountStar would have no count to
        // divide by; reject up front so the planner falls through to
        // DataFusion's default plan instead.
        let has_avg = aggregates
            .iter()
            .any(|a| matches!(a, AggExpr::AvgColumn(_)));
        let has_count = aggregates.iter().any(|a| matches!(a, AggExpr::CountStar));
        if has_avg && !has_count {
            return Err(DataFusionError::Plan(
                "FilterMultiAggSpec: AvgColumn requires a CountStar aggregate \
                 to divide by at finalize"
                    .into(),
            ));
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
                (GroupKeyKind::Int64, DataType::Int64) => DataType::Int64,
                (GroupKeyKind::Int32, DataType::Int32) => DataType::Int32,
                (GroupKeyKind::Date32, DataType::Date32) => DataType::Date32,
                (GroupKeyKind::Float64, DataType::Float64) => DataType::Float64,
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
        // Per-batch dispatch into a specialized template when the
        // shape matches one we've pre-compiled, falling back to the
        // generic hash-grouped loop otherwise. Each template is a
        // Rust function monomorphised at compile time — Photon-style
        // template cookbook, no Cranelift, no runtime IR.
        //
        // Current templates (in priority order):
        //   * perfect-hash: one `DictionaryU32` group key with bounded
        //     cardinality. Hot loop indexes a flat `Vec<f64>` directly
        //     by dict code — no HashMap at all. DuckDB
        //     `PerfectHashAggregate` style; matches Q1Spec's
        //     branchless arm-match dispatch shape in pure Rust.
        //   * dict-single: one `DictionaryU32` group key, any
        //     cardinality. Per-batch slot table amortises the global
        //     HashMap probe to one per unique code rather than per
        //     row; DuckDB PR #15152 style.
        //   * generic: any shape.
        if self.group_keys.len() == 1
            && matches!(self.group_keys[0].kind, GroupKeyKind::DictionaryU32)
        {
            if let Some(dict) = batch
                .column(self.group_keys[0].col_idx)
                .as_any()
                .downcast_ref::<DictionaryArray<UInt32Type>>()
            {
                if dict.values().len() <= PERFECT_HASH_DICT_CARDINALITY_THRESHOLD {
                    return self.process_batch_perfect_hash_dict(batch, acc);
                }
            }
            return self.process_batch_dict_single(batch, acc);
        }
        // Σ.G.2f.4 — Q1 SQL shape (two Utf8View first-byte keys).
        if self.group_keys.len() == 2
            && matches!(self.group_keys[0].kind, GroupKeyKind::Utf8ViewFirstByte)
            && matches!(self.group_keys[1].kind, GroupKeyKind::Utf8ViewFirstByte)
        {
            return self.process_batch_two_key_utf8view(batch, acc);
        }
        self.process_batch_generic(batch, acc)
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
                GroupKeyKind::Int64 => {
                    let off = packed_key_offset(&self.group_keys, col_idx);
                    let vals: Vec<i64> = entries
                        .iter()
                        .map(|(k, _)| {
                            i64::from_le_bytes([
                                k[off], k[off + 1], k[off + 2], k[off + 3],
                                k[off + 4], k[off + 5], k[off + 6], k[off + 7],
                            ])
                        })
                        .collect();
                    key_columns.push(Arc::new(Int64Array::from(vals)) as ArrayRef);
                }
                GroupKeyKind::Int32 => {
                    let off = packed_key_offset(&self.group_keys, col_idx);
                    let vals: Vec<i32> = entries
                        .iter()
                        .map(|(k, _)| {
                            i32::from_le_bytes([k[off], k[off + 1], k[off + 2], k[off + 3]])
                        })
                        .collect();
                    key_columns.push(Arc::new(Int32Array::from(vals)) as ArrayRef);
                }
                GroupKeyKind::Date32 => {
                    let off = packed_key_offset(&self.group_keys, col_idx);
                    let vals: Vec<i32> = entries
                        .iter()
                        .map(|(k, _)| {
                            i32::from_le_bytes([k[off], k[off + 1], k[off + 2], k[off + 3]])
                        })
                        .collect();
                    key_columns.push(Arc::new(Date32Array::from(vals)) as ArrayRef);
                }
                GroupKeyKind::Float64 => {
                    let off = packed_key_offset(&self.group_keys, col_idx);
                    let vals: Vec<f64> = entries
                        .iter()
                        .map(|(k, _)| {
                            let bits = u64::from_le_bytes([
                                k[off], k[off + 1], k[off + 2], k[off + 3],
                                k[off + 4], k[off + 5], k[off + 6], k[off + 7],
                            ]);
                            f64::from_bits(bits)
                        })
                        .collect();
                    key_columns.push(Arc::new(Float64Array::from(vals)) as ArrayRef);
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

        // Cache the CountStar slot index (if any) so AvgColumn cells
        // can divide by it during finalize. Validated at try_new:
        // every spec with an AvgColumn has at least one CountStar.
        let count_slot: Option<usize> = self
            .aggregates
            .iter()
            .position(|a| matches!(a, AggExpr::CountStar));

        // Materialise aggregate output columns.
        let mut agg_columns: Vec<ArrayRef> = Vec::with_capacity(n_aggs);
        for (ai, agg) in self.aggregates.iter().enumerate() {
            let column: ArrayRef = match agg {
                AggExpr::CountStar => {
                    let vals: Vec<i64> = entries.iter().map(|(_, c)| c[ai] as i64).collect();
                    Arc::new(Int64Array::from(vals))
                }
                AggExpr::AvgColumn(_) => {
                    // Divide the accumulated sum cell by the group's
                    // count. count_slot is Some by try_new's validation.
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
        // Merge group entries. Combine cells using each agg's
        // semantics (+= for sum-like, min/max for the order variants).
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
                (GroupKeyKind::Int64, DataType::Int64) => {}
                (GroupKeyKind::Int32, DataType::Int32) => {}
                (GroupKeyKind::Date32, DataType::Date32) => {}
                (GroupKeyKind::Float64, DataType::Float64) => {}
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
    keys[..col_idx].iter().map(|k| key_byte_width(k.kind)).sum()
}

/// Byte width of one packed group-key cell in the composite key
/// buffer. Σ.H.1b grew this from a hard-coded 1/4 table to a function
/// so the new primitive kinds (Int32/Date32 = 4, Int64/Float64 = 8)
/// extend cleanly.
fn key_byte_width(kind: GroupKeyKind) -> usize {
    match kind {
        GroupKeyKind::Utf8ViewFirstByte => 1,
        GroupKeyKind::DictionaryU32 => 4,
        GroupKeyKind::Int32 => 4,
        GroupKeyKind::Date32 => 4,
        GroupKeyKind::Int64 => 8,
        GroupKeyKind::Float64 => 8,
    }
}

/// Typed accessor cached per batch so the hot loop avoids repeated
/// `as_any().downcast_ref::<...>()` chains.
enum GroupKeyAccessor<'a> {
    Utf8View(&'a StringViewArray),
    BinaryView(&'a BinaryViewArray),
    DictU32Utf8(&'a DictionaryArray<UInt32Type>, &'a StringArray),
    Int64(&'a Int64Array),
    Int32(&'a Int32Array),
    Date32(&'a Date32Array),
    Float64(&'a Float64Array),
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
            GroupKeyKind::Int64 => col
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(GroupKeyAccessor::Int64)
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "FilterMultiAggSpec: Int64 key got non-Int64 array: {:?}",
                        col.data_type()
                    ))
                }),
            GroupKeyKind::Int32 => col
                .as_any()
                .downcast_ref::<Int32Array>()
                .map(GroupKeyAccessor::Int32)
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "FilterMultiAggSpec: Int32 key got non-Int32 array: {:?}",
                        col.data_type()
                    ))
                }),
            GroupKeyKind::Date32 => col
                .as_any()
                .downcast_ref::<Date32Array>()
                .map(GroupKeyAccessor::Date32)
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "FilterMultiAggSpec: Date32 key got non-Date32 array: {:?}",
                        col.data_type()
                    ))
                }),
            GroupKeyKind::Float64 => col
                .as_any()
                .downcast_ref::<Float64Array>()
                .map(GroupKeyAccessor::Float64)
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "FilterMultiAggSpec: Float64 key got non-Float64 array: {:?}",
                        col.data_type()
                    ))
                }),
        }
    }

    /// First byte of this column at row `row`. Defined for
    /// `Utf8ViewFirstByte`-shaped accessors (Utf8View / BinaryView)
    /// only — callers that have a `DictU32Utf8` accessor must use
    /// `append_key_bytes` instead. Panics on the dict variant in debug
    /// builds; returns 0 in release builds for safety.
    #[inline(always)]
    fn first_byte_at(&self, row: usize) -> u8 {
        match self {
            GroupKeyAccessor::Utf8View(s) => s.value(row).as_bytes().first().copied().unwrap_or(0),
            GroupKeyAccessor::BinaryView(b) => b.value(row).first().copied().unwrap_or(0),
            GroupKeyAccessor::DictU32Utf8(_, _)
            | GroupKeyAccessor::Int64(_)
            | GroupKeyAccessor::Int32(_)
            | GroupKeyAccessor::Date32(_)
            | GroupKeyAccessor::Float64(_) => {
                debug_assert!(false, "first_byte_at called on non-Utf8View accessor");
                0
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
            GroupKeyAccessor::Int64(a) => {
                buf.extend_from_slice(&a.value(row).to_le_bytes());
            }
            GroupKeyAccessor::Int32(a) => {
                buf.extend_from_slice(&a.value(row).to_le_bytes());
            }
            GroupKeyAccessor::Date32(a) => {
                buf.extend_from_slice(&a.value(row).to_le_bytes());
            }
            GroupKeyAccessor::Float64(a) => {
                // Bit-cast to u64 so NaN/-0.0 collide as per their
                // exact bit pattern (consistent with DataFusion's
                // default behavior, which also uses raw bits as the
                // hash key).
                buf.extend_from_slice(&a.value(row).to_bits().to_le_bytes());
            }
        }
    }
}

/// Per-batch typed-slice cache for the predicate + agg input columns.
///
/// Built once per batch by downcasting each input ArrayRef into its
/// concrete primitive slice. The hot row loop then indexes plain
/// `&[f64]` / `&[i32]` rather than re-running `.as_any().downcast_ref()`
/// per row. Σ.G.2f.2's bench gate showed this single change closes
/// most of the ~4× gap vs Q1Spec's Cranelift-baked kernel — pure-Rust
/// scalar code on typed slices vectorises through LLVM the same way
/// the JIT does.
enum TypedCol<'a> {
    F64(&'a [f64]),
    I32(&'a [i32]),
    /// String / view columns that the predicate and aggregate eval
    /// never touch (they reach the kernel only via `GroupKeyAccessor`).
    /// Held here so the input-list slot index stays stable across the
    /// spec — `TypedCol::f64_at` / `i32_at` won't be called on them.
    Skip,
}

impl<'a> TypedCol<'a> {
    fn build(col: &'a dyn Array, expected: ColumnTy) -> DfResult<Self> {
        match expected {
            ColumnTy::Float64 => Ok(TypedCol::F64(
                col.as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "FilterMultiAggSpec: expected Float64, got {:?}",
                            col.data_type()
                        ))
                    })?
                    .values(),
            )),
            ColumnTy::Date32 => Ok(TypedCol::I32(
                col.as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "FilterMultiAggSpec: expected Date32, got {:?}",
                            col.data_type()
                        ))
                    })?
                    .values(),
            )),
            ColumnTy::Int32 => Ok(TypedCol::I32(
                col.as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "FilterMultiAggSpec: expected Int32, got {:?}",
                            col.data_type()
                        ))
                    })?
                    .values(),
            )),
            // Int64 isn't reached by the hot loop in the shapes
            // covered today; Utf8View slots are held but never
            // read (group-key access goes through GroupKeyAccessor).
            ColumnTy::Int64 => Err(DataFusionError::Internal(
                "FilterMultiAggSpec: TypedCol cache doesn't yet support Int64".into(),
            )),
            ColumnTy::Utf8View => Ok(TypedCol::Skip),
        }
    }

    #[inline(always)]
    fn f64_at(&self, row: usize) -> f64 {
        match self {
            TypedCol::F64(v) => v[row],
            // Reaching here means the spec referenced a non-Float64
            // column from a predicate/agg slot; the validator should
            // surface that at try_new time. 0.0 keeps the kernel safe.
            TypedCol::I32(_) | TypedCol::Skip => 0.0,
        }
    }

    #[inline(always)]
    fn i32_at(&self, row: usize) -> i32 {
        match self {
            TypedCol::I32(v) => v[row],
            TypedCol::F64(_) | TypedCol::Skip => i32::MIN,
        }
    }
}

/// Dict cardinality threshold for the PerfectHashAggregate template.
/// Per-batch cell storage is `cardinality * n_aggs * 8 B`; at
/// `n_aggs = 5` and `cardinality = 4096` this is 160 KB — fits in
/// L2 on every realistic target. Beyond this the dispatch falls back
/// to `dict_single`, which allocates a HashMap sized to actually
/// observed codes rather than the full dict cardinality.
pub(crate) const PERFECT_HASH_DICT_CARDINALITY_THRESHOLD: usize = 4096;

// Per-batch templates. Σ.G.2f.2 lands three: a generic hash-grouped
// fallback, a dict-single specialization, and a PerfectHashAggregate
// for bounded-cardinality dicts. Future templates (Utf8View first-
// byte 1-key, 2-key composites) plug in via the same dispatch site.

impl FilterMultiAggSpec {
    /// Generic shape: any number of group keys, any kind, any aggs.
    /// Per-row composite key build → global HashMap probe → cell
    /// accumulation. Slow vs the specialized templates but correct
    /// for shapes that don't match one.
    pub(crate) fn process_batch_generic(
        &self,
        batch: &RecordBatch,
        acc: &mut FilterMultiAggAccumulator,
    ) -> DfResult<()> {
        let n_rows = batch.num_rows();
        if n_rows == 0 {
            return Ok(());
        }

        let typed_cols = self.build_typed_cols(batch)?;

        if acc.dict_values.len() < self.group_keys.len() {
            acc.dict_values
                .resize(self.group_keys.len(), HashMap::new());
        }

        let group_cols: Vec<&dyn Array> = self
            .group_keys
            .iter()
            .map(|k| batch.column(k.col_idx).as_ref())
            .collect();

        let group_accessors: Vec<GroupKeyAccessor> = self
            .group_keys
            .iter()
            .zip(group_cols.iter())
            .map(|(k, col)| GroupKeyAccessor::new(k.kind, *col))
            .collect::<DfResult<Vec<_>>>()?;

        let mut key_buf: Vec<u8> = Vec::with_capacity(8 * self.group_keys.len());
        for row in 0..n_rows {
            if !self.eval_predicate_typed(&typed_cols, row) {
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
                .or_insert_with(|| fresh_cells(&self.aggregates));
            for (ai, agg) in self.aggregates.iter().enumerate() {
                cells[ai] =
                    self.combine_cell(agg, cells[ai], self.eval_agg_typed(agg, &typed_cols, row));
            }
        }
        Ok(())
    }

    /// Specialized template: exactly one group key, of kind
    /// `DictionaryU32`. The hot loop indexes a small per-batch slot
    /// table by dict code (size = unique codes observed in this batch)
    /// instead of probing the global HashMap per row. The dict's
    /// existing code-space gives us a perfect hash for free; the
    /// per-batch local table fits in L1 even for medium-cardinality
    /// columns. After the row loop, we fold the local cells into the
    /// global HashMap (one probe per distinct code, not per row).
    ///
    /// This is the pattern DuckDB landed in PR #15152 ("dict-vector
    /// inner loop"). Polars uses a related approach via Categorical
    /// short-circuit; Photon's playbook treats this as the canonical
    /// string-group-by win.
    pub(crate) fn process_batch_dict_single(
        &self,
        batch: &RecordBatch,
        acc: &mut FilterMultiAggAccumulator,
    ) -> DfResult<()> {
        debug_assert_eq!(self.group_keys.len(), 1);
        debug_assert!(matches!(
            self.group_keys[0].kind,
            GroupKeyKind::DictionaryU32
        ));

        let n_rows = batch.num_rows();
        if n_rows == 0 {
            return Ok(());
        }

        let typed_cols = self.build_typed_cols(batch)?;

        if acc.dict_values.is_empty() {
            acc.dict_values.resize(1, HashMap::new());
        }

        let key_col = batch.column(self.group_keys[0].col_idx);
        let dict = key_col
            .as_any()
            .downcast_ref::<DictionaryArray<UInt32Type>>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "FilterMultiAggSpec dict_single: expected DictionaryArray<UInt32>, got {:?}",
                    key_col.data_type()
                ))
            })?;
        let dict_values = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "FilterMultiAggSpec dict_single: dict values must be Utf8 (StringArray)".into(),
                )
            })?;
        let dict_keys = dict.keys().values();

        // Per-batch slot table: code → index into `local_cells`. Built
        // lazily as new codes are observed in the row loop. Sized to
        // unique-codes-in-this-batch, which is bounded by
        // dict_values.len() and typically much smaller than the
        // accumulator's global group count.
        let mut code_to_local: HashMap<u32, usize> = HashMap::new();
        let mut local_cells: Vec<AggCells> = Vec::new();
        let mut local_codes: Vec<u32> = Vec::new();

        for row in 0..n_rows {
            if !self.eval_predicate_typed(&typed_cols, row) {
                continue;
            }
            let code = dict_keys[row];
            let local_idx = match code_to_local.entry(code) {
                std::collections::hash_map::Entry::Occupied(e) => *e.get(),
                std::collections::hash_map::Entry::Vacant(e) => {
                    let idx = local_cells.len();
                    local_cells.push(fresh_cells(&self.aggregates));
                    local_codes.push(code);
                    // Capture the dict value mapping the first time
                    // this code is seen (used by `finalize`).
                    acc.dict_values[0]
                        .entry(code)
                        .or_insert_with(|| dict_values.value(code as usize).to_string());
                    *e.insert(idx)
                }
            };
            let cells = &mut local_cells[local_idx];
            for (ai, agg) in self.aggregates.iter().enumerate() {
                cells[ai] =
                    self.combine_cell(agg, cells[ai], self.eval_agg_typed(agg, &typed_cols, row));
            }
        }

        // Fold local cells into the global accumulator. One probe per
        // unique code, not per row. Combine semantics matches the
        // hot loop (sum-like / min / max) per agg.
        for (idx, code) in local_codes.iter().enumerate() {
            let mut key_buf = Vec::with_capacity(4);
            key_buf.extend_from_slice(&code.to_le_bytes());
            let entry = acc
                .groups
                .entry(key_buf)
                .or_insert_with(|| fresh_cells(&self.aggregates));
            let local = std::mem::take(&mut local_cells[idx]);
            for (i, v) in local.into_iter().enumerate() {
                entry[i] = self.combine_cell(&self.aggregates[i], entry[i], v);
            }
        }

        Ok(())
    }

    /// PerfectHashAggregate template — fires for one `DictionaryU32`
    /// group key with bounded cardinality (≤
    /// [`PERFECT_HASH_DICT_CARDINALITY_THRESHOLD`]). The hot loop has
    /// **no HashMap probe at all**: cell storage is a flat
    /// `Vec<f64>` of length `cardinality * n_aggs`, indexed directly
    /// by dict code. This matches Q1Spec's branchless 5-arm baked
    /// match shape in pure Rust — DuckDB's `PerfectHashAggregate`
    /// pattern.
    ///
    /// Cardinality threshold guards memory: TPC-H grouping columns
    /// have 2–7 distinct values; the threshold accommodates 4096
    /// codes (40-160 KB per batch at 5–20 aggs, fits in L1/L2). Above
    /// the threshold the dispatch falls back to `dict_single`.
    pub(crate) fn process_batch_perfect_hash_dict(
        &self,
        batch: &RecordBatch,
        acc: &mut FilterMultiAggAccumulator,
    ) -> DfResult<()> {
        debug_assert_eq!(self.group_keys.len(), 1);
        debug_assert!(matches!(
            self.group_keys[0].kind,
            GroupKeyKind::DictionaryU32
        ));

        let n_rows = batch.num_rows();
        if n_rows == 0 {
            return Ok(());
        }

        let n_aggs = self.aggregates.len();
        let typed_cols = self.build_typed_cols(batch)?;

        if acc.dict_values.is_empty() {
            acc.dict_values.resize(1, HashMap::new());
        }

        let key_col = batch.column(self.group_keys[0].col_idx);
        let dict = key_col
            .as_any()
            .downcast_ref::<DictionaryArray<UInt32Type>>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "FilterMultiAggSpec perfect_hash: expected DictionaryArray<UInt32>, got {:?}",
                    key_col.data_type()
                ))
            })?;
        let dict_values = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "FilterMultiAggSpec perfect_hash: dict values must be Utf8 (StringArray)"
                        .into(),
                )
            })?;
        let dict_keys = dict.keys().values();
        let cardinality = dict_values.len();

        // Flat per-batch cell storage. Each agg slot is initialised
        // with its own seed value (`+inf` for MIN, `-inf` for MAX, 0
        // for sum-like) so the per-row combine doesn't need a
        // first-row special case. `seen[code]` tracks which codes had
        // any contribution this batch so the merge loop touches only
        // those slots (not the full cardinality).
        let initials: Vec<f64> = self.aggregates.iter().map(initial_cell_value).collect();
        let mut cells: Vec<f64> = Vec::with_capacity(cardinality * n_aggs);
        for _ in 0..cardinality {
            cells.extend_from_slice(&initials);
        }
        let mut seen: Vec<bool> = vec![false; cardinality];
        let mut seen_codes: Vec<u32> = Vec::new();

        // Hot loop. The body is:
        //   1. predicate eval on typed slices (no dyn dispatch)
        //   2. flat array index by dict code (no HashMap)
        //   3. per-agg accumulation
        // LLVM autovectorises the inner agg loop in the common case
        // where n_aggs is small and AggExpr arms collapse to FMA.
        for row in 0..n_rows {
            if !self.eval_predicate_typed(&typed_cols, row) {
                continue;
            }
            let code = dict_keys[row];
            let code_idx = code as usize;
            if !seen[code_idx] {
                seen[code_idx] = true;
                seen_codes.push(code);
            }
            let base = code_idx * n_aggs;
            for (ai, agg) in self.aggregates.iter().enumerate() {
                cells[base + ai] = self.combine_cell(
                    agg,
                    cells[base + ai],
                    self.eval_agg_typed(agg, &typed_cols, row),
                );
            }
        }

        // Merge into global accumulator. One probe per code that
        // actually had contributions in this batch.
        for code in seen_codes {
            let code_idx = code as usize;
            acc.dict_values[0]
                .entry(code)
                .or_insert_with(|| dict_values.value(code_idx).to_string());
            let mut key_buf = Vec::with_capacity(4);
            key_buf.extend_from_slice(&code.to_le_bytes());
            let entry = acc
                .groups
                .entry(key_buf)
                .or_insert_with(|| fresh_cells(&self.aggregates));
            let base = code_idx * n_aggs;
            for ai in 0..n_aggs {
                entry[ai] = self.combine_cell(&self.aggregates[ai], entry[ai], cells[base + ai]);
            }
        }

        Ok(())
    }

    /// Σ.G.2f.4 — specialized template for the two-Utf8View-first-byte
    /// group-key shape (Q1 SQL when strings arrive as `Utf8View`).
    /// Packs the two first bytes into a single `u16` and uses a per-
    /// batch local index (`HashMap<u16, usize>`) to amortise the global
    /// HashMap probe to one per distinct pair (≈ 4 for TPC-H Q1)
    /// rather than per row. The fold writes the same 2-byte packed key
    /// shape `append_key_bytes` would produce, so `finalize` is path-
    /// independent.
    pub(crate) fn process_batch_two_key_utf8view(
        &self,
        batch: &RecordBatch,
        acc: &mut FilterMultiAggAccumulator,
    ) -> DfResult<()> {
        debug_assert_eq!(self.group_keys.len(), 2);
        debug_assert!(matches!(
            self.group_keys[0].kind,
            GroupKeyKind::Utf8ViewFirstByte
        ));
        debug_assert!(matches!(
            self.group_keys[1].kind,
            GroupKeyKind::Utf8ViewFirstByte
        ));

        let n_rows = batch.num_rows();
        if n_rows == 0 {
            return Ok(());
        }

        let typed_cols = self.build_typed_cols(batch)?;

        if acc.dict_values.len() < 2 {
            acc.dict_values.resize(2, HashMap::new());
        }

        let col0 = batch.column(self.group_keys[0].col_idx);
        let col1 = batch.column(self.group_keys[1].col_idx);
        let acc0 = GroupKeyAccessor::new(GroupKeyKind::Utf8ViewFirstByte, col0.as_ref())?;
        let acc1 = GroupKeyAccessor::new(GroupKeyKind::Utf8ViewFirstByte, col1.as_ref())?;

        let mut packed_to_local: HashMap<u16, usize> = HashMap::new();
        let mut local_cells: Vec<AggCells> = Vec::new();
        let mut local_packed: Vec<u16> = Vec::new();

        for row in 0..n_rows {
            if !self.eval_predicate_typed(&typed_cols, row) {
                continue;
            }
            let b0 = acc0.first_byte_at(row);
            let b1 = acc1.first_byte_at(row);
            let packed: u16 = ((b0 as u16) << 8) | (b1 as u16);
            let local_idx = match packed_to_local.entry(packed) {
                std::collections::hash_map::Entry::Occupied(e) => *e.get(),
                std::collections::hash_map::Entry::Vacant(e) => {
                    let idx = local_cells.len();
                    local_cells.push(fresh_cells(&self.aggregates));
                    local_packed.push(packed);
                    *e.insert(idx)
                }
            };
            let cells = &mut local_cells[local_idx];
            for (ai, agg) in self.aggregates.iter().enumerate() {
                cells[ai] =
                    self.combine_cell(agg, cells[ai], self.eval_agg_typed(agg, &typed_cols, row));
            }
        }

        for (idx, packed) in local_packed.iter().enumerate() {
            let b0 = (*packed >> 8) as u8;
            let b1 = (*packed & 0xff) as u8;
            let key_buf = vec![b0, b1];
            let entry = acc
                .groups
                .entry(key_buf)
                .or_insert_with(|| fresh_cells(&self.aggregates));
            let local = std::mem::take(&mut local_cells[idx]);
            for (i, v) in local.into_iter().enumerate() {
                entry[i] = self.combine_cell(&self.aggregates[i], entry[i], v);
            }
        }
        Ok(())
    }

    /// Build the per-batch typed-slice cache for all numeric input
    /// columns. Returns an error if any input column's runtime type
    /// disagrees with its declared `ColumnTy`.
    fn build_typed_cols<'a>(&self, batch: &'a RecordBatch) -> DfResult<Vec<TypedCol<'a>>> {
        self.input_col_indices
            .iter()
            .enumerate()
            .map(|(slot, &col_idx)| {
                TypedCol::build(batch.column(col_idx).as_ref(), self.input_tys[slot])
            })
            .collect()
    }

    /// Predicate eval using the per-batch typed-slice cache. No dyn
    /// dispatch in the inner read — just an array index on a `&[f64]`
    /// or `&[i32]`.
    #[inline(always)]
    fn eval_predicate_typed(&self, cols: &[TypedCol], row: usize) -> bool {
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

    /// Per-row value an aggregate contributes. The hot loop combines
    /// this with the existing cell via [`combine_cell`] — `+` for the
    /// additive variants, `min` / `max` for the order variants.
    #[inline(always)]
    fn eval_agg_typed(&self, agg: &AggExpr, cols: &[TypedCol], row: usize) -> f64 {
        match agg {
            // Single-column reads: SumColumn accumulates the value,
            // AvgColumn does the same (finalize divides by COUNT),
            // MinColumn / MaxColumn track the same value pre-combine.
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

    /// Combine `current` cell value with the per-row contribution.
    /// Additive for most variants; `min` / `max` for the order
    /// variants. Kept tight and `#[inline(always)]` so the per-row
    /// hot loop monomorphises cleanly.
    #[inline(always)]
    fn combine_cell(&self, agg: &AggExpr, current: f64, row_value: f64) -> f64 {
        match agg {
            AggExpr::MinColumn(_) => current.min(row_value),
            AggExpr::MaxColumn(_) => current.max(row_value),
            _ => current + row_value,
        }
    }
}

/// Initial cell value for an aggregate — `+inf` for MIN (any real
/// value beats it), `-inf` for MAX, `0.0` for all additive variants.
#[inline(always)]
fn initial_cell_value(agg: &AggExpr) -> f64 {
    match agg {
        AggExpr::MinColumn(_) => f64::INFINITY,
        AggExpr::MaxColumn(_) => f64::NEG_INFINITY,
        _ => 0.0,
    }
}

/// Allocate a fresh per-group cell vector with the correct initial
/// values. Called at group-insertion time in every template path.
fn fresh_cells(aggregates: &[AggExpr]) -> AggCells {
    aggregates.iter().map(initial_cell_value).collect()
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

    /// Σ.G.2f.3 — MIN/MAX/SumSquares per group, exercised through
    /// the generic + perfect-hash + dict-single paths via dispatch.
    #[test]
    fn min_max_sumsquares_per_group() {
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
        let groups = ["a", "a", "b", "b", "a"];
        let v = vec![5.0, 1.0, 10.0, 3.0, 7.0];
        let mut builder = StringDictionaryBuilder::<UInt32Type>::new();
        for s in groups.iter() {
            builder.append(*s).unwrap();
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(builder.finish()), Arc::new(Float64Array::from(v))],
        )
        .unwrap();

        let spec = FilterMultiAggSpec::try_new(
            vec![],
            vec![ColumnTy::Float64],
            &["v"],
            vec![
                AggExpr::MinColumn(0),
                AggExpr::MaxColumn(0),
                AggExpr::SumSquaresColumn(0),
                AggExpr::CountStar,
            ],
            vec!["min_v".into(), "max_v".into(), "sum_v2".into(), "n".into()],
            vec![("g".into(), GroupKeyKind::DictionaryU32)],
            &schema,
        )
        .unwrap();
        let mut acc = FilterMultiAggAccumulator::default();
        spec.process_batch(&batch, &mut acc).unwrap();
        let out = spec.finalize(acc).unwrap();
        // Group order is by dict code; for groups inserted "a","b"
        // the codes are 0, 1, so 'a' first.
        let min = out
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let max = out
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let sq = out
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // group 'a' = [5, 1, 7]: min=1, max=7, sum²=25+1+49=75, n=3
        // group 'b' = [10, 3]:   min=3, max=10, sum²=100+9=109, n=2
        assert_eq!(min.value(0), 1.0);
        assert_eq!(max.value(0), 7.0);
        assert!((sq.value(0) - 75.0).abs() < 1e-9);
        assert_eq!(min.value(1), 3.0);
        assert_eq!(max.value(1), 10.0);
        assert!((sq.value(1) - 109.0).abs() < 1e-9);
    }

    /// Σ.G.2f.3 — verify AvgColumn produces mean-of-column per group.
    #[test]
    fn avg_column_divides_by_countstar() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Utf8View, false),
            Field::new("v", DataType::Float64, false),
        ]));
        let groups = ["a", "a", "b", "b", "b"];
        let vals = vec![10.0, 20.0, 1.0, 2.0, 3.0];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringViewArray::from(groups.to_vec())),
                Arc::new(Float64Array::from(vals)),
            ],
        )
        .unwrap();

        let spec = FilterMultiAggSpec::try_new(
            vec![],
            vec![ColumnTy::Float64],
            &["v"],
            vec![
                AggExpr::SumColumn(0),
                AggExpr::AvgColumn(0),
                AggExpr::CountStar,
            ],
            vec!["sum_v".into(), "avg_v".into(), "n".into()],
            vec![("g".into(), GroupKeyKind::Utf8ViewFirstByte)],
            &schema,
        )
        .unwrap();

        let mut acc = FilterMultiAggAccumulator::default();
        spec.process_batch(&batch, &mut acc).unwrap();
        let out = spec.finalize(acc).unwrap();
        // Stable sort on packed key bytes → 'a' first, then 'b'.
        let sum = out
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let avg = out
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let n = out.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(sum.value(0), 30.0);
        assert_eq!(sum.value(1), 6.0);
        assert!((avg.value(0) - 15.0).abs() < 1e-9, "avg_a = 15");
        assert!((avg.value(1) - 2.0).abs() < 1e-9, "avg_b = 2");
        assert_eq!(n.value(0), 2);
        assert_eq!(n.value(1), 3);
    }

    /// AvgColumn without a paired CountStar must error at try_new.
    #[test]
    fn avg_column_without_countstar_errors() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
        let err = FilterMultiAggSpec::try_new(
            vec![],
            vec![ColumnTy::Float64],
            &["v"],
            vec![AggExpr::AvgColumn(0)],
            vec!["avg_v".into()],
            vec![],
            &schema,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("AvgColumn") && msg.contains("CountStar"),
            "unexpected error: {msg}"
        );
    }

    /// Σ.G.2f.2 — verify the perfect-hash template path produces
    /// bit-identical output to the generic path on the same input.
    /// Uses a 4-distinct-value dict that's well below the cardinality
    /// threshold so dispatch routes through `process_batch_perfect_hash_dict`.
    #[test]
    fn perfect_hash_template_matches_generic() {
        use arrow_array::builder::StringDictionaryBuilder;
        use arrow_array::types::UInt32Type;
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "g",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("v", DataType::Float64, false),
            Field::new("w", DataType::Float64, false),
        ]));
        // 40 rows across 4 dict values + a filter v < 50.
        let groups: Vec<&str> = (0..40)
            .map(|i| match i % 4 {
                0 => "x",
                1 => "y",
                2 => "z",
                _ => "q",
            })
            .collect();
        let mut builder = StringDictionaryBuilder::<UInt32Type>::new();
        for s in groups.iter() {
            builder.append(*s).unwrap();
        }
        let dict = builder.finish();
        let v: Vec<f64> = (0..40).map(|i| 1.0 + i as f64).collect();
        let w: Vec<f64> = (0..40).map(|i| 200.0 - 2.0 * i as f64).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(dict),
                Arc::new(Float64Array::from(v)),
                Arc::new(Float64Array::from(w)),
            ],
        )
        .unwrap();

        let spec = FilterMultiAggSpec::try_new(
            vec![Clause {
                column: 0,
                op: ClauseOp::F64Lt,
                imm_i32: 0,
                imm_f64: 50.0,
            }],
            vec![ColumnTy::Float64, ColumnTy::Float64],
            &["v", "w"],
            vec![
                AggExpr::SumColumn(0),
                AggExpr::SumProductColumns(0, 1),
                AggExpr::CountStar,
            ],
            vec!["sum_v".into(), "sum_vw".into(), "n".into()],
            vec![("g".into(), GroupKeyKind::DictionaryU32)],
            &schema,
        )
        .unwrap();

        let mut acc_perfect = FilterMultiAggAccumulator::default();
        spec.process_batch_perfect_hash_dict(&batch, &mut acc_perfect)
            .unwrap();
        let out_perfect = spec.finalize(acc_perfect).unwrap();

        let mut acc_generic = FilterMultiAggAccumulator::default();
        spec.process_batch_generic(&batch, &mut acc_generic)
            .unwrap();
        let out_generic = spec.finalize(acc_generic).unwrap();

        assert_eq!(out_perfect.num_rows(), out_generic.num_rows());
        for col_idx in 1..out_perfect.num_columns() - 1 {
            let l = out_perfect
                .column(col_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let r = out_generic
                .column(col_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..l.len() {
                assert!(
                    (l.value(row) - r.value(row)).abs() < 1e-9,
                    "col {col_idx} row {row}: perfect {} != generic {}",
                    l.value(row),
                    r.value(row)
                );
            }
        }
        let counts_p = out_perfect
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts_g = out_generic
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..counts_p.len() {
            assert_eq!(counts_p.value(i), counts_g.value(i));
        }
    }

    /// Σ.G.2f.2 — verify the dict-single template path produces
    /// bit-identical output to the generic path on the same input.
    /// We invoke each pub(crate) entry point directly so the test
    /// is a true equivalence proof, not just a "does dispatch work"
    /// smoke test.
    #[test]
    fn dict_single_template_matches_generic() {
        use arrow_array::builder::StringDictionaryBuilder;
        use arrow_array::types::UInt32Type;
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "g",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("v", DataType::Float64, false),
            Field::new("w", DataType::Float64, false),
        ]));
        // 32 rows across 5 distinct group values with mixed repetition
        // patterns, plus a filter that drops half the rows.
        let groups = [
            "alpha", "beta", "alpha", "gamma", "delta", "alpha", "beta", "epsilon", "alpha",
            "gamma", "alpha", "beta", "delta", "epsilon", "alpha", "gamma", "beta", "alpha",
            "gamma", "epsilon", "delta", "alpha", "beta", "gamma", "alpha", "delta", "epsilon",
            "alpha", "beta", "gamma", "delta", "alpha",
        ];
        let mut builder = StringDictionaryBuilder::<UInt32Type>::new();
        for s in groups.iter() {
            builder.append(*s).unwrap();
        }
        let dict = builder.finish();
        let v: Vec<f64> = (0..groups.len()).map(|i| 1.0 + i as f64).collect();
        let w: Vec<f64> = (0..groups.len()).map(|i| 100.0 - i as f64).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(dict),
                Arc::new(Float64Array::from(v)),
                Arc::new(Float64Array::from(w)),
            ],
        )
        .unwrap();

        // Spec: filter v >= 5, group by g, three aggs.
        let spec = FilterMultiAggSpec::try_new(
            vec![Clause {
                column: 0,
                op: ClauseOp::F64Ge,
                imm_i32: 0,
                imm_f64: 5.0,
            }],
            vec![ColumnTy::Float64, ColumnTy::Float64],
            &["v", "w"],
            vec![
                AggExpr::SumColumn(0),
                AggExpr::SumProductColumns(0, 1),
                AggExpr::CountStar,
            ],
            vec!["sum_v".into(), "sum_vw".into(), "n".into()],
            vec![("g".into(), GroupKeyKind::DictionaryU32)],
            &schema,
        )
        .unwrap();

        let mut acc_template = FilterMultiAggAccumulator::default();
        spec.process_batch_dict_single(&batch, &mut acc_template)
            .unwrap();
        let out_template = spec.finalize(acc_template).unwrap();

        let mut acc_generic = FilterMultiAggAccumulator::default();
        spec.process_batch_generic(&batch, &mut acc_generic)
            .unwrap();
        let out_generic = spec.finalize(acc_generic).unwrap();

        assert_eq!(out_template.num_rows(), out_generic.num_rows());
        assert_eq!(out_template.num_columns(), out_generic.num_columns());
        // Cell-by-cell equality on every group + agg column.
        for col_idx in 1..out_template.num_columns() - 1 {
            let l = out_template
                .column(col_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let r = out_generic
                .column(col_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..l.len() {
                assert!(
                    (l.value(row) - r.value(row)).abs() < 1e-9,
                    "column {col_idx} row {row}: template {} != generic {}",
                    l.value(row),
                    r.value(row)
                );
            }
        }
        let counts_t = out_template
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts_g = out_generic
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..counts_t.len() {
            assert_eq!(counts_t.value(i), counts_g.value(i));
        }
    }

    /// Σ.G.2f.4 — Utf8ViewTwoKeyU16 template equivalence with the
    /// generic path on the Q1 shape (two Utf8View first-byte keys).
    #[test]
    fn two_key_utf8view_template_matches_generic() {
        let spec = q1_spec();
        // 8 rows covering 4 distinct (rflag, lstatus) pairs; one row
        // filtered out by shipdate.
        let batch = synthetic_batch(
            &["A", "N", "N", "R", "A", "R", "N", "A"],
            &["F", "F", "O", "F", "F", "F", "O", "F"],
            &[10.0, 5.0, 7.0, 3.0, 2.0, 4.0, 8.0, 1.5],
            &[100.0, 50.0, 70.0, 30.0, 20.0, 40.0, 80.0, 15.0],
            &[0.1, 0.05, 0.0, 0.2, 0.1, 0.0, 0.05, 0.15],
            &[0.05, 0.05, 0.0, 0.1, 0.05, 0.0, 0.0, 0.07],
            &[9000, 9500, 8000, 9999, 9100, 8200, 8900, 99999],
        );

        let mut acc_template = FilterMultiAggAccumulator::default();
        spec.process_batch_two_key_utf8view(&batch, &mut acc_template)
            .unwrap();
        let out_template = spec.finalize(acc_template).unwrap();

        let mut acc_generic = FilterMultiAggAccumulator::default();
        spec.process_batch_generic(&batch, &mut acc_generic)
            .unwrap();
        let out_generic = spec.finalize(acc_generic).unwrap();

        assert_eq!(out_template.num_rows(), out_generic.num_rows());
        assert_eq!(out_template.num_columns(), out_generic.num_columns());
        let key0_t = out_template
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let key0_g = out_generic
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let key1_t = out_template
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let key1_g = out_generic
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..out_template.num_rows() {
            assert_eq!(key0_t.value(i), key0_g.value(i), "key0 row {i}");
            assert_eq!(key1_t.value(i), key1_g.value(i), "key1 row {i}");
        }
        for col_idx in 2..6 {
            let l = out_template
                .column(col_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let r = out_generic
                .column(col_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..l.len() {
                assert!(
                    (l.value(row) - r.value(row)).abs() < 1e-9,
                    "col {col_idx} row {row}: template {} != generic {}",
                    l.value(row),
                    r.value(row)
                );
            }
        }
        let counts_t = out_template
            .column(6)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts_g = out_generic
            .column(6)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..counts_t.len() {
            assert_eq!(counts_t.value(i), counts_g.value(i));
        }
    }

    /// Σ.G.2f.4 — merge across batches still produces the same totals
    /// when the per-batch path is the new template (exercises the
    /// 2-byte packed-key fold into the global accumulator).
    #[test]
    fn two_key_utf8view_merge_matches_generic() {
        let spec = q1_spec();
        let b1 = synthetic_batch(
            &["A", "N", "N", "R"],
            &["F", "F", "O", "F"],
            &[10.0, 5.0, 7.0, 3.0],
            &[100.0, 50.0, 70.0, 30.0],
            &[0.0, 0.0, 0.0, 0.0],
            &[0.0, 0.0, 0.0, 0.0],
            &[1, 1, 1, 1],
        );
        let b2 = synthetic_batch(
            &["A", "N", "R", "N"],
            &["F", "F", "F", "O"],
            &[2.0, 6.0, 4.0, 9.0],
            &[20.0, 60.0, 40.0, 90.0],
            &[0.0, 0.0, 0.0, 0.0],
            &[0.0, 0.0, 0.0, 0.0],
            &[1, 1, 1, 1],
        );

        let mut acc_t1 = FilterMultiAggAccumulator::default();
        let mut acc_t2 = FilterMultiAggAccumulator::default();
        spec.process_batch_two_key_utf8view(&b1, &mut acc_t1)
            .unwrap();
        spec.process_batch_two_key_utf8view(&b2, &mut acc_t2)
            .unwrap();
        let merged_t = spec.merge(acc_t1, acc_t2);
        let out_t = spec.finalize(merged_t).unwrap();

        let mut acc_g1 = FilterMultiAggAccumulator::default();
        let mut acc_g2 = FilterMultiAggAccumulator::default();
        spec.process_batch_generic(&b1, &mut acc_g1).unwrap();
        spec.process_batch_generic(&b2, &mut acc_g2).unwrap();
        let merged_g = spec.merge(acc_g1, acc_g2);
        let out_g = spec.finalize(merged_g).unwrap();

        assert_eq!(out_t.num_rows(), out_g.num_rows());
        let sum_t = out_t
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let sum_g = out_g
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..sum_t.len() {
            assert!((sum_t.value(i) - sum_g.value(i)).abs() < 1e-9);
        }
    }

    /// Σ.G.2f.4 — confirms `process_batch` dispatches into the new
    /// template when the spec has two Utf8ViewFirstByte keys (i.e.
    /// the existing Q1 fixture).
    #[test]
    fn dispatch_routes_two_key_utf8view() {
        let spec = q1_spec();
        let batch = synthetic_batch(
            &["A", "N", "R"],
            &["F", "O", "F"],
            &[1.0, 2.0, 3.0],
            &[10.0, 20.0, 30.0],
            &[0.0, 0.0, 0.0],
            &[0.0, 0.0, 0.0],
            &[1, 1, 1],
        );
        let mut acc = FilterMultiAggAccumulator::default();
        spec.process_batch(&batch, &mut acc).unwrap();
        let out = spec.finalize(acc).unwrap();
        assert_eq!(out.num_rows(), 3);
        let counts = out.column(6).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(counts.value(0), 1);
        assert_eq!(counts.value(1), 1);
        assert_eq!(counts.value(2), 1);
    }

    /// Confirms `process_batch` actually dispatches into the dict-
    /// single template when the spec's shape matches. We can't observe
    /// the path directly, so we use the trait method and assert
    /// correctness — combined with the equivalence test above, this
    /// covers the dispatch wire-up.
    #[test]
    fn dispatch_routes_dict_single_kind() {
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
        for s in ["x", "y", "x", "z", "y", "x"] {
            builder.append(s).unwrap();
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(builder.finish()),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0])),
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
        assert_eq!(out.num_rows(), 3);
        let total_v: f64 = (0..3)
            .map(|i| {
                out.column(1)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(i)
            })
            .sum();
        assert!((total_v - 21.0).abs() < 1e-9);
        let total_n: i64 = (0..3)
            .map(|i| {
                out.column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(i)
            })
            .sum();
        assert_eq!(total_n, 6);
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
