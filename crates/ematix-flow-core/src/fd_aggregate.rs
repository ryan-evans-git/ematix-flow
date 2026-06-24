//! FD-aware `SUM(Float64) GROUP BY <composite key + FD-payload>` kernel — the Q10
//! SF=100 lever.
//!
//! ## Why this exists
//!
//! Q10's customer aggregate groups by 7 columns (`c_custkey` + 5 wide strings +
//! `n_name`). DataFusion encodes all 7 into a comparable row-format group key for
//! every one of ~11.5M input rows — measured at **4.86 CPU-s** of
//! `time_calculating_group_ids` (98% of the aggregate), vs DuckDB's 1.65 CPU-s on
//! the identical grouping.
//!
//! But the 5 wide customer columns are *functionally determined* by `c_custkey`
//! (the customer PK), so the group identity is fully decided by the **FD-minimal
//! key** — `{c_custkey, n_name}` for Q10 (`n_name` is included because the FD
//! machinery cannot prove `c_custkey -> n_name` transitively across the join, even
//! though it holds). This kernel row-encodes **only that narrow key** (cheap), and
//! carries the wide group columns by recording the **first-occurrence row** of each
//! group, gathering them once at finalize via Arrow `interleave`.
//!
//! A Phase-0 microbench measured a **1.76× wall win** (correct, eff 10.4) over
//! DataFusion's 7-column group-by on the composite `{c_custkey, n_name}` key.
//! (See `q10_agg_kernel_bench`.)
//!
//! ## Design
//!
//! The grouping core mirrors DataFusion's own `GroupValuesRows`: an arrow
//! [`RowConverter`] turns the key columns into a byte-comparable [`Rows`] buffer, a
//! raw [`HashTable`] maps `(row_hash, group_idx)` (storing only hashes, never the
//! keys), and hash collisions are resolved by an exact [`Row`] equality check
//! against the stored group representative. This is collision-free and type-general
//! (any combination of key column types), with **no probabilistic assumption** — the
//! correctness rests on `RowConverter`, not on a hash.
//!
//! ## CORRECTNESS CONTRACT
//!
//! This groups by the `key_cols` subset ALONE and carries every other group column
//! by first-occurrence. The result is correct **only** when those carried columns
//! are functionally determined by `key_cols`. The optimizer rule that constructs the
//! operator (`fd_aggregate_rule`) is responsible for proving that functional
//! dependency before swapping the operator in; this kernel never assumes it on its
//! own. Null key values are supported (`RowConverter` encodes them as a distinct,
//! consistent group, matching SQL `GROUP BY` semantics); `SUM` of an all-null group
//! yields `NULL`.

use ahash::RandomState;
use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Float64Builder};
use datafusion::arrow::compute::interleave;
use datafusion::arrow::row::{RowConverter, Rows, SortField};
use datafusion::common::Result as DfResult;
use datafusion::error::DataFusionError;
use hashbrown::HashTable;

/// Stateful accumulator for `SUM(f64) GROUP BY <composite key>`, carrying the
/// (FD-determined) output group columns by first-occurrence. Ingest a partition's
/// batches one at a time, then `finalize`.
pub struct FdSumAccumulator {
    /// Row converter for the FD-minimal key columns. Built lazily from the first
    /// batch's key column types (stable across batches), and reused so every
    /// batch's rows are mutually comparable.
    converter: Option<RowConverter>,
    /// One representative key row per group (`group_reps.row(gid)`), used for the
    /// exact equality check on a hash hit. Holds only the narrow key, not the
    /// wide carried columns.
    group_reps: Option<Rows>,
    /// `(row_hash, group_id)` — the raw open-addressing table. Stores hashes only,
    /// never the key bytes (those live in `group_reps`). Mirrors
    /// `GroupValuesRows`.
    map: HashTable<(u64, u32)>,
    /// Fixed-seed hasher so grouping is deterministic across runs.
    random_state: RandomState,
    /// Per-group running sum (valid iff `seen_non_null[gid]`).
    sums: Vec<f64>,
    /// Whether a group has seen ≥1 non-null sum input (SQL `SUM` of an all-null
    /// group is `NULL`, not 0).
    seen_non_null: Vec<bool>,
    /// `(batch_idx, row_idx)` of each group's first occurrence, for the gather.
    first: Vec<(usize, usize)>,
    /// Retained output group columns per ingested (non-empty) batch (Arc clones —
    /// cheap). Indexed `[batch_idx][group_col]`. Alive until `finalize`'s gather.
    group_batches: Vec<Vec<ArrayRef>>,
    /// Number of output group columns (fixed across batches). `None` until first
    /// non-empty ingest.
    n_group_cols: Option<usize>,
}

impl Default for FdSumAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl FdSumAccumulator {
    pub fn new() -> Self {
        Self {
            converter: None,
            group_reps: None,
            map: HashTable::new(),
            // Fixed seeds: grouping correctness is seed-independent, but a fixed
            // seed keeps group-insertion order deterministic for a given input.
            random_state: RandomState::with_seeds(0, 0, 0, 0),
            sums: Vec::new(),
            seen_non_null: Vec::new(),
            first: Vec::new(),
            group_batches: Vec::new(),
            n_group_cols: None,
        }
    }

    /// Number of distinct groups accumulated so far.
    pub fn num_groups(&self) -> usize {
        self.sums.len()
    }

    /// Ingest one batch: the FD-minimal `key_cols` (≥1 column, any types), the f64
    /// `sum_input`, and the full set of output `group_cols` (in output order),
    /// carried by first-occurrence. All arrays must have the same length; the
    /// `key_cols` and `group_cols` column counts must be stable across batches.
    pub fn ingest(
        &mut self,
        key_cols: &[ArrayRef],
        sum_input: &Float64Array,
        group_cols: Vec<ArrayRef>,
    ) -> DfResult<()> {
        let n = sum_input.len();
        if key_cols.iter().any(|c| c.len() != n) || group_cols.iter().any(|c| c.len() != n) {
            return Err(DataFusionError::Internal(
                "FdSumAccumulator::ingest length mismatch".into(),
            ));
        }
        if key_cols.is_empty() {
            return Err(DataFusionError::Internal(
                "FdSumAccumulator::ingest needs ≥1 key column".into(),
            ));
        }
        match self.n_group_cols {
            Some(c) if c != group_cols.len() => {
                return Err(DataFusionError::Internal(
                    "FdSumAccumulator::ingest group column count changed".into(),
                ));
            }
            None => self.n_group_cols = Some(group_cols.len()),
            _ => {}
        }
        // Empty batch: nothing to group, and we must NOT advance the batch index
        // (finalize's gather indexes `group_batches` by batch position).
        if n == 0 {
            return Ok(());
        }

        // Lazily build the converter from the key columns' types (stable schema).
        if self.converter.is_none() {
            let fields = key_cols
                .iter()
                .map(|c| SortField::new(c.data_type().clone()))
                .collect::<Vec<_>>();
            let conv = RowConverter::new(fields)?;
            self.group_reps = Some(conv.empty_rows(0, 0));
            self.converter = Some(conv);
        }
        // Row-encode ONLY the narrow FD-minimal key — never the wide carried cols.
        let key_rows = self.converter.as_ref().unwrap().convert_columns(key_cols)?;

        let batch_idx = self.group_batches.len();
        self.group_batches.push(group_cols);

        // Take `group_reps` out so the find-closure can borrow it immutably while we
        // mutate `self.map`/`self.sums` (the GroupValuesRows take/replace dance).
        let mut group_reps = self.group_reps.take().unwrap();

        for ri in 0..n {
            let key_row = key_rows.row(ri);
            let h = self.random_state.hash_one(key_row);
            let found = self
                .map
                .find(h, |&(eh, gid)| {
                    eh == h && group_reps.row(gid as usize) == key_row
                })
                .map(|&(_, gid)| gid);
            let gid = match found {
                Some(g) => g,
                None => {
                    let g = group_reps.num_rows() as u32;
                    group_reps.push(key_row);
                    self.map.insert_unique(h, (h, g), |&(eh, _)| eh);
                    self.sums.push(0.0);
                    self.seen_non_null.push(false);
                    self.first.push((batch_idx, ri));
                    g
                }
            };
            if sum_input.is_valid(ri) {
                self.sums[gid as usize] += sum_input.value(ri);
                self.seen_non_null[gid as usize] = true;
            }
        }

        self.group_reps = Some(group_reps);
        Ok(())
    }

    /// Produce the grouped output: the carried group columns (each gathered at the
    /// per-group first-occurrence rows, in the order passed to `ingest`) and the
    /// `SUM` column (`NULL` for all-null groups). Output row order is
    /// group-insertion order (deterministic for a given input order).
    pub fn finalize(self) -> DfResult<(Vec<ArrayRef>, ArrayRef)> {
        let n_groups = self.sums.len();

        // SUM: null for all-null groups.
        let sum_arr: ArrayRef = {
            let mut b = Float64Builder::with_capacity(n_groups);
            for gid in 0..n_groups {
                if self.seen_non_null[gid] {
                    b.append_value(self.sums[gid]);
                } else {
                    b.append_null();
                }
            }
            std::sync::Arc::new(b.finish())
        };

        let n_group_cols = self.n_group_cols.unwrap_or(0);
        if n_group_cols == 0 || self.group_batches.is_empty() {
            return Ok((Vec::new(), sum_arr));
        }

        // Gather each output group column across batches at the first-occurrence
        // `(batch_idx, row_idx)` pairs.
        let mut group_out = Vec::with_capacity(n_group_cols);
        for col in 0..n_group_cols {
            let values: Vec<&dyn Array> =
                self.group_batches.iter().map(|b| b[col].as_ref()).collect();
            group_out.push(interleave(&values, &self.first)?);
        }
        Ok((group_out, sum_arr))
    }
}

#[cfg(test)]
mod tests {
    // `acc.ingest(&[keys.clone()], …, vec![keys, …])` clones `keys` for the
    // 1-element slice because `keys` is ALSO moved in the same call — a borrow
    // (the `from_ref` the lint suggests) would conflict with that move.
    #![allow(clippy::cloned_ref_to_slice_refs)]
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
    use std::sync::Arc;

    fn i64a(v: Vec<Option<i64>>) -> ArrayRef {
        Arc::new(Int64Array::from(v))
    }
    fn f64a(v: Vec<Option<f64>>) -> Float64Array {
        Float64Array::from(v)
    }
    fn stra(v: Vec<&str>) -> ArrayRef {
        Arc::new(StringArray::from(v))
    }

    /// Render one group-output row as a comparable `(key-strings, sum)` tuple.
    fn render(group_cols: &[ArrayRef], sum: &ArrayRef) -> Vec<(Vec<String>, Option<f64>)> {
        let sa = sum.as_any().downcast_ref::<Float64Array>().unwrap();
        let n = sa.len();
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let mut key = Vec::with_capacity(group_cols.len());
            for c in group_cols {
                if let Some(a) = c.as_any().downcast_ref::<Int64Array>() {
                    key.push(if a.is_null(i) {
                        "∅".to_string()
                    } else {
                        a.value(i).to_string()
                    });
                } else if let Some(a) = c.as_any().downcast_ref::<StringArray>() {
                    key.push(if a.is_null(i) {
                        "∅".to_string()
                    } else {
                        a.value(i).to_string()
                    });
                } else if let Some(a) = c.as_any().downcast_ref::<Float64Array>() {
                    key.push(if a.is_null(i) {
                        "∅".to_string()
                    } else {
                        format!("{:.4}", a.value(i))
                    });
                } else {
                    panic!("unhandled group col type in test renderer");
                }
            }
            let s = if sa.is_null(i) {
                None
            } else {
                Some(sa.value(i))
            };
            out.push((key, s));
        }
        // Sort by the rendered group-key columns (f64 sums aren't Ord); keys are
        // unique per group so this is a total order for comparison.
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// THE correctness property of the composite key: two rows that share the i64
    /// lead column but DIFFER in a residual key column must form DISTINCT groups
    /// (the kernel must not collapse them on the i64 alone). If this ever regresses,
    /// the FD operator would silently merge customers across nations on Q10.
    #[test]
    fn composite_key_same_lead_distinct_residual() {
        let mut acc = FdSumAccumulator::new();
        // custkey 1 appears with nations "A" and "B" — these are two groups.
        let key_lead = i64a(vec![Some(1), Some(1), Some(2)]);
        let key_res = stra(vec!["A", "B", "A"]);
        let sums = f64a(vec![Some(10.0), Some(100.0), Some(5.0)]);
        acc.ingest(
            &[key_lead.clone(), key_res.clone()],
            &sums,
            vec![key_lead, key_res],
        )
        .unwrap();
        assert_eq!(
            acc.num_groups(),
            3,
            "1/A, 1/B, 2/A are three distinct groups"
        );
        let (g, s) = acc.finalize().unwrap();
        let got = render(&g, &s);
        assert_eq!(
            got,
            vec![
                (vec!["1".into(), "A".into()], Some(10.0)),
                (vec!["1".into(), "B".into()], Some(100.0)),
                (vec!["2".into(), "A".into()], Some(5.0)),
            ]
        );
    }

    /// Composite grouping with an FD-determined payload carried by first-occurrence,
    /// across two batches (exercises the cross-batch interleave gather).
    #[test]
    fn composite_payload_first_occurrence_multi_batch() {
        // group key = (custkey, nation); payload = name, FD-determined by custkey.
        // Output cols order: [custkey, nation, name]. Sum over the f64.
        let mut acc = FdSumAccumulator::new();
        // batch 1
        let k1 = i64a(vec![Some(1), Some(2), Some(1)]);
        let nat1 = stra(vec!["US", "FR", "US"]);
        let nm1 = stra(vec!["alice", "bob", "alice"]);
        acc.ingest(
            &[k1.clone(), nat1.clone()],
            &f64a(vec![Some(1.0), Some(2.0), Some(3.0)]),
            vec![k1, nat1, nm1],
        )
        .unwrap();
        // batch 2: custkey 1 again (US/alice), plus a new custkey 3.
        let k2 = i64a(vec![Some(3), Some(1)]);
        let nat2 = stra(vec!["FR", "US"]);
        let nm2 = stra(vec!["carol", "alice"]);
        acc.ingest(
            &[k2.clone(), nat2.clone()],
            &f64a(vec![Some(7.0), Some(0.5)]),
            vec![k2, nat2, nm2],
        )
        .unwrap();

        let (g, s) = acc.finalize().unwrap();
        let got = render(&g, &s);
        assert_eq!(
            got,
            vec![
                (vec!["1".into(), "US".into(), "alice".into()], Some(4.5)), // 1+3+0.5
                (vec!["2".into(), "FR".into(), "bob".into()], Some(2.0)),
                (vec!["3".into(), "FR".into(), "carol".into()], Some(7.0)),
            ]
        );
    }

    /// Single-column key (the degenerate composite) still matches a plain group-by.
    #[test]
    fn single_col_key_matches_reference() {
        let mut acc = FdSumAccumulator::new();
        let keys = i64a(vec![Some(1), Some(2), Some(1), Some(3), Some(2), Some(1)]);
        let payload = stra(vec!["p1", "p2", "p1", "p3", "p2", "p1"]);
        let sums = f64a(vec![
            Some(10.0),
            Some(20.0),
            Some(1.0),
            Some(5.0),
            Some(2.0),
            Some(0.5),
        ]);
        acc.ingest(&[keys.clone()], &sums, vec![keys, payload])
            .unwrap();
        let (g, s) = acc.finalize().unwrap();
        let got = render(&g, &s);
        assert_eq!(
            got,
            vec![
                (vec!["1".into(), "p1".into()], Some(11.5)),
                (vec!["2".into(), "p2".into()], Some(22.0)),
                (vec!["3".into(), "p3".into()], Some(5.0)),
            ]
        );
    }

    /// A null in the key column forms its own group (SQL `GROUP BY` semantics).
    #[test]
    fn null_key_group() {
        let mut acc = FdSumAccumulator::new();
        let keys = i64a(vec![Some(1), None, Some(1), None]);
        let payload = stra(vec!["p1", "pn", "p1", "pn"]);
        let sums = f64a(vec![Some(10.0), Some(3.0), Some(5.0), Some(7.0)]);
        acc.ingest(&[keys.clone()], &sums, vec![keys, payload])
            .unwrap();
        let (g, s) = acc.finalize().unwrap();
        let got = render(&g, &s);
        assert_eq!(
            got,
            vec![
                (vec!["1".into(), "p1".into()], Some(15.0)),
                (vec!["∅".into(), "pn".into()], Some(10.0)),
            ]
        );
    }

    /// A group whose every SUM input is null must emit `NULL`, not `0.0`.
    #[test]
    fn all_null_sum_group_is_null() {
        let mut acc = FdSumAccumulator::new();
        let keys = i64a(vec![Some(1), Some(1), Some(2)]);
        let payload = stra(vec!["p1", "p1", "p2"]);
        let sums = f64a(vec![None, None, Some(4.0)]);
        acc.ingest(&[keys.clone()], &sums, vec![keys, payload])
            .unwrap();
        let (g, s) = acc.finalize().unwrap();
        let got = render(&g, &s);
        assert_eq!(
            got,
            vec![
                (vec!["1".into(), "p1".into()], None),
                (vec!["2".into(), "p2".into()], Some(4.0)),
            ]
        );
    }

    #[test]
    fn length_mismatch_errors() {
        let mut acc = FdSumAccumulator::new();
        let r = acc.ingest(
            &[i64a(vec![Some(1), Some(2)])],
            &f64a(vec![Some(1.0)]),
            vec![stra(vec!["a", "b"])],
        );
        assert!(r.is_err(), "mismatched lengths must error");
    }

    #[test]
    fn group_col_count_change_errors() {
        let mut acc = FdSumAccumulator::new();
        acc.ingest(
            &[i64a(vec![Some(1)])],
            &f64a(vec![Some(1.0)]),
            vec![i64a(vec![Some(1)]), stra(vec!["a"])],
        )
        .unwrap();
        let r = acc.ingest(
            &[i64a(vec![Some(2)])],
            &f64a(vec![Some(1.0)]),
            vec![i64a(vec![Some(2)])], // only 1 group col now
        );
        assert!(r.is_err(), "changing group-col count must error");
    }
}
