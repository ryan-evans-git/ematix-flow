//! FD-aware `SUM(Float64) GROUP BY <i64 key>` kernel — the Q10 SF=100 lever.
//!
//! ## Why this exists
//!
//! Q10's customer aggregate groups by 7 columns (`c_custkey` + 5 wide strings +
//! `n_name`). DataFusion encodes all 7 into a comparable row-format group key for
//! every one of ~11.5M input rows — measured at **4.86 CPU-s** of
//! `time_calculating_group_ids` (98% of the aggregate), vs DuckDB's 1.65 CPU-s on
//! the identical grouping. But the 6 non-key columns are *functionally determined*
//! by `c_custkey` (the customer PK), so the group identity is fully decided by the
//! i64 key alone.
//!
//! This kernel hashes **only the i64 key** (cheap), and carries the payload columns
//! by recording the **first-occurrence row** of each group, gathering them once at
//! finalize via Arrow `interleave`. A Phase-0 microbench measured a **2.35× wall
//! win** (correct, eff 10.8) over DataFusion's 7-column group-by when partitioned by
//! key. (See `q10_agg_kernel_bench`.)
//!
//! ## CORRECTNESS CONTRACT
//!
//! This groups by `key` ALONE. The result is correct **only** when the payload
//! columns are functionally determined by `key`. The optimizer rule that constructs
//! the operator (`fd_aggregate_rule`) is responsible for proving that functional
//! dependency before swapping the operator in; this kernel never assumes it on its
//! own. Null keys are supported (one "null group", matching SQL `GROUP BY`
//! semantics); `SUM` of an all-null group yields `NULL`.

use std::collections::HashMap;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Float64Builder, Int64Array};
use datafusion::arrow::compute::interleave;
use datafusion::common::Result as DfResult;
use datafusion::error::DataFusionError;

/// Stateful accumulator for `SUM(f64) GROUP BY i64-key`, carrying payload columns
/// by first-occurrence. Ingest a partition's batches one at a time, then `finalize`.
#[derive(Default)]
pub struct FdSumAccumulator {
    /// i64 key -> dense group id.
    map: HashMap<i64, u32>,
    /// Dense group id of the (single) null-key group, if any rows had a null key.
    null_gid: Option<u32>,
    /// Per-group running sum (valid iff `seen_non_null[gid]`).
    sums: Vec<f64>,
    /// Whether a group has seen at least one non-null sum input (SQL `SUM` of an
    /// all-null group is `NULL`, not 0).
    seen_non_null: Vec<bool>,
    /// Per-group representative key (`keys[gid]`); ignored for the null group.
    keys: Vec<i64>,
    /// `(batch_idx, row_idx)` of each group's first occurrence, for the payload gather.
    first: Vec<(usize, usize)>,
    /// Retained payload columns per ingested batch (Arc clones — cheap). Indexed
    /// `[batch_idx][payload_col]`. Needed alive until `finalize`'s interleave gather.
    payload_batches: Vec<Vec<ArrayRef>>,
    /// Number of payload columns (fixed across batches). `None` until first ingest.
    n_payload: Option<usize>,
}

impl FdSumAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of distinct groups accumulated so far.
    pub fn num_groups(&self) -> usize {
        self.sums.len()
    }

    /// Ingest one batch's worth of columns: the i64 `key`, the f64 `sum_input`, and
    /// the `payload` columns (the other GROUP BY columns, carried by first-occurrence).
    /// All three must have the same length; `payload` must have a stable column count.
    pub fn ingest(
        &mut self,
        key: &Int64Array,
        sum_input: &Float64Array,
        payload: Vec<ArrayRef>,
    ) -> DfResult<()> {
        let n = key.len();
        if sum_input.len() != n || payload.iter().any(|p| p.len() != n) {
            return Err(DataFusionError::Internal(
                "FdSumAccumulator::ingest length mismatch".into(),
            ));
        }
        match self.n_payload {
            Some(p) if p != payload.len() => {
                return Err(DataFusionError::Internal(
                    "FdSumAccumulator::ingest payload column count changed".into(),
                ));
            }
            None => self.n_payload = Some(payload.len()),
            _ => {}
        }
        let batch_idx = self.payload_batches.len();
        self.payload_batches.push(payload);

        for row in 0..n {
            let gid = if key.is_null(row) {
                match self.null_gid {
                    Some(g) => g,
                    None => {
                        let g = self.new_group(0, true, batch_idx, row);
                        self.null_gid = Some(g);
                        g
                    }
                }
            } else {
                let k = key.value(row);
                match self.map.get(&k) {
                    Some(&g) => g,
                    None => {
                        let g = self.new_group(k, false, batch_idx, row);
                        self.map.insert(k, g);
                        g
                    }
                }
            };
            if sum_input.is_valid(row) {
                self.sums[gid as usize] += sum_input.value(row);
                self.seen_non_null[gid as usize] = true;
            }
        }
        Ok(())
    }

    #[inline]
    fn new_group(&mut self, key: i64, _is_null: bool, batch_idx: usize, row: usize) -> u32 {
        let gid = self.sums.len() as u32;
        self.sums.push(0.0);
        self.seen_non_null.push(false);
        self.keys.push(key);
        self.first.push((batch_idx, row));
        gid
    }

    /// Produce the grouped output: the key column (with the null group's key null),
    /// the `SUM` column (null for all-null groups), and each payload column gathered
    /// at the per-group first-occurrence rows. Output row order is group-insertion
    /// order (deterministic for a given input order).
    pub fn finalize(self) -> DfResult<(ArrayRef, ArrayRef, Vec<ArrayRef>)> {
        let n_groups = self.sums.len();
        let n_payload = self.n_payload.unwrap_or(0);

        // Keys: Int64 with a null at the null group.
        let key_arr: ArrayRef = {
            let mut b = datafusion::arrow::array::Int64Builder::with_capacity(n_groups);
            for gid in 0..n_groups {
                if Some(gid as u32) == self.null_gid {
                    b.append_null();
                } else {
                    b.append_value(self.keys[gid]);
                }
            }
            std::sync::Arc::new(b.finish())
        };

        // Sums: null for all-null groups.
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

        // Payload: interleave each column across batches at the first-occurrence
        // `(batch_idx, row_idx)` pairs.
        let mut payload_out = Vec::with_capacity(n_payload);
        for col in 0..n_payload {
            let values: Vec<&dyn Array> = self
                .payload_batches
                .iter()
                .map(|b| b[col].as_ref())
                .collect();
            let gathered = interleave(&values, &self.first)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            payload_out.push(gathered);
        }

        Ok((key_arr, sum_arr, payload_out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn i64a(v: Vec<Option<i64>>) -> Int64Array {
        Int64Array::from(v)
    }
    fn f64a(v: Vec<Option<f64>>) -> Float64Array {
        Float64Array::from(v)
    }
    fn stra(v: Vec<&str>) -> ArrayRef {
        Arc::new(StringArray::from(v))
    }

    /// Reference: group by key, sum, first payload string — the ground truth the
    /// kernel must match (when the FD holds, which the test data respects).
    fn reference(
        keys: &[Option<i64>],
        sums: &[Option<f64>],
        payload: &[&str],
    ) -> BTreeMap<Option<i64>, (Option<f64>, String)> {
        let mut m: BTreeMap<Option<i64>, (Option<f64>, String)> = BTreeMap::new();
        for i in 0..keys.len() {
            let e = m.entry(keys[i]).or_insert((None, payload[i].to_string()));
            if let Some(s) = sums[i] {
                e.0 = Some(e.0.unwrap_or(0.0) + s);
            }
        }
        m
    }

    fn run(
        keys: Vec<Option<i64>>,
        sums: Vec<Option<f64>>,
        payload: Vec<&str>,
    ) -> BTreeMap<Option<i64>, (Option<f64>, String)> {
        // Split into two batches to exercise multi-batch + interleave gather.
        let mid = keys.len() / 2;
        let mut acc = FdSumAccumulator::new();
        acc.ingest(
            &i64a(keys[..mid].to_vec()),
            &f64a(sums[..mid].to_vec()),
            vec![stra(payload[..mid].to_vec())],
        )
        .unwrap();
        acc.ingest(
            &i64a(keys[mid..].to_vec()),
            &f64a(sums[mid..].to_vec()),
            vec![stra(payload[mid..].to_vec())],
        )
        .unwrap();
        let (k, s, p) = acc.finalize().unwrap();
        let ka = k.as_any().downcast_ref::<Int64Array>().unwrap();
        let sa = s.as_any().downcast_ref::<Float64Array>().unwrap();
        let pa = p[0].as_any().downcast_ref::<StringArray>().unwrap();
        let mut out: BTreeMap<Option<i64>, (Option<f64>, String)> = BTreeMap::new();
        for i in 0..ka.len() {
            let key = if ka.is_null(i) { None } else { Some(ka.value(i)) };
            let sum = if sa.is_null(i) { None } else { Some(sa.value(i)) };
            out.insert(key, (sum, pa.value(i).to_string()));
        }
        out
    }

    #[test]
    fn matches_reference_simple() {
        // FD-consistent payload: key k always carries "p{k}".
        let keys = vec![Some(1), Some(2), Some(1), Some(3), Some(2), Some(1)];
        let sums = vec![
            Some(10.0),
            Some(20.0),
            Some(1.0),
            Some(5.0),
            Some(2.0),
            Some(0.5),
        ];
        let payload = vec!["p1", "p2", "p1", "p3", "p2", "p1"];
        let got = run(keys.clone(), sums.clone(), payload.clone());
        let want = reference(&keys, &sums, &payload);
        assert_eq!(got.len(), want.len(), "group count");
        for (k, (ws, _wp)) in &want {
            let (gs, _gp) = got.get(k).expect("missing group");
            assert_eq!(gs, ws, "sum for key {k:?}");
        }
        // Spot-check the payload (first-occurrence — FD-consistent so deterministic).
        assert_eq!(got[&Some(1)].1, "p1");
        assert_eq!(got[&Some(2)].1, "p2");
        assert_eq!(got[&Some(3)].1, "p3");
    }

    #[test]
    fn null_key_group() {
        let keys = vec![Some(1), None, Some(1), None];
        let sums = vec![Some(10.0), Some(3.0), Some(5.0), Some(7.0)];
        let payload = vec!["p1", "pn", "p1", "pn"];
        let got = run(keys, sums, payload);
        assert_eq!(got.len(), 2);
        assert_eq!(got[&Some(1)].0, Some(15.0));
        assert_eq!(got[&None].0, Some(10.0));
        assert_eq!(got[&None].1, "pn");
    }

    #[test]
    fn all_null_sum_group_is_null() {
        // A group whose every SUM input is null must emit NULL, not 0.0.
        let keys = vec![Some(1), Some(1), Some(2)];
        let sums = vec![None, None, Some(4.0)];
        let payload = vec!["p1", "p1", "p2"];
        let got = run(keys, sums, payload);
        assert_eq!(got[&Some(1)].0, None, "all-null SUM group => NULL");
        assert_eq!(got[&Some(2)].0, Some(4.0));
    }

    #[test]
    fn single_batch_and_empty() {
        let mut acc = FdSumAccumulator::new();
        acc.ingest(
            &i64a(vec![Some(7), Some(7)]),
            &f64a(vec![Some(1.5), Some(2.5)]),
            vec![stra(vec!["a", "a"])],
        )
        .unwrap();
        assert_eq!(acc.num_groups(), 1);
        let (k, s, p) = acc.finalize().unwrap();
        assert_eq!(k.len(), 1);
        assert_eq!(s.as_any().downcast_ref::<Float64Array>().unwrap().value(0), 4.0);
        assert_eq!(
            p[0].as_any().downcast_ref::<StringArray>().unwrap().value(0),
            "a"
        );
    }

    #[test]
    fn length_mismatch_errors() {
        let mut acc = FdSumAccumulator::new();
        let r = acc.ingest(
            &i64a(vec![Some(1), Some(2)]),
            &f64a(vec![Some(1.0)]),
            vec![stra(vec!["a", "b"])],
        );
        assert!(r.is_err(), "mismatched lengths must error");
    }
}
