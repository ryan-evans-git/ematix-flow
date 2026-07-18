//! Spilling hash aggregate — the engine's first pipeline breaker that
//! stays correct beyond a memory budget.
//!
//! Design = GRACE partitioned aggregation. Rows are radix-partitioned by a
//! hash of the group key, so **every occurrence of a key lands in the same
//! partition**. That invariant is what makes spill correct *and* bounded:
//! a partition can be aggregated independently (no key is split across
//! partitions, so the union of per-partition outputs is the complete
//! result), and a partition's group map holds only ≈ groups/npart entries,
//! so it fits even when the whole table did not.
//!
//! When the in-memory row buffers exceed the byte budget, every
//! partition's buffer is flushed to its own spill file ([`crate::spill`])
//! and buffering resumes. At [`finish`](SpillableSumAgg::finish) each
//! partition is aggregated from its in-memory remainder plus its
//! streamed-back spill run, so only one partition's map is ever resident.
//!
//! Scope (kill-gate): `SUM(i64) GROUP BY i64` — exact and
//! order-independent, so the spill and no-spill paths are **bit-identical**
//! (what `tests/spill_agg.rs` asserts). Deliberately narrow follow-ons:
//! f64 measures (revenue) are correct only up to FP re-association across
//! the changed summation order — standard for a spilling aggregate — and a
//! per-worker parallel spill + merge (this aggregate is single-threaded).

use std::collections::HashMap;
use std::io;

use crate::chunk::{DataChunk, Selection};
use crate::spill::{PartitionSpill, part_of};

/// Bytes per buffered/spilled row (i64 key + i64 val).
const REC_BYTES: usize = 16;

/// A `SUM(val) GROUP BY key` aggregate over i64 columns that spills to disk
/// once its in-memory buffers exceed `budget_bytes`.
pub struct SpillableSumAgg {
    part_bits: u32,
    npart: usize,
    budget_bytes: usize,
    /// Per-partition in-memory row buffers (parallel key/val vectors).
    keys: Vec<Vec<i64>>,
    vals: Vec<Vec<i64>>,
    buffered_rows: usize,
    spill: PartitionSpill,
}

impl SpillableSumAgg {
    /// A new aggregate with a `budget_bytes` ceiling on in-memory row
    /// buffers and `2^part_bits` partitions. `budget_bytes = usize::MAX`
    /// keeps everything in memory (never spills); a small budget forces
    /// spilling. `part_bits` must be in `1..=16` in practice (partition
    /// count = fan-out of spill files).
    pub fn new(budget_bytes: usize, part_bits: u32) -> Self {
        assert!(
            (1..=16).contains(&part_bits),
            "part_bits must be in 1..=16 (got {part_bits})"
        );
        let npart = 1usize << part_bits;
        let mut keys = Vec::with_capacity(npart);
        let mut vals = Vec::with_capacity(npart);
        keys.resize_with(npart, Vec::new);
        vals.resize_with(npart, Vec::new);
        Self {
            part_bits,
            npart,
            budget_bytes,
            keys,
            vals,
            buffered_rows: 0,
            spill: PartitionSpill::new(npart),
        }
    }

    /// Buffer one row into its partition.
    #[inline]
    fn route(&mut self, k: i64, v: i64) {
        let p = part_of(k, self.part_bits);
        self.keys[p].push(k);
        self.vals[p].push(v);
    }

    /// Absorb the live rows of `chunk`, grouping by i64 column `key_col`
    /// and summing i64 column `val_col`. Flushes to disk if the buffers
    /// cross the budget after this batch.
    pub fn consume(&mut self, chunk: &DataChunk, key_col: usize, val_col: usize) -> io::Result<()> {
        let kcol = chunk.col(key_col).as_i64();
        let vcol = chunk.col(val_col).as_i64();
        match &chunk.sel {
            Selection::All(n) => {
                for i in 0..*n {
                    self.route(kcol[i], vcol[i]);
                }
            }
            Selection::Indices(idx) => {
                for &i in idx {
                    let i = i as usize;
                    self.route(kcol[i], vcol[i]);
                }
            }
        }
        self.buffered_rows += chunk.sel.len();
        if self.buffered_rows.saturating_mul(REC_BYTES) > self.budget_bytes {
            self.flush_all()?;
        }
        Ok(())
    }

    /// Flush every non-empty partition buffer to its spill file and reset
    /// the in-memory footprint (spill-all-on-overflow: big sequential
    /// appends, simple accounting).
    fn flush_all(&mut self) -> io::Result<()> {
        for p in 0..self.npart {
            if !self.keys[p].is_empty() {
                self.spill.append(p, &self.keys[p], &self.vals[p])?;
                self.keys[p].clear();
                self.vals[p].clear();
            }
        }
        self.buffered_rows = 0;
        Ok(())
    }

    /// Rows spilled to disk so far (0 if everything fit in the budget).
    pub fn spilled_records(&self) -> u64 {
        self.spill.spilled_records()
    }

    /// Aggregate every partition (in-memory remainder + streamed-back spill
    /// run) and return the `(key, sum)` pairs sorted by key. Consumes the
    /// aggregate. Peak memory is one partition's group map.
    pub fn finish(mut self) -> io::Result<Vec<(i64, i64)>> {
        let mut out: Vec<(i64, i64)> = Vec::new();
        for p in 0..self.npart {
            if self.keys[p].is_empty() && !self.spill.has_spill(p) {
                continue;
            }
            let mut map: HashMap<i64, i64> = HashMap::new();
            for (k, v) in self.keys[p].iter().zip(&self.vals[p]) {
                *map.entry(*k).or_insert(0) += *v;
            }
            if self.spill.has_spill(p) {
                self.spill.drain_partition(p, |k, v| {
                    *map.entry(k).or_insert(0) += v;
                })?;
            }
            out.extend(map);
        }
        out.sort_unstable_by_key(|&(k, _)| k);
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::Vector;

    #[test]
    fn in_memory_sum_group_by_is_correct() {
        // keys: 5,5,7,5,7  vals: 1,2,3,4,5  → {5:1+2+4=7, 7:3+5=8}
        let chunk = DataChunk::new(vec![
            Vector::i64(vec![5, 5, 7, 5, 7]),
            Vector::i64(vec![1, 2, 3, 4, 5]),
        ]);
        let mut agg = SpillableSumAgg::new(usize::MAX, 4);
        agg.consume(&chunk, 0, 1).unwrap();
        assert_eq!(agg.spilled_records(), 0);
        assert_eq!(agg.finish().unwrap(), vec![(5, 7), (7, 8)]);
    }
}
