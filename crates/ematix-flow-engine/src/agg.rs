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
//! (what `tests/spill_agg.rs` asserts). The parallel face is
//! [`SpillingSumSink`]: one aggregate per worker with its own spill files,
//! combined by the bounded cross-worker [`merge`](SpillableSumAgg::merge)
//! (proven at SF-1 scale by `tests/spill_agg_parallel.rs`). The one
//! deliberately narrow follow-on left: f64 measures (revenue) are correct
//! only up to FP re-association across the changed summation order —
//! standard for a spilling aggregate.

use std::collections::HashMap;
use std::io;

use crate::chunk::{DataChunk, Selection};
use crate::exec::Sink;
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
    /// aggregate. Peak memory is one partition's group map. The
    /// single-worker case of [`merge`](Self::merge).
    pub fn finish(self) -> io::Result<Vec<(i64, i64)>> {
        Self::merge(vec![self])
    }

    /// Merge several partitioned aggregates that share the same partitioning
    /// — e.g. one per worker from [`run_scan_pipeline`](crate::exec) — into
    /// the final `(key, sum)` result, sorted by key. Consumes them, draining
    /// each one's spill runs.
    ///
    /// **Bounded, and that is the whole point.** Because every occurrence of
    /// a key routes to the same partition index in *every* aggregate (all
    /// share [`part_of`]), a partition can be finished across all workers
    /// with a single group map: the union of the workers' partition-`p` data
    /// is exactly the rows of the keys that hash to `p`, complete and
    /// disjoint from other partitions. So a parallel high-cardinality
    /// group-by holds only ≈ groups/npart entries at a time — the same
    /// memory envelope as the single-threaded aggregate, never the whole
    /// result set.
    pub fn merge(mut aggs: Vec<Self>) -> io::Result<Vec<(i64, i64)>> {
        let Some(first) = aggs.first() else {
            return Ok(Vec::new());
        };
        let npart = first.npart;
        debug_assert!(
            aggs.iter().all(|a| a.npart == npart),
            "merged aggregates must share the same partition count"
        );
        let mut out: Vec<(i64, i64)> = Vec::new();
        for p in 0..npart {
            let mut map: HashMap<i64, i64> = HashMap::new();
            for agg in &mut aggs {
                for (k, v) in agg.keys[p].iter().zip(&agg.vals[p]) {
                    *map.entry(*k).or_insert(0) += *v;
                }
                if agg.spill.has_spill(p) {
                    agg.spill.drain_partition(p, |k, v| {
                        *map.entry(k).or_insert(0) += v;
                    })?;
                }
            }
            out.extend(map);
        }
        out.sort_unstable_by_key(|&(k, _)| k);
        Ok(out)
    }
}

/// A [`Sink`] adapter that drives a **per-worker** [`SpillableSumAgg`]
/// through [`run_scan_pipeline`](crate::exec::run_scan_pipeline): one
/// instance per worker thread, each spilling to its *own* [`PartitionSpill`],
/// then combined by [`merge`](Self::merge) in a single bounded per-partition
/// pass. This is how a high-cardinality `SUM(i64) GROUP BY i64` stays correct
/// beyond RAM *and* runs row-group-parallel — the driver-level face of the
/// spilling aggregate, behind the same one-sink-per-worker + merge contract
/// as the in-memory [`HashAggregateSink`].
///
/// The key/value columns are fixed positions in the decoded chunk. The
/// driver's [`Sink::consume`] is infallible, so a disk error is stashed and
/// surfaced at [`merge`](Self::merge) rather than panicking a worker thread.
pub struct SpillingSumSink {
    agg: SpillableSumAgg,
    key_col: usize,
    val_col: usize,
    err: Option<io::Error>,
}

impl SpillingSumSink {
    /// A per-worker spilling aggregate: `budget_bytes` in-memory ceiling,
    /// `2^part_bits` partitions, grouping by decoded-chunk column `key_col`
    /// and summing column `val_col`.
    pub fn new(budget_bytes: usize, part_bits: u32, key_col: usize, val_col: usize) -> Self {
        Self {
            agg: SpillableSumAgg::new(budget_bytes, part_bits),
            key_col,
            val_col,
            err: None,
        }
    }

    /// Rows this worker has spilled to disk so far (0 if it stayed in budget).
    pub fn spilled_records(&self) -> u64 {
        self.agg.spilled_records()
    }

    /// Combine the per-worker sinks into the final `(key, sum)` result,
    /// sorted by key. Surfaces the first disk error any worker hit during
    /// consumption, then runs the bounded cross-worker GRACE merge
    /// ([`SpillableSumAgg::merge`]).
    pub fn merge(sinks: Vec<Self>) -> io::Result<Vec<(i64, i64)>> {
        let mut aggs = Vec::with_capacity(sinks.len());
        for s in sinks {
            if let Some(e) = s.err {
                return Err(e);
            }
            aggs.push(s.agg);
        }
        SpillableSumAgg::merge(aggs)
    }
}

impl Sink for SpillingSumSink {
    fn consume(&mut self, chunk: &DataChunk) {
        if self.err.is_some() {
            return; // already failed; stop touching the disk.
        }
        if let Err(e) = self.agg.consume(chunk, self.key_col, self.val_col) {
            self.err = Some(e);
        }
    }
}

/// A query's binding to the general hash aggregate: given a decoded
/// `DataChunk`, emit `(group_key, measure_contributions)` for each live row
/// that belongs to a group. Rows with no group — an inner-join miss, a
/// filtered row — are simply not emitted. `N` is the number of parallel
/// `SUM` measures; implementations hoist column access out of the row loop.
///
/// This is the seam a SQL front-end (P3) will target: the aggregation
/// *machinery* (hash accumulate + parallel merge) lives in
/// [`HashAggregateSink`]; the query supplies only how to derive the key and
/// measures — so a query needs no bespoke aggregation `Sink` of its own.
pub trait AggBinding<const N: usize>: Send + Sync {
    fn for_each_group(&self, chunk: &DataChunk, emit: impl FnMut(i64, [f64; N]));
}

/// The general `GROUP BY key → SUM(measures)` breaker: a [`Sink`] the
/// row-group-parallel driver ([`crate::exec::run_scan_pipeline`]) runs
/// one-per-worker, each accumulating into its own map; the caller sums them
/// with [`merge`](Self::merge). Holds `N` running f64 sums per group.
///
/// In-memory, general over `N` f64 measures and an arbitrary key/measure
/// binding (Q08's group cardinality is tiny — two years). Its spilling
/// sibling for high-cardinality, exact-integer group-bys is
/// [`SpillingSumSink`], behind this same one-sink-per-worker + `merge`
/// contract.
pub struct HashAggregateSink<const N: usize, B: AggBinding<N>> {
    binding: std::sync::Arc<B>,
    map: HashMap<i64, [f64; N]>,
}

impl<const N: usize, B: AggBinding<N>> HashAggregateSink<N, B> {
    /// A fresh accumulator sharing `binding` (the probes / derived
    /// expressions) with the other workers' sinks.
    pub fn new(binding: std::sync::Arc<B>) -> Self {
        Self {
            binding,
            map: HashMap::new(),
        }
    }

    /// This worker's partial group→sums.
    pub fn into_map(self) -> HashMap<i64, [f64; N]> {
        self.map
    }

    /// Sum the per-worker partials element-wise into the final result.
    pub fn merge(sinks: Vec<Self>) -> HashMap<i64, [f64; N]> {
        let mut out: HashMap<i64, [f64; N]> = HashMap::new();
        for s in sinks {
            for (k, m) in s.map {
                let e = out.entry(k).or_insert([0.0; N]);
                for j in 0..N {
                    e[j] += m[j];
                }
            }
        }
        out
    }
}

impl<const N: usize, B: AggBinding<N>> Sink for HashAggregateSink<N, B> {
    fn consume(&mut self, chunk: &DataChunk) {
        // Split the borrow: the binding emits, this closure accumulates.
        let Self { binding, map } = self;
        binding.for_each_group(chunk, |k, m| {
            let e = map.entry(k).or_insert([0.0; N]);
            for j in 0..N {
                e[j] += m[j];
            }
        });
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
