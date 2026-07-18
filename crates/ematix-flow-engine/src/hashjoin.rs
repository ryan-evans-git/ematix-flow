//! Spilling hash **inner equi-join** — the breaker behind the engine's
//! headline win (the Q08 join path), made correct beyond RAM.
//!
//! Design = GRACE hash join. Both sides radix-partition by the *same*
//! [`part_of`] hash, so a build row and a probe row sharing a key always
//! land in the same partition — hence each partition can be joined on its
//! own, and the union of per-partition results is the complete join with
//! no matching pair split across partitions. Only **one** partition's build
//! hash table is resident at a time (peak memory ≈ build-rows / npart),
//! which is what lets the join run when the whole build side would not fit.
//!
//! When the in-memory row buffers exceed the byte budget, the side being
//! fed flushes its per-partition buffers to disk ([`PartitionSpill`], reused
//! verbatim — its `(i64,i64)` record is exactly `(key, payload)`). At
//! [`run`](SpillableHashJoin::run) each partition builds its hash table from
//! the build side's in-memory remainder + streamed-back spill run, then
//! probes it with the probe side's remainder + streamed spill run, emitting
//! every matched `(key, build_payload, probe_payload)` — the probe side is
//! never fully materialized.
//!
//! Scope (kill-gate): i64 key + one i64 payload per side, single-threaded.
//! Multi-match (duplicate build keys) and inner-join drop (unmatched probe
//! keys vanish) are handled — the semantics a semijoin ([`crate::join`])
//! can't express. Deliberate follow-ons: multi-column payloads, recursive
//! re-partition for a single build partition that itself overflows (key
//! skew), and per-worker parallel partitioning + a merge.

use std::collections::HashMap;
use std::io;

use crate::chunk::{DataChunk, Selection};
use crate::spill::{PartitionSpill, part_of};

/// Bytes per buffered/spilled row (i64 key + i64 payload).
const REC_BYTES: usize = 16;

/// A spilling inner equi-join on an i64 key, carrying one i64 payload from
/// each side into the output.
pub struct SpillableHashJoin {
    part_bits: u32,
    npart: usize,
    budget_bytes: usize,
    // Per-partition in-memory build rows (parallel key/payload buffers).
    build_keys: Vec<Vec<i64>>,
    build_pay: Vec<Vec<i64>>,
    // Per-partition in-memory probe rows.
    probe_keys: Vec<Vec<i64>>,
    probe_pay: Vec<Vec<i64>>,
    build_buffered: usize,
    probe_buffered: usize,
    build_spill: PartitionSpill,
    probe_spill: PartitionSpill,
}

impl SpillableHashJoin {
    /// A join whose in-memory row buffers (build + probe combined) are
    /// capped at `budget_bytes`, over `2^part_bits` partitions.
    /// `budget_bytes = usize::MAX` keeps everything in memory.
    pub fn new(budget_bytes: usize, part_bits: u32) -> Self {
        assert!(
            (1..=16).contains(&part_bits),
            "part_bits must be in 1..=16 (got {part_bits})"
        );
        let npart = 1usize << part_bits;
        let mk = || {
            let mut v: Vec<Vec<i64>> = Vec::with_capacity(npart);
            v.resize_with(npart, Vec::new);
            v
        };
        Self {
            part_bits,
            npart,
            budget_bytes,
            build_keys: mk(),
            build_pay: mk(),
            probe_keys: mk(),
            probe_pay: mk(),
            build_buffered: 0,
            probe_buffered: 0,
            build_spill: PartitionSpill::new(npart),
            probe_spill: PartitionSpill::new(npart),
        }
    }

    #[inline]
    fn over_budget(&self) -> bool {
        (self.build_buffered + self.probe_buffered).saturating_mul(REC_BYTES) > self.budget_bytes
    }

    /// Absorb the live rows of `chunk` as build rows, keyed by i64 column
    /// `key_col` with i64 payload column `pay_col`.
    pub fn consume_build(
        &mut self,
        chunk: &DataChunk,
        key_col: usize,
        pay_col: usize,
    ) -> io::Result<()> {
        let kcol = chunk.col(key_col).as_i64();
        let vcol = chunk.col(pay_col).as_i64();
        let bits = self.part_bits;
        each_live(&chunk.sel, |i| {
            let p = part_of(kcol[i], bits);
            self.build_keys[p].push(kcol[i]);
            self.build_pay[p].push(vcol[i]);
        });
        self.build_buffered += chunk.sel.len();
        if self.over_budget() {
            flush(
                &mut self.build_keys,
                &mut self.build_pay,
                &mut self.build_spill,
            )?;
            self.build_buffered = 0;
        }
        Ok(())
    }

    /// Absorb the live rows of `chunk` as probe rows.
    pub fn consume_probe(
        &mut self,
        chunk: &DataChunk,
        key_col: usize,
        pay_col: usize,
    ) -> io::Result<()> {
        let kcol = chunk.col(key_col).as_i64();
        let vcol = chunk.col(pay_col).as_i64();
        let bits = self.part_bits;
        each_live(&chunk.sel, |i| {
            let p = part_of(kcol[i], bits);
            self.probe_keys[p].push(kcol[i]);
            self.probe_pay[p].push(vcol[i]);
        });
        self.probe_buffered += chunk.sel.len();
        if self.over_budget() {
            flush(
                &mut self.probe_keys,
                &mut self.probe_pay,
                &mut self.probe_spill,
            )?;
            self.probe_buffered = 0;
        }
        Ok(())
    }

    /// Build rows spilled to disk so far.
    pub fn build_spilled(&self) -> u64 {
        self.build_spill.spilled_records()
    }
    /// Probe rows spilled to disk so far.
    pub fn probe_spilled(&self) -> u64 {
        self.probe_spill.spilled_records()
    }

    /// Join every partition and emit each matched `(key, build_payload,
    /// probe_payload)`. Consumes the join. Peak memory is one partition's
    /// build hash table; the probe side is streamed, not materialized.
    pub fn run(mut self, mut emit: impl FnMut(i64, i64, i64)) -> io::Result<()> {
        for p in 0..self.npart {
            // Build this partition's hash table: key → all its build payloads
            // (a Vec, so duplicate build keys all match — multi-match).
            let mut ht: HashMap<i64, Vec<i64>> = HashMap::new();
            for (k, pay) in self.build_keys[p].iter().zip(&self.build_pay[p]) {
                ht.entry(*k).or_default().push(*pay);
            }
            if self.build_spill.has_spill(p) {
                self.build_spill
                    .drain_partition(p, |k, pay| ht.entry(k).or_default().push(pay))?;
            }
            // Empty build partition ⇒ no probe row here can match; skip
            // (unmatched probe rows simply never emit — inner-join drop).
            if ht.is_empty() {
                continue;
            }
            // Probe the resident rows, then the streamed-back spill run.
            for (k, ppay) in self.probe_keys[p].iter().zip(&self.probe_pay[p]) {
                if let Some(bps) = ht.get(k) {
                    for &bp in bps {
                        emit(*k, bp, *ppay);
                    }
                }
            }
            if self.probe_spill.has_spill(p) {
                self.probe_spill.drain_partition(p, |k, ppay| {
                    if let Some(bps) = ht.get(&k) {
                        for &bp in bps {
                            emit(k, bp, ppay);
                        }
                    }
                })?;
            }
        }
        Ok(())
    }
}

/// Visit each live row index of a selection.
#[inline]
fn each_live(sel: &Selection, mut f: impl FnMut(usize)) {
    match sel {
        Selection::All(n) => {
            for i in 0..*n {
                f(i);
            }
        }
        Selection::Indices(idx) => {
            for &i in idx {
                f(i as usize);
            }
        }
    }
}

/// Flush every non-empty partition buffer to its spill file and clear it.
fn flush(
    keys: &mut [Vec<i64>],
    pay: &mut [Vec<i64>],
    spill: &mut PartitionSpill,
) -> io::Result<()> {
    for p in 0..keys.len() {
        if !keys[p].is_empty() {
            spill.append(p, &keys[p], &pay[p])?;
            keys[p].clear();
            pay[p].clear();
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::Vector;

    /// Collect all matches of a fully in-memory join, sorted.
    fn join_all(build: &[(i64, i64)], probe: &[(i64, i64)]) -> Vec<(i64, i64, i64)> {
        let mut j = SpillableHashJoin::new(usize::MAX, 4);
        let bk: Vec<i64> = build.iter().map(|&(k, _)| k).collect();
        let bp: Vec<i64> = build.iter().map(|&(_, p)| p).collect();
        let pk: Vec<i64> = probe.iter().map(|&(k, _)| k).collect();
        let pp: Vec<i64> = probe.iter().map(|&(_, p)| p).collect();
        j.consume_build(
            &DataChunk::new(vec![Vector::i64(bk), Vector::i64(bp)]),
            0,
            1,
        )
        .unwrap();
        j.consume_probe(
            &DataChunk::new(vec![Vector::i64(pk), Vector::i64(pp)]),
            0,
            1,
        )
        .unwrap();
        let mut out = Vec::new();
        j.run(|k, b, p| out.push((k, b, p))).unwrap();
        out.sort_unstable();
        out
    }

    #[test]
    fn multi_match_and_inner_drop() {
        // build key 1 has two rows (payloads 100, 101); key 2 one (200).
        let build = [(1, 100), (1, 101), (2, 200)];
        // probe key 1 matches both 1-rows; key 2 matches; key 9 drops.
        let probe = [(1, 10), (2, 20), (9, 90)];
        let got = join_all(&build, &probe);
        assert_eq!(got, vec![(1, 100, 10), (1, 101, 10), (2, 200, 20)]);
    }

    #[test]
    fn empty_inputs_emit_nothing() {
        assert!(join_all(&[], &[(1, 1)]).is_empty());
        assert!(join_all(&[(1, 1)], &[]).is_empty());
    }
}
