//! Adaptive re-plan at the join breaker — the "capabilities DF can't
//! express" capstone.
//!
//! A static plan commits to a join strategy from the optimizer's
//! *estimated* cardinalities, before seeing a single row. When the
//! estimate is wrong — correlated predicates, skew, stale stats — it can
//! commit to spilling a build that actually fits in cache, or to an
//! in-memory build that actually blows past RAM. This join instead defers
//! the choice to the build breaker and picks from the **observed** build
//! size, so a wrong estimate can't lock in the wrong physical strategy.
//!
//! Mechanism = the **hybrid hash join**. Build rows are buffered flat and
//! optimistically kept in memory; the moment the observed build crosses the
//! budget it **transitions** to the partitioned/spilling path
//! ([`SpillableHashJoin`], P2.5) — feeding it everything buffered so far and
//! everything after. If the build never overflows, the whole join runs as a
//! single in-memory hash table, skipping all partition/spill machinery.
//! Either way the result is identical.
//!
//! [`plan_would_choose`](AdaptiveHashJoin::plan_would_choose) reports the
//! strategy the estimate implies; [`chosen`](AdaptiveHashJoin::chosen)
//! reports what runtime observation actually used; their divergence is the
//! re-plan ([`replanned`](AdaptiveHashJoin::replanned)).
//!
//! Scope (kill-gate): the in-memory ⇄ partitioned/spilling switch, driven
//! by observed build bytes. Richer adaptive decisions — build-side swap
//! (build from the smaller input), broadcast vs shuffle in the parallel
//! setting, recursive re-partition under key skew — are follow-ons.
//! Contract: consume the whole build side before the probe side (the
//! standard build/probe protocol).

use std::collections::HashMap;
use std::io;

use crate::chunk::DataChunk;
use crate::hashjoin::SpillableHashJoin;
use crate::vector::Vector;

/// Bytes per build row (i64 key + i64 payload) — the unit the budget and
/// the estimate are measured in.
const REC_BYTES: usize = 16;

/// Which physical join strategy is in play.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Strategy {
    /// One in-memory hash table, no partitioning or spill (build fit budget).
    InMemory,
    /// Radix-partitioned, spilling GRACE join (build exceeded budget).
    Partitioned,
}

/// The strategy a given build size implies against `budget_bytes`.
fn strategy_for(build_bytes: usize, budget_bytes: usize) -> Strategy {
    if build_bytes > budget_bytes {
        Strategy::Partitioned
    } else {
        Strategy::InMemory
    }
}

/// A hash inner equi-join that selects its physical strategy at runtime
/// from the observed build size, overriding the planner's estimate.
pub struct AdaptiveHashJoin {
    budget_bytes: usize,
    part_bits: u32,
    /// The optimizer's predicted build-side byte size (drives the readout of
    /// what a static plan *would* have committed to).
    estimate_bytes: usize,
    /// Optimistic in-memory build buffer (flat, unpartitioned).
    build_keys: Vec<i64>,
    build_pay: Vec<i64>,
    /// In-memory probe buffer (flat) — only used while still in-memory.
    probe_keys: Vec<i64>,
    probe_pay: Vec<i64>,
    /// Total build rows observed (never reset — the runtime cardinality).
    observed_build_rows: usize,
    /// The partitioned/spilling fallback, present once the build overflowed.
    partitioned: Option<SpillableHashJoin>,
}

impl AdaptiveHashJoin {
    /// A join with an in-memory `budget_bytes` ceiling, `2^part_bits`
    /// partitions for the spilling fallback, and the planner's
    /// `estimated_build_bytes`.
    pub fn new(budget_bytes: usize, part_bits: u32, estimated_build_bytes: usize) -> Self {
        assert!(
            (1..=16).contains(&part_bits),
            "part_bits must be in 1..=16 (got {part_bits})"
        );
        Self {
            budget_bytes,
            part_bits,
            estimate_bytes: estimated_build_bytes,
            build_keys: Vec::new(),
            build_pay: Vec::new(),
            probe_keys: Vec::new(),
            probe_pay: Vec::new(),
            observed_build_rows: 0,
            partitioned: None,
        }
    }

    /// The strategy the planner's estimate would have committed to.
    pub fn plan_would_choose(&self) -> Strategy {
        strategy_for(self.estimate_bytes, self.budget_bytes)
    }

    /// The strategy runtime observation actually selected (final once the
    /// build side is fully consumed).
    pub fn chosen(&self) -> Strategy {
        if self.partitioned.is_some() {
            Strategy::Partitioned
        } else {
            Strategy::InMemory
        }
    }

    /// Whether runtime observation overrode the plan's estimate.
    pub fn replanned(&self) -> bool {
        self.chosen() != self.plan_would_choose()
    }

    /// Observed build-side byte size (the runtime cardinality × row width).
    pub fn observed_build_bytes(&self) -> usize {
        self.observed_build_rows * REC_BYTES
    }

    /// Absorb the live rows of `chunk` as build rows. Stays in memory until
    /// the observed build crosses the budget, then transitions to the
    /// spilling path.
    pub fn consume_build(
        &mut self,
        chunk: &DataChunk,
        key_col: usize,
        pay_col: usize,
    ) -> io::Result<()> {
        self.observed_build_rows += chunk.sel.len();
        if let Some(sj) = self.partitioned.as_mut() {
            return sj.consume_build(chunk, key_col, pay_col);
        }
        let kcol = chunk.col(key_col).as_i64();
        let vcol = chunk.col(pay_col).as_i64();
        chunk.sel.for_each(|i| {
            let i = i as usize;
            self.build_keys.push(kcol[i]);
            self.build_pay.push(vcol[i]);
        });
        if self.build_keys.len() * REC_BYTES > self.budget_bytes {
            self.transition_to_partitioned()?;
        }
        Ok(())
    }

    /// Spin up the spilling join and migrate everything buffered so far.
    fn transition_to_partitioned(&mut self) -> io::Result<()> {
        let mut sj = SpillableHashJoin::new(self.budget_bytes, self.part_bits);
        let bk = std::mem::take(&mut self.build_keys);
        let bp = std::mem::take(&mut self.build_pay);
        sj.consume_build(
            &DataChunk::new(vec![Vector::i64(bk), Vector::i64(bp)]),
            0,
            1,
        )?;
        // Any probe already buffered (out-of-contract interleaving) migrates too.
        if !self.probe_keys.is_empty() {
            let pk = std::mem::take(&mut self.probe_keys);
            let pp = std::mem::take(&mut self.probe_pay);
            sj.consume_probe(
                &DataChunk::new(vec![Vector::i64(pk), Vector::i64(pp)]),
                0,
                1,
            )?;
        }
        self.partitioned = Some(sj);
        Ok(())
    }

    /// Absorb the live rows of `chunk` as probe rows.
    pub fn consume_probe(
        &mut self,
        chunk: &DataChunk,
        key_col: usize,
        pay_col: usize,
    ) -> io::Result<()> {
        if let Some(sj) = self.partitioned.as_mut() {
            return sj.consume_probe(chunk, key_col, pay_col);
        }
        let kcol = chunk.col(key_col).as_i64();
        let vcol = chunk.col(pay_col).as_i64();
        chunk.sel.for_each(|i| {
            let i = i as usize;
            self.probe_keys.push(kcol[i]);
            self.probe_pay.push(vcol[i]);
        });
        Ok(())
    }

    /// Build rows spilled to disk (0 on the in-memory path).
    pub fn build_spilled(&self) -> u64 {
        self.partitioned.as_ref().map_or(0, |s| s.build_spilled())
    }
    /// Probe rows spilled to disk (0 on the in-memory path).
    pub fn probe_spilled(&self) -> u64 {
        self.partitioned.as_ref().map_or(0, |s| s.probe_spilled())
    }

    /// Run the chosen strategy, emitting each matched `(key, build_payload,
    /// probe_payload)`. Consumes the join.
    pub fn run(self, mut emit: impl FnMut(i64, i64, i64)) -> io::Result<()> {
        if let Some(sj) = self.partitioned {
            return sj.run(emit);
        }
        // In-memory hash join over the flat buffers.
        let mut ht: HashMap<i64, Vec<i64>> = HashMap::new();
        for (k, p) in self.build_keys.iter().zip(&self.build_pay) {
            ht.entry(*k).or_default().push(*p);
        }
        for (k, pp) in self.probe_keys.iter().zip(&self.probe_pay) {
            if let Some(bps) = ht.get(k) {
                for &bp in bps {
                    emit(*k, bp, *pp);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(keys: Vec<i64>, pay: Vec<i64>) -> DataChunk {
        DataChunk::new(vec![Vector::i64(keys), Vector::i64(pay)])
    }

    #[test]
    fn tiny_build_stays_in_memory() {
        let mut j = AdaptiveHashJoin::new(usize::MAX, 4, 0);
        j.consume_build(&chunk(vec![1, 1, 2], vec![10, 11, 20]), 0, 1)
            .unwrap();
        j.consume_probe(&chunk(vec![1, 2, 9], vec![100, 200, 900]), 0, 1)
            .unwrap();
        assert_eq!(j.chosen(), Strategy::InMemory);
        assert!(!j.replanned());
        assert_eq!(j.build_spilled(), 0);
        let mut out = Vec::new();
        j.run(|k, b, p| out.push((k, b, p))).unwrap();
        out.sort_unstable();
        // key 1 → both build rows; key 2 → one; key 9 drops.
        assert_eq!(out, vec![(1, 10, 100), (1, 11, 100), (2, 20, 200)]);
    }

    #[test]
    fn transition_flips_strategy_and_reports_replan() {
        // Budget = 2 rows; feed 5 build rows → overflow → Partitioned.
        let mut j = AdaptiveHashJoin::new(2 * REC_BYTES, 4, 0 /* estimate: tiny */);
        assert_eq!(j.plan_would_choose(), Strategy::InMemory);
        j.consume_build(&chunk(vec![1, 2, 3, 4, 5], vec![1, 2, 3, 4, 5]), 0, 1)
            .unwrap();
        assert_eq!(j.chosen(), Strategy::Partitioned, "overflow → Partitioned");
        assert!(
            j.replanned(),
            "tiny estimate overridden by observed overflow"
        );
        assert_eq!(j.observed_build_bytes(), 5 * REC_BYTES);
    }
}
