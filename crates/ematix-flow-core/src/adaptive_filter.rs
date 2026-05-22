//! Σ.L.3 — adaptive predicate reordering across row groups.
//!
//! ## Why this is the right lever
//!
//! TPC-H Q19 is the canonical case: filter chain has a `p_brand = 'X'`
//! Eq + an `l_quantity BETWEEN ...` range. The Eq is much more
//! selective (1/25 brands → 4% pass) than the range (~33% pass).
//! Running the range first wastes CPU on rows the Eq would have
//! killed.
//!
//! Static planners pick an order at plan time, with no idea of real
//! data distribution. DataFusion 53 ships its EnforceFilterPushdown
//! pass but the order WITHIN a remaining FilterExec is plan-time fixed.
//!
//! Our streaming reader iterates row groups explicitly — we get to
//! re-evaluate after each one. After the first ~2 row groups every
//! predicate has an observed pass-rate; we resort the remaining
//! predicates by ascending pass-rate (cheapest + most selective first).
//!
//! ## API
//!
//! [`AdaptiveFilterOrder`] is a small struct: per-query, tracks one
//! sample per predicate per row-group. After [`WARMUP_ROW_GROUPS`] row
//! groups it provides an ordering hint for the rest of the query.
//! Resets at query end.
//!
//! ## Why not in `BridgeFilter`
//!
//! The bridge filter today takes a `Vec<ColumnPredicate>` in plan
//! order. Plumbing per-row-group selectivity feedback INTO it would
//! require widening the trait. This module ships the data structure;
//! wire-in is a follow-up bite once the streaming reader exposes its
//! per-RG hook.

/// Number of row groups to observe before the first reorder. Lower =
/// faster adaptation, higher = less variance on tiny RGs.
pub const WARMUP_ROW_GROUPS: usize = 2;

/// Per-predicate cumulative observation. `cum_in` = rows seen before
/// this predicate; `cum_out` = rows that survived it.
#[derive(Debug, Clone, Copy, Default)]
pub struct PredicateStat {
    pub cum_in: u64,
    pub cum_out: u64,
}

impl PredicateStat {
    pub fn pass_rate(&self) -> f64 {
        if self.cum_in == 0 {
            1.0
        } else {
            self.cum_out as f64 / self.cum_in as f64
        }
    }
}

/// Σ.L.3 — adaptive ordering hint for a chain of N predicates.
#[derive(Debug, Clone)]
pub struct AdaptiveFilterOrder {
    /// Per-predicate cumulative stats, in *original plan order*.
    /// Indexing matches the caller's `Vec<ColumnPredicate>` indices.
    stats: Vec<PredicateStat>,
    row_groups_observed: usize,
    /// Current ordering of predicate indices. Initially `0..N`; flips
    /// to selectivity-sorted after [`WARMUP_ROW_GROUPS`].
    order: Vec<usize>,
}

impl AdaptiveFilterOrder {
    pub fn new(n_predicates: usize) -> Self {
        Self {
            stats: vec![PredicateStat::default(); n_predicates],
            row_groups_observed: 0,
            order: (0..n_predicates).collect(),
        }
    }

    /// Returns the predicate index order to apply *for the next row
    /// group*. After WARMUP this is selectivity-sorted; before it's
    /// plan order.
    pub fn current_order(&self) -> &[usize] {
        &self.order
    }

    /// Record a row-group's worth of observations. `samples[i]` is
    /// (rows_in_to_predicate_i, rows_out_of_predicate_i). Caller
    /// passes samples *in the order they applied predicates* — i.e.
    /// the current order, not the original.
    pub fn observe_row_group(&mut self, samples_by_orig_idx: &[(u64, u64)]) {
        for (i, (rin, rout)) in samples_by_orig_idx.iter().enumerate() {
            if let Some(s) = self.stats.get_mut(i) {
                s.cum_in = s.cum_in.saturating_add(*rin);
                s.cum_out = s.cum_out.saturating_add(*rout);
            }
        }
        self.row_groups_observed += 1;
        if self.row_groups_observed == WARMUP_ROW_GROUPS {
            self.reorder();
        }
    }

    /// Force a reorder now (test/admin use).
    pub fn reorder(&mut self) {
        // Sort by ascending pass_rate (most selective first). Ties
        // broken by original index for stability.
        let mut idx: Vec<usize> = (0..self.stats.len()).collect();
        idx.sort_by(|&a, &b| {
            let pa = self.stats[a].pass_rate();
            let pb = self.stats[b].pass_rate();
            pa.partial_cmp(&pb).unwrap_or(std::cmp::Ordering::Equal)
        });
        self.order = idx;
    }

    /// Selectivity of each predicate in the *current* order — useful
    /// for telemetry / Σ.L.2 workload log.
    pub fn selectivities_in_current_order(&self) -> Vec<(usize, f64)> {
        self.order
            .iter()
            .map(|&i| (i, self.stats[i].pass_rate()))
            .collect()
    }

    pub fn stats(&self) -> &[PredicateStat] {
        &self.stats
    }

    pub fn row_groups_observed(&self) -> usize {
        self.row_groups_observed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_order_during_warmup() {
        let mut o = AdaptiveFilterOrder::new(3);
        // After 1 row group (below WARMUP=2), order is plan order.
        o.observe_row_group(&[(1000, 100), (100, 50), (50, 10)]);
        assert_eq!(o.current_order(), &[0, 1, 2]);
    }

    #[test]
    fn reorders_after_warmup_by_ascending_pass_rate() {
        let mut o = AdaptiveFilterOrder::new(3);
        // Predicate 0: 10% pass. Predicate 1: 50%. Predicate 2: 100%.
        o.observe_row_group(&[(1000, 100), (1000, 500), (1000, 1000)]);
        o.observe_row_group(&[(1000, 100), (1000, 500), (1000, 1000)]);
        // After 2 row groups: cumulative same ratios.
        // Most selective (lowest pass-rate) first → 0, 1, 2.
        assert_eq!(o.current_order(), &[0, 1, 2]);
    }

    #[test]
    fn reorders_after_warmup_flipping_when_observed_selectivity_differs() {
        let mut o = AdaptiveFilterOrder::new(3);
        // Predicate 0 is plan-listed first but is the LEAST selective.
        // After warmup, the order should put predicate 2 first.
        o.observe_row_group(&[(1000, 900), (1000, 500), (1000, 100)]);
        o.observe_row_group(&[(1000, 900), (1000, 500), (1000, 100)]);
        // pass_rate: p0=0.9, p1=0.5, p2=0.1 → order [2, 1, 0]
        assert_eq!(o.current_order(), &[2, 1, 0]);
    }

    #[test]
    fn selectivity_snapshot_in_current_order() {
        let mut o = AdaptiveFilterOrder::new(2);
        o.observe_row_group(&[(100, 10), (100, 80)]);
        o.observe_row_group(&[(100, 10), (100, 80)]);
        let snap = o.selectivities_in_current_order();
        // p0 0.1, p1 0.8 → order [0, 1] (already sorted)
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0].0, 0);
        assert!((snap[0].1 - 0.1).abs() < 1e-6);
        assert!((snap[1].1 - 0.8).abs() < 1e-6);
    }

    #[test]
    fn pass_rate_handles_zero_in() {
        let s = PredicateStat::default();
        assert!((s.pass_rate() - 1.0).abs() < 1e-6);
    }

    #[test]
    fn stable_under_repeated_observations() {
        let mut o = AdaptiveFilterOrder::new(3);
        // Predicate 0 is most selective; should remain first.
        for _ in 0..10 {
            o.observe_row_group(&[(1000, 50), (1000, 200), (1000, 500)]);
        }
        assert_eq!(o.current_order()[0], 0);
    }
}
