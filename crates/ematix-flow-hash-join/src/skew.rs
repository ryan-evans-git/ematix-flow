//! Σ.T V5 L13 Story 2.4 — skew detection + overflow partitioning.
//!
//! At end-of-build, the operator runs [`observe`] over the
//! build-side table to find keys whose per-key chain length is so
//! much larger than the rest that they'd dominate probe latency.
//! "Hot" keys then move into a separate flat `HashMap<i64,
//! Vec<u32>>` overflow table; the probe path checks the hot map
//! first for an O(1) lookup, falling back to the main Robin Hood
//! table for non-skewed keys.
//!
//! ## Threshold (parametric, not TPC-H-tuned)
//!
//! A key is **hot** when:
//!
//! - `count > mean + HOT_SIGMA_MULTIPLIER * stddev` of the
//!   per-key count distribution AND
//! - `count > MIN_HOT_ABSOLUTE` (avoids flagging tiny tables
//!   where 3σ pulls in keys with count=2)
//!
//! `HOT_SIGMA_MULTIPLIER = 3.0` (loose 99.7th percentile under a
//! normal distribution — actual count distributions for TPC-H FK
//! shapes are right-skewed so this catches the worst offenders
//! without false positives on uniform distributions).
//!
//! `MIN_HOT_ABSOLUTE = 100` matches the spec floor in CURRENT.md;
//! the cost of the overflow split + per-probe hot/cold dispatch
//! only pays off above this row count.
//!
//! Both constants live in this module rather than feature-gated
//! per-query so the operator never inherits TPC-H assumptions
//! (per `feedback_no_tpch_hardcoding.md`).

use std::collections::HashMap;

use crate::table::RobinHoodHashJoinI64Table;

/// `count > mean + HOT_SIGMA_MULTIPLIER * stddev` is the
/// statistical part of the hot-key test.
const HOT_SIGMA_MULTIPLIER: f64 = 3.0;

/// `count > MIN_HOT_ABSOLUTE` is the absolute floor. Keeps small
/// tables from triggering false positives.
const MIN_HOT_ABSOLUTE: u64 = 100;

/// Summary of the build-side key distribution.
#[derive(Debug, Clone)]
pub struct SkewAnalysis {
    /// Keys above the hot threshold, sorted by descending count.
    pub hot_keys: Vec<i64>,
    /// Per-key count statistics for diagnostic / metric purposes.
    pub n_keys: usize,
    pub mean: f64,
    pub stddev: f64,
    /// Resolved hot-threshold count. A key is hot iff its count
    /// exceeds this value AND exceeds `MIN_HOT_ABSOLUTE`.
    pub threshold_count: f64,
}

impl SkewAnalysis {
    /// True if any key is hot. When false, the operator should
    /// skip the overflow-partition step entirely — building the
    /// hot map adds O(n_hot) work + a per-probe branch, both
    /// wasted on uniformly-distributed builds.
    pub fn is_skewed(&self) -> bool {
        !self.hot_keys.is_empty()
    }

    pub fn n_hot(&self) -> usize {
        self.hot_keys.len()
    }
}

/// Walk the table once, compute per-key counts, derive mean +
/// stddev, return the hot-key set.
///
/// Complexity: `O(n_buckets + n_rows)`. The pass is cheap enough
/// to run at end-of-build before any probe; on a 15M-row build
/// with ~1M distinct keys it's a few ms.
pub fn observe(table: &RobinHoodHashJoinI64Table) -> SkewAnalysis {
    let key_counts: Vec<(i64, usize)> = table.iter_key_counts().collect();
    let n_keys = key_counts.len();

    if n_keys == 0 {
        return SkewAnalysis {
            hot_keys: Vec::new(),
            n_keys: 0,
            mean: 0.0,
            stddev: 0.0,
            threshold_count: 0.0,
        };
    }

    // Mean.
    let sum: u64 = key_counts.iter().map(|&(_, c)| c as u64).sum();
    let mean = sum as f64 / n_keys as f64;

    // Stddev (population — we have the full distribution at this point).
    let variance: f64 = key_counts
        .iter()
        .map(|&(_, c)| {
            let d = c as f64 - mean;
            d * d
        })
        .sum::<f64>()
        / n_keys as f64;
    let stddev = variance.sqrt();

    // Hot threshold combines the statistical test with the
    // absolute floor.
    let stat_threshold = mean + HOT_SIGMA_MULTIPLIER * stddev;
    let threshold_count = stat_threshold.max(MIN_HOT_ABSOLUTE as f64);

    let mut hot_keys: Vec<(i64, usize)> = key_counts
        .iter()
        .filter(|&&(_, c)| c as f64 > threshold_count)
        .copied()
        .collect();
    // Sort by descending count so callers that cap the overflow
    // set drop the least-skewed first.
    hot_keys.sort_by_key(|b| std::cmp::Reverse(b.1));
    let hot_keys: Vec<i64> = hot_keys.into_iter().map(|(k, _)| k).collect();

    SkewAnalysis {
        hot_keys,
        n_keys,
        mean,
        stddev,
        threshold_count,
    }
}

/// Side table mapping hot keys → all build-side row indices for
/// that key. The operator's probe path checks this first.
///
/// Built once per query from [`observe`]'s output; the original
/// Robin Hood table stays intact so the cold-path probe is
/// unchanged.
pub struct OverflowTable {
    inner: HashMap<i64, Vec<u32>>,
}

impl OverflowTable {
    /// Build the overflow side-table for the keys flagged hot by
    /// `analysis`. Pulls each key's full row list from `table` via
    /// [`RobinHoodHashJoinI64Table::build_rows_for_key`].
    ///
    /// Returns an empty table when the analysis flags no skew —
    /// callers can branch on `is_empty()` to skip the per-probe
    /// hot-check entirely.
    pub fn from_skew_analysis(table: &RobinHoodHashJoinI64Table, analysis: &SkewAnalysis) -> Self {
        let mut inner = HashMap::with_capacity(analysis.hot_keys.len());
        for &k in &analysis.hot_keys {
            let rows = table.build_rows_for_key(k);
            if !rows.is_empty() {
                inner.insert(k, rows);
            }
        }
        Self { inner }
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn contains(&self, key: i64) -> bool {
        self.inner.contains_key(&key)
    }

    pub fn get(&self, key: i64) -> Option<&[u32]> {
        self.inner.get(&key).map(|v| v.as_slice())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RobinHoodHashJoinI64Table;

    #[test]
    fn empty_table_has_no_skew() {
        let table = RobinHoodHashJoinI64Table::new();
        let analysis = observe(&table);
        assert!(!analysis.is_skewed());
        assert_eq!(analysis.n_keys, 0);
    }

    #[test]
    fn uniform_distribution_below_absolute_floor_is_not_hot() {
        // 50 keys, each appearing exactly twice → mean = 2, stddev = 0.
        // Statistical threshold (mean + 3*stddev) = 2; absolute floor
        // 100 dominates → no hot keys.
        let mut table = RobinHoodHashJoinI64Table::new();
        for k in 0..50_i64 {
            for _ in 0..2 {
                table.insert(k, 0);
            }
        }
        let analysis = observe(&table);
        assert!(!analysis.is_skewed());
        assert_eq!(analysis.n_keys, 50);
        // threshold_count clamped up to MIN_HOT_ABSOLUTE (=100) since
        // statistical threshold (2.0) is well below it.
        assert!(analysis.threshold_count >= MIN_HOT_ABSOLUTE as f64);
    }
}
