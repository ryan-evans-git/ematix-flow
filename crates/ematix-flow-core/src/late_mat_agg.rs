//! Q10 wide-string late-materialization (prod-B) — the SOUND core: computing the
//! FD-minimal grouping key from proven functional dependencies.
//!
//! The late-materialization rewrite groups on a compact key (the FD-minimal subset
//! of the group-by columns) and re-attaches the functionally-determined columns at
//! the aggregate output. Correctness HINGES on the reduced key `K` being a true
//! superkey of the original group columns — i.e. `K` functionally determines every
//! original group column. This module computes `K` from the declared-PK-derived
//! functional dependencies (see [`crate::ematix_fast_parquet::EmatixFastParquetTableProvider::with_primary_key`])
//! using closure under the FDs. If no PROPER reduction is provable, it returns
//! `None` and the rule must not fire — a wrong `K` would silently change results.
//!
//! This file is the detector + its tests only; the recognizer rule, the logical
//! extension node, and the physical `ExtensionPlanner` (prod-B/C) build on it.

use std::collections::BTreeSet;

use datafusion::common::FunctionalDependencies;

/// Functional-dependency closure of `seed` under `fds`: the full set of columns
/// determined by `seed` (fixpoint — adds an FD's targets whenever its full source
/// is already in the set). Indices are positions in the schema the FDs describe.
fn fd_closure(seed: &BTreeSet<usize>, fds: &FunctionalDependencies) -> BTreeSet<usize> {
    let mut closure = seed.clone();
    loop {
        let mut grew = false;
        for fd in fds.iter() {
            if fd.source_indices.iter().all(|s| closure.contains(s)) {
                for &t in &fd.target_indices {
                    if closure.insert(t) {
                        grew = true;
                    }
                }
            }
        }
        if !grew {
            break;
        }
    }
    closure
}

/// Compute the FD-minimal grouping key: the smallest subset `K` of `group_cols`
/// (schema column indices) that functionally determines ALL of `group_cols` under
/// the proven dependencies `fds`. Returns `Some(K)` (ascending, a PROPER subset of
/// `group_cols`) when a reduction is provable, else `None`.
///
/// SOUND: a group column is dropped from the key only when the REMAINING key still
/// determines it via `fds` (closure check) — so grouping by `K` yields exactly the
/// same groups as grouping by all of `group_cols`. Greedy minimal-key reduction:
/// each column is removed iff the rest remain a superkey. Returns `None` if
/// `group_cols` has duplicates-collapsed size < 2, if there are no usable FDs, or
/// if no column can be removed (no reduction possible).
pub fn fd_minimal_group_key(
    group_cols: &[usize],
    fds: &FunctionalDependencies,
) -> Option<Vec<usize>> {
    let gset: BTreeSet<usize> = group_cols.iter().copied().collect();
    if gset.len() < 2 || fds.is_empty() {
        return None;
    }
    // Greedy reduction: drop a column whenever the remaining key still determines
    // the FULL original group set. Iterate in a stable order for determinism.
    let mut key = gset.clone();
    for &g in gset.iter() {
        if !key.contains(&g) {
            continue;
        }
        let mut trial = key.clone();
        trial.remove(&g);
        if !trial.is_empty() && fd_closure(&trial, fds).is_superset(&gset) {
            key = trial;
        }
    }
    // Only fire on a real reduction, and re-verify the soundness invariant.
    if key.len() < gset.len() && fd_closure(&key, fds).is_superset(&gset) {
        Some(key.into_iter().collect())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::FunctionalDependence;

    fn fds(deps: Vec<(Vec<usize>, Vec<usize>)>) -> FunctionalDependencies {
        FunctionalDependencies::new(
            deps.into_iter()
                .map(|(s, t)| FunctionalDependence::new(s, t, false))
                .collect(),
        )
    }

    /// Q10 shape: group cols [c_custkey=0, c_name=1, c_acctbal=2, c_phone=3,
    /// n_name=4, c_address=5, c_comment=6]; FD {0} -> {1,2,3,5,6} (customer PK;
    /// n_name@4 NOT covered — it's from nation). FD-minimal key = {0, 4}.
    #[test]
    fn q10_shape_reduces_to_custkey_plus_nname() {
        let group = [0usize, 1, 2, 3, 4, 5, 6];
        let f = fds(vec![(vec![0], vec![1, 2, 3, 5, 6])]);
        let k = fd_minimal_group_key(&group, &f).expect("reducible");
        assert_eq!(k, vec![0, 4], "key = c_custkey + the un-covered n_name");
    }

    /// Full coverage: {0} -> {1,2,3,4,5,6} (a single-table PK group) → key = {0}.
    #[test]
    fn full_pk_coverage_reduces_to_single_key() {
        let group = [0usize, 1, 2, 3, 4, 5, 6];
        let f = fds(vec![(vec![0], vec![1, 2, 3, 4, 5, 6])]);
        assert_eq!(fd_minimal_group_key(&group, &f), Some(vec![0]));
    }

    /// No FD → no reduction.
    #[test]
    fn no_fd_no_reduction() {
        let group = [0usize, 1, 2];
        assert_eq!(fd_minimal_group_key(&group, &fds(vec![])), None);
    }

    /// FD exists but its source is OUTSIDE the group set → cannot reduce (the
    /// determinant isn't grouped). {7} -> {1} but 7 not in group.
    #[test]
    fn fd_source_outside_group_no_reduction() {
        let group = [0usize, 1, 2];
        let f = fds(vec![(vec![7], vec![1])]);
        assert_eq!(fd_minimal_group_key(&group, &f), None);
    }

    /// All group cols mutually independent (FD targets none of them) → no reduction.
    #[test]
    fn independent_cols_no_reduction() {
        let group = [0usize, 1, 2];
        let f = fds(vec![(vec![0], vec![9, 10])]); // determines non-group cols only
        assert_eq!(fd_minimal_group_key(&group, &f), None);
    }

    /// Single group column never reduces.
    #[test]
    fn single_col_no_reduction() {
        let f = fds(vec![(vec![0], vec![1])]);
        assert_eq!(fd_minimal_group_key(&[0], &f), None);
    }

    /// Transitive chain: {0}->{1}, {1}->{2}. Closure of {0} covers {0,1,2} → key={0}.
    #[test]
    fn transitive_chain_reduces_via_closure() {
        let group = [0usize, 1, 2];
        let f = fds(vec![(vec![0], vec![1]), (vec![1], vec![2])]);
        assert_eq!(fd_minimal_group_key(&group, &f), Some(vec![0]));
    }
}
