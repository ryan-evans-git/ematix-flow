//! Story 2.4 TDD anchors for skew detection + overflow partitioning.

use ematix_flow_hash_join::RobinHoodHashJoinI64Table;
use ematix_flow_hash_join::skew::{OverflowTable, observe};

/// Build a table with `n` keys uniformly + N hot keys each
/// inserted `hot_count` times.
fn build_skewed_table(
    n_uniform_keys: i64,
    uniform_count: usize,
    hot_keys: &[i64],
    hot_count: usize,
) -> RobinHoodHashJoinI64Table {
    let total_rows = (n_uniform_keys as usize) * uniform_count + hot_keys.len() * hot_count;
    let mut table = RobinHoodHashJoinI64Table::with_capacity(total_rows);

    let mut row_idx: u32 = 0;
    // Hot keys appear first so the chain prepend order is stable
    // for the test's assertions.
    for &k in hot_keys {
        for _ in 0..hot_count {
            table.insert(k, row_idx);
            row_idx += 1;
        }
    }
    for k in 0..n_uniform_keys {
        // Use a key range disjoint from the hot keys.
        let key = 1_000_000 + k;
        for _ in 0..uniform_count {
            table.insert(key, row_idx);
            row_idx += 1;
        }
    }
    table
}

// ---------------------------------------------------------------------
// detects_top_k_hot_keys
//
// 1M rows: 10 keys × 10K hits each + remainder uniform. The 10 hot
// keys must be detected.
// ---------------------------------------------------------------------
#[test]
fn detects_top_k_hot_keys() {
    let hot_keys = [1_i64, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let table = build_skewed_table(
        /* n_uniform_keys = */ 900_000, /* uniform_count = */ 1, &hot_keys,
        /* hot_count = */ 10_000,
    );

    let analysis = observe(&table);
    assert!(
        analysis.is_skewed(),
        "10× 10K hits each should register as skew"
    );
    assert_eq!(analysis.n_hot(), 10, "all 10 hot keys should be detected");

    // Each hot key from the input set must appear in the analysis.
    for k in hot_keys {
        assert!(
            analysis.hot_keys.contains(&k),
            "hot_keys missing expected key {k}"
        );
    }

    // No uniform keys should leak in.
    for k in &analysis.hot_keys {
        assert!(*k < 1_000_000, "unexpected uniform key {k} flagged as hot");
    }
}

// ---------------------------------------------------------------------
// skewed_keys_partition_into_overflow_table
//
// Given the skew detection result, hot keys must materialise into the
// overflow `HashMap<i64, Vec<u32>>` with their full row lists; probes
// for hot keys hit the overflow first.
// ---------------------------------------------------------------------
#[test]
fn skewed_keys_partition_into_overflow_table() {
    let hot_keys = [42_i64, 7];
    let table = build_skewed_table(
        /* n_uniform_keys = */ 50_000, /* uniform_count = */ 1, &hot_keys,
        /* hot_count = */ 500,
    );

    let analysis = observe(&table);
    let overflow = OverflowTable::from_skew_analysis(&table, &analysis);

    assert!(!overflow.is_empty());
    assert_eq!(overflow.len(), 2);

    for &k in &hot_keys {
        assert!(overflow.contains(k), "overflow should contain hot key {k}");
        let rows = overflow.get(k).expect("hot key rows");
        assert_eq!(rows.len(), 500, "key {k} should have 500 build rows");
        // Build row indices should be unique within the chain.
        let mut sorted = rows.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), 500, "row indices should be unique");
    }

    // Non-hot keys should NOT be in the overflow table.
    assert!(!overflow.contains(1_000_000_i64));
}

// ---------------------------------------------------------------------
// skew_handling_neutral_when_no_skew
//
// Uniform-distribution build: no hot keys, empty overflow table,
// downstream operator skips per-probe hot/cold dispatch.
// ---------------------------------------------------------------------
#[test]
fn skew_handling_neutral_when_no_skew() {
    let table = build_skewed_table(
        /* n_uniform_keys = */ 10_000,
        /* uniform_count = */ 5,
        /* hot_keys = */ &[],
        /* hot_count = */ 0,
    );

    let analysis = observe(&table);
    assert!(!analysis.is_skewed());
    assert_eq!(analysis.n_hot(), 0);

    let overflow = OverflowTable::from_skew_analysis(&table, &analysis);
    assert!(overflow.is_empty());
    assert_eq!(overflow.len(), 0);
}

// ---------------------------------------------------------------------
// threshold_respects_absolute_floor
//
// 100 keys × 50 hits each — statistical threshold is well below 50
// (mean=50, stddev=0), but the absolute floor (MIN_HOT_ABSOLUTE=100)
// stops anything from being flagged. Prevents false positives on
// small uniform tables.
// ---------------------------------------------------------------------
#[test]
fn threshold_respects_absolute_floor() {
    let mut table = RobinHoodHashJoinI64Table::new();
    for k in 0..100_i64 {
        for _ in 0..50 {
            table.insert(k, 0);
        }
    }
    let analysis = observe(&table);
    assert!(
        !analysis.is_skewed(),
        "uniform 50-hit distribution must not register as skew"
    );
}

// ---------------------------------------------------------------------
// build_rows_for_key_is_accurate_for_all_keys
//
// Sanity-check the helper that feeds the overflow constructor — must
// return every row inserted for the key, no more, no fewer.
// ---------------------------------------------------------------------
#[test]
fn build_rows_for_key_is_accurate_for_all_keys() {
    let mut table = RobinHoodHashJoinI64Table::new();
    table.insert(10, 100);
    table.insert(20, 200);
    table.insert(10, 101);
    table.insert(10, 102);

    let mut rows10 = table.build_rows_for_key(10);
    rows10.sort_unstable();
    assert_eq!(rows10, vec![100, 101, 102]);

    assert_eq!(table.build_rows_for_key(20), vec![200]);
    assert_eq!(table.build_rows_for_key(999).len(), 0);
}
