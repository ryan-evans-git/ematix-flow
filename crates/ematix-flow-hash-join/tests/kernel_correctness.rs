//! Story 2.1 TDD anchors for the Σ.T V5 L13 hash-join kernel.
//!
//! These tests pin the kernel's correctness contract before any
//! optimization work lands. The kernel ships with no DataFusion or
//! Arrow dependency (Story 2.5 wires it into an ExecutionPlan); the
//! tests therefore validate against a simple `HashMap<i64, Vec<u32>>`
//! reference implementation.

use std::collections::HashMap;

use ematix_flow_hash_join::{ProbeMatch, RobinHoodHashJoinI64Table};

// ---------------------------------------------------------------------
// Reference implementation — the ground truth for the kernel's
// behaviour. Build side is a flat list of (key, build_row_idx); probe
// side is the same shape. The reference emits every (probe_row_idx,
// build_row_idx) pair where the keys match. NULLs never match.
// ---------------------------------------------------------------------
fn naive_inner_join(
    build_keys: &[i64],
    build_nulls: Option<&[bool]>, // true = valid (not null)
    probe_keys: &[i64],
    probe_nulls: Option<&[bool]>,
) -> Vec<ProbeMatch> {
    let mut build: HashMap<i64, Vec<u32>> = HashMap::new();
    for (i, &k) in build_keys.iter().enumerate() {
        let valid = build_nulls.map(|m| m[i]).unwrap_or(true);
        if valid {
            build.entry(k).or_default().push(i as u32);
        }
    }
    let mut out = Vec::new();
    for (i, &k) in probe_keys.iter().enumerate() {
        let valid = probe_nulls.map(|m| m[i]).unwrap_or(true);
        if !valid {
            continue;
        }
        if let Some(matches) = build.get(&k) {
            for &bi in matches {
                out.push(ProbeMatch {
                    probe_row_idx: i as u32,
                    build_row_idx: bi,
                });
            }
        }
    }
    out
}

fn sorted(mut v: Vec<ProbeMatch>) -> Vec<ProbeMatch> {
    v.sort_by_key(|m| (m.probe_row_idx, m.build_row_idx));
    v
}

// ---------------------------------------------------------------------
// kernel_correctness::single_threaded_inner_join_i64_keys_matches_naive
// ---------------------------------------------------------------------
#[test]
fn single_threaded_inner_join_i64_keys_matches_naive() {
    // Pseudo-random but deterministic — small enough to also pass for
    // a debug build, large enough to exercise grow() at least once.
    let n_build = 1_024;
    let n_probe = 2_048;
    let key_range = 256_i64; // ~4 build rows per key on average

    let build_keys: Vec<i64> = (0..n_build)
        .map(|i| (i as i64 * 7919) % key_range)
        .collect();
    let probe_keys: Vec<i64> = (0..n_probe)
        .map(|i| (i as i64 * 6151) % key_range)
        .collect();

    // ---- kernel ----
    let mut table = RobinHoodHashJoinI64Table::new();
    table.insert_batch(&build_keys, None, 0);
    let mut got = Vec::new();
    table.probe_batch(&probe_keys, None, 0, &mut got);

    // ---- reference ----
    let want = naive_inner_join(&build_keys, None, &probe_keys, None);

    assert_eq!(
        sorted(got),
        sorted(want),
        "kernel output does not match HashMap<i64, Vec<u32>> reference impl"
    );
}

// ---------------------------------------------------------------------
// kernel_correctness::kernel_handles_duplicate_keys
// ---------------------------------------------------------------------
#[test]
fn kernel_handles_duplicate_keys() {
    // Build side: 100 copies of key=42, 50 copies of key=7,
    // 1 copy of key=99.
    let mut build_keys: Vec<i64> = Vec::with_capacity(151);
    build_keys.extend(std::iter::repeat_n(42_i64, 100));
    build_keys.extend(std::iter::repeat_n(7_i64, 50));
    build_keys.push(99_i64);

    let probe_keys = vec![42_i64, 7_i64, 99_i64, 999_i64];

    let mut table = RobinHoodHashJoinI64Table::new();
    table.insert_batch(&build_keys, None, 0);

    let mut got = Vec::new();
    table.probe_batch(&probe_keys, None, 0, &mut got);

    // Probe row 0 (key=42) must emit 100 matches.
    let matches_for_42: Vec<u32> = got
        .iter()
        .filter(|m| m.probe_row_idx == 0)
        .map(|m| m.build_row_idx)
        .collect();
    assert_eq!(matches_for_42.len(), 100, "key=42 must emit 100 matches");
    // Build row indices for key=42 are 0..100.
    let mut sorted_42 = matches_for_42;
    sorted_42.sort();
    assert_eq!(sorted_42, (0u32..100).collect::<Vec<u32>>());

    // Probe row 1 (key=7) must emit 50 matches; build rows 100..150.
    let matches_for_7: Vec<u32> = got
        .iter()
        .filter(|m| m.probe_row_idx == 1)
        .map(|m| m.build_row_idx)
        .collect();
    assert_eq!(matches_for_7.len(), 50);
    let mut sorted_7 = matches_for_7;
    sorted_7.sort();
    assert_eq!(sorted_7, (100u32..150).collect::<Vec<u32>>());

    // Probe row 2 (key=99) must emit 1 match; build row 150.
    let matches_for_99: Vec<u32> = got
        .iter()
        .filter(|m| m.probe_row_idx == 2)
        .map(|m| m.build_row_idx)
        .collect();
    assert_eq!(matches_for_99, vec![150]);

    // Probe row 3 (key=999) — not in build → no matches.
    let matches_for_999_count = got.iter().filter(|m| m.probe_row_idx == 3).count();
    assert_eq!(matches_for_999_count, 0);
}

// ---------------------------------------------------------------------
// kernel_correctness::kernel_handles_null_keys
// ---------------------------------------------------------------------
#[test]
fn kernel_handles_null_keys() {
    // Build: rows 0..4 valid with keys 1, 2, 3, 4; row 4 is NULL with
    // a sentinel-valued key that would otherwise match probe row 0.
    let build_keys = vec![1_i64, 2, 3, 4, 99];
    let build_nulls = vec![true, true, true, true, false]; // row 4 is null

    // Probe: rows 0..4 keys are 99 (would match build row 4 *but* it's
    // null), 2, NULL (sentinel value 1 would otherwise match), 4, 5.
    let probe_keys = vec![99_i64, 2, 1, 4, 5];
    let probe_nulls = vec![true, true, false, true, true]; // row 2 is null

    let mut table = RobinHoodHashJoinI64Table::new();
    table.insert_batch(&build_keys, Some(&build_nulls), 0);

    let mut got = Vec::new();
    table.probe_batch(&probe_keys, Some(&probe_nulls), 0, &mut got);

    let want = naive_inner_join(
        &build_keys,
        Some(&build_nulls),
        &probe_keys,
        Some(&probe_nulls),
    );

    let sorted_got = sorted(got);
    let sorted_want = sorted(want);
    assert_eq!(
        sorted_got, sorted_want,
        "NULL-key handling diverges from reference impl"
    );

    // Spell out what the reference says:
    //   - Probe row 0 (key=99, valid): build row 4's key is 99 but the
    //     row is NULL, so no match. → 0 matches.
    //   - Probe row 1 (key=2, valid): build row 1 (key=2, valid). → 1 match.
    //   - Probe row 2 (NULL): no matches.
    //   - Probe row 3 (key=4, valid): build row 3 (key=4, valid). → 1 match.
    //   - Probe row 4 (key=5, valid): nothing in build. → 0 matches.
    assert_eq!(
        sorted_want,
        vec![
            ProbeMatch {
                probe_row_idx: 1,
                build_row_idx: 1
            },
            ProbeMatch {
                probe_row_idx: 3,
                build_row_idx: 3
            },
        ],
        "explicit expected pairs"
    );
}

// ---------------------------------------------------------------------
// kernel_correctness::row_idx_base_offsets_build_side_indices
//
// The build phase ingests in batches; each batch carries an absolute
// row_idx_base so the join can preserve the caller's row numbering
// across multi-batch builds.
// ---------------------------------------------------------------------
#[test]
fn row_idx_base_offsets_build_side_indices() {
    let batch_a = vec![1_i64, 2];
    let batch_b = vec![3_i64, 4];

    let mut table = RobinHoodHashJoinI64Table::new();
    table.insert_batch(&batch_a, None, 0); // build rows 0, 1
    table.insert_batch(&batch_b, None, 2); // build rows 2, 3

    let probe = vec![1_i64, 4];
    let mut got = Vec::new();
    table.probe_batch(&probe, None, 0, &mut got);

    let want = vec![
        ProbeMatch {
            probe_row_idx: 0,
            build_row_idx: 0,
        },
        ProbeMatch {
            probe_row_idx: 1,
            build_row_idx: 3,
        },
    ];
    assert_eq!(sorted(got), sorted(want));
}
