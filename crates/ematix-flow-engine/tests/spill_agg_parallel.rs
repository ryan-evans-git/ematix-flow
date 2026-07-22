//! Parallel-spill wiring gate: the spilling `SUM(i64) GROUP BY i64`
//! aggregate, run through the row-group-parallel driver
//! ([`run_scan_pipeline`]) with a **per-worker** [`PartitionSpill`], must
//!
//!   1. actually spill under a tiny budget (proving the disk path is live),
//!      and
//!   2. return the exact same groups as a plain in-memory hashmap oracle —
//!      **bit-identical**, because integer SUM is order-independent.
//!
//! Together that proves the per-worker spill plus the bounded cross-worker
//! GRACE merge is correct at scale: parallelism and spilling change *how*
//! the sum is computed, never *what* it is. This is the driver-level
//! generalization of the single-threaded `tests/spill_agg.rs` gate.

use std::collections::HashMap;
use std::path::PathBuf;

use ematix_flow_engine::agg::SpillingSumSink;
use ematix_flow_engine::exec::{PushOp, run_scan_pipeline};
use ematix_flow_engine::scan_native::{NativeColKind, scan_row_groups};

fn sf1_lineitem() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpch/data/sf1/lineitem.parquet")
}

/// Leaf indices in TPC-H lineitem: `l_orderkey` (group key) and `l_partkey`
/// (the i64 measure). Both INT64, so the SUM is exact — 1.5M distinct
/// orderkeys makes this a genuinely high-cardinality group-by.
const ORDERKEY_LEAF: usize = 0;
const PARTKEY_LEAF: usize = 1;

/// The dead-simple reference: one in-memory hashmap, no spill machinery.
fn oracle(path: &std::path::Path) -> Vec<(i64, i64)> {
    let chunks = scan_row_groups(
        path,
        &[
            (ORDERKEY_LEAF, NativeColKind::I64),
            (PARTKEY_LEAF, NativeColKind::I64),
        ],
    )
    .expect("scan");
    let mut m: HashMap<i64, i64> = HashMap::new();
    for c in &chunks {
        let k = c.col(0).as_i64();
        let v = c.col(1).as_i64();
        c.sel.for_each(|i| {
            let i = i as usize;
            *m.entry(k[i]).or_insert(0) += v[i];
        });
    }
    let mut out: Vec<(i64, i64)> = m.into_iter().collect();
    out.sort_unstable_by_key(|&(k, _)| k);
    out
}

/// Run `SUM(l_partkey) GROUP BY l_orderkey` through the parallel driver with
/// the given per-worker byte budget; return the merged result plus the total
/// rows spilled across workers.
fn run_parallel(path: &std::path::Path, budget_bytes: usize) -> (Vec<(i64, i64)>, u64) {
    let ops: Vec<Box<dyn PushOp>> = vec![];
    // Decoded chunk column order is (orderkey, partkey) ⇒ in-chunk 0, 1.
    let cols = &[
        (ORDERKEY_LEAF, NativeColKind::I64),
        (PARTKEY_LEAF, NativeColKind::I64),
    ];
    let sinks = run_scan_pipeline(
        path,
        cols,
        &ops,
        || SpillingSumSink::new(budget_bytes, 6, 0, 1),
        4,
    )
    .expect("driver failed");
    let spilled: u64 = sinks.iter().map(|s| s.spilled_records()).sum();
    let got = SpillingSumSink::merge(sinks).expect("merge failed");
    (got, spilled)
}

#[test]
fn parallel_spilling_agg_matches_oracle() {
    let path = sf1_lineitem();
    if !path.exists() {
        eprintln!(
            "SKIP parallel_spilling_agg_matches_oracle: {} absent",
            path.display()
        );
        return;
    }

    let want = oracle(&path);
    assert_eq!(
        want.len(),
        1_500_000,
        "SF1 lineitem has 1.5M distinct orderkeys"
    );

    // A generous budget never spills; the bounded merge must still be exact.
    let (nospill, nospill_recs) = run_parallel(&path, usize::MAX);
    assert_eq!(nospill_recs, 0, "huge budget must not spill");
    assert_eq!(
        nospill, want,
        "parallel (no-spill) SUM must equal the in-memory oracle"
    );

    // A 1 MiB budget over 6M rows forces every worker down its disk path.
    let (spilled, spilled_recs) = run_parallel(&path, 1 << 20);
    assert!(spilled_recs > 0, "tiny budget must force spilling (got 0)");
    assert_eq!(
        spilled, want,
        "parallel spilled SUM must equal the in-memory oracle, bit-identical"
    );
}
