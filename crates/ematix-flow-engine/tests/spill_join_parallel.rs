//! Two-input build→probe driver gate: a spilling inner equi-join whose
//! **both** sides come from parallel parquet scans must (a) actually spill
//! under a tiny budget and (b) produce exactly the same match multiset as a
//! plain in-memory hashmap oracle.
//!
//! The join is `orders ⋈ lineitem` on `orderkey` — build = orders
//! (`o_orderkey → o_custkey`), probe = lineitem (`l_orderkey → l_partkey`).
//! Every lineitem row matches its one parent order, so the match count is a
//! structural invariant (6,001,215) an oracle can't fudge. The result is
//! checked by an order-independent checksum (count + per-column sums), so
//! the parallel/spill path's changed emit order can't hide a wrong answer.
//!
//! What this proves beyond the single-threaded `tests/spill_join.rs`: the
//! **cross-worker merge**. A given orderkey's build row (decoded by some
//! build-scan worker) and its probe rows (decoded by some probe-scan
//! worker) land in *different* per-worker joins; the bounded per-partition
//! merge must still bring them together.

use std::collections::HashMap;
use std::path::PathBuf;

use ematix_flow_engine::exec::run_join_pipeline;
use ematix_flow_engine::scan_native::{NativeColKind, scan_row_groups};

fn sf1(table: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(format!("../../examples/tpch/data/sf1/{table}.parquet"))
}

/// Order-independent fingerprint of a match multiset: a count plus per-column
/// sums (i128 so nothing overflows). Two joins that emit the same matches in
/// any order have the same checksum.
#[derive(Default, Debug, PartialEq, Eq)]
struct Checksum {
    count: u64,
    key_sum: i128,
    build_sum: i128,
    probe_sum: i128,
}
impl Checksum {
    fn add(&mut self, key: i64, build_pay: i64, probe_pay: i64) {
        self.count += 1;
        self.key_sum += key as i128;
        self.build_sum += build_pay as i128;
        self.probe_sum += probe_pay as i128;
    }
}

/// The dead-simple reference: one in-memory hashmap `orderkey → custkey` from
/// orders, streamed against lineitem, no spill machinery.
fn oracle(orders: &std::path::Path, lineitem: &std::path::Path) -> Checksum {
    let ob = scan_row_groups(orders, &[(0, NativeColKind::I64), (1, NativeColKind::I64)])
        .expect("scan orders");
    let mut ht: HashMap<i64, i64> = HashMap::new();
    for c in &ob {
        let k = c.col(0).as_i64();
        let v = c.col(1).as_i64();
        c.sel.for_each(|i| {
            let i = i as usize;
            ht.insert(k[i], v[i]); // o_orderkey is unique.
        });
    }
    let lb = scan_row_groups(
        lineitem,
        &[(0, NativeColKind::I64), (1, NativeColKind::I64)],
    )
    .expect("scan lineitem");
    let mut cs = Checksum::default();
    for c in &lb {
        let k = c.col(0).as_i64();
        let p = c.col(1).as_i64();
        c.sel.for_each(|i| {
            let i = i as usize;
            if let Some(&cust) = ht.get(&k[i]) {
                cs.add(k[i], cust, p[i]);
            }
        });
    }
    cs
}

/// Run `orders ⋈ lineitem` through the two-input driver with the given
/// per-worker byte budget; return the match checksum and total rows spilled.
fn run_parallel(
    orders: &std::path::Path,
    lineitem: &std::path::Path,
    budget_bytes: usize,
) -> (Checksum, u64) {
    let mut cs = Checksum::default();
    let stats = run_join_pipeline(
        orders,
        &[(0, NativeColKind::I64), (1, NativeColKind::I64)],
        0, // build key   = o_orderkey (in-chunk col 0)
        1, // build payload = o_custkey (in-chunk col 1)
        lineitem,
        &[(0, NativeColKind::I64), (1, NativeColKind::I64)],
        0, // probe key   = l_orderkey
        1, // probe payload = l_partkey
        budget_bytes,
        6, // 64 partitions
        4, // worker threads
        |key, build_pay, probe_pay| cs.add(key, build_pay, probe_pay),
    )
    .expect("join driver failed");
    (cs, stats.build_spilled + stats.probe_spilled)
}

#[test]
fn parallel_spilling_join_matches_oracle() {
    let orders = sf1("orders");
    let lineitem = sf1("lineitem");
    if !orders.exists() || !lineitem.exists() {
        eprintln!("SKIP parallel_spilling_join_matches_oracle: SF1 orders/lineitem absent");
        return;
    }

    let want = oracle(&orders, &lineitem);
    assert_eq!(
        want.count, 6_001_215,
        "every lineitem row joins its one parent order"
    );

    // A generous budget never spills; the cross-worker merge must still match.
    let (nospill, nospill_recs) = run_parallel(&orders, &lineitem, usize::MAX);
    assert_eq!(nospill_recs, 0, "huge budget must not spill");
    assert_eq!(
        nospill, want,
        "parallel (no-spill) join must equal the in-memory oracle"
    );

    // A 4 MiB per-worker budget over 7.5M total rows forces both sides down
    // their disk paths, in different workers.
    let (spilled, spilled_recs) = run_parallel(&orders, &lineitem, 4 << 20);
    assert!(spilled_recs > 0, "tiny budget must force spilling (got 0)");
    assert_eq!(
        spilled, want,
        "parallel spilled join must equal the in-memory oracle"
    );
}
