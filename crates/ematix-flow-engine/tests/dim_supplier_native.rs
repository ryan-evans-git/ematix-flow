//! Gate for the native supplier reduction — Q08's second dimension build
//! off DataFusion, and the engine's join operator exercised on real data.
//!
//! `supplier ⋈ nation` on nationkey, carrying the nation's `n_name =
//! 'BRAZIL'` flag as the payload, built on the engine's `AdaptiveHashJoin`
//! (25-row nation build stays in memory). Checked against an independent
//! pyarrow oracle over SF1: all 10000 suppliers present, 397 flagged BRAZIL,
//! brazil-key checksum 1940760.

use std::path::PathBuf;

use ematix_flow_engine::dim::supplier_nation_flag;

fn sf1(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("../../examples/tpch/data/sf1/{name}"))
}

#[test]
fn supplier_is_brazil_matches_oracle() {
    let sup = sf1("supplier.parquet");
    let nat = sf1("nation.parquet");
    if !sup.exists() || !nat.exists() {
        eprintln!("SKIP supplier_is_brazil_matches_oracle: data absent");
        return;
    }
    let (keys, pay) = supplier_nation_flag(&sup, &nat, "BRAZIL").expect("supplier reduction");

    // Inner join over a complete FK → every supplier appears exactly once.
    assert_eq!(keys.len(), 10_000, "all suppliers present");
    assert_eq!(pay.len(), keys.len(), "one flag per supplier");
    assert_eq!(keys.iter().sum::<i64>(), 50_005_000, "all suppkeys present");

    // The BRAZIL flag: exactly 397 suppliers, and the RIGHT ones (checksum).
    assert!(pay.iter().all(|&p| p == 0 || p == 1), "flag is 0 or 1");
    let brazil_count = pay.iter().filter(|&&p| p == 1).count();
    assert_eq!(brazil_count, 397, "brazil supplier count");
    let brazil_key_sum: i64 = keys
        .iter()
        .zip(&pay)
        .filter(|&(_, &p)| p == 1)
        .map(|(&k, _)| k)
        .sum();
    assert_eq!(brazil_key_sum, 1_940_760, "brazil suppkey checksum");
}
