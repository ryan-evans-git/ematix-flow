//! Gate for the native part-probe reduction — the first of Q08's three
//! dimension builds moved off DataFusion.
//!
//! Scans SF1 `part.parquet` and collects `p_partkey WHERE p_type =
//! 'ECONOMY ANODIZED STEEL'` via the engine's own string decode + filter,
//! checked against an independent oracle (pyarrow over the same file):
//! 1451 matching rows, key checksum 145231383, keys distinct and in range.

use std::path::PathBuf;

use ematix_flow_engine::dim::collect_i64_keys_where_str_eq;

fn sf1_part() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpch/data/sf1/part.parquet")
}

#[test]
fn part_probe_matches_oracle() {
    let path = sf1_part();
    if !path.exists() {
        eprintln!("SKIP part_probe_matches_oracle: {} absent", path.display());
        return;
    }
    let keys =
        collect_i64_keys_where_str_eq(&path, "p_partkey", "p_type", "ECONOMY ANODIZED STEEL")
            .expect("native part reduction");

    // Independent oracle (pyarrow): count + checksum over the SAME predicate.
    assert_eq!(keys.len(), 1451, "match count");
    let checksum: i64 = keys.iter().sum();
    assert_eq!(checksum, 145_231_383, "p_partkey checksum");

    // p_partkey is a primary key → the survivors are distinct and bounded.
    let mut sorted = keys.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), keys.len(), "survivor keys must be distinct");
    assert_eq!(*sorted.first().unwrap(), 157, "min surviving key");
    assert_eq!(*sorted.last().unwrap(), 199_949, "max surviving key");
}
