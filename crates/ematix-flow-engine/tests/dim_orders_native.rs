//! Gate for the native orders reduction — Q08's last dimension build off
//! DataFusion, a chain of semijoins plus a date-windowed bucket.
//!
//! `region 'AMERICA' → nation → customer → orders`, keeping orders whose
//! customer is in the AMERICA region and whose `o_orderdate` is in
//! [1995-01-01, 1996-12-31], bucketed 0 (1995) / 1 (1996). Checked against
//! an independent pyarrow oracle over SF1: the chain reduces to 1 region,
//! 5 nations, 29952 customers; 91179 qualifying orders (45630 / 45549),
//! order-key sum 273979458755, bucket-0 key sum 137136132715.

use std::path::PathBuf;

use ematix_flow_engine::dim::{
    collect_i64_keys_where_str_eq, collect_i64_where_i64_member, orders_semijoin_datebucket,
};

// Date32 days-since-epoch (1970-01-01 = 0): 1995-01-01 = 9131,
// 1996-01-01 = 9496, 1996-12-31 = 9861 (matches the Q6 kernel's 9131).
const D_LO: i32 = 9131;
const D_HI: i32 = 9861;
const D_SPLIT: i32 = 9496; // < split ⇒ 1995 (bucket 0), ≥ split ⇒ 1996 (bucket 1)

fn sf1(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("../../examples/tpch/data/sf1/{name}"))
}

#[test]
fn orders_reduction_matches_oracle() {
    for t in ["region", "nation", "customer", "orders"] {
        if !sf1(&format!("{t}.parquet")).exists() {
            eprintln!("SKIP orders_reduction_matches_oracle: {t}.parquet absent");
            return;
        }
    }

    // region 'AMERICA' → nation → customer semijoin chain.
    let regions =
        collect_i64_keys_where_str_eq(&sf1("region.parquet"), "r_regionkey", "r_name", "AMERICA")
            .expect("region");
    let nations = collect_i64_where_i64_member(
        &sf1("nation.parquet"),
        "n_nationkey",
        "n_regionkey",
        &regions,
    )
    .expect("nation");
    let custs = collect_i64_where_i64_member(
        &sf1("customer.parquet"),
        "c_custkey",
        "c_nationkey",
        &nations,
    )
    .expect("customer");
    let (keys, bucket) = orders_semijoin_datebucket(
        &sf1("orders.parquet"),
        &custs,
        "o_orderkey",
        "o_custkey",
        "o_orderdate",
        D_LO,
        D_HI,
        D_SPLIT,
    )
    .expect("orders");

    // The chain reduces as the oracle says.
    assert_eq!(regions.len(), 1, "AMERICA is one region");
    assert_eq!(nations.len(), 5, "5 nations in AMERICA");
    assert_eq!(custs.len(), 29_952, "AMERICA customers");

    // Orders reduction vs oracle.
    assert_eq!(keys.len(), 91_179, "qualifying orders");
    assert_eq!(bucket.len(), keys.len());
    assert!(bucket.iter().all(|&b| b == 0 || b == 1), "bucket is 0/1");
    assert_eq!(
        bucket.iter().filter(|&&b| b == 0).count(),
        45_630,
        "1995 orders"
    );
    assert_eq!(
        bucket.iter().filter(|&&b| b == 1).count(),
        45_549,
        "1996 orders"
    );
    assert_eq!(keys.iter().sum::<i64>(), 273_979_458_755, "order-key sum");
    let k0: i64 = keys
        .iter()
        .zip(&bucket)
        .filter(|&(_, &b)| b == 0)
        .map(|(&k, _)| k)
        .sum();
    assert_eq!(k0, 137_136_132_715, "bucket-0 key sum");
}
