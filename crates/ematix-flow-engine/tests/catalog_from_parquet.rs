//! Gate: `Catalog::register_parquet` derives a table's schema from the
//! parquet footer — names, leaf indices, engine logical types, decimal
//! scales, and nullability — with no hand-written column lists.

use std::path::PathBuf;

use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::vector::LogicalType;

fn data(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples")
        .join(rel)
}

#[test]
fn tpch_lineitem_all_required() {
    let mut c = Catalog::new();
    c.register_parquet("lineitem", data("tpch/data/sf1/lineitem.parquet"))
        .expect("register");
    let t = c.table("lineitem").expect("table");
    assert_eq!(t.columns.len(), 16);

    let ok = t.column("l_orderkey").expect("l_orderkey");
    assert_eq!((ok.leaf, ok.ty), (0, LogicalType::Int64));
    assert!(!ok.nullable);
    assert_eq!(ok.dec_scale, None);

    // TPC-H files store decimals as DOUBLE — no scaling on decode.
    let ep = t.column("l_extendedprice").expect("l_extendedprice");
    assert_eq!((ep.leaf, ep.ty), (5, LogicalType::Float64));
    assert_eq!(ep.dec_scale, None);

    let sd = t.column("l_shipdate").expect("l_shipdate");
    assert_eq!((sd.leaf, sd.ty), (10, LogicalType::Date32));

    let cm = t.column("l_comment").expect("l_comment");
    assert_eq!((cm.leaf, cm.ty), (15, LogicalType::Utf8));
}

#[test]
fn tpcds_store_sales_nullable_and_decimal() {
    let mut c = Catalog::new();
    c.register_parquet("store_sales", data("tpcds/data/sf1/store_sales.parquet"))
        .expect("register");
    let t = c.table("store_sales").expect("table");
    assert_eq!(t.columns.len(), 23);

    // Surrogate keys: INT64, nullable in TPC-DS.
    let dk = t.column("ss_sold_date_sk").expect("ss_sold_date_sk");
    assert_eq!((dk.leaf, dk.ty), (0, LogicalType::Int64));
    assert!(dk.nullable);

    // Measures: INT32-backed DECIMAL(7,2) → engine Float64 with a decode
    // scale of 2 (value / 10^2).
    let sp = t.column("ss_ext_sales_price").expect("ss_ext_sales_price");
    assert_eq!(sp.ty, LogicalType::Float64);
    assert_eq!(sp.dec_scale, Some(2));
    assert!(sp.nullable);
}

#[test]
fn tpcds_date_dim_dates_and_strings() {
    let mut c = Catalog::new();
    c.register_parquet("date_dim", data("tpcds/data/sf1/date_dim.parquet"))
        .expect("register");
    let t = c.table("date_dim").expect("table");

    let d = t.column("d_date").expect("d_date");
    assert_eq!(d.ty, LogicalType::Date32);

    let dow = t.column("d_day_name").expect("d_day_name");
    assert_eq!(dow.ty, LogicalType::Utf8);

    // Spark-written TPC-DS files declare every column `optional` — the
    // footer's word governs decode (def levels read), even for columns
    // that happen to contain no nulls.
    let sk = t.column("d_date_sk").expect("d_date_sk");
    assert_eq!(sk.ty, LogicalType::Int64);
    assert!(sk.nullable);
}
