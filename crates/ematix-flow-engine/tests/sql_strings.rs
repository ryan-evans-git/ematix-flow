//! Stage-C gate: strings end-to-end from SQL — a Utf8 column decodes
//! (routed through the stock low-level reader), a string-equality filter
//! evaluates, and the count matches the known part-reduction oracle from
//! the hand-built Q08 dim work: **1,451** parts of type
//! 'ECONOMY ANODIZED STEEL' at SF-1 (pyarrow-verified there).

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::expr::ScalarValue;
use ematix_flow_engine::plan::execute;
use ematix_flow_engine::vector::LogicalType;

fn sf1_part() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpch/data/sf1/part.parquet")
}

#[test]
fn string_filter_from_sql() {
    let path = sf1_part();
    if !path.exists() {
        eprintln!("SKIP string_filter_from_sql: {} absent", path.display());
        return;
    }
    let mut catalog = Catalog::new();
    catalog.register_table(
        "part",
        &path,
        &[
            ("p_partkey", 0, LogicalType::Int64),
            ("p_type", 4, LogicalType::Utf8),
        ],
    );
    let sql = "select count(*) as n, sum(p_partkey) as ksum from part \
               where p_type = 'ECONOMY ANODIZED STEEL'";
    let q = bind_sql(sql, &catalog).expect("bind failed");
    let result = execute(&q).expect("execute failed");

    let row = &result.rows[0];
    assert_eq!(
        row[0],
        ScalarValue::Int64(1451),
        "the hand-built dim gate's part count"
    );
    // Key checksum from the same pyarrow oracle as tests/dim_part_native.rs.
    assert_eq!(row[1], ScalarValue::Float64(145_231_383.0));
}
