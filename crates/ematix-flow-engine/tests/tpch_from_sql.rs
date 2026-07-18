//! P4 gate: **canonical TPC-H query texts** — read verbatim from
//! `examples/tpch/queries/*.sql`, the same files production runs — planned
//! and executed on the clean-room engine over SF-1, each against an
//! independently computed pyarrow/python oracle.
//!
//! Q6 and Q08 got here first (their own gates); this file adds Q1, Q3, Q5,
//! Q10, Q12, Q14, Q19 — together exercising string/float group keys,
//! ORDER BY (asc/desc, multi-key), interval date arithmetic, IN lists,
//! LIKE, CASE, OR-factoring (Q19's join condition lives inside each OR
//! branch), join cycles (Q5's customer-nation = supplier-nation residual
//! equality), and payload chains (Q10's customer columns through orders).
//!
//! Dates in results are Date32 day numbers (display formatting is a
//! result-surface concern, not a planning one).

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::expr::ScalarValue;
use ematix_flow_engine::plan::{QueryResult, execute};
use ematix_flow_engine::vector::LogicalType;

fn repo(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(rel)
}

fn sf1(table: &str) -> PathBuf {
    repo(&format!("examples/tpch/data/sf1/{table}.parquet"))
}

/// The full TPC-H schema (real parquet leaf indices) — harness-registered.
fn catalog() -> Catalog {
    use LogicalType::*;
    let mut c = Catalog::new();
    c.register_table(
        "lineitem",
        sf1("lineitem"),
        &[
            ("l_orderkey", 0, Int64),
            ("l_partkey", 1, Int64),
            ("l_suppkey", 2, Int64),
            ("l_linenumber", 3, Int32),
            ("l_quantity", 4, Float64),
            ("l_extendedprice", 5, Float64),
            ("l_discount", 6, Float64),
            ("l_tax", 7, Float64),
            ("l_returnflag", 8, Utf8),
            ("l_linestatus", 9, Utf8),
            ("l_shipdate", 10, Date32),
            ("l_commitdate", 11, Date32),
            ("l_receiptdate", 12, Date32),
            ("l_shipinstruct", 13, Utf8),
            ("l_shipmode", 14, Utf8),
        ],
    );
    c.register_table(
        "orders",
        sf1("orders"),
        &[
            ("o_orderkey", 0, Int64),
            ("o_custkey", 1, Int64),
            ("o_totalprice", 3, Float64),
            ("o_orderdate", 4, Date32),
            ("o_orderpriority", 5, Utf8),
            ("o_shippriority", 7, Int32),
        ],
    );
    c.register_table(
        "customer",
        sf1("customer"),
        &[
            ("c_custkey", 0, Int64),
            ("c_name", 1, Utf8),
            ("c_address", 2, Utf8),
            ("c_nationkey", 3, Int64),
            ("c_phone", 4, Utf8),
            ("c_acctbal", 5, Float64),
            ("c_mktsegment", 6, Utf8),
            ("c_comment", 7, Utf8),
        ],
    );
    c.register_table(
        "part",
        sf1("part"),
        &[
            ("p_partkey", 0, Int64),
            ("p_brand", 3, Utf8),
            ("p_type", 4, Utf8),
            ("p_size", 5, Int32),
            ("p_container", 6, Utf8),
        ],
    );
    c.register_table(
        "supplier",
        sf1("supplier"),
        &[
            ("s_suppkey", 0, Int64),
            ("s_nationkey", 3, Int64),
            ("s_comment", 6, Utf8),
        ],
    );
    c.register_table(
        "partsupp",
        sf1("partsupp"),
        &[
            ("ps_partkey", 0, Int64),
            ("ps_suppkey", 1, Int64),
            ("ps_availqty", 2, Int32),
            ("ps_supplycost", 3, Float64),
        ],
    );
    c.register_table(
        "nation",
        sf1("nation"),
        &[
            ("n_nationkey", 0, Int64),
            ("n_name", 1, Utf8),
            ("n_regionkey", 2, Int64),
        ],
    );
    c.register_table(
        "region",
        sf1("region"),
        &[("r_regionkey", 0, Int64), ("r_name", 1, Utf8)],
    );
    c
}

/// Load a canonical query file (strip the trailing `;`).
fn canonical(qname: &str) -> Option<String> {
    let p = repo(&format!("examples/tpch/queries/{qname}.sql"));
    let sql = std::fs::read_to_string(p).ok()?;
    Some(sql.trim().trim_end_matches(';').to_string())
}

/// Bind + execute a canonical query, or None (skip) if data/files absent.
fn run(qname: &str) -> Option<QueryResult> {
    let tables = [
        "lineitem", "orders", "customer", "part", "supplier", "nation", "region", "partsupp",
    ];
    if tables.iter().any(|t| !sf1(t).exists()) {
        eprintln!("SKIP {qname}: SF1 data absent");
        return None;
    }
    let sql = canonical(qname)?;
    let q = bind_sql(&sql, &catalog()).unwrap_or_else(|e| panic!("{qname} bind: {e}"));
    Some(execute(&q).unwrap_or_else(|e| panic!("{qname} execute: {e}")))
}

fn f(v: &ScalarValue) -> f64 {
    match v {
        ScalarValue::Float64(x) => *x,
        ScalarValue::Int64(x) => *x as f64,
        other => panic!("expected numeric, got {other:?}"),
    }
}

fn i(v: &ScalarValue) -> i64 {
    match v {
        ScalarValue::Int64(x) => *x,
        other => panic!("expected Int64, got {other:?}"),
    }
}

fn s(v: &ScalarValue) -> &str {
    match v {
        ScalarValue::Utf8(x) => x,
        other => panic!("expected Utf8, got {other:?}"),
    }
}

fn close(got: f64, want: f64, what: &str) {
    let rel = if want == 0.0 {
        got.abs()
    } else {
        (got - want).abs() / want.abs()
    };
    assert!(rel < 1e-9, "{what}: {got} != oracle {want} (rel {rel:.3e})");
}

#[test]
fn q01_pricing_summary() {
    let Some(r) = run("q01") else { return };
    // 4 groups ordered by (l_returnflag, l_linestatus); columns:
    // rf, ls, sum_qty, sum_base_price, sum_disc_price, sum_charge,
    // avg_qty, avg_price, avg_disc, count_order.
    assert_eq!(r.rows.len(), 4);
    type Q1Row<'a> = (&'a str, &'a str, f64, f64, f64, f64, f64, f64, f64, i64);
    let want: [Q1Row; 4] = [
        (
            "A",
            "F",
            37734107.0,
            56586554400.729996,
            53758257134.87,
            55909065222.82769,
            25.522005853257337,
            38273.12973462167,
            0.049985295838397614,
            1478493,
        ),
        (
            "N",
            "F",
            991417.0,
            1487504710.38,
            1413082168.0541,
            1469649223.194375,
            25.516471920522985,
            38284.4677608483,
            0.050093426674216304,
            38854,
        ),
        (
            "N",
            "O",
            74476040.0,
            111701729697.74,
            106118230307.60559,
            110367043872.49701,
            25.50222676958499,
            38249.11798890827,
            0.049996586053704085,
            2920374,
        ),
        (
            "R",
            "F",
            37719753.0,
            56568041380.9,
            53741292684.604004,
            55889619119.83193,
            25.50579361269077,
            38250.85462609966,
            0.05000940583012706,
            1478870,
        ),
    ];
    for (row, w) in r.rows.iter().zip(&want) {
        assert_eq!(s(&row[0]), w.0);
        assert_eq!(s(&row[1]), w.1);
        close(f(&row[2]), w.2, "sum_qty");
        close(f(&row[3]), w.3, "sum_base_price");
        close(f(&row[4]), w.4, "sum_disc_price");
        close(f(&row[5]), w.5, "sum_charge");
        close(f(&row[6]), w.6, "avg_qty");
        close(f(&row[7]), w.7, "avg_price");
        close(f(&row[8]), w.8, "avg_disc");
        assert_eq!(i(&row[9]), w.9, "count_order");
    }
}

#[test]
fn q03_shipping_priority() {
    let Some(r) = run("q03") else { return };
    // 11,620 groups ordered by revenue desc, o_orderdate; top-5 revenues
    // are distinct so the head is deterministic. o_orderdate is a Date32
    // day number.
    assert_eq!(r.rows.len(), 11620);
    let want = [
        (2456423i64, 406181.0111f64, 9194i64, 0i64),
        (3459808, 405838.69889999996, 9193, 0),
        (492164, 390324.061, 9180, 0),
        (1188320, 384537.9359, 9198, 0),
        (2435712, 378673.05580000003, 9187, 0),
    ];
    for (row, w) in r.rows.iter().take(5).zip(&want) {
        assert_eq!(i(&row[0]), w.0, "l_orderkey");
        close(f(&row[1]), w.1, "revenue");
        assert_eq!(i(&row[2]), w.2, "o_orderdate");
        assert_eq!(i(&row[3]), w.3, "o_shippriority");
    }
}

#[test]
fn q05_local_supplier_volume() {
    let Some(r) = run("q05") else { return };
    // The join cycle (c_nationkey = s_nationkey) resolves as a residual
    // post-join equality. 5 ASIA nations, revenue desc.
    let want = [
        ("INDONESIA", 55502041.169699945),
        ("VIETNAM", 55295086.99669996),
        ("CHINA", 53724494.25659997),
        ("INDIA", 52035512.00020005),
        ("JAPAN", 45410175.695400015),
    ];
    assert_eq!(r.rows.len(), 5);
    for (row, w) in r.rows.iter().zip(&want) {
        assert_eq!(s(&row[0]), w.0);
        close(f(&row[1]), w.1, w.0);
    }
}

#[test]
fn q10_returned_items() {
    let Some(r) = run("q10") else { return };
    // 37,967 customers, revenue desc (top-5 distinct). Exercises float
    // (c_acctbal) and string group keys through the payload chain.
    assert_eq!(r.rows.len(), 37967);
    // Columns: c_custkey, c_name, revenue, c_acctbal, n_name, …
    let want = [
        (
            57040i64,
            "Customer#000057040",
            734235.2455f64,
            632.87f64,
            "JAPAN",
        ),
        (143347, "Customer#000143347", 721002.6948, 2557.47, "EGYPT"),
        (60838, "Customer#000060838", 679127.3077, 2454.77, "BRAZIL"),
        (
            101998,
            "Customer#000101998",
            637029.5666999999,
            3790.89,
            "UNITED KINGDOM",
        ),
        (
            125341,
            "Customer#000125341",
            633508.0859999999,
            4983.51,
            "GERMANY",
        ),
    ];
    for (row, w) in r.rows.iter().take(5).zip(&want) {
        assert_eq!(i(&row[0]), w.0, "c_custkey");
        assert_eq!(s(&row[1]), w.1, "c_name");
        close(f(&row[2]), w.2, "revenue");
        close(f(&row[3]), w.3, "c_acctbal");
        assert_eq!(s(&row[4]), w.4, "n_name");
    }
}

#[test]
fn q12_shipping_modes() {
    let Some(r) = run("q12") else { return };
    // IN list + CASE over a string payload (o_orderpriority).
    assert_eq!(r.rows.len(), 2);
    let want = [("MAIL", 6202.0f64, 9324.0f64), ("SHIP", 6200.0, 9262.0)];
    for (row, w) in r.rows.iter().zip(&want) {
        assert_eq!(s(&row[0]), w.0);
        close(f(&row[1]), w.1, "high_line_count");
        close(f(&row[2]), w.2, "low_line_count");
    }
}

#[test]
fn q14_promotion_effect() {
    let Some(r) = run("q14") else { return };
    // LIKE 'PROMO%' inside CASE; 100 * sum/sum output projection.
    assert_eq!(r.rows.len(), 1);
    close(f(&r.rows[0][0]), 16.380778626395674, "promo_revenue");
}

#[test]
fn q19_discounted_revenue() {
    let Some(r) = run("q19") else { return };
    // OR-factoring: the join condition + shared filters hoist out of the
    // three OR branches; the residue evaluates post-join. 121 rows.
    assert_eq!(r.rows.len(), 1);
    close(f(&r.rows[0][0]), 3083843.0578, "revenue");
}

#[test]
fn q11_important_stock() {
    let Some(r) = run("q11") else { return };
    // Scalar subquery in HAVING (the 0.0001 threshold), value desc.
    assert_eq!(r.rows.len(), 1048);
    let want = [
        (129760i64, 17538456.86f64),
        (166726, 16503353.92),
        (191287, 16474801.969999999),
    ];
    for (row, w) in r.rows.iter().take(3).zip(&want) {
        assert_eq!(i(&row[0]), w.0, "ps_partkey");
        close(f(&row[1]), w.1, "value");
    }
}

#[test]
fn q16_parts_supplier_relationship() {
    let Some(r) = run("q16") else { return };
    // NOT IN (subquery), NOT LIKE, int IN-list, count(distinct), 4-key
    // ORDER BY (fully deterministic).
    assert_eq!(r.rows.len(), 18314);
    let want = [
        ("Brand#41", "MEDIUM BRUSHED TIN", 3i64, 28i64),
        ("Brand#54", "STANDARD BRUSHED COPPER", 14, 27),
        ("Brand#11", "STANDARD BRUSHED TIN", 23, 24),
        ("Brand#11", "STANDARD BURNISHED BRASS", 36, 24),
    ];
    for (row, w) in r.rows.iter().take(4).zip(&want) {
        assert_eq!(s(&row[0]), w.0, "p_brand");
        assert_eq!(s(&row[1]), w.1, "p_type");
        assert_eq!(i(&row[2]), w.2, "p_size");
        assert_eq!(i(&row[3]), w.3, "supplier_cnt");
    }
}

#[test]
fn q18_large_volume_customer() {
    let Some(r) = run("q18") else { return };
    // IN (grouped HAVING subquery) -> membership set; payload chain
    // customer->orders; 57 qualifying orders.
    assert_eq!(r.rows.len(), 57);
    let want = [
        (
            "Customer#000128120",
            128120i64,
            4722021i64,
            8862i64,
            544089.09f64,
            323.0f64,
        ),
        (
            "Customer#000144617",
            144617,
            3043270,
            9904,
            530604.44,
            317.0,
        ),
        ("Customer#000013940", 13940, 2232932, 9964, 522720.61, 304.0),
    ];
    for (row, w) in r.rows.iter().take(3).zip(&want) {
        assert_eq!(s(&row[0]), w.0, "c_name");
        assert_eq!(i(&row[1]), w.1, "c_custkey");
        assert_eq!(i(&row[2]), w.2, "o_orderkey");
        assert_eq!(i(&row[3]), w.3, "o_orderdate");
        close(f(&row[4]), w.4, "o_totalprice");
        close(f(&row[5]), w.5, "sum_qty");
    }
}
