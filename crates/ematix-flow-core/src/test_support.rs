//! Test-only TPC-H mini fixture.
//!
//! Generates a tiny, schema-valid TPC-H dataset (lineitem / part /
//! orders / customer / supplier / nation / region / partsupp) inside
//! a process-scoped tempdir. Used as a fallback so the existing
//! integration tests that previously skipped without
//! `examples/tpch/data/sf1/` populated can now exercise the plan-
//! rewrite / oracle paths in CI.
//!
//! Resolution order in the call sites:
//!   1. `$TPCH_DATA_DIR` (developer override)
//!   2. `examples/tpch/data/sf1/` (real SF=1, used by benches / local)
//!   3. `tpch_mini_dir()` (this module — synthetic fallback)
//!
//! Tests with hard-coded SF=1 cardinality assertions (e.g.
//! `assert_eq!(num_rows, 6_001_215)`, `num_row_groups() == 6`) gate
//! on `is_real_sf1()` so they keep skipping when the fallback is in
//! play — only the SF=1 cardinalities are correct for the real data.
//! Every other test runs against the mini fixture and gets the same
//! equivalence guarantees the SF=1 path provides.
//!
//! Sizing: ~100 rows per table arranged into 2 row groups for lineitem
//! and part (so multi-RG-pruning paths exercise more than one chunk).
//! Foreign keys are dense and consistent so joins find matching rows.
//! Predicate-window matches are seeded for Q1 (4 returnflag/linestatus
//! groups), Q3 (BUILDING customers + matching orders + shipdate >
//! 1995-03-15), Q5 (ASIA region chain), Q6 (shipdate in 1994,
//! discount 0.05-0.07, qty < 24), Q12 (MAIL/SHIP shipmode), Q14
//! (PROMO p_type with shipdate in 1995-09).

use std::path::PathBuf;
use std::sync::OnceLock;

use arrow_array::{Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::sync::Arc;
use tempfile::TempDir;

/// Returns the path to a tempdir populated with mini TPC-H parquet
/// files. Built lazily once per process; the `TempDir` is held in a
/// `OnceLock` so it survives until process exit, at which point its
/// `Drop` removes the directory.
pub fn tpch_mini_dir() -> &'static str {
    static FIXTURE: OnceLock<MiniFixture> = OnceLock::new();
    let f = FIXTURE.get_or_init(MiniFixture::build);
    &f.path_str
}

/// Returns true when `$TPCH_DATA_DIR` or `examples/tpch/data/sf1`
/// resolves — i.e. the *real* SF=1 dataset is on disk. The mini
/// fixture is intentionally tiny and cannot satisfy hard-coded SF=1
/// cardinality assertions (`6_001_215` rows, `6` row groups, etc.);
/// those tests gate on this helper so they keep skipping under the
/// fallback.
pub fn is_real_sf1() -> bool {
    if let Ok(s) = std::env::var("TPCH_DATA_DIR") {
        if PathBuf::from(s).join("lineitem.parquet").exists() {
            return true;
        }
    }
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let p = manifest
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.join("examples/tpch/data/sf1/lineitem.parquet"));
    p.map(|p| p.exists()).unwrap_or(false)
}

struct MiniFixture {
    _tmp: TempDir,
    path_str: String,
}

impl MiniFixture {
    fn build() -> Self {
        let tmp = TempDir::new().expect("create tempdir for tpch mini fixture");
        let dir = tmp.path().to_path_buf();
        write_lineitem(&dir);
        write_part(&dir);
        write_orders(&dir);
        write_customer(&dir);
        write_supplier(&dir);
        write_nation(&dir);
        write_region(&dir);
        write_partsupp(&dir);
        let path_str = dir.to_string_lossy().into_owned();
        Self {
            _tmp: tmp,
            path_str,
        }
    }
}

/// Write a RecordBatch to parquet with multiple row groups by setting
/// `max_row_group_size` smaller than the batch length.
fn write_parquet_multi_rg(path: PathBuf, batch: RecordBatch, max_rg: usize) {
    let file = File::create(&path).unwrap_or_else(|e| panic!("create {path:?}: {e}"));
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(max_rg))
        .build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
}

// ----- lineitem -------------------------------------------------------

fn write_lineitem(dir: &std::path::Path) {
    // 280 rows across 14 row groups (max_row_group_size=20). Why 14:
    // matches the test session's `target_partitions=14`, so the
    // DataFusion planner doesn't synthesise an extra `RoundRobinBatch`
    // repartition between the Q1/Q6 CSE-projection and the scan — a
    // repartition there breaks the rule-matcher's body-walker (it
    // expects `FilterExec` directly under the CSE projection, like
    // SF=1 naturally produces). Why 280: 14 RGs × 20 rows/RG keeps
    // the fixture fast to build.
    //
    // Cycle returnflag/linestatus through the 4 TPC-H pairs so Q1
    // yields 4 groups. Shipdates span 1992-1998 with deliberate hits
    // in [1994-01-01,1995-01-01), [1995-09-01,1995-10-01), and
    // >1995-03-15 for Q3/Q6/pruning tests. Discounts cycle through
    // [0.00..0.09] so some rows fall in the Q6 [0.05,0.07] window;
    // quantities cycle 1..50 so the stats min/max test (which only
    // gates on SF=1 dataset) sees a representative range.
    let n: usize = 280;
    let l_returnflag_vals = ["N", "N", "A", "R"];
    let l_linestatus_vals = ["O", "F", "F", "F"];
    // Length 4 (coprime with shipdate-cycle 7) so (shipmode × shipdate)
    // covers all 28 combos. Q12 needs MAIL and SHIP rows whose
    // receiptdate also falls in the predicate window — this cycle
    // guarantees both shipmodes co-occur with the in-window shipdates.
    let shipmodes = ["MAIL", "SHIP", "TRUCK", "AIR"];
    let shipinstructs = [
        "DELIVER IN PERSON",
        "TAKE BACK RETURN",
        "NONE",
        "COLLECT COD",
    ];

    let mut l_orderkey = Vec::<i64>::with_capacity(n);
    let mut l_partkey = Vec::<i64>::with_capacity(n);
    let mut l_suppkey = Vec::<i64>::with_capacity(n);
    let mut l_linenumber = Vec::<i32>::with_capacity(n);
    let mut l_quantity = Vec::<f64>::with_capacity(n);
    let mut l_extendedprice = Vec::<f64>::with_capacity(n);
    let mut l_discount = Vec::<f64>::with_capacity(n);
    let mut l_tax = Vec::<f64>::with_capacity(n);
    let mut l_returnflag = Vec::<&str>::with_capacity(n);
    let mut l_linestatus = Vec::<&str>::with_capacity(n);
    let mut l_shipdate = Vec::<i32>::with_capacity(n);
    let mut l_commitdate = Vec::<i32>::with_capacity(n);
    let mut l_receiptdate = Vec::<i32>::with_capacity(n);
    let mut l_shipinstruct = Vec::<&str>::with_capacity(n);
    let mut l_shipmode = Vec::<&str>::with_capacity(n);
    let mut l_comment = Vec::<String>::with_capacity(n);

    // Shipdate seeds: each entry is a Date32 (days from 1970-01-01).
    // 1994-06-15 = 8931  (Q6 window: 1994-01-01..1995-01-01 = 8766..9131)
    // 1995-04-01 = 9221  (> 1995-03-15 for Q3 lineitem; Q12 window 1994..1995)
    // 1995-09-15 = 9388  (in [9374, 9404) Q14/pruning window)
    // 1996-08-01 = 9709
    // 1997-03-01 = 9921
    // 1998-09-01 = 10470 (<= 1998-09-02 — passes Q1 filter)
    // 1992-06-01 = 8188
    //
    // Length 7 (coprime with orderkey-cycle 20 and partkey-cycle 30)
    // so the (orderkey × shipdate) cross-cycle covers every
    // (orderkey, shipdate) pair across 140 rows — guarantees Q3/Q5
    // find a non-empty join result regardless of which BUILDING /
    // ASIA chains end up filtered.
    let shipdate_pool: [i32; 7] = [8931, 9221, 9388, 9709, 9921, 10470, 8188];

    for i in 0..n {
        let orderkey = ((i % 20) as i64) + 1; // 20 distinct orders
        let partkey = ((i % 30) as i64) + 1; // 30 distinct parts
        let suppkey = ((i % 10) as i64) + 1; // 10 distinct suppliers
        l_orderkey.push(orderkey);
        l_partkey.push(partkey);
        l_suppkey.push(suppkey);
        l_linenumber.push(((i % 7) as i32) + 1);
        // quantity cycles 1..=50
        let q = ((i % 50) as f64) + 1.0;
        l_quantity.push(q);
        l_extendedprice.push(1000.0 + (i as f64) * 13.5);
        // Discount cycles 0.00..0.09; lands in [0.05,0.07] for i%10 in {5,6,7}.
        l_discount.push(((i % 10) as f64) * 0.01);
        l_tax.push(((i % 8) as f64) * 0.01);
        l_returnflag.push(l_returnflag_vals[i % 4]);
        l_linestatus.push(l_linestatus_vals[i % 4]);
        let sd = shipdate_pool[i % shipdate_pool.len()];
        l_shipdate.push(sd);
        l_commitdate.push(sd + 30);
        l_receiptdate.push(sd + 60);
        l_shipinstruct.push(shipinstructs[i % shipinstructs.len()]);
        l_shipmode.push(shipmodes[i % shipmodes.len()]);
        l_comment.push(format!("lineitem comment {i:03}"));
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("l_linenumber", DataType::Int32, false),
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
        Field::new("l_shipdate", DataType::Date32, false),
        Field::new("l_commitdate", DataType::Date32, false),
        Field::new("l_receiptdate", DataType::Date32, false),
        Field::new("l_shipinstruct", DataType::Utf8, false),
        Field::new("l_shipmode", DataType::Utf8, false),
        Field::new("l_comment", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(l_orderkey)),
            Arc::new(Int64Array::from(l_partkey)),
            Arc::new(Int64Array::from(l_suppkey)),
            Arc::new(Int32Array::from(l_linenumber)),
            Arc::new(Float64Array::from(l_quantity)),
            Arc::new(Float64Array::from(l_extendedprice)),
            Arc::new(Float64Array::from(l_discount)),
            Arc::new(Float64Array::from(l_tax)),
            Arc::new(StringArray::from(l_returnflag)),
            Arc::new(StringArray::from(l_linestatus)),
            Arc::new(Date32Array::from(l_shipdate)),
            Arc::new(Date32Array::from(l_commitdate)),
            Arc::new(Date32Array::from(l_receiptdate)),
            Arc::new(StringArray::from(l_shipinstruct)),
            Arc::new(StringArray::from(l_shipmode)),
            Arc::new(StringArray::from(
                l_comment.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();

    // 280 rows / row group size — aim for >= 14 row groups so the
    // FastParquetExec scan produces at least target_partitions=14
    // partitions and the planner doesn't synthesise a
    // RoundRobinBatch repartition between the scan and the rule-
    // matcher's expected FilterExec body. ArrowWriter may pack rows
    // into fewer RGs than the bound suggests, so we set the bound
    // aggressively (14 = exactly the target; relies on flush_buffer
    // bound, hence the smaller-than-expected number).
    write_parquet_multi_rg(dir.join("lineitem.parquet"), batch, 14);
}

// ----- part -----------------------------------------------------------

fn write_part(dir: &std::path::Path) {
    // 30 parts to match the lineitem.l_partkey FK domain. Every 3rd
    // part has p_type starting with "PROMO" so Q14 finds matches.
    let n: usize = 30;
    let mut p_partkey = Vec::<i64>::with_capacity(n);
    let mut p_name = Vec::<String>::with_capacity(n);
    let mut p_mfgr = Vec::<String>::with_capacity(n);
    let mut p_brand = Vec::<String>::with_capacity(n);
    let mut p_type = Vec::<String>::with_capacity(n);
    let mut p_size = Vec::<i32>::with_capacity(n);
    let mut p_container = Vec::<String>::with_capacity(n);
    let mut p_retailprice = Vec::<f64>::with_capacity(n);
    let mut p_comment = Vec::<String>::with_capacity(n);

    for i in 1..=(n as i64) {
        p_partkey.push(i);
        p_name.push(format!("part-{i:03}"));
        p_mfgr.push(format!("Manufacturer#{}", (i % 5) + 1));
        p_brand.push(format!("Brand#{}", (i % 5) + 1));
        // Every 3rd part is PROMO-classed → ~33% of lineitem rows
        // will match Q14's PROMO predicate.
        let ptype = if (i % 3) == 0 {
            "PROMO BRUSHED COPPER".to_string()
        } else {
            "STANDARD POLISHED STEEL".to_string()
        };
        p_type.push(ptype);
        p_size.push(((i % 50) as i32) + 1);
        p_container.push(format!("SM CASE"));
        p_retailprice.push(900.0 + (i as f64) * 0.5);
        p_comment.push(format!("part comment {i}"));
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("p_partkey", DataType::Int64, false),
        Field::new("p_name", DataType::Utf8, false),
        Field::new("p_mfgr", DataType::Utf8, false),
        Field::new("p_brand", DataType::Utf8, false),
        Field::new("p_type", DataType::Utf8, false),
        Field::new("p_size", DataType::Int32, false),
        Field::new("p_container", DataType::Utf8, false),
        Field::new("p_retailprice", DataType::Float64, false),
        Field::new("p_comment", DataType::Utf8, false),
    ]));

    let to_arr = |v: &[String]| -> StringArray {
        StringArray::from(v.iter().map(|s| s.as_str()).collect::<Vec<_>>())
    };

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(p_partkey)),
            Arc::new(to_arr(&p_name)),
            Arc::new(to_arr(&p_mfgr)),
            Arc::new(to_arr(&p_brand)),
            Arc::new(to_arr(&p_type)),
            Arc::new(Int32Array::from(p_size)),
            Arc::new(to_arr(&p_container)),
            Arc::new(Float64Array::from(p_retailprice)),
            Arc::new(to_arr(&p_comment)),
        ],
    )
    .unwrap();

    // 30 rows / max_row_group_size=15 → 2 row groups.
    write_parquet_multi_rg(dir.join("part.parquet"), batch, 15);
}

// ----- orders ---------------------------------------------------------

fn write_orders(dir: &std::path::Path) {
    // 20 orders covering the lineitem.l_orderkey domain (1..=20).
    // o_orderdate < 1995-03-15 for Q3 (≈ days < 9204); cycle so half
    // are before and half after the cutoff. Q5 wants orderdate in
    // [1994,1995) so the 1994 dates seed that match too.
    let n: usize = 20;
    let orderdate_pool: [i32; 4] = [
        8766, // 1994-01-01 (Q5)
        9000, // 1994-08-23
        9204, // 1995-03-15 (Q3 cutoff; <= excluded by `<`, but Q5 lies before)
        9404, // 1995-10-01
    ];
    let priorities = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];

    let mut o_orderkey = Vec::<i64>::with_capacity(n);
    let mut o_custkey = Vec::<i64>::with_capacity(n);
    let mut o_orderstatus = Vec::<String>::with_capacity(n);
    let mut o_totalprice = Vec::<f64>::with_capacity(n);
    let mut o_orderdate = Vec::<i32>::with_capacity(n);
    let mut o_orderpriority = Vec::<String>::with_capacity(n);
    let mut o_clerk = Vec::<String>::with_capacity(n);
    let mut o_shippriority = Vec::<i32>::with_capacity(n);
    let mut o_comment = Vec::<String>::with_capacity(n);

    for i in 1..=(n as i64) {
        o_orderkey.push(i);
        // Customer 1..=15; ensure FK covers the 15-customer table.
        o_custkey.push(((i - 1) % 15) + 1);
        o_orderstatus.push(if i % 2 == 0 { "O" } else { "F" }.to_string());
        o_totalprice.push(10_000.0 + (i as f64) * 100.0);
        o_orderdate.push(orderdate_pool[(i as usize - 1) % orderdate_pool.len()]);
        o_orderpriority.push(priorities[(i as usize - 1) % priorities.len()].to_string());
        o_clerk.push(format!("Clerk#{i:05}"));
        o_shippriority.push(0);
        o_comment.push(format!("order comment {i}"));
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderstatus", DataType::Utf8, false),
        Field::new("o_totalprice", DataType::Float64, false),
        Field::new("o_orderdate", DataType::Date32, false),
        Field::new("o_orderpriority", DataType::Utf8, false),
        Field::new("o_clerk", DataType::Utf8, false),
        Field::new("o_shippriority", DataType::Int32, false),
        Field::new("o_comment", DataType::Utf8, false),
    ]));

    let to_arr = |v: &[String]| -> StringArray {
        StringArray::from(v.iter().map(|s| s.as_str()).collect::<Vec<_>>())
    };

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(o_orderkey)),
            Arc::new(Int64Array::from(o_custkey)),
            Arc::new(to_arr(&o_orderstatus)),
            Arc::new(Float64Array::from(o_totalprice)),
            Arc::new(Date32Array::from(o_orderdate)),
            Arc::new(to_arr(&o_orderpriority)),
            Arc::new(to_arr(&o_clerk)),
            Arc::new(Int32Array::from(o_shippriority)),
            Arc::new(to_arr(&o_comment)),
        ],
    )
    .unwrap();

    write_parquet_multi_rg(dir.join("orders.parquet"), batch, 10);
}

// ----- customer -------------------------------------------------------

fn write_customer(dir: &std::path::Path) {
    // 15 customers; mktsegment cycles through 5 TPC-H values including
    // BUILDING (every 5th customer) so Q3 finds matches.
    let n: usize = 15;
    let segments = [
        "BUILDING",
        "AUTOMOBILE",
        "FURNITURE",
        "HOUSEHOLD",
        "MACHINERY",
    ];

    let mut c_custkey = Vec::<i64>::with_capacity(n);
    let mut c_name = Vec::<String>::with_capacity(n);
    let mut c_address = Vec::<String>::with_capacity(n);
    let mut c_nationkey = Vec::<i64>::with_capacity(n);
    let mut c_phone = Vec::<String>::with_capacity(n);
    let mut c_acctbal = Vec::<f64>::with_capacity(n);
    let mut c_mktsegment = Vec::<String>::with_capacity(n);
    let mut c_comment = Vec::<String>::with_capacity(n);

    for i in 1..=(n as i64) {
        c_custkey.push(i);
        c_name.push(format!("Customer#{i:09}"));
        c_address.push(format!("addr {i}"));
        // nation 1..=5; supplier nationkey domain shares this so Q5
        // joins line up.
        c_nationkey.push(((i - 1) % 5) + 1);
        c_phone.push(format!("phone-{i:03}"));
        c_acctbal.push(500.0 + (i as f64) * 13.0);
        c_mktsegment.push(segments[(i as usize - 1) % segments.len()].to_string());
        c_comment.push(format!("customer comment {i}"));
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_address", DataType::Utf8, false),
        Field::new("c_nationkey", DataType::Int64, false),
        Field::new("c_phone", DataType::Utf8, false),
        Field::new("c_acctbal", DataType::Float64, false),
        Field::new("c_mktsegment", DataType::Utf8, false),
        Field::new("c_comment", DataType::Utf8, false),
    ]));

    let to_arr = |v: &[String]| -> StringArray {
        StringArray::from(v.iter().map(|s| s.as_str()).collect::<Vec<_>>())
    };

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(c_custkey)),
            Arc::new(to_arr(&c_name)),
            Arc::new(to_arr(&c_address)),
            Arc::new(Int64Array::from(c_nationkey)),
            Arc::new(to_arr(&c_phone)),
            Arc::new(Float64Array::from(c_acctbal)),
            Arc::new(to_arr(&c_mktsegment)),
            Arc::new(to_arr(&c_comment)),
        ],
    )
    .unwrap();

    write_parquet_multi_rg(dir.join("customer.parquet"), batch, 15);
}

// ----- supplier -------------------------------------------------------

fn write_supplier(dir: &std::path::Path) {
    // 10 suppliers covering the lineitem.l_suppkey domain (1..=10).
    let n: usize = 10;
    let mut s_suppkey = Vec::<i64>::with_capacity(n);
    let mut s_name = Vec::<String>::with_capacity(n);
    let mut s_address = Vec::<String>::with_capacity(n);
    let mut s_nationkey = Vec::<i64>::with_capacity(n);
    let mut s_phone = Vec::<String>::with_capacity(n);
    let mut s_acctbal = Vec::<f64>::with_capacity(n);
    let mut s_comment = Vec::<String>::with_capacity(n);

    for i in 1..=(n as i64) {
        s_suppkey.push(i);
        s_name.push(format!("Supplier#{i:09}"));
        s_address.push(format!("supplier-addr {i}"));
        s_nationkey.push(((i - 1) % 5) + 1);
        s_phone.push(format!("s-phone-{i:03}"));
        s_acctbal.push(1000.0 + (i as f64) * 17.0);
        s_comment.push(format!("supplier comment {i}"));
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("s_suppkey", DataType::Int64, false),
        Field::new("s_name", DataType::Utf8, false),
        Field::new("s_address", DataType::Utf8, false),
        Field::new("s_nationkey", DataType::Int64, false),
        Field::new("s_phone", DataType::Utf8, false),
        Field::new("s_acctbal", DataType::Float64, false),
        Field::new("s_comment", DataType::Utf8, false),
    ]));

    let to_arr = |v: &[String]| -> StringArray {
        StringArray::from(v.iter().map(|s| s.as_str()).collect::<Vec<_>>())
    };

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(s_suppkey)),
            Arc::new(to_arr(&s_name)),
            Arc::new(to_arr(&s_address)),
            Arc::new(Int64Array::from(s_nationkey)),
            Arc::new(to_arr(&s_phone)),
            Arc::new(Float64Array::from(s_acctbal)),
            Arc::new(to_arr(&s_comment)),
        ],
    )
    .unwrap();

    write_parquet_multi_rg(dir.join("supplier.parquet"), batch, 10);
}

// ----- nation ---------------------------------------------------------

fn write_nation(dir: &std::path::Path) {
    // 5 ASIA-region nations + 5 from other regions for breadth.
    // n_nationkey covers 1..=5 to match customer/supplier nationkey.
    // n_regionkey: 1=ASIA, 2=EUROPE, etc.
    let nations = [
        (1i64, "VIETNAM", 1i64),
        (2, "CHINA", 1),
        (3, "INDIA", 1),
        (4, "INDONESIA", 1),
        (5, "JAPAN", 1),
    ];
    let n_nationkey: Vec<i64> = nations.iter().map(|(k, _, _)| *k).collect();
    let n_name: Vec<&str> = nations.iter().map(|(_, n, _)| *n).collect();
    let n_regionkey: Vec<i64> = nations.iter().map(|(_, _, r)| *r).collect();
    let n_comment: Vec<String> = nations
        .iter()
        .map(|(_, n, _)| format!("nation {n}"))
        .collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("n_nationkey", DataType::Int64, false),
        Field::new("n_name", DataType::Utf8, false),
        Field::new("n_regionkey", DataType::Int64, false),
        Field::new("n_comment", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(n_nationkey)),
            Arc::new(StringArray::from(n_name)),
            Arc::new(Int64Array::from(n_regionkey)),
            Arc::new(StringArray::from(
                n_comment.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();

    write_parquet_multi_rg(dir.join("nation.parquet"), batch, 5);
}

// ----- region ---------------------------------------------------------

fn write_region(dir: &std::path::Path) {
    // Includes ASIA so the Q5 r_name='ASIA' predicate finds a match.
    let regions = [(1i64, "ASIA"), (2, "EUROPE"), (3, "AMERICA")];
    let r_regionkey: Vec<i64> = regions.iter().map(|(k, _)| *k).collect();
    let r_name: Vec<&str> = regions.iter().map(|(_, n)| *n).collect();
    let r_comment: Vec<String> = regions.iter().map(|(_, n)| format!("region {n}")).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("r_regionkey", DataType::Int64, false),
        Field::new("r_name", DataType::Utf8, false),
        Field::new("r_comment", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(r_regionkey)),
            Arc::new(StringArray::from(r_name)),
            Arc::new(StringArray::from(
                r_comment.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();

    write_parquet_multi_rg(dir.join("region.parquet"), batch, 3);
}

// ----- partsupp -------------------------------------------------------

fn write_partsupp(dir: &std::path::Path) {
    // Cross of part (30) × supplier (10) would be 300 rows; for the
    // mini fixture pick the diagonal — 30 rows where each part has
    // exactly one supplier in {1..=10}. Enough to register the table
    // without inflating the fixture.
    let n: usize = 30;
    let mut ps_partkey = Vec::<i64>::with_capacity(n);
    let mut ps_suppkey = Vec::<i64>::with_capacity(n);
    let mut ps_availqty = Vec::<i32>::with_capacity(n);
    let mut ps_supplycost = Vec::<f64>::with_capacity(n);
    let mut ps_comment = Vec::<String>::with_capacity(n);

    for i in 1..=(n as i64) {
        ps_partkey.push(i);
        ps_suppkey.push(((i - 1) % 10) + 1);
        ps_availqty.push(((i % 100) as i32) + 1);
        ps_supplycost.push(50.0 + (i as f64) * 0.7);
        ps_comment.push(format!("partsupp comment {i}"));
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("ps_partkey", DataType::Int64, false),
        Field::new("ps_suppkey", DataType::Int64, false),
        Field::new("ps_availqty", DataType::Int32, false),
        Field::new("ps_supplycost", DataType::Float64, false),
        Field::new("ps_comment", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ps_partkey)),
            Arc::new(Int64Array::from(ps_suppkey)),
            Arc::new(Int32Array::from(ps_availqty)),
            Arc::new(Float64Array::from(ps_supplycost)),
            Arc::new(StringArray::from(
                ps_comment.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();

    write_parquet_multi_rg(dir.join("partsupp.parquet"), batch, 15);
}
