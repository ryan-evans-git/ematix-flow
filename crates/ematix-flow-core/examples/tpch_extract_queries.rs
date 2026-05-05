//! Σ.C extension: dump the 22 TPC-H spec queries from `tpchgen` to
//! disk under `examples/tpch/queries/qNN.sql`, with each query's
//! validation-set parameters substituted in so the files are
//! directly runnable. Idempotent: re-running overwrites with the
//! same canonical content.
//!
//! Validation parameters (from the TPC-H spec, clause 2.x.x for
//! each query) are the same set that `tpchgen::q_and_a::answers_sf1`
//! uses for its expected-result strings; using them here keeps the
//! .sql files in lockstep with what the smoke test asserts against.
//!
//! Usage:
//!     cargo run --release -p ematix-flow-core --example tpch_extract_queries

use std::fs;
use std::path::PathBuf;

use tpchgen::q_and_a::queries::{
    Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q11, Q12, Q13, Q14, Q15, Q16, Q17, Q18, Q19, Q20, Q21,
    Q22,
};

/// `(query_number, replacements)` — each pair is applied as a literal
/// `from -> to` string replacement on the spec query body. The
/// replacements run in order; ordering matters when a longer pattern
/// would be a substring of a shorter one.
struct Sub<'a>(u8, &'a [(&'a str, &'a str)]);

const SUBS: &[Sub] = &[
    // Q1: DELTA = 90 days. Drops the spec's `(3)` leading-
    // precision suffix on the interval — DataFusion 53.1's planner
    // rejects intervals with explicit leading_precision, and the
    // suffix carries no semantic information once the literal is
    // substituted in.
    Sub(1, &[("interval ':1' day (3)", "interval '90' day")]),
    // Q2: SIZE=15, TYPE ends with 'BRASS', REGION='EUROPE'
    Sub(
        2,
        &[
            ("p_size = :1", "p_size = 15"),
            ("p_type like '%:2'", "p_type like '%BRASS'"),
            ("r_name = ':3'", "r_name = 'EUROPE'"),
        ],
    ),
    // Q3: SEGMENT='BUILDING', DATE='1995-03-15'
    Sub(
        3,
        &[
            ("c_mktsegment = ':1'", "c_mktsegment = 'BUILDING'"),
            ("o_orderdate < date ':2'", "o_orderdate < date '1995-03-15'"),
            ("l_shipdate > date ':2'", "l_shipdate > date '1995-03-15'"),
        ],
    ),
    // Q4: DATE='1993-07-01' (start; +3 months)
    Sub(
        4,
        &[
            (
                "o_orderdate >= date ':1'",
                "o_orderdate >= date '1993-07-01'",
            ),
            ("date ':1' + interval '3' month", "date '1993-10-01'"),
        ],
    ),
    // Q5: REGION='ASIA', DATE='1994-01-01'
    Sub(
        5,
        &[
            ("r_name = ':1'", "r_name = 'ASIA'"),
            (
                "o_orderdate >= date ':2'",
                "o_orderdate >= date '1994-01-01'",
            ),
            ("date ':2' + interval '1' year", "date '1995-01-01'"),
        ],
    ),
    // Q6: DATE='1994-01-01', DISCOUNT=0.06, QUANTITY=24.
    // The TPC-H spec form is `l_discount BETWEEN :2 - 0.01 AND
    // :2 + 0.01` so a single discount parameter substitutes into
    // both bounds. After substitution that's `0.06 - 0.01 AND
    // 0.06 + 0.01`, which f64 rounds to `0.04999... AND
    // 0.06999...` — and `0.06999...` is *just under* the literal
    // 0.07, so `l_discount = 0.07` rows get silently excluded.
    // (l_discount in lineitem.parquet is Float64; 0.07 stored
    // there is `0.06999999999999999556...`, lexically equal to
    // the upper bound but BETWEEN's right-side compare is `<=`,
    // not `<`, and the upper-bound expression rounds *below*
    // 0.07.) Pre-compute the bounds as literal `0.05 AND 0.07`
    // so the canonical TPC-H Q6 reference revenue (123141078.23
    // at SF=1) actually appears. tpch_smoke's in-process check
    // uses the same literal-form Q6 for the same reason.
    Sub(
        6,
        &[
            ("l_shipdate >= date ':1'", "l_shipdate >= date '1994-01-01'"),
            ("date ':1' + interval '1' year", "date '1995-01-01'"),
            (
                "l_discount between :2 - 0.01 and :2 + 0.01",
                "l_discount between 0.05 and 0.07",
            ),
            ("l_quantity < :3", "l_quantity < 24"),
        ],
    ),
    // Q7: NATION1='FRANCE', NATION2='GERMANY'
    Sub(
        7,
        &[
            (
                "(n1.n_name = ':1' and n2.n_name = ':2')",
                "(n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')",
            ),
            (
                "(n1.n_name = ':2' and n2.n_name = ':1')",
                "(n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')",
            ),
        ],
    ),
    // Q8: NATION='BRAZIL', REGION='AMERICA', TYPE='ECONOMY ANODIZED STEEL'
    Sub(
        8,
        &[
            ("when nation = ':1'", "when nation = 'BRAZIL'"),
            ("r_name = ':2'", "r_name = 'AMERICA'"),
            ("p_type = ':3'", "p_type = 'ECONOMY ANODIZED STEEL'"),
        ],
    ),
    // Q9: COLOR='green'
    Sub(9, &[("p_name like '%:1%'", "p_name like '%green%'")]),
    // Q10: DATE='1993-10-01' (start; +3 months)
    Sub(
        10,
        &[
            (
                "o_orderdate >= date ':1'",
                "o_orderdate >= date '1993-10-01'",
            ),
            ("date ':1' + interval '3' month", "date '1994-01-01'"),
        ],
    ),
    // Q11: NATION='GERMANY', FRACTION=0.0001
    Sub(
        11,
        &[
            ("n_name = ':1'", "n_name = 'GERMANY'"),
            (": 2", "0.0001"), // tpchgen renders FRACTION as ': 2' with a stray space
            (":2", "0.0001"),
        ],
    ),
    // Q12: SHIPMODE1='MAIL', SHIPMODE2='SHIP', DATE='1994-01-01'
    Sub(
        12,
        &[
            ("(':1', ':2')", "('MAIL', 'SHIP')"),
            (
                "l_receiptdate >= date ':3'",
                "l_receiptdate >= date '1994-01-01'",
            ),
            ("date ':3' + interval '1' year", "date '1995-01-01'"),
        ],
    ),
    // Q13: WORD1='special', WORD2='requests'
    Sub(
        13,
        &[(
            "o_comment not like '%:1%:2%'",
            "o_comment not like '%special%requests%'",
        )],
    ),
    // Q14: DATE='1995-09-01'
    Sub(
        14,
        &[
            ("l_shipdate >= date ':1'", "l_shipdate >= date '1995-09-01'"),
            ("date ':1' + interval '1' month", "date '1995-10-01'"),
        ],
    ),
    // Q15: DATE='1996-01-01' (rewritten as a CTE — DataFusion doesn't
    // support CREATE VIEW in ctx.sql() and the colon in the spec view
    // name `revenue:s` isn't a valid identifier).
    Sub(
        15,
        &[
            ("l_shipdate >= date ':1'", "l_shipdate >= date '1996-01-01'"),
            ("date ':1' + interval '3' month", "date '1996-04-01'"),
        ],
    ),
    // Q16: BRAND='Brand#45', TYPE='MEDIUM POLISHED %', SIZE_LIST canonical 8 values
    Sub(
        16,
        &[
            ("p_brand <> ':1'", "p_brand <> 'Brand#45'"),
            (
                "p_type not like ':2%'",
                "p_type not like 'MEDIUM POLISHED %'",
            ),
            (
                "p_size in (:3, :4, :5, :6, :7, :8, :9, :10)",
                "p_size in (49, 14, 23, 45, 19, 3, 36, 9)",
            ),
        ],
    ),
    // Q17: BRAND='Brand#23', CONTAINER='MED BOX'
    Sub(
        17,
        &[
            ("p_brand = ':1'", "p_brand = 'Brand#23'"),
            ("p_container = ':2'", "p_container = 'MED BOX'"),
        ],
    ),
    // Q18: QUANTITY=300
    Sub(18, &[("sum(l_quantity) > :1", "sum(l_quantity) > 300")]),
    // Q19: BRAND1='Brand#12', BRAND2='Brand#23', BRAND3='Brand#34',
    //      QUANTITY1=1, QUANTITY2=10, QUANTITY3=20
    Sub(
        19,
        &[
            ("p_brand = ':1'", "p_brand = 'Brand#12'"),
            ("p_brand = ':2'", "p_brand = 'Brand#23'"),
            ("p_brand = ':3'", "p_brand = 'Brand#34'"),
            ("l_quantity >= :4", "l_quantity >= 1"),
            ("l_quantity <= :4 + 10", "l_quantity <= 1 + 10"),
            ("l_quantity >= :5", "l_quantity >= 10"),
            ("l_quantity <= :5 + 10", "l_quantity <= 10 + 10"),
            ("l_quantity >= :6", "l_quantity >= 20"),
            ("l_quantity <= :6 + 10", "l_quantity <= 20 + 10"),
        ],
    ),
    // Q20: COLOR='forest', DATE='1994-01-01', NATION='CANADA'
    Sub(
        20,
        &[
            ("p_name like ':1%'", "p_name like 'forest%'"),
            ("l_shipdate >= date ':2'", "l_shipdate >= date '1994-01-01'"),
            ("date ':2' + interval '1' year", "date '1995-01-01'"),
            ("n_name = ':3'", "n_name = 'CANADA'"),
        ],
    ),
    // Q21: NATION='SAUDI ARABIA'
    Sub(21, &[("n_name = ':1'", "n_name = 'SAUDI ARABIA'")]),
    // Q22: COUNTRY_CODES list (canonical 7 codes). The spec
    // formats the IN clause across multiple lines, so a single
    // multi-line pattern is brittle — replace each `':N'` token
    // individually instead. Order matters: do `:10..:99` first
    // (none here, just :1..:7) so longer patterns don't get
    // shadowed by shorter ones.
    Sub(
        22,
        &[
            ("':1'", "'13'"),
            ("':2'", "'31'"),
            ("':3'", "'23'"),
            ("':4'", "'29'"),
            ("':5'", "'30'"),
            ("':6'", "'18'"),
            ("':7'", "'17'"),
        ],
    ),
];

/// Q15's spec form uses `create view revenue:s ... ; SELECT ... FROM
/// revenue:s ...; DROP VIEW revenue:s;` — three statements, with a
/// colon in the view name that DataFusion's parser rejects. Rewrite
/// to a single SELECT with a CTE so it round-trips cleanly through
/// `ctx.sql(...)`. This is how DuckDB / Polars sample TPC-H suites
/// handle Q15 too.
fn rewrite_q15(sql: &str) -> String {
    // The substituted Q15 starts with `create view revenue:s ...;`
    // and ends with `drop view revenue:s;`. Split on the inner SELECT
    // and rebuild as a CTE.
    let cte_body_start = sql.find("as\n\tselect").unwrap_or(0);
    let cte_body_end = sql.find("group by\n\t\tl_suppkey;").unwrap_or(0);
    if cte_body_start == 0 || cte_body_end == 0 {
        return sql.to_string();
    }
    let inner_select_start = sql
        .find("select\n\ts_suppkey,")
        .expect("Q15 outer SELECT marker");
    let drop_view = sql.find("drop view").unwrap_or(sql.len());
    let cte = &sql[cte_body_start + "as\n".len()..cte_body_end + "group by\n\t\tl_suppkey".len()];
    let outer = sql[inner_select_start..drop_view]
        .trim_end_matches(|c: char| c == ';' || c.is_whitespace());
    let outer = outer.replace("revenue:s", "revenue_s");
    format!("with revenue_s (supplier_no, total_revenue) as (\n{cte}\n)\n{outer}\n")
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .ok_or("workspace root not found")?
        .join("examples/tpch/queries");
    fs::create_dir_all(&out_dir)?;

    let raw: &[(u8, &str)] = &[
        (1, Q1),
        (2, Q2),
        (3, Q3),
        (4, Q4),
        (5, Q5),
        (6, Q6),
        (7, Q7),
        (8, Q8),
        (9, Q9),
        (10, Q10),
        (11, Q11),
        (12, Q12),
        (13, Q13),
        (14, Q14),
        (15, Q15),
        (16, Q16),
        (17, Q17),
        (18, Q18),
        (19, Q19),
        (20, Q20),
        (21, Q21),
        (22, Q22),
    ];

    for (n, sql) in raw {
        let mut body = sql.trim_end().trim_start_matches('\n').to_string();
        if let Some(sub) = SUBS.iter().find(|s| s.0 == *n) {
            for (from, to) in sub.1 {
                body = body.replace(from, to);
            }
        }
        if *n == 15 {
            body = rewrite_q15(&body);
        }
        // Sanity-check: no unsubstituted `:N` placeholders left.
        // (`:s` from Q15's view name is OK after the rewrite —
        // we replaced it with `revenue_s`.)
        for ch in body.bytes() {
            if ch == b':' {
                let idx = body.bytes().position(|c| c == b':').unwrap();
                let around = &body[idx.saturating_sub(20)..(idx + 20).min(body.len())];
                eprintln!("warning: Q{n} contains ':' — check substitution: ...{around}...");
                break;
            }
        }
        let path = out_dir.join(format!("q{n:02}.sql"));
        fs::write(&path, format!("{body}\n"))?;
        println!("wrote {}", path.display());
    }
    Ok(())
}
