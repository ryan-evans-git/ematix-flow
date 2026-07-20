//! **General SQL-surface parity harness** — the durable robustness asset for
//! the breadth campaign. For an arbitrary SQL string it runs the native
//! clean-room engine AND in-process DuckDB over the *same* parquet, then
//! compares full result sets as a sorted multiset (FP tolerance, cross-type
//! numeric/date equivalence) — exactly the `tpcds_native_oracle` contract,
//! generalized to any query.
//!
//! Every new SQL feature lands a case here: if the native engine binds and
//! executes it, the answer must match DuckDB row-for-row. A feature the
//! binder still rejects fails LOUDLY (that's the TDD signal), not silently.
//!
//! Over TPC-H SF-1 `lineitem` (int / float / decimal / string / date columns,
//! plus the low-card `l_returnflag` / `l_linestatus`).

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::expr::ScalarValue;
use ematix_flow_engine::plan::execute;

fn sf1(table: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(format!("../../examples/tpch/data/sf1/{table}.parquet"))
}

fn lineitem() -> PathBuf {
    sf1("lineitem")
}

fn have_data() -> bool {
    lineitem().exists()
}

// ---- comparable cell (mirrors the tpcds_native_oracle contract) ----

#[derive(Debug, Clone)]
enum Cell {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Date(i32),
    Str(String),
}

impl Cell {
    fn sort_key(&self) -> String {
        match self {
            Cell::Null => "\x00".into(),
            Cell::Bool(b) => format!("\x01{b}"),
            Cell::Int(i) => format!("\x02{i:020}"),
            Cell::Float(f) => format!("\x03{f:.6e}"),
            Cell::Date(d) => format!("\x02{d:020}"),
            Cell::Str(s) => format!("\x05{}", s.trim()),
        }
    }
}

fn fp_eq(a: f64, b: f64) -> bool {
    if a == b || (a.is_nan() && b.is_nan()) {
        return true;
    }
    let mag = a.abs().max(b.abs()).max(1.0);
    (a - b).abs() / mag <= 1e-6
}

fn cell_eq(a: &Cell, b: &Cell) -> bool {
    use Cell::*;
    match (a, b) {
        (Null, Null) => true,
        (Bool(x), Bool(y)) => x == y,
        (Int(x), Int(y)) => x == y,
        (Date(x), Date(y)) => x == y,
        (Str(x), Str(y)) => x.trim() == y.trim(),
        (Float(x), Float(y)) => fp_eq(*x, *y),
        (Int(x), Float(y)) | (Float(y), Int(x)) => fp_eq(*x as f64, *y),
        (Int(x), Date(y)) | (Date(y), Int(x)) => *x == *y as i64,
        (Float(x), Date(y)) | (Date(y), Float(x)) => fp_eq(*x, *y as f64),
        _ => false,
    }
}

fn row_lt(a: &[Cell], b: &[Cell]) -> std::cmp::Ordering {
    for (ca, cb) in a.iter().zip(b.iter()) {
        let o = ca.sort_key().cmp(&cb.sort_key());
        if o != std::cmp::Ordering::Equal {
            return o;
        }
    }
    a.len().cmp(&b.len())
}

fn fmt_row(r: &[Cell]) -> String {
    r.iter()
        .map(|c| match c {
            Cell::Null => "NULL".into(),
            Cell::Bool(b) => b.to_string(),
            Cell::Int(i) => i.to_string(),
            Cell::Float(f) => format!("{f:.4}"),
            Cell::Date(d) => format!("d{d}"),
            Cell::Str(s) => format!("\"{}\"", s.trim()),
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

fn native_cell(v: &ScalarValue) -> Cell {
    match v {
        ScalarValue::Null => Cell::Null,
        ScalarValue::Boolean(b) => Cell::Bool(*b),
        ScalarValue::Int32(i) => Cell::Int(*i as i64),
        ScalarValue::Int64(i) => Cell::Int(*i),
        ScalarValue::Date32(d) => Cell::Date(*d),
        ScalarValue::Float64(f) => Cell::Float(*f),
        ScalarValue::Utf8(s) => Cell::Str(s.to_string()),
    }
}

fn duck_cell(row: &duckdb::Row, idx: usize) -> Cell {
    use duckdb::types::ValueRef;
    match row.get_ref(idx) {
        Err(_) => Cell::Str("ERR".into()),
        Ok(v) => match v {
            ValueRef::Null => Cell::Null,
            ValueRef::Boolean(b) => Cell::Bool(b),
            ValueRef::TinyInt(i) => Cell::Int(i as i64),
            ValueRef::SmallInt(i) => Cell::Int(i as i64),
            ValueRef::Int(i) => Cell::Int(i as i64),
            ValueRef::BigInt(i) => Cell::Int(i),
            ValueRef::HugeInt(i) => Cell::Int(i as i64),
            ValueRef::UTinyInt(i) => Cell::Int(i as i64),
            ValueRef::USmallInt(i) => Cell::Int(i as i64),
            ValueRef::UInt(i) => Cell::Int(i as i64),
            ValueRef::UBigInt(i) => Cell::Int(i as i64),
            ValueRef::Float(f) => Cell::Float(f as f64),
            ValueRef::Double(f) => Cell::Float(f),
            ValueRef::Text(b) => Cell::Str(String::from_utf8_lossy(b).into_owned()),
            ValueRef::Date32(d) => Cell::Date(d),
            // DuckDB's date_trunc on a DATE returns a TIMESTAMP (micros);
            // a whole-day timestamp is the same instant as the engine's
            // Date32 day-number, so normalize it to Date for comparison.
            ValueRef::Timestamp(tu, t) => Cell::Date((tu.to_micros(t) / 86_400_000_000) as i32),
            ValueRef::Decimal(d) => Cell::Float(d.to_string().parse().unwrap_or(0.0)),
            other => Cell::Str(format!("{other:?}")),
        },
    }
}

fn duck_rows(conn: &duckdb::Connection, sql: &str) -> Result<Vec<Vec<Cell>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let mut it = stmt.query([]).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    while let Some(row) = it.next().map_err(|e| e.to_string())? {
        let nc = row.as_ref().column_count();
        out.push((0..nc).map(|i| duck_cell(row, i)).collect());
    }
    Ok(out)
}

/// A harness bound to one registered table set: `check(sql)` asserts native
/// == DuckDB, panicking with a readable diff (or the native bind/exec error)
/// on any divergence.
struct Parity {
    catalog: Catalog,
    duck: duckdb::Connection,
}

impl Parity {
    fn new() -> Self {
        Self::with_tables(&["lineitem"])
    }

    /// Register the named SF1 tables into BOTH the native catalog and DuckDB
    /// (as views over the same parquet), so join parity is over identical data.
    fn with_tables(tables: &[&str]) -> Self {
        let mut catalog = Catalog::new();
        let duck = duckdb::Connection::open_in_memory().expect("duckdb");
        for &t in tables {
            catalog
                .register_parquet(t, sf1(t))
                .unwrap_or_else(|e| panic!("register {t}: {e}"));
            duck.execute_batch(&format!(
                "CREATE VIEW {t} AS SELECT * FROM read_parquet('{}');",
                sf1(t).display()
            ))
            .unwrap_or_else(|e| panic!("duck view {t}: {e}"));
        }
        Self { catalog, duck }
    }

    #[track_caller]
    fn check(&self, sql: &str) {
        let bq = match bind_sql(sql, &self.catalog) {
            Ok(q) => q,
            Err(e) => panic!("NATIVE BIND FAILED for `{sql}`:\n  {e}"),
        };
        let native: Vec<Vec<Cell>> = match execute(&bq) {
            Ok(r) => r.rows.iter().map(|r| r.iter().map(native_cell).collect()).collect(),
            Err(e) => panic!("NATIVE EXEC FAILED for `{sql}`:\n  {e}"),
        };
        let duck = duck_rows(&self.duck, sql).unwrap_or_else(|e| panic!("DUCK FAILED for `{sql}`:\n  {e}"));

        let mut n = native.clone();
        let mut d = duck.clone();
        n.sort_by(|a, b| row_lt(a, b));
        d.sort_by(|a, b| row_lt(a, b));
        if n.len() != d.len() {
            panic!("ROW COUNT DIFFERS for `{sql}`: native {} vs duck {}", n.len(), d.len());
        }
        for (i, (nr, dr)) in n.iter().zip(&d).enumerate() {
            let same = nr.len() == dr.len() && nr.iter().zip(dr).all(|(a, b)| cell_eq(a, b));
            if !same {
                panic!(
                    "MISMATCH for `{sql}` at sorted row {i}:\n  native: {}\n  duck:   {}",
                    fmt_row(nr),
                    fmt_row(dr)
                );
            }
        }
    }

    /// Assert the native binder STILL rejects `sql` (documents an
    /// intentional gap so a later accidental half-support is noticed).
    #[track_caller]
    fn check_rejected(&self, sql: &str) {
        if bind_sql(sql, &self.catalog).is_ok() {
            panic!("expected `{sql}` to be rejected by the binder, but it bound");
        }
    }
}

// ---------------------------------------------------------------------------
// Tier-1 breadth features. Each asserts native == DuckDB over the same data.
// ---------------------------------------------------------------------------

/// EXTRACT beyond YEAR: month/day/quarter/dow/week/doy over a date column.
#[test]
fn extract_date_fields() {
    if !have_data() {
        eprintln!("SKIP extract_date_fields: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    for field in ["year", "month", "day", "quarter", "dow", "doy", "week"] {
        p.check(&format!(
            "select extract({field} from l_shipdate) f, count(*) n \
             from lineitem group by extract({field} from l_shipdate)"
        ));
    }
}

/// Scalar functions: lower, floor, ceil, mod, trim, length, replace.
#[test]
fn scalar_functions() {
    if !have_data() {
        eprintln!("SKIP scalar_functions: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    p.check("select lower(l_returnflag) f, count(*) n from lineitem group by lower(l_returnflag)");
    p.check("select floor(l_quantity) f, count(*) n from lineitem group by floor(l_quantity)");
    p.check("select ceil(l_discount * 100) f, count(*) n from lineitem group by ceil(l_discount * 100)");
    p.check("select mod(l_orderkey, 7) m, count(*) n from lineitem group by mod(l_orderkey, 7)");
    p.check("select length(l_returnflag) f, count(*) n from lineitem group by length(l_returnflag)");
    p.check("select trim(l_returnflag) f, count(*) n from lineitem group by trim(l_returnflag)");
    p.check(
        "select replace(l_shipinstruct, ' ', '_') f, count(*) n \
         from lineitem group by replace(l_shipinstruct, ' ', '_')",
    );
    // Scalar string fns in a WHERE comparison (the inline owned-string path).
    p.check("select count(*) n from lineitem where lower(l_returnflag) = 'n'");
    p.check("select count(*) n from lineitem where trim(l_returnflag) <> 'A'");
    // Numeric fns in a predicate.
    p.check("select count(*) n from lineitem where mod(l_orderkey, 100) = 0");
    p.check("select count(*) n from lineitem where floor(l_quantity) > 30");
}

/// Unary minus on an expression (not just a literal).
#[test]
fn unary_minus_on_expr() {
    if !have_data() {
        eprintln!("SKIP unary_minus_on_expr: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    p.check("select sum(-l_extendedprice) s from lineitem where l_shipdate >= date '1998-01-01'");
    p.check("select -l_linenumber v, count(*) n from lineitem group by -l_linenumber");
}

/// Positional GROUP BY / ORDER BY (`GROUP BY 1`, `ORDER BY 2`).
#[test]
fn positional_group_order() {
    if !have_data() {
        eprintln!("SKIP positional_group_order: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    p.check("select l_returnflag, count(*) n from lineitem group by 1 order by 1");
    p.check(
        "select l_returnflag, l_linestatus, sum(l_quantity) q \
         from lineitem group by 1, 2 order by 2, 1",
    );
    p.check("select l_linestatus, count(*) n from lineitem group by 1 order by 2 desc");
}

// ---------------------------------------------------------------------------
// Tier-2a: aggregate variants + date_trunc.
// ---------------------------------------------------------------------------

/// Population/sample variance + population stddev (all from sum/sumsq/count).
#[test]
fn variance_aggregates() {
    if !have_data() {
        eprintln!("SKIP variance_aggregates: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    p.check(
        "select l_returnflag, \
                var_samp(l_quantity) vs, var_pop(l_quantity) vp, \
                stddev_pop(l_extendedprice) sp, stddev_samp(l_discount) ss, \
                variance(l_tax) v \
         from lineitem group by 1",
    );
    // A single-row group: var_pop = 0, var_samp = NULL.
    p.check(
        "select l_orderkey, var_pop(l_quantity) vp, var_samp(l_quantity) vs \
         from lineitem where l_orderkey = 1 group by 1",
    );
}

/// date_trunc to year/quarter/month/week/day.
#[test]
fn date_trunc_units() {
    if !have_data() {
        eprintln!("SKIP date_trunc_units: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    for unit in ["year", "quarter", "month", "week", "day"] {
        p.check(&format!(
            "select date_trunc('{unit}', l_shipdate) d, count(*) n \
             from lineitem group by date_trunc('{unit}', l_shipdate)"
        ));
    }
}

// ---------------------------------------------------------------------------
// Tier-2b: navigation/positional window functions. A TOTAL order in each
// OVER (l_orderkey, l_linenumber unique per lineitem) keeps lead/lag/ntile
// deterministic so parity is meaningful. (last_value deferred — default
// RANGE-frame peer semantics is a separate careful unit.)
// ---------------------------------------------------------------------------

#[test]
fn window_lead_lag_first_value() {
    if !have_data() {
        eprintln!("SKIP window_lead_lag_first_value: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    let over = "over (partition by l_returnflag order by l_orderkey, l_linenumber)";
    p.check(&format!(
        "select l_orderkey, l_linenumber, \
                lag(l_quantity) {over} lg, \
                lag(l_quantity, 2) {over} lg2, \
                lead(l_extendedprice) {over} ld, \
                first_value(l_discount) {over} fv \
         from lineitem where l_orderkey < 800"
    ));
}

#[test]
fn window_ntile() {
    if !have_data() {
        eprintln!("SKIP window_ntile: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    p.check(
        "select l_orderkey, l_linenumber, \
                ntile(4) over (order by l_orderkey, l_linenumber) q, \
                ntile(7) over (partition by l_returnflag order by l_orderkey, l_linenumber) q7 \
         from lineitem where l_orderkey < 400",
    );
}

// ---------------------------------------------------------------------------
// Tier-3 join residuals: RIGHT [OUTER] JOIN + non-equi joins. Over
// orders ⋈ lineitem (a 1-to-many key) with a filtered ON so preserved rows
// go unmatched (NULL-extended). RIGHT is implemented as a mirrored LEFT
// (the joined table is preserved, the keyed OLD table becomes nullable), so
// every LEFT capability — grouped NULL-extension, matched-only COUNT, and
// WHERE-side demote-to-INNER — must hold for RIGHT too.
// ---------------------------------------------------------------------------

fn have_joins() -> bool {
    sf1("orders").exists() && sf1("customer").exists()
}

#[test]
fn right_outer_join() {
    if !have_joins() {
        eprintln!("SKIP right_outer_join: SF1 orders/customer absent");
        return;
    }
    let p = Parity::with_tables(&["lineitem", "orders", "customer"]);
    // RIGHT preserves orders; a filtered ON leaves many orders unmatched.
    let on = "on l.l_orderkey = o.o_orderkey and l.l_quantity > 49";
    // Plain count over the preserved side.
    p.check(&format!(
        "select count(*) n from lineitem l right join orders o {on} where o.o_orderkey < 3000"
    ));
    // Grouped, with matched-only COUNT(l.col) vs total COUNT(*): exercises the
    // NULL-extension projection and the CountMatched path together.
    p.check(&format!(
        "select o.o_orderpriority pr, count(*) n, count(l.l_orderkey) m \
         from lineitem l right join orders o {on} where o.o_orderkey < 3000 group by 1"
    ));
    // A WHERE predicate on the (nullable) lineitem side that rejects NULLs
    // must demote RIGHT → INNER.
    p.check(
        "select count(*) n from lineitem l right join orders o \
         on l.l_orderkey = o.o_orderkey where o.o_orderkey < 3000 and l.l_quantity > 40",
    );
    // RIGHT join first, then an inner cross-linked third table.
    p.check(&format!(
        "select count(*) n from lineitem l right join orders o {on}, customer c \
         where c.c_custkey = o.o_custkey and o.o_orderkey < 3000"
    ));
    // RIGHT/RIGHT OUTER are the same operator.
    p.check(&format!(
        "select count(*) n from lineitem l right outer join orders o {on} where o.o_orderkey < 3000"
    ));
    // Boundary: a RIGHT join whose joined table is keyed to TWO different old
    // tables would need two preserved-root tables — intentionally rejected.
    p.check_rejected(
        "select count(*) n from customer c, orders o right join lineitem l \
         on l.l_orderkey = o.o_orderkey and l.l_suppkey = c.c_custkey",
    );
}

#[test]
fn non_equi_join() {
    if !have_joins() {
        eprintln!("SKIP non_equi_join: SF1 orders/customer absent");
        return;
    }
    let p = Parity::with_tables(&["lineitem", "orders"]);
    // A cross-table inequality alongside the equi key (post-join filter).
    p.check(
        "select count(*) n from orders o join lineitem l \
         on l.l_orderkey = o.o_orderkey and o.o_orderdate < l.l_shipdate \
         where o.o_orderkey < 3000",
    );
    // A pure non-equi ON (no equi key at all) — a filtered cross join whose
    // ON inequality prunes post-expansion.
    p.check(
        "select count(*) n from orders o join lineitem l \
         on o.o_totalprice < l.l_extendedprice where o.o_orderkey < 6 and l.l_orderkey < 6",
    );
    // Non-equi with an extra BETWEEN-style range and a grouped result.
    p.check(
        "select o.o_orderstatus s, count(*) n from orders o join lineitem l \
         on l.l_orderkey = o.o_orderkey and l.l_extendedprice > o.o_totalprice * 0.1 \
         where o.o_orderkey < 3000 group by 1",
    );
}

// ---------------------------------------------------------------------------
// Tier-3: NOT + three-valued NULL logic. NULLs are injected with
// nullif(l_linenumber, 1) (NULL when linenumber == 1) since SF1 lineitem
// has no NULL columns. The point is that NOT over a NULL is NULL (drops at
// WHERE), NOT over AND/OR follows De Morgan under 3VL, and projected NOT
// values are NULL (not false) where the operand is unknown.
// ---------------------------------------------------------------------------

#[test]
fn not_and_three_valued_null_logic() {
    if !have_data() {
        eprintln!("SKIP not_and_three_valued_null_logic: SF1 lineitem absent");
        return;
    }
    let p = Parity::new();
    // NOT over comparisons / IS NULL / IN / LIKE (row counts must match).
    p.check("select count(*) n from lineitem where not (l_discount > 0.05)");
    p.check("select count(*) n from lineitem where not (nullif(l_linenumber, 1) > 3)");
    p.check("select count(*) n from lineitem where not (l_returnflag in ('A', 'N'))");
    p.check("select count(*) n from lineitem where not (l_shipinstruct like 'DELIVER%')");
    p.check("select count(*) n from lineitem where not (nullif(l_linenumber, 1) is null)");
    // NOT over AND / OR with a NULL-bearing operand (De Morgan under 3VL).
    p.check(
        "select count(*) n from lineitem \
         where not (nullif(l_linenumber, 1) > 3 and l_discount > 0.05)",
    );
    p.check(
        "select count(*) n from lineitem \
         where not (nullif(l_linenumber, 1) > 3 or l_discount > 0.09)",
    );
    // Double negation and nested.
    p.check("select count(*) n from lineitem where not (not (l_quantity > 25))");
    p.check(
        "select count(*) n from lineitem \
         where l_tax > 0.04 and not (l_returnflag = 'A' or nullif(l_linenumber,1) = 2)",
    );
    // Projected boolean: NOT over a NULL operand must render NULL, not false.
    p.check(
        "select l_orderkey, l_linenumber, \
                not (nullif(l_linenumber, 1) > 3) f, \
                not (l_linenumber > 2 and nullif(l_linenumber, 1) > 3) g \
         from lineitem where l_orderkey < 60",
    );
}
